// Command bench compares isolated AVC and Git workloads. It never uses the
// caller's repository as a test fixture. History and durability differ by mode;
// the report deliberately does not calculate a speedup factor.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type result struct {
	Suite         string         `json:"suite"`
	Mode          string         `json:"mode"`
	Round         int            `json:"round"`
	Agents        int            `json:"agents"`
	Policy        string         `json:"commit_policy,omitempty"`
	SetupMS       float64        `json:"setup_ms"`
	WorkMS        float64        `json:"work_ms"`
	IntegrationMS float64        `json:"integration_ms"`
	TotalMS       float64        `json:"total_ms"`
	Throughput    float64        `json:"tasks_per_second"`
	P50MS         float64        `json:"p50_ms"`
	P95MS         float64        `json:"p95_ms"`
	P99MS         float64        `json:"p99_ms"`
	DiskBytes     int64          `json:"disk_logical_bytes"`
	GitCommits    int            `json:"git_commits_excluding_initial"`
	Conflicts     int            `json:"integration_conflicts"`
	Executions    int            `json:"actual_worker_executions,omitempty"`
	ExpectedRuns  int            `json:"expected_worker_executions,omitempty"`
	Correct       bool           `json:"correct"`
	Error         string         `json:"error,omitempty"`
	CPUSeconds    *float64       `json:"process_tree_cpu_seconds"`
	PeakRSSBytes  *int64         `json:"process_tree_peak_rss_bytes"`
	Features      map[string]any `json:"features,omitempty"`
	Stats         map[string]any `json:"avc_stats,omitempty"`
	Notes         []string       `json:"notes"`
}

type runner struct {
	avc     string
	timeout time.Duration
	monitor *processMonitor
	ctx     context.Context
}

// Discover descendants before killing the owned group: a job may create its own
// process group. Never signal a process merely because its name matches avc.
func terminateOwnedTree(pid int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	data, _ := exec.CommandContext(ctx, "ps", "-axo", "pid=,ppid=").Output()
	children := map[int][]int{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		child, e1 := strconv.Atoi(fields[0])
		parent, e2 := strconv.Atoi(fields[1])
		if e1 == nil && e2 == nil {
			children[parent] = append(children[parent], child)
		}
	}
	var kill func(int)
	kill = func(p int) {
		for _, c := range children[p] {
			kill(c)
		}
		_ = syscall.Kill(-p, syscall.SIGKILL)
		_ = syscall.Kill(p, syscall.SIGKILL)
	}
	kill(pid)
	return nil
}

func fixtureEnv(dir string) []string {
	var env []string
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if !strings.HasPrefix(key, "GIT_") && !strings.HasPrefix(key, "AVC_") {
			env = append(env, item)
		}
	}
	return append(env, "AVC_ROOT="+dir, "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_SYSTEM="+os.DevNull, "GIT_TERMINAL_PROMPT=0")
}

func fileHash(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return "unavailable: " + err.Error()
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "unavailable: " + err.Error()
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Process-tree observations are sampled lower bounds. Summed RSS includes shared
// pages more than once; short-lived descendants can finish between observations.
type processMonitor struct {
	mu      sync.Mutex
	roots   map[int]bool
	stop    chan struct{}
	done    chan struct{}
	peakRSS int64
	cpu     map[int]float64
	samples int
	err     string
}

func newProcessMonitor() *processMonitor {
	m := &processMonitor{roots: map[int]bool{}, stop: make(chan struct{}), done: make(chan struct{}), cpu: map[int]float64{}}
	go func() {
		defer close(m.done)
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-tick.C:
				m.sample()
			}
		}
	}()
	return m
}

func (m *processMonitor) track(pid int, active bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if active {
		m.roots[pid] = true
	} else {
		delete(m.roots, pid)
	}
	m.mu.Unlock()
}

func (m *processMonitor) sample() {
	m.mu.Lock()
	selected := make(map[int]bool, len(m.roots))
	for pid := range m.roots {
		selected[pid] = true
	}
	m.mu.Unlock()
	if len(selected) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	data, err := exec.CommandContext(ctx, "ps", "-axo", "pid=,ppid=,rss=,time=").Output()
	if err != nil {
		m.err = err.Error()
		return
	}
	type row struct {
		pid, parent int
		rss         int64
		cpu         float64
	}
	var rows []row
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 4 {
			continue
		}
		pid, e1 := strconv.Atoi(fields[0])
		parent, e2 := strconv.Atoi(fields[1])
		rss, e3 := strconv.ParseInt(fields[2], 10, 64)
		if e1 != nil || e2 != nil || e3 != nil {
			continue
		}
		var cpu float64
		value := fields[3]
		if d, clock, ok := strings.Cut(value, "-"); ok {
			days, _ := strconv.ParseFloat(d, 64)
			cpu, value = days*24*60*60, clock
		}
		var clock float64
		for _, part := range strings.Split(value, ":") {
			number, _ := strconv.ParseFloat(part, 64)
			clock = clock*60 + number
		}
		rows = append(rows, row{pid, parent, rss * 1024, cpu + clock})
	}
	for changed := true; changed; {
		changed = false
		for _, row := range rows {
			if selected[row.parent] && !selected[row.pid] {
				selected[row.pid], changed = true, true
			}
		}
	}
	var rss int64
	for _, row := range rows {
		if selected[row.pid] {
			rss += row.rss
			if row.cpu > m.cpu[row.pid] {
				m.cpu[row.pid] = row.cpu
			}
		}
	}
	if rss > m.peakRSS {
		m.peakRSS = rss
	}
	m.samples++
}

func (m *processMonitor) finish(out *result) {
	close(m.stop)
	<-m.done
	if m.samples > 0 {
		var cpu float64
		for _, seconds := range m.cpu {
			cpu += seconds
		}
		out.CPUSeconds, out.PeakRSSBytes = &cpu, &m.peakRSS
	}
	out.Notes = append(out.Notes, fmt.Sprintf("Process-tree ps sampling every 50 ms, %d samples; CPU/peak RSS are observed lower bounds, summed RSS duplicates shared pages. Sampler overhead is outside the observed tree. %s", m.samples, m.err))
}

func (r runner) command(dir, name string, args ...string) ([]byte, error) {
	parent := r.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, r.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = fixtureEnv(dir)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return terminateOwnedTree(cmd.Process.Pid) }
	cmd.WaitDelay = time.Second
	var buffer bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buffer, &buffer
	err := cmd.Start()
	if err == nil {
		r.monitor.track(cmd.Process.Pid, true)
		err = cmd.Wait()
		r.monitor.track(cmd.Process.Pid, false)
	}
	output := buffer.Bytes()
	if err != nil {
		if len(output) > 2048 {
			output = output[:2048]
		}
		return output, fmt.Errorf("%s %v: %w: %s", filepath.Base(name), args, err, output)
	}
	return output, nil
}

func (r runner) git(dir string, args ...string) ([]byte, error) {
	return r.command(dir, "git", append([]string{"-c", "commit.gpgsign=false", "-c", "core.hooksPath=" + os.DevNull}, args...)...)
}

func (r runner) initGit(dir string) error {
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.name", "AVC benchmark"}, {"config", "user.email", "benchmark@example.invalid"}, {"config", "gc.auto", "0"}} {
		if _, err := r.git(dir, args...); err != nil {
			return err
		}
	}
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte(".avc/\n"), 0600); err != nil {
		return err
	}
	return nil
}

func (r runner) commit(dir, message string) error {
	if _, err := r.git(dir, "add", "--", "shared.txt", ".gitignore"); err != nil {
		return err
	}
	_, err := r.git(dir, "commit", "-q", "-m", message)
	return err
}

func fixture(n int, done bool) []byte {
	var b strings.Builder
	state := "pending"
	if done {
		state = "done"
	}
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "agent-%04d=%s\n", i, state)
		// Distinct unchanged context lets Git perform real nonoverlapping merges.
		for j := 0; j < 8; j++ {
			fmt.Fprintf(&b, "context-%04d-%d\n", i, j)
		}
	}
	return []byte(b.String())
}

func replace(dir string, id int) error {
	path := filepath.Join(dir, "shared.txt")
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	old := []byte(fmt.Sprintf("agent-%04d=pending", id))
	if bytes.Count(data, old) != 1 {
		return fmt.Errorf("agent %d: expected exactly one anchor", id)
	}
	data = bytes.Replace(data, old, []byte(fmt.Sprintf("agent-%04d=done", id)), 1)
	f, err := os.CreateTemp(dir, ".bench-write-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (r runner) startDaemon(dir string) (func(), error) {
	log, err := os.Create(filepath.Join(filepath.Dir(dir), "daemon.log"))
	if err != nil {
		return nil, err
	}
	parent := r.ctx
	if parent == nil {
		parent = context.Background()
	}
	cmd := exec.CommandContext(parent, r.avc, "daemon", "--root", dir, "--max-jobs", "1", "--workers", "1", "--max-load", "100000", "--min-free-mb", "1")
	cmd.Dir, cmd.Stdout, cmd.Stderr = dir, log, log
	cmd.Env = fixtureEnv(dir)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return terminateOwnedTree(cmd.Process.Pid) }
	cmd.WaitDelay = time.Second
	if err = cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	r.monitor.track(cmd.Process.Pid, true)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	stop := func() {
		cleanup := r
		cleanup.ctx, cleanup.timeout = context.Background(), 2*time.Second
		_, _ = cleanup.command(dir, r.avc, "shutdown")
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = terminateOwnedTree(cmd.Process.Pid)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
			}
		}
		_ = log.Close()
		r.monitor.track(cmd.Process.Pid, false)
	}
	deadline := time.After(10 * time.Second)
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			log.Close()
			r.monitor.track(cmd.Process.Pid, false)
			return nil, fmt.Errorf("daemon exited before readiness: %v", err)
		case <-deadline:
			stop()
			return nil, fmt.Errorf("daemon readiness timeout")
		case <-tick.C:
			probe := r
			probe.timeout = time.Second
			if _, err := probe.command(dir, r.avc, "stats"); err == nil {
				return stop, nil
			}
		}
	}
}

func (r runner) stats(dir string) map[string]any {
	data, err := r.command(dir, r.avc, "stats")
	if err != nil {
		return map[string]any{"read_error": err.Error()}
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return map[string]any{"decode_error": err.Error()}
	}
	return out
}

func parallel(n int, fn func(int) error) ([]float64, error) {
	start := make(chan struct{})
	latencies, errs := make([]float64, n), make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			began := time.Now()
			errs[id] = fn(id)
			latencies[id] = float64(time.Since(began)) / float64(time.Millisecond)
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return latencies, fmt.Errorf("worker %d: %w", i, err)
		}
	}
	return latencies, nil
}

func timings(out *result, all, work time.Time, latencies []float64) {
	out.WorkMS = float64(time.Since(work)) / float64(time.Millisecond)
	out.TotalMS = float64(time.Since(all)) / float64(time.Millisecond)
	if len(latencies) > 0 {
		sort.Float64s(latencies)
		percentile := func(p float64) float64 { return latencies[int(math.Ceil(p*float64(len(latencies))))-1] }
		out.P50MS, out.P95MS, out.P99MS = percentile(.50), percentile(.95), percentile(.99)
	}
}

func diskBytes(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}

func (r runner) edits(mode, policy string, n, round int) (out result) {
	out = result{Suite: "edits", Mode: mode, Round: round, Agents: n, Policy: policy, Features: map[string]any{}, Notes: []string{
		"NON-EQUIVALENT HISTORY: Git per-agent commits, worktree branch/integration commits, and AVC's one final checkpoint are reported separately; no equivalent speedup claim.",
		"Latency is worker submission-to-ACK/commit, not final integration. Total includes fixture setup, daemon/worktrees, integration, flush and final checkpoint.",
		"Git fixture writes fsync file and directory. AVC promises durable WAL ACK. This is not a power-loss durability equivalence test.",
		"CPU and process-tree RSS are null: this harness does not collect trustworthy process-tree samples. Disk is logical bytes, not allocated physical storage.",
	}}
	all := time.Now()
	// Short Unix paths avoid forcing the daemon to put a socket outside the
	// disposable fixture (macOS sockaddr_un has a small path limit).
	tmp, err := os.MkdirTemp("/tmp", "avc-bench-")
	if err != nil {
		out.Error = err.Error()
		return
	}
	defer os.RemoveAll(tmp)
	dir := filepath.Join(tmp, "repo")
	if err = os.Mkdir(dir, 0700); err != nil {
		out.Error = err.Error()
		return
	}
	fail := func(err error) {
		out.Correct = false
		out.Error = err.Error()
		out.TotalMS = float64(time.Since(all)) / float64(time.Millisecond)
	}
	if err = r.initGit(dir); err != nil {
		fail(err)
		return
	}
	if err = os.WriteFile(filepath.Join(dir, "shared.txt"), fixture(n, false), 0600); err != nil {
		fail(err)
		return
	}
	if err = r.commit(dir, "initial fixture"); err != nil {
		fail(err)
		return
	}
	if mode == "optimized" {
		for _, entry := range []struct {
			name string
			args []string
		}{
			{"split_index", []string{"update-index", "--split-index"}},
			{"untracked_cache", []string{"update-index", "--test-untracked-cache"}},
		} {
			_, e := r.git(dir, entry.args...)
			if e != nil {
				out.Features[entry.name] = "unavailable: " + e.Error()
			} else {
				out.Features[entry.name] = "enabled"
				if entry.name == "untracked_cache" {
					_, e = r.git(dir, "update-index", "--untracked-cache")
					if e != nil {
						out.Features[entry.name] = "enable failed: " + e.Error()
					}
				}
			}
		}
		probe := r
		probe.timeout = 5 * time.Second
		if _, e := probe.git(dir, "fsmonitor--daemon", "start"); e != nil {
			out.Features["fsmonitor"] = "unavailable: " + e.Error()
		} else {
			defer probe.git(dir, "fsmonitor--daemon", "stop")
			_, e = r.git(dir, "config", "core.fsmonitor", "true")
			if e != nil {
				out.Features["fsmonitor"] = "enable failed: " + e.Error()
			} else {
				out.Features["fsmonitor"] = "daemon started, core.fsmonitor=true"
			}
		}
		out.Features["scalar"] = "not tested; no global registration or configuration was changed"
	}
	if mode == "avc" {
		out.Policy = "one-final-checkpoint"
		stop, e := r.startDaemon(dir)
		if e != nil {
			fail(e)
			return
		}
		defer stop()
	}
	worktrees := make([]string, n)
	if mode == "worktree" {
		out.Policy = "per-agent-and-integration"
		for i := range worktrees {
			worktrees[i] = filepath.Join(tmp, fmt.Sprintf("worktree-%04d", i))
			if _, err = r.git(dir, "worktree", "add", "-q", "--detach", worktrees[i], "HEAD"); err != nil {
				fail(err)
				return
			}
		}
		out.Notes = append(out.Notes, "Worktree always commits each branch and cherry-picks it; batch policy only applies to shared-tree Git. Conflicts are errors, never silently replayed.")
	}
	out.SetupMS = float64(time.Since(all)) / float64(time.Millisecond)
	work := time.Now()
	var mu sync.Mutex
	commits := make([]string, n)
	latencies, err := parallel(n, func(id int) error {
		if mode == "avc" {
			_, e := r.command(dir, r.avc, "edit", "shared.txt", "--old", fmt.Sprintf("agent-%04d=pending", id), "--new", fmt.Sprintf("agent-%04d=done", id))
			return e
		}
		wd := dir
		if mode == "worktree" {
			wd = worktrees[id]
		} else {
			mu.Lock()
			defer mu.Unlock()
		}
		for i := 0; i < 2; i++ {
			if _, e := r.git(wd, "status", "--porcelain"); e != nil {
				return e
			}
		}
		if e := replace(wd, id); e != nil {
			return e
		}
		if mode == "worktree" || policy == "per-agent" {
			if e := r.commit(wd, fmt.Sprintf("agent %d", id)); e != nil {
				return e
			}
		}
		if mode == "worktree" {
			hash, e := r.git(wd, "rev-parse", "HEAD")
			commits[id] = strings.TrimSpace(string(hash))
			return e
		}
		return nil
	})
	timings(&out, all, work, latencies)
	if err != nil {
		fail(err)
		return
	}
	integration := time.Now()
	if mode == "worktree" {
		for _, hash := range commits {
			if _, err = r.git(dir, "cherry-pick", "--no-edit", hash); err != nil {
				if paths, _ := r.git(dir, "diff", "--name-only", "--diff-filter=U"); len(bytes.TrimSpace(paths)) > 0 {
					out.Conflicts++
				}
				fail(err)
				return
			}
		}
	} else if mode == "avc" {
		if _, err = r.command(dir, r.avc, "flush"); err != nil {
			fail(err)
			return
		}
		if _, err = r.command(dir, r.avc, "checkpoint", "-m", "benchmark final"); err != nil {
			fail(err)
			return
		}
		out.Stats = r.stats(dir)
		if out.Stats["read_error"] != nil || out.Stats["decode_error"] != nil {
			fail(fmt.Errorf("required AVC stats unavailable: %v", out.Stats))
			return
		}
	} else if policy == "batch" {
		if err = r.commit(dir, "benchmark final"); err != nil {
			fail(err)
			return
		}
	}
	out.IntegrationMS = float64(time.Since(integration)) / float64(time.Millisecond)
	out.TotalMS = float64(time.Since(all)) / float64(time.Millisecond)
	out.Throughput = float64(n) / (out.TotalMS / 1000)
	data, err := os.ReadFile(filepath.Join(dir, "shared.txt"))
	if err != nil {
		fail(err)
		return
	}
	out.Correct = bytes.Equal(data, fixture(n, true))
	if !out.Correct {
		out.Error = "final file differs from exact expected fixture"
	}
	out.DiskBytes = diskBytes(tmp)
	count, err := r.git(dir, "rev-list", "--all", "--count")
	if err != nil {
		fail(err)
		return
	}
	{
		out.GitCommits, err = strconv.Atoi(strings.TrimSpace(string(count)))
		if err != nil {
			fail(fmt.Errorf("invalid Git commit count: %w", err))
			return
		}
		out.GitCommits--
		if mode == "worktree" {
			// Detached branch tips are not included by --all; union all tips.
			args := append([]string{"rev-list", "--count", "HEAD"}, commits...)
			count, e := r.git(dir, args...)
			if e != nil {
				fail(e)
				return
			}
			out.GitCommits, err = strconv.Atoi(strings.TrimSpace(string(count)))
			if err != nil {
				fail(fmt.Errorf("invalid worktree commit count: %w", err))
				return
			}
			out.GitCommits--
		}
	}
	return
}

func worker(ms, mib, hashes int, eventLog string) error {
	if ms < 1 || ms > 5000 || mib < 1 || mib > 128 || hashes < 1 || hashes > 256 {
		return fmt.Errorf("worker limits: 1..5000 ms, 1..128 MiB, 1..256 hashes")
	}
	data := make([]byte, mib<<20)
	for i := range data {
		data[i] = byte(i % 251)
	}
	expected := sha256.Sum256(data)
	until := time.Now().Add(time.Duration(ms) * time.Millisecond)
	for i := 0; i < hashes; i++ {
		if sha256.Sum256(data) != expected {
			return fmt.Errorf("worker checksum changed")
		}
	}
	// Equal computation per execution, with a minimum overlap window so sharing
	// can be observed even on fast machines. The padding performs no extra hashes.
	if left := time.Until(until); left > 0 {
		time.Sleep(left)
	}
	id := fmt.Sprintf("pid=%d time=%d hashes=%d sha256=%s", os.Getpid(), time.Now().UnixNano(), hashes, hex.EncodeToString(expected[:]))
	f, err := os.OpenFile(eventLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(f, id)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	fmt.Println("worker completed", id)
	runtime.KeepAlive(data)
	return nil
}

func (r runner) resources(mode string, round, ms, mib, hashes int) (out result) {
	out = result{Suite: "resources", Mode: mode, Round: round, Agents: 3, ExpectedRuns: 3, Notes: []string{
		"Three bounded synthetic CPU+memory jobs, not a full project test suite. No claim about actual Mac memory pressure or real test-suite speedup.",
		"Actual executions are independently counted in an append-only fixture log outside the managed tree. Process-tree observations include started AVC clients, daemon and worker descendants, but not the benchmark coordinator.",
		"AVC uses --freeze for a managed stable version; --share explicitly permits coalescing this synthetic job. The event log is observation, not a required per-request side effect.",
		"Controlled max-jobs comparison: daemon max-load=100000 and min-free-mb=1 avoid unrelated host pressure delaying jobs. This does not validate adaptive load/memory admission thresholds.",
	}}
	all := time.Now()
	tmp, err := os.MkdirTemp("/tmp", "avc-resources-")
	if err != nil {
		out.Error = err.Error()
		return
	}
	defer os.RemoveAll(tmp)
	dir := filepath.Join(tmp, "repo")
	if err = os.Mkdir(dir, 0700); err != nil {
		out.Error = err.Error()
		return
	}
	if err = os.WriteFile(filepath.Join(dir, "shared.txt"), []byte("stable fixture\n"), 0600); err != nil {
		out.Error = err.Error()
		return
	}
	r.monitor = newProcessMonitor()
	defer r.monitor.finish(&out)
	if mode != "direct" {
		stop, e := r.startDaemon(dir)
		if e != nil {
			out.Error = e.Error()
			return
		}
		defer stop()
	}
	if mode == "share" {
		out.ExpectedRuns = 1
	}
	self, err := os.Executable()
	if err != nil {
		out.Error = err.Error()
		return
	}
	eventLog := filepath.Join(tmp, "executions.log")
	args := []string{"--worker", "--worker-ms", strconv.Itoa(ms), "--worker-mib", strconv.Itoa(mib), "--worker-hashes", strconv.Itoa(hashes), "--event-log", eventLog}
	out.SetupMS = float64(time.Since(all)) / float64(time.Millisecond)
	work := time.Now()
	latencies, err := parallel(3, func(_ int) error {
		name, commandArgs := self, append([]string(nil), args...)
		if mode != "direct" {
			name = r.avc
			commandArgs = []string{"run", "--freeze"}
			if mode == "share" {
				commandArgs = append(commandArgs, "--share")
			}
			commandArgs = append(commandArgs, "--", self)
			commandArgs = append(commandArgs, args...)
		}
		output, e := r.command(dir, name, commandArgs...)
		if e == nil && !bytes.Contains(output, []byte("worker completed")) {
			e = fmt.Errorf("worker output was not forwarded")
		}
		return e
	})
	timings(&out, all, work, latencies)
	out.Throughput = 3 / (out.TotalMS / 1000)
	if err != nil {
		out.Error = err.Error()
		return
	}
	data, err := os.ReadFile(eventLog)
	if err != nil {
		out.Error = err.Error()
		return
	}
	out.Executions = len(strings.FieldsFunc(strings.TrimSpace(string(data)), func(r rune) bool { return r == '\n' }))
	out.Correct = out.Executions == out.ExpectedRuns
	if !out.Correct {
		out.Error = fmt.Sprintf("expected %d executions, got %d", out.ExpectedRuns, out.Executions)
	}
	if mode != "direct" {
		out.Stats = r.stats(dir)
		if governor, ok := out.Stats["governor"].(map[string]any); ok {
			if peak, ok := governor["peak_running"].(float64); !ok || peak != 1 {
				out.Correct, out.Error = false, fmt.Sprintf("max-jobs=1 but peak_running=%g", peak)
			}
			if started, ok := governor["started"].(float64); !ok || int(started) != out.ExpectedRuns {
				out.Correct, out.Error = false, fmt.Sprintf("governor started=%v, expected %d", governor["started"], out.ExpectedRuns)
			}
		} else {
			out.Correct, out.Error = false, fmt.Sprintf("required governor stats unavailable: %v", out.Stats)
		}
	}
	out.DiskBytes = diskBytes(tmp)
	return
}

func writeReport(dir string, results []result, metadata map[string]any) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(map[string]any{"metadata": metadata, "results": results}, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(dir, "results.json"), append(data, '\n'), 0644); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(dir, "results.csv"))
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	w.Write([]string{"suite", "mode", "round", "agents", "commit_policy", "correct", "setup_ms", "work_ms", "integration_ms", "total_ms", "p50_ms", "p95_ms", "p99_ms", "tasks_per_second", "disk_logical_bytes", "git_commits", "actual_executions", "error"})
	for _, r := range results {
		f64 := func(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
		w.Write([]string{r.Suite, r.Mode, strconv.Itoa(r.Round), strconv.Itoa(r.Agents), r.Policy, strconv.FormatBool(r.Correct), f64(r.SetupMS), f64(r.WorkMS), f64(r.IntegrationMS), f64(r.TotalMS), f64(r.P50MS), f64(r.P95MS), f64(r.P99MS), f64(r.Throughput), strconv.FormatInt(r.DiskBytes, 10), strconv.Itoa(r.GitCommits), strconv.Itoa(r.Executions), r.Error})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func main() {
	avc := flag.String("avc", "./avc", "path to the built avc binary")
	agents := flag.Int("agents", 10, "simulated edit clients (1..200)")
	rounds := flag.Int("rounds", 3, "fresh fixtures per mode (1..20)")
	out := flag.String("out", "./bench-results", "result directory")
	suite := flag.String("suite", "all", "edits, resources, or all")
	modes := flag.String("modes", "native,worktree,optimized,avc", "edit modes, comma separated")
	policy := flag.String("commit-policy", "batch", "shared-tree Git: batch or per-agent; other modes report their actual policy")
	timeout := flag.Duration("timeout", 2*time.Minute, "deadline for each child command")
	isWorker := flag.Bool("worker", false, "internal bounded resource worker")
	ms := flag.Int("worker-ms", 300, "minimum worker lifetime in ms; fixed computation then idle padding")
	mib := flag.Int("worker-mib", 16, "resource worker allocation in MiB")
	hashes := flag.Int("worker-hashes", 32, "fixed SHA256 passes over the worker buffer")
	eventLog := flag.String("event-log", "", "internal worker observation log")
	flag.Parse()
	if *isWorker {
		if err := worker(*ms, *mib, *hashes, *eventLog); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *agents < 1 || *agents > 200 || *rounds < 1 || *rounds > 20 || (*suite != "all" && *suite != "edits" && *suite != "resources") || (*policy != "batch" && *policy != "per-agent") || *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "invalid agents/rounds/suite/commit-policy/timeout")
		os.Exit(2)
	}
	selected := strings.Split(*modes, ",")
	for _, mode := range selected {
		if mode != "native" && mode != "worktree" && mode != "optimized" && mode != "avc" {
			fmt.Fprintln(os.Stderr, "unknown mode:", mode)
			os.Exit(2)
		}
	}
	binary, err := filepath.Abs(*avc)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	r := runner{avc: binary, timeout: *timeout, ctx: ctx}
	gitVersion, _ := r.command("", "git", "--version")
	metadata := map[string]any{"timestamp": time.Now().UTC().Format(time.RFC3339), "os": runtime.GOOS, "arch": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "go": runtime.Version(), "git": strings.TrimSpace(string(gitVersion)), "agents": *agents, "rounds": *rounds, "fixture_seed": "deterministic-agent-blocks-v1", "worker_ms": *ms, "worker_mib": *mib, "comparison": "descriptive; history and durable-ACK semantics differ; no speedup conclusion", "measurement_limits": "Edit CPU/RSS unmeasured. Resource CPU/RSS are 50ms process-tree samples (lower bounds, RSS includes shared pages). No swap/physical disk/system load samples, real project workload, or power-loss simulation."}
	metadata["worker_hashes"] = *hashes
	metadata["daemon_limits"] = map[string]any{"max_jobs": 1, "workers": 1, "max_load": 100000, "min_free_mb": 1}
	metadata["avc_binary_sha256"] = fileHash(binary)
	if self, err := os.Executable(); err == nil {
		metadata["benchmark_binary_sha256"] = fileHash(self)
	}
	var results []result
	failed := false
	for round := 1; round <= *rounds; round++ {
		var current []result
		if *suite != "resources" {
			for _, mode := range selected {
				current = append(current, r.edits(mode, *policy, *agents, round))
			}
		}
		if *suite != "edits" {
			for _, mode := range []string{"direct", "scheduled", "share"} {
				current = append(current, r.resources(mode, round, *ms, *mib, *hashes))
			}
		}
		for _, row := range current {
			results = append(results, row)
			fmt.Printf("%s/%s round=%d correct=%t total=%.1fms commits=%d executions=%d\n", row.Suite, row.Mode, row.Round, row.Correct, row.TotalMS, row.GitCommits, row.Executions)
			if !row.Correct {
				failed = true
				fmt.Fprintln(os.Stderr, row.Error)
			}
		}
		if err := writeReport(*out, results, metadata); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if failed {
			break
		}
	}
	if failed {
		os.Exit(1)
	}
}
