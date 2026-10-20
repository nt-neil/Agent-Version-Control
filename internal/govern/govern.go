// Package govern runs expensive commands (builds, test suites) through one
// admission point so many agents cannot start them all at once.
//
// Limits here are soft: they cap how many jobs start, delay starts while the
// machine is already loaded, and hint a worker budget to each job. A single
// job that ignores the hint can still use every core; that needs OS limits.
package govern

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	maxOutput   = 8 << 20
	pollEvery   = 2 * time.Second
	soloWaitCap = 60 * time.Second
)

type Config struct {
	MaxJobs      int     // jobs running at once; <=0 means 1
	MaxLoad      float64 // with a job running, wait while load1 exceeds this; <=0 means NumCPU*1.5
	MinFreeMemMB int     // with a job running, wait while available memory is below this; <=0 means 1024
	Workers      int     // worker budget hinted to each job; <=0 means max(1, NumCPU/2)
	Nice         int     // nice value applied to jobs; 0 leaves priority alone
}

type Job struct {
	Name     string
	Argv     []string
	Dir      string
	Env      []string // appended to the daemon's environment; later entries win
	Share    bool     // identical concurrent jobs may share one execution
	ShareKey string   // caller-supplied version of the inputs, e.g. tree generation
	// Prepare, if set, runs once the job is admitted and returns the directory
	// to run in plus a cleanup. Doing it after admission keeps per-job state
	// (such as snapshots) bounded by MaxJobs.
	Prepare func(ctx context.Context) (dir string, cleanup func(), err error)
	// Light marks a quick command (formatters, vet, linters on a few files).
	// It skips the load gate and the main queue but has its own small cap.
	Light bool
}

type Result struct {
	ExitCode                   int
	Output                     []byte
	Coalesced                  bool
	QueuedMs, PrepareMs, RunMs int64
	Err                        string
}

type flight struct {
	done    chan struct{}
	res     Result
	waiters int
	cancel  context.CancelFunc
}

type Governor struct {
	cfg   Config
	slots chan struct{}
	light chan struct{}

	mu      sync.Mutex
	running int
	flights map[string]*flight
	stats   map[string]int64
}

func New(cfg Config) *Governor {
	if cfg.MaxJobs <= 0 {
		cfg.MaxJobs = 1
	}
	if cfg.MaxLoad <= 0 {
		cfg.MaxLoad = float64(runtime.NumCPU()) * 1.5
	}
	if cfg.MinFreeMemMB <= 0 {
		cfg.MinFreeMemMB = 1024
	}
	if cfg.Workers <= 0 {
		cfg.Workers = max(1, runtime.NumCPU()/2)
	}
	return &Governor{
		cfg:     cfg,
		slots:   make(chan struct{}, cfg.MaxJobs),
		light:   make(chan struct{}, max(2, runtime.NumCPU()/2)),
		flights: map[string]*flight{},
		stats:   map[string]int64{},
	}
}

func (g *Governor) Run(ctx context.Context, j Job) Result {
	if !j.Share {
		return g.execute(ctx, j)
	}
	key := shareKey(j)
	g.mu.Lock()
	f := g.flights[key]
	if f == nil {
		fctx, cancel := context.WithCancel(context.Background())
		f = &flight{done: make(chan struct{}), cancel: cancel}
		g.flights[key] = f
		go func() {
			f.res = g.execute(fctx, j)
			g.mu.Lock()
			delete(g.flights, key)
			g.mu.Unlock()
			cancel()
			close(f.done)
		}()
	} else {
		g.stats["coalesced"]++
	}
	joined := f.waiters > 0
	f.waiters++
	g.mu.Unlock()

	select {
	case <-f.done:
		r := f.res
		r.Coalesced = joined
		return r
	case <-ctx.Done():
		g.mu.Lock()
		f.waiters--
		if f.waiters == 0 {
			f.cancel() // nobody wants the result any more
		}
		g.mu.Unlock()
		return Result{ExitCode: -1, Err: ctx.Err().Error()}
	}
}

func (g *Governor) execute(ctx context.Context, j Job) Result {
	start := time.Now()
	g.bump("queued", 1)
	slots := g.slots
	if j.Light {
		slots = g.light
		g.bump("light_started", 1)
	}
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		g.bump("queued", -1)
		return Result{ExitCode: -1, Err: ctx.Err().Error()}
	}
	defer func() { <-slots }()
	if !j.Light {
		if err := g.waitForHeadroom(ctx); err != nil {
			g.bump("queued", -1)
			return Result{ExitCode: -1, Err: err.Error()}
		}
	}
	g.bump("queued", -1)
	g.mu.Lock()
	g.running++
	g.stats["started"]++
	if int64(g.running) > g.stats["peak_running"] {
		g.stats["peak_running"] = int64(g.running)
	}
	g.mu.Unlock()
	defer g.bump("running", -1)

	res := Result{QueuedMs: time.Since(start).Milliseconds()}
	dir := j.Dir
	if j.Prepare != nil {
		t := time.Now()
		d, cleanup, err := j.Prepare(ctx)
		res.PrepareMs = time.Since(t).Milliseconds()
		if err != nil {
			res.ExitCode, res.Err = 1, "prepare: "+err.Error()
			return res
		}
		defer cleanup()
		dir = d
	}
	runStart := time.Now()
	cmd := exec.Command(j.Argv[0], j.Argv[1:]...)
	cmd.Dir = dir
	cmd.Env = g.env(j.Env)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out := &capped{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		res.ExitCode, res.Err = 127, err.Error()
		return res
	}
	if g.cfg.Nice > 0 {
		syscall.Setpriority(syscall.PRIO_PGRP, cmd.Process.Pid, g.cfg.Nice)
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	var err error
	select {
	case err = <-waited:
	case <-ctx.Done():
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		err = <-waited
		res.Err = "cancelled"
	}
	res.RunMs = time.Since(runStart).Milliseconds()
	res.Output = out.Bytes()
	var ee *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &ee):
		res.ExitCode = ee.ExitCode()
		if res.ExitCode < 0 {
			res.ExitCode = 137
		}
	default:
		res.ExitCode, res.Err = 1, err.Error()
	}
	return res
}

// waitForHeadroom delays a start while the machine is already loaded, whether
// by avc jobs or anything else. With nothing of ours running, the wait is
// capped so a machine that stays busy for other reasons cannot block forever.
// Polling is slow on purpose: reading load costs a process spawn.
func (g *Governor) waitForHeadroom(ctx context.Context) error {
	deadline := time.Now().Add(soloWaitCap)
	for {
		load, lok := loadAvg()
		mem, mok := availableMB()
		switch {
		case lok && load > g.cfg.MaxLoad:
			g.bump("waits_for_load", 1)
		case mok && mem < g.cfg.MinFreeMemMB:
			g.bump("waits_for_mem", 1)
		default:
			return nil
		}
		g.mu.Lock()
		solo := g.running == 0
		g.mu.Unlock()
		if solo && time.Now().After(deadline) {
			g.bump("headroom_timeouts", 1)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollEvery):
		}
	}
}

func (g *Governor) env(extra []string) []string {
	env := append(os.Environ(), extra...)
	n := strconv.Itoa(g.cfg.Workers)
	for k, v := range map[string]string{"AVC_WORKERS": n, "MAKEFLAGS": "-j" + n, "GOMAXPROCS": n, "JEST_WORKERS": n} {
		if !hasKey(env, k) {
			env = append(env, k+"="+v)
		}
	}
	return env
}

func (g *Governor) bump(k string, d int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if k == "running" {
		g.running += int(d)
		return
	}
	g.stats[k] += d
}

func (g *Governor) Stats() map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	m := map[string]any{"running": g.running, "max_jobs": g.cfg.MaxJobs, "workers": g.cfg.Workers}
	for k, v := range g.stats {
		m[k] = v
	}
	return m
}

func shareKey(j Job) string {
	h := sha256.New()
	// The whole effective environment joins the key (last value per name wins,
	// sorted): callers whose environments differ must not share a result.
	eff := map[string]string{}
	for _, kv := range j.Env {
		k, v, _ := strings.Cut(kv, "=")
		eff[k] = v
	}
	env := make([]string, 0, len(eff))
	for k, v := range eff {
		env = append(env, k+"="+v)
	}
	sort.Strings(env)
	for _, part := range [][]string{j.Argv, {j.Dir}, env, {j.ShareKey}} {
		for _, s := range part {
			h.Write([]byte(s))
			h.Write([]byte{0})
		}
		h.Write([]byte{1})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func hasKey(env []string, k string) bool {
	for _, e := range env {
		if strings.HasPrefix(e, k+"=") {
			return true
		}
	}
	return false
}

// capped keeps the first maxOutput bytes of a job's output.
type capped struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	truncated bool
}

func (c *capped) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if room := maxOutput - c.buf.Len(); room < len(p) {
		c.buf.Write(p[:max(room, 0)])
		c.truncated = true
	} else {
		c.buf.Write(p)
	}
	return len(p), nil
}

func (c *capped) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.truncated {
		return append(c.buf.Bytes(), []byte("\n[avc: output truncated at 8MB]\n")...)
	}
	return c.buf.Bytes()
}
