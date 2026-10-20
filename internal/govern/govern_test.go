package govern

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// Huge thresholds so the machine's real load never delays these tests.
func quiet(maxJobs int) Config {
	return Config{MaxJobs: maxJobs, MaxLoad: 1e9, MinFreeMemMB: 1}
}

func TestExitCodeAndOutput(t *testing.T) {
	g := New(quiet(1))
	r := g.Run(context.Background(), Job{Argv: []string{"sh", "-c", "echo hi; exit 3"}, Dir: t.TempDir()})
	if r.ExitCode != 3 || strings.TrimSpace(string(r.Output)) != "hi" {
		t.Fatalf("got %d %q %s", r.ExitCode, r.Output, r.Err)
	}
}

func TestMaxJobsCapsConcurrency(t *testing.T) {
	g := New(quiet(2))
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Run(context.Background(), Job{Argv: []string{"sh", "-c", "sleep 0.2"}, Dir: t.TempDir()})
		}()
	}
	wg.Wait()
	if peak := g.Stats()["peak_running"].(int64); peak != 2 {
		t.Fatalf("peak_running = %d, want 2", peak)
	}
}

func TestShareRunsOnceOnlyWhenAsked(t *testing.T) {
	dir := t.TempDir()
	run := func(share bool, env string) {
		g := New(quiet(4))
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				g.Run(context.Background(), Job{Argv: []string{"sh", "-c", "sleep 0.3"}, Dir: dir,
					Env: []string{env}, Share: share, ShareKey: "gen1"})
			}()
			time.Sleep(20 * time.Millisecond)
		}
		wg.Wait()
		started := g.Stats()["started"].(int64)
		if share && started != 1 || !share && started != 4 {
			t.Errorf("share=%v started=%d", share, started)
		}
	}
	run(true, "X=1")
	run(false, "X=1")
}

func TestDifferentEnvNeverShares(t *testing.T) {
	a := shareKey(Job{Argv: []string{"npm", "test"}, Env: []string{"NODE_ENV=test"}, ShareKey: "1"})
	b := shareKey(Job{Argv: []string{"npm", "test"}, Env: []string{"NODE_ENV=prod"}, ShareKey: "1"})
	c := shareKey(Job{Argv: []string{"npm", "test"}, Env: []string{"NODE_ENV=prod", "NODE_ENV=test"}, ShareKey: "1"})
	if a == b || a != c {
		t.Fatal("share key must follow the effective environment")
	}
}

func TestCancelKillsProcessGroup(t *testing.T) {
	g := New(quiet(1))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	r := g.Run(ctx, Job{Argv: []string{"sh", "-c", "sleep 30 & wait"}, Dir: t.TempDir()})
	if time.Since(start) > 5*time.Second || r.Err == "" {
		t.Fatalf("not cancelled: %+v after %v", r, time.Since(start))
	}
}

func TestPrepareRunsAfterAdmission(t *testing.T) {
	g := New(quiet(1))
	dir := t.TempDir()
	cleaned := false
	r := g.Run(context.Background(), Job{Argv: []string{"pwd"}, Dir: "/",
		Prepare: func(context.Context) (string, func(), error) { return dir, func() { cleaned = true }, nil }})
	if !strings.Contains(string(r.Output), dir[len(dir)-8:]) || !cleaned {
		t.Fatalf("prepare not used: %q cleaned=%v", r.Output, cleaned)
	}
}

func TestLightJobsSkipTheQueue(t *testing.T) {
	g := New(quiet(1))
	done := make(chan Result, 1)
	go func() {
		done <- g.Run(context.Background(), Job{Argv: []string{"sh", "-c", "sleep 1"}, Dir: t.TempDir()})
	}()
	time.Sleep(100 * time.Millisecond) // the heavy job now holds the only slot
	start := time.Now()
	r := g.Run(context.Background(), Job{Argv: []string{"echo", "ok"}, Dir: t.TempDir(), Light: true})
	if r.ExitCode != 0 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("light job waited %v behind a heavy one (exit %d)", time.Since(start), r.ExitCode)
	}
	<-done
}

func TestLightJobsSkipTheLoadGate(t *testing.T) {
	g := New(Config{MaxJobs: 1, MaxLoad: 0.000001, MinFreeMemMB: 1}) // gate always closed
	start := time.Now()
	r := g.Run(context.Background(), Job{Argv: []string{"echo", "ok"}, Dir: t.TempDir(), Light: true})
	if r.ExitCode != 0 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("light job waited %v at the load gate (exit %d)", time.Since(start), r.ExitCode)
	}
}
