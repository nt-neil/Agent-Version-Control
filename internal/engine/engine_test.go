package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/nt-neil/Agent-Version-Control/internal/journal"
	"github.com/nt-neil/Agent-Version-Control/internal/proto"
)

func open(t *testing.T, root string) *Engine {
	t.Helper()
	e, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e
}

func TestDisjointEditsToOneFileAllLand(t *testing.T) {
	root := t.TempDir()
	var b strings.Builder
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&b, "slot-%03d: todo\n", i)
	}
	os.WriteFile(filepath.Join(root, "shared.txt"), []byte(b.String()), 0o644)
	e := open(t, root)

	var wg sync.WaitGroup
	codes := make([]int, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = e.Do(proto.Request{Op: "edit", Path: "shared.txt",
				Old: fmt.Sprintf("slot-%03d: todo", i), New: fmt.Sprintf("slot-%03d: agent-%03d", i, i)}).Code
		}(i)
	}
	wg.Wait()
	for i, c := range codes {
		if c != proto.ExitOK {
			t.Fatalf("agent %d got code %d", i, c)
		}
	}
	got, _ := os.ReadFile(filepath.Join(root, "shared.txt"))
	if strings.Contains(string(got), "todo") || strings.Count(string(got), "agent-") != 100 {
		t.Fatalf("lost edits:\n%s", got)
	}
}

func TestSameAnchorExactlyOneWins(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "f"), []byte("x = 1;\n"), 0o644)
	e := open(t, root)
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := e.Do(proto.Request{Op: "edit", Path: "f", Old: "x = 1;", New: fmt.Sprintf("x = %d;", i+2)})
			if r.Code == proto.ExitOK {
				mu.Lock()
				wins++
				mu.Unlock()
			} else if r.Code != proto.ExitConflict {
				t.Errorf("unexpected code %d", r.Code)
			}
		}(i)
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("want 1 winner, got %d", wins)
	}
}

func TestRecoveryReplaysAcknowledgedBatch(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, ".avc"), 0o755)
	j, _, err := journal.Open(filepath.Join(root, ".avc", "journal"))
	if err != nil {
		t.Fatal(err)
	}
	// A batch acknowledged before a crash, whose file writes never happened.
	j.Append(journal.Record{Seq: 7, Files: map[string][]byte{"a/b.txt": []byte("after")}})
	j.Close()
	f, _ := os.OpenFile(filepath.Join(root, ".avc", "journal"), os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{9, 9, 9}) // torn tail from a second, unacknowledged append
	f.Close()

	e := open(t, root)
	got, err := os.ReadFile(filepath.Join(root, "a", "b.txt"))
	if err != nil || string(got) != "after" {
		t.Fatalf("recovered %q, %v", got, err)
	}
	if st := e.Do(proto.Request{Op: "stats"}).Info; st["journal_bytes"] != int64(0) || st["seq"] != uint64(7) {
		t.Fatalf("stats after recovery: %v", st)
	}
}

func TestSecondDaemonIsRefused(t *testing.T) {
	root := t.TempDir()
	open(t, root)
	if _, err := Open(root); err == nil {
		t.Fatal("second Open on the same root succeeded")
	}
}

func TestLeaseFencing(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "f"), []byte("a\n"), 0o644)
	e := open(t, root)
	tok := e.Do(proto.Request{Op: "lease", Path: "f", Agent: "x"}).Token
	if c := e.Do(proto.Request{Op: "edit", Path: "f", Old: "a", New: "b"}).Code; c != proto.ExitLease {
		t.Fatalf("edit without token: %d", c)
	}
	if e.fold {
		if c := e.Do(proto.Request{Op: "edit", Path: "F", Old: "a", New: "b"}).Code; c != proto.ExitLease {
			t.Fatalf("other-case path bypassed lease: %d", c)
		}
	}
	if c := e.Do(proto.Request{Op: "edit", Path: "f", Old: "a", New: "b", Token: tok}).Code; c != proto.ExitOK {
		t.Fatalf("edit with token: %d", c)
	}
	e.Do(proto.Request{Op: "release", Path: "f", Token: tok})
	if c := e.Do(proto.Request{Op: "edit", Path: "f", Old: "b", New: "c", Token: tok}).Code; c != proto.ExitLease {
		t.Fatalf("stale token accepted: %d", c)
	}
}

func TestReservedAndEscapingPaths(t *testing.T) {
	root := t.TempDir()
	e := open(t, root)
	os.Symlink(os.TempDir(), filepath.Join(root, "link"))
	for _, p := range []string{"../x", ".git/config", ".AVC/journal", "link/x"} {
		if c := e.Do(proto.Request{Op: "write", Path: p, Content: []byte("x")}).Code; c != proto.ExitUsage {
			t.Errorf("%s: code %d", p, c)
		}
	}
}

func TestExternalChangeFailsClosed(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "f")
	os.WriteFile(p, []byte("one\n"), 0o644)
	e := open(t, root)
	e.Do(proto.Request{Op: "edit", Path: "f", Old: "one", New: "two"})
	os.WriteFile(p, []byte("two\nexternal\n"), 0o644)
	if c := e.Do(proto.Request{Op: "edit", Path: "f", Old: "two", New: "three"}).Code; c != proto.ExitDrift {
		t.Fatalf("drift not detected: %d", c)
	}
	if c := e.Do(proto.Request{Op: "edit", Path: "f", Old: "two", New: "three"}).Code; c != proto.ExitOK {
		t.Fatalf("retry after drift: %d", c)
	}
	got, _ := os.ReadFile(p)
	if string(got) != "three\nexternal\n" {
		t.Fatalf("got %q", got)
	}
}
