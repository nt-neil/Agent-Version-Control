package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/nt-neil/Agent-Version-Control/internal/proto"
)

const snapshotTimeout = 2 * time.Minute

// snapshot clones the tree into .avc/snaps/<id> and returns the job's
// directory inside it, the tree generation it holds, and a cleanup that is
// safe to call on every path. Managed edits are paused only while the clone is
// made: freeze, read the generation, list and copy, unfreeze, so the copy and
// its generation describe the same accepted state.
//
// The copy must be copy-on-write (APFS clonefile via `cp -c`, or
// `--reflink=always` elsewhere). Where that is unsupported cp fails, and so
// does the snapshot, rather than silently copying every byte.
func (s *Server) snapshot(ctx context.Context, dir string) (string, any, func(), error) {
	root := s.eng.Root()
	var id [6]byte
	rand.Read(id[:])
	snap := filepath.Join(root, ".avc", "snaps", hex.EncodeToString(id[:]))
	cleanup := func() { os.RemoveAll(snap) }
	fail := func(err error) (string, any, func(), error) {
		cleanup()
		return "", nil, func() {}, err
	}
	if err := os.MkdirAll(snap, 0o755); err != nil {
		return fail(err)
	}

	gen := s.eng.Do(proto.Request{Op: "freeze"}).Info["gen"]
	defer s.eng.Do(proto.Request{Op: "unfreeze"})

	ents, err := os.ReadDir(root)
	if err != nil {
		return fail(err)
	}
	args := []string{"-cRp"}
	if runtime.GOOS != "darwin" {
		args = []string{"-a", "--reflink=always"}
	}
	n := len(args)
	for _, e := range ents {
		if !strings.EqualFold(e.Name(), ".avc") {
			args = append(args, filepath.Join(root, e.Name()))
		}
	}
	if len(args) > n {
		cctx, cancel := context.WithTimeout(ctx, snapshotTimeout)
		defer cancel()
		out, err := exec.CommandContext(cctx, "cp", append(args, snap+"/")...).CombinedOutput()
		if err != nil {
			return fail(fmt.Errorf("copy-on-write clone failed (%v): %s", err, strings.TrimSpace(string(out))))
		}
	}
	rel, err := filepath.Rel(root, dir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		rel = "."
	}
	return filepath.Join(snap, rel), gen, cleanup, nil
}
