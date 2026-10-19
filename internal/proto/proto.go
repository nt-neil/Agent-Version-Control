// Package proto defines the newline-delimited JSON protocol spoken over the
// daemon's Unix socket. One request and one response per connection.
package proto

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// Exit codes shared by the CLI and the daemon.
const (
	ExitOK       = 0
	ExitError    = 1
	ExitUsage    = 2
	ExitConflict = 3 // anchor missing/ambiguous, base mismatch
	ExitLease    = 4 // path leased by someone else, or stale token
	ExitDrift    = 5 // file changed on disk outside avc
	ExitBusy     = 6 // a hard limit is reached; retry later
)

// Hard limits that keep the daemon's memory and disk bounded.
const (
	MaxRequestBytes = 8 << 20   // one request on the wire (~6 MiB of file content)
	MaxDecoding     = 32        // requests being read at once; more wait in the accept backlog
	MaxLive         = 512       // connections alive at once, whatever they are doing
	MaxLiveBytes    = 128 << 20 // decoded request bytes held by live connections
	MaxPendingJobs  = 64        // avc run requests queued or running; leaves room for edits
	MaxLeases       = 65536     // active leases
	MaxCanon        = 65536     // remembered path spellings
	MaxNameBytes    = 4096      // path, agent id, message, dir
	MaxHeldOps      = 4096      // edits queued while frozen
	MaxHeldBytes    = 64 << 20  // bytes of those queued edits
	MaxJournalBytes = 256 << 20 // journal size at which new edits are refused
	MaxCacheBytes   = 256 << 20 // cached file contents before the cache is dropped
)

type Request struct {
	Op       string   `json:"op"` // edit, write, cat, lease, release, run, checkpoint, flush, stats, shutdown
	Path     string   `json:"path,omitempty"`
	Old      string   `json:"old,omitempty"`
	New      string   `json:"new,omitempty"`
	Content  []byte   `json:"content,omitempty"`
	Base     string   `json:"base,omitempty"` // sha256 hex the writer expects to replace
	Agent    string   `json:"agent,omitempty"`
	Token    uint64   `json:"token,omitempty"`
	TTLms    int64    `json:"ttl_ms,omitempty"`
	Message  string   `json:"message,omitempty"`
	Argv     []string `json:"argv,omitempty"`
	Dir      string   `json:"dir,omitempty"`
	Env      []string `json:"env,omitempty"`
	Share    bool     `json:"share,omitempty"`
	Freeze   bool     `json:"freeze,omitempty"`
	Snapshot bool     `json:"snapshot,omitempty"`
}

type Response struct {
	Code    int            `json:"code"`
	Error   string         `json:"error,omitempty"`
	Content []byte         `json:"content,omitempty"`
	SHA     string         `json:"sha,omitempty"`
	Token   uint64         `json:"token,omitempty"`
	Seq     uint64         `json:"seq,omitempty"`
	Output  []byte         `json:"output,omitempty"`
	Info    map[string]any `json:"info,omitempty"`
}

// SocketPath returns the daemon socket for a repository root. Unix socket
// paths are limited to ~104 bytes on macOS, so deep roots get a short path
// under /tmp keyed by the root's hash.
func SocketPath(root string) string {
	p := filepath.Join(root, ".avc", "sock")
	if len(p) < 100 {
		return p
	}
	h := sha256.Sum256([]byte(root))
	return filepath.Join(fmt.Sprintf("/tmp/avc-%d", os.Getuid()), hex.EncodeToString(h[:8])+".sock")
}

// Canonical resolves symlinks so a root has one spelling (/tmp vs /private/tmp).
func Canonical(p string) string {
	if abs, err := filepath.Abs(p); err == nil {
		p = abs
	}
	if real, err := filepath.EvalSymlinks(p); err == nil {
		p = real
	}
	return p
}

// FindRoot walks up from dir looking for a directory whose daemon socket exists.
func FindRoot(dir string) (string, bool) {
	dir = Canonical(dir)
	for {
		if _, err := os.Stat(SocketPath(dir)); err == nil {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}
