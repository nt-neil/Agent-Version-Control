// Package engine owns the shared working tree. A single integrator goroutine
// takes every mutation, applies whatever has queued up as one batch, makes the
// batch durable with one journal fsync, writes each touched file once, and
// only then acknowledges. Nothing per-agent is copied and no history is kept.
package engine

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nt-neil/Agent-Version-Control/internal/journal"
	"github.com/nt-neil/Agent-Version-Control/internal/proto"
)

const (
	maxBatch     = 4096
	idleSync     = 200 * time.Millisecond
	journalLimit = 64 << 20
)

type file struct {
	data   []byte
	exists bool
	size   int64
	mtime  time.Time
}

type lease struct {
	agent   string
	token   uint64
	expires time.Time
}

type op struct {
	req  proto.Request
	resp chan proto.Response
}

// staged is a file's new content within the current batch.
type staged struct {
	path string
	data []byte
}

// Maps are keyed by a path's identity: its canonical spelling, lowercased on
// case-insensitive volumes so Foo.txt and foo.txt are one file.
type Engine struct {
	root    string
	fold    bool
	lock    *os.File
	ops     chan *op
	j       *journal.Journal
	seq     uint64
	gen     uint64
	files   map[string]*file
	leases  map[string]*lease
	canon   map[string]string // identity -> on-disk spelling
	token   uint64
	stalled error // set when the tree fell behind the journal; edits are refused
	frozen  int
	held    []*op
	heldB   int
	unsyncd map[string]string // identity -> path
	stats   map[string]int64
	done    chan struct{}
}

// Open takes the repository lock, recovers any journaled batches into the
// tree and starts the integrator.
func Open(root string) (*Engine, error) {
	root = proto.Canonical(root)
	dir := filepath.Join(root, ".avc")
	// Verify .avc before touching anything inside it: a symlink here would
	// send the journal, lock and snapshot cleanup outside the repository.
	if err := os.Mkdir(dir, 0o700); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if err := proto.CheckOwned(dir, true); err != nil {
		return nil, fmt.Errorf("unsafe %s: %w", dir, err)
	}
	// Keep private state out of git without touching the user's .gitignore.
	gi, err := proto.OpenState(filepath.Join(dir, ".gitignore"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return nil, err
	}
	_, err = gi.WriteString("*\n")
	gi.Close()
	if err != nil {
		return nil, err
	}
	lock, err := proto.OpenState(filepath.Join(dir, "lock"), os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("another avc daemon holds " + dir)
	}
	j, recs, err := journal.Open(filepath.Join(dir, "journal"))
	if err != nil {
		lock.Close()
		return nil, err
	}
	e := &Engine{
		root:    root,
		fold:    caseInsensitive(dir),
		lock:    lock,
		ops:     make(chan *op, maxBatch),
		j:       j,
		files:   map[string]*file{},
		leases:  map[string]*lease{},
		canon:   map[string]string{},
		token:   bootEpoch(), // random per boot, so old tokens never match
		unsyncd: map[string]string{},
		stats:   map[string]int64{},
		done:    make(chan struct{}),
	}
	for _, r := range recs {
		for p, data := range r.Files {
			if err := e.writeFile(p, data); err != nil {
				return nil, fmt.Errorf("recover %s: %w", p, err)
			}
		}
		e.seq = r.Seq
		e.stats["recovered_batches"]++
	}
	// Make the journal file itself and .avc durable before accepting work.
	for _, p := range []string{dir, root} {
		if err := fsync(p); err != nil {
			return nil, err
		}
	}
	if err := e.syncAll(); err != nil {
		return nil, err
	}
	go e.loop()
	return e, nil
}

func (e *Engine) Root() string { return e.root }

// Do submits a request to the integrator and waits for its answer.
func (e *Engine) Do(req proto.Request) proto.Response {
	o := &op{req: req, resp: make(chan proto.Response, 1)}
	select {
	case e.ops <- o:
	case <-e.done:
		return proto.Response{Code: proto.ExitError, Error: "engine closed"}
	}
	return <-o.resp
}

func (e *Engine) Close() {
	e.Do(proto.Request{Op: "close"})
}

func (e *Engine) loop() {
	idle := time.NewTimer(time.Hour)
	for {
		var batch []*op
		if e.frozen == 0 && len(e.held) > 0 {
			batch, e.held, e.heldB = e.held, nil, 0
		} else {
			select {
			case o := <-e.ops:
				batch = append(batch, o)
			case <-idle.C:
				if err := e.syncAll(); err != nil {
					e.stats["sync_errors"]++
				}
				continue
			}
		}
	drain:
		for len(batch) < maxBatch {
			select {
			case o := <-e.ops:
				batch = append(batch, o)
			default:
				break drain
			}
		}
		if e.process(batch) {
			e.j.Close()
			e.lock.Close()
			close(e.done)
			return
		}
		e.trimCache()
		if len(e.unsyncd) > 0 {
			idle.Reset(idleSync)
		}
	}
}

type pending struct {
	o      *op
	resp   proto.Response
	staged bool // the answer depends on this batch committing
}

// process applies one batch; it returns true when the engine should stop.
func (e *Engine) process(batch []*op) (stop bool) {
	stage := map[string]staged{}
	var acks []pending
	e.stats["batches"]++

	commit := func() {
		if len(stage) > 0 {
			files := make(map[string][]byte, len(stage))
			for _, s := range stage {
				files[s.path] = s.data
			}
			e.seq++
			err := e.j.Append(journal.Record{Seq: e.seq, Files: files})
			if err != nil {
				e.seq--
			} else {
				for p, data := range files {
					if werr := e.writeFile(p, data); werr != nil {
						// The batch is durable but the tree is now behind the
						// journal. Stop: keep the journal, refuse new edits,
						// and let recovery on restart finish these writes.
						e.stalled = fmt.Errorf("batch %d is in the journal but writing %s failed (%v); restart the daemon to apply it", e.seq, p, werr)
						err = e.stalled
						break
					}
				}
			}
			if err != nil {
				e.stats["commit_errors"]++
				for i := range acks {
					if acks[i].staged {
						acks[i].resp = proto.Response{Code: proto.ExitError, Error: err.Error()}
					}
				}
			} else {
				e.gen++
				e.stats["files_written"] += int64(len(files))
				e.stats["fsyncs"]++
				for i := range acks {
					if acks[i].resp.Code == proto.ExitOK {
						acks[i].resp.Seq = e.seq
					}
				}
			}
		}
		for _, a := range acks {
			a.o.resp <- a.resp
		}
		stage = map[string]staged{}
		acks = nil
		if e.j.Size() > journalLimit {
			if err := e.syncAll(); err != nil {
				e.stats["sync_errors"]++
			}
		}
	}

	for _, o := range batch {
		r := o.req
		switch r.Op {
		case "edit", "write", "append":
			if e.frozen > 0 {
				n := len(r.Content) + len(r.Old) + len(r.New)
				if len(e.held) >= proto.MaxHeldOps || e.heldB+n > proto.MaxHeldBytes {
					e.stats["busy_held"]++
					o.resp <- proto.Response{Code: proto.ExitBusy, Error: "tree is frozen and the edit queue is full; retry later"}
					continue
				}
				e.held = append(e.held, o)
				e.heldB += n
				continue
			}
			if e.stalled != nil {
				o.resp <- proto.Response{Code: proto.ExitBusy, Error: e.stalled.Error()}
				continue
			}
			if e.j.Size() > proto.MaxJournalBytes {
				commit() // flush what this batch already accepted, then try to reclaim
				if err := e.syncAll(); err != nil || e.j.Size() > proto.MaxJournalBytes {
					e.stats["busy_journal"]++
					o.resp <- proto.Response{Code: proto.ExitBusy, Error: "journal is full and cannot be reclaimed; retry later"}
					continue
				}
			}
			resp := e.mutate(r, stage)
			if resp.Code != proto.ExitOK {
				e.stats["rejected_"+r.Op]++
				o.resp <- resp
				continue
			}
			e.stats["ok_"+r.Op]++
			acks = append(acks, pending{o, resp, true})
		case "cat":
			p, id, err := e.clean(r.Path)
			if err != nil {
				o.resp <- usage(err)
				continue
			}
			s, ok := stage[id]
			data := s.data
			if !ok {
				f, err := e.load(p, id)
				if errors.Is(err, errDrift) {
					f, err = e.load(p, id) // reads never conflict; take what is on disk
				}
				if err != nil {
					o.resp <- fail(err)
					continue
				}
				if !f.exists {
					o.resp <- proto.Response{Code: proto.ExitError, Error: "no such file: " + p}
					continue
				}
				data = f.data
			}
			acks = append(acks, pending{o, proto.Response{Content: data, SHA: sum(data)}, ok})
		case "lease":
			o.resp <- e.acquire(r)
		case "release":
			o.resp <- e.release(r)
		default:
			// Everything else is a barrier: settle the batch so far first.
			commit()
			switch r.Op {
			case "flush":
				o.resp <- e.result(e.syncAll())
			case "freeze":
				e.frozen++
				o.resp <- proto.Response{Info: map[string]any{"gen": e.gen}}
			case "unfreeze":
				if e.frozen > 0 {
					e.frozen--
				}
				o.resp <- proto.Response{Info: map[string]any{"gen": e.gen}}
			case "gen":
				o.resp <- proto.Response{Info: map[string]any{"gen": e.gen}}
			case "checkpoint":
				o.resp <- e.checkpoint(r.Message)
			case "stats":
				o.resp <- proto.Response{Info: e.snapshot()}
			case "close":
				err := e.syncAll()
				for _, h := range e.held {
					h.resp <- proto.Response{Code: proto.ExitError, Error: "engine closed"}
				}
				o.resp <- e.result(err)
				return true
			default:
				o.resp <- proto.Response{Code: proto.ExitUsage, Error: "unknown op: " + r.Op}
			}
		}
	}
	commit()
	return false
}

func (e *Engine) mutate(r proto.Request, stage map[string]staged) proto.Response {
	p, id, err := e.clean(r.Path)
	if err != nil {
		return usage(err)
	}
	if resp, ok := e.checkLease(p, id, r); !ok {
		return resp
	}
	cur, exists := stage[id].data, true
	if s, inBatch := stage[id]; inBatch {
		p = s.path
	} else {
		f, err := e.load(p, id)
		if errors.Is(err, errDrift) {
			e.stats["drift"]++
			return proto.Response{Code: proto.ExitDrift, Error: p + " changed on disk outside avc; reloaded, retry against the new content"}
		}
		if err != nil {
			return fail(err)
		}
		cur, exists = f.data, f.exists
	}
	var next []byte
	switch r.Op {
	case "edit":
		if !exists {
			return proto.Response{Code: proto.ExitConflict, Error: "no such file: " + p}
		}
		if r.Old == "" {
			return proto.Response{Code: proto.ExitUsage, Error: "edit needs a non-empty --old anchor"}
		}
		switch n := bytes.Count(cur, []byte(r.Old)); n {
		case 0:
			return proto.Response{Code: proto.ExitConflict, Error: "anchor not found in " + p}
		case 1:
			next = bytes.Replace(cur, []byte(r.Old), []byte(r.New), 1)
		default:
			return proto.Response{Code: proto.ExitConflict, Error: fmt.Sprintf("anchor matches %d times in %s; give more context", n, p)}
		}
	case "append":
		// Appends never conflict with each other: two agents adding to the
		// same log both land, in arrival order.
		next = append(append([]byte{}, cur...), r.Content...)
	case "write":
		if r.Base != "" {
			have := "absent"
			if exists {
				have = sum(cur)
			}
			if r.Base != have {
				return proto.Response{Code: proto.ExitConflict, Error: "base mismatch for " + p + ": file is " + have}
			}
		}
		next = r.Content
	}
	stage[id] = staged{path: p, data: next}
	return proto.Response{SHA: sum(next)}
}

var errDrift = errors.New("drift")

// load returns the cached file, refreshing it from disk on first use and
// reporting errDrift when someone else changed it since avc last wrote it.
func (e *Engine) load(p, id string) (*file, error) {
	abs := filepath.Join(e.root, p)
	st, err := os.Stat(abs)
	f := e.files[id]
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	same := f != nil && ((err != nil && !f.exists) ||
		(err == nil && f.exists && st.Size() == f.size && st.ModTime().Equal(f.mtime)))
	if same {
		return f, nil
	}
	nf := &file{}
	if err == nil {
		if nf.data, err = os.ReadFile(abs); err != nil {
			return nil, err
		}
		nf.exists, nf.size, nf.mtime = true, st.Size(), st.ModTime()
	}
	e.files[id] = nf
	if f != nil {
		return nil, errDrift
	}
	return nf, nil
}

// writeFile replaces the file atomically (temp + rename) without fsync; the
// journal record already covers it until syncAll.
func (e *Engine) writeFile(p string, data []byte) error {
	abs := filepath.Join(e.root, p)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	mode := os.FileMode(0o644)
	if st, err := os.Stat(abs); err == nil {
		mode = st.Mode().Perm()
	}
	tmp := filepath.Join(filepath.Dir(abs), ".avc-tmp-"+filepath.Base(abs))
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	if err := os.Rename(tmp, abs); err != nil {
		os.Remove(tmp)
		return err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return err
	}
	id := e.identity(p)
	e.files[id] = &file{data: data, exists: true, size: st.Size(), mtime: st.ModTime()}
	e.unsyncd[id] = p
	return nil
}

// syncAll fsyncs every file written since the last sync and every directory
// from it up to the root (new directories need their parents synced too),
// then empties the journal.
func (e *Engine) syncAll() error {
	if e.stalled != nil {
		return e.stalled // never drop a journal the tree has not caught up with
	}
	if len(e.unsyncd) == 0 && e.j.Size() == 0 {
		return nil
	}
	dirs := map[string]bool{}
	for _, p := range e.unsyncd {
		abs := filepath.Join(e.root, p)
		if err := fsync(abs); err != nil {
			return err
		}
		for d := filepath.Dir(abs); !dirs[d]; d = filepath.Dir(d) {
			dirs[d] = true
			if d == e.root || len(d) <= len(e.root) {
				break
			}
		}
	}
	for d := range dirs {
		if err := fsync(d); err != nil {
			return err
		}
	}
	e.unsyncd = map[string]string{}
	// Once durable, drop cached contents: memory stays proportional to the
	// files being edited right now, and the next edit rereads fresh content.
	e.files = map[string]*file{}
	e.canon = map[string]string{}
	e.stats["syncs"]++
	return e.j.Reset()
}

// trimCache drops cached contents once they pass the cap. Dropping is always
// safe: the next edit rereads the file, and anything unsynced is still on disk
// and in the journal.
func (e *Engine) trimCache() {
	n := 0
	for _, f := range e.files {
		n += len(f.data)
	}
	if n > proto.MaxCacheBytes {
		e.files = map[string]*file{}
		e.stats["cache_drops"]++
	}
}

func fsync(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (e *Engine) checkLease(p, id string, r proto.Request) (proto.Response, bool) {
	l := e.leases[id]
	if l != nil && time.Now().After(l.expires) {
		delete(e.leases, id)
		l = nil
	}
	switch {
	case l != nil && r.Token != l.token:
		return proto.Response{Code: proto.ExitLease, Error: p + " is leased by " + l.agent}, false
	case l == nil && r.Token != 0:
		return proto.Response{Code: proto.ExitLease, Error: "lease token for " + p + " is expired or released"}, false
	}
	return proto.Response{}, true
}

func (e *Engine) acquire(r proto.Request) proto.Response {
	p, id, err := e.clean(r.Path)
	if err != nil {
		return usage(err)
	}
	if r.Agent == "" {
		return proto.Response{Code: proto.ExitUsage, Error: "lease needs --agent"}
	}
	ttl := time.Duration(r.TTLms) * time.Millisecond
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	now := time.Now()
	if l := e.leases[id]; l != nil && now.Before(l.expires) && l.agent != r.Agent {
		return proto.Response{Code: proto.ExitLease, Error: p + " is leased by " + l.agent}
	}
	if _, held := e.leases[id]; !held && len(e.leases) >= proto.MaxLeases {
		for k, l := range e.leases {
			if now.After(l.expires) {
				delete(e.leases, k)
			}
		}
		if len(e.leases) >= proto.MaxLeases {
			return proto.Response{Code: proto.ExitBusy, Error: "too many active leases; retry later"}
		}
	}
	e.token++
	e.leases[id] = &lease{agent: r.Agent, token: e.token, expires: now.Add(ttl)}
	e.stats["leases"]++
	return proto.Response{Token: e.token}
}

func (e *Engine) release(r proto.Request) proto.Response {
	p, id, err := e.clean(r.Path)
	if err != nil {
		return usage(err)
	}
	if l := e.leases[id]; l != nil && l.token == r.Token {
		delete(e.leases, id)
		return proto.Response{}
	}
	return proto.Response{Code: proto.ExitLease, Error: "token does not hold " + p}
}

func (e *Engine) checkpoint(msg string) proto.Response {
	if err := e.syncAll(); err != nil {
		return fail(err)
	}
	if msg == "" {
		msg = "avc checkpoint"
	}
	if out, err := exec.Command("git", "-C", e.root, "add", "-A").CombinedOutput(); err != nil {
		return proto.Response{Code: proto.ExitError, Error: "git add: " + string(out)}
	}
	if exec.Command("git", "-C", e.root, "diff", "--cached", "--quiet").Run() == nil {
		return proto.Response{Info: map[string]any{"committed": false, "reason": "nothing to commit"}}
	}
	out, err := exec.Command("git", "-C", e.root, "commit", "-q", "-m", msg).CombinedOutput()
	if err != nil {
		return proto.Response{Code: proto.ExitError, Error: "git commit: " + string(out)}
	}
	e.stats["checkpoints"]++
	return proto.Response{Output: out}
}

func (e *Engine) snapshot() map[string]any {
	m := map[string]any{}
	for k, v := range e.stats {
		m[k] = v
	}
	m["gen"], m["seq"] = e.gen, e.seq
	m["journal_bytes"] = e.j.Size()
	m["cached_files"] = len(e.files)
	m["active_leases"], m["canon_paths"] = len(e.leases), len(e.canon)
	m["frozen"], m["held"], m["held_bytes"] = e.frozen, len(e.held), e.heldB
	m["case_insensitive"] = e.fold
	if e.stalled != nil {
		m["stalled"] = e.stalled.Error()
	}
	var n int
	for _, f := range e.files {
		n += len(f.data)
	}
	m["cache_bytes"] = n
	if b := e.stats["batches"]; b > 0 {
		m["avg_ops_per_batch"] = float64(e.stats["ok_edit"]+e.stats["ok_write"]+e.stats["rejected_edit"]+e.stats["rejected_write"]) / float64(b)
	}
	return m
}

// clean turns a user path into its on-disk spelling relative to root plus its
// identity key, rejecting anything that could escape the tree or touch
// .git/.avc (in any letter case).
func (e *Engine) clean(p string) (path, id string, err error) {
	if p == "" {
		return "", "", errors.New("missing path")
	}
	if filepath.IsAbs(p) {
		rel, err := filepath.Rel(e.root, p)
		if err != nil {
			return "", "", err
		}
		p = rel
	}
	p = filepath.ToSlash(filepath.Clean(p))
	if p == "." || p == ".." || strings.HasPrefix(p, "../") {
		return "", "", fmt.Errorf("path %q is outside the tree", p)
	}
	parts := strings.Split(p, "/")
	for i, part := range parts {
		if strings.EqualFold(part, ".git") || (i == 0 && strings.EqualFold(part, ".avc")) {
			return "", "", fmt.Errorf("path %q is reserved", p)
		}
	}
	id = e.identity(p)
	if c, ok := e.canon[id]; ok {
		return c, id, nil
	}
	cur := e.root
	for i, part := range parts {
		if e.fold {
			parts[i] = spelling(cur, part)
		}
		cur = filepath.Join(cur, parts[i])
		st, err := os.Lstat(cur)
		if err != nil {
			break
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return "", "", fmt.Errorf("path %q goes through a symlink", p)
		}
	}
	path = strings.Join(parts, "/")
	if _, err := os.Lstat(filepath.Join(e.root, path)); err == nil && len(e.canon) < proto.MaxCanon {
		e.canon[id] = path // only cache spellings that exist on disk
	}
	return path, id, nil
}

func (e *Engine) identity(p string) string {
	if e.fold {
		return strings.ToLower(p)
	}
	return p
}

// spelling returns the existing entry in dir that matches name ignoring case.
func spelling(dir, name string) string {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return name
	}
	for _, ent := range ents {
		if ent.Name() == name {
			return name
		}
	}
	for _, ent := range ents {
		if strings.EqualFold(ent.Name(), name) {
			return ent.Name()
		}
	}
	return name
}

func caseInsensitive(dir string) bool {
	a, err1 := os.Stat(dir)
	b, err2 := os.Stat(filepath.Join(filepath.Dir(dir), strings.ToUpper(filepath.Base(dir))))
	return err1 == nil && err2 == nil && os.SameFile(a, b)
}

func bootEpoch() uint64 {
	var b [4]byte
	rand.Read(b[:])
	return uint64(binary.BigEndian.Uint32(b[:])) << 32
}

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func usage(err error) proto.Response {
	return proto.Response{Code: proto.ExitUsage, Error: err.Error()}
}

func fail(err error) proto.Response {
	return proto.Response{Code: proto.ExitError, Error: err.Error()}
}

func (e *Engine) result(err error) proto.Response {
	if err != nil {
		return fail(err)
	}
	return proto.Response{}
}
