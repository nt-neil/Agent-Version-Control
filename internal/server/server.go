// Package server exposes the engine and the job governor on a Unix socket.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/nt-neil/Agent-Version-Control/internal/engine"
	"github.com/nt-neil/Agent-Version-Control/internal/govern"
	"github.com/nt-neil/Agent-Version-Control/internal/proto"
)

type Server struct {
	eng  *engine.Engine
	gov  *govern.Governor
	ln   net.Listener
	wg   sync.WaitGroup
	sem  chan struct{} // decoding
	live chan struct{} // whole connection lifetime

	ctx    context.Context // cancelled by Close: stops admission and running jobs
	cancel context.CancelFunc

	mu        sync.Mutex
	snapErr   error // set when a snapshot could not be removed
	liveBytes int
	jobs      int
	conns     map[net.Conn]struct{}
}

const readTimeout = 10 * time.Second // a client must send its request this fast

// Listen opens the engine for root and binds its socket. A live daemon on the
// same root is an error; a stale socket file is replaced.
func Listen(root string, cfg govern.Config) (*Server, error) {
	root = proto.Canonical(root)
	// engine.Open takes the repository lock, so only the lock holder ever
	// touches the socket path.
	eng, err := engine.Open(root)
	if err != nil {
		return nil, err
	}
	sock := proto.SocketPath(root)
	if err := removeTree(filepath.Join(root, ".avc", "snaps")); err != nil { // leftovers from a crash
		eng.Close()
		return nil, fmt.Errorf("cannot clear old snapshots: %w", err)
	}
	os.MkdirAll(filepath.Dir(sock), 0o700)
	if err := proto.CheckOwned(filepath.Dir(sock), true); err != nil {
		eng.Close()
		return nil, fmt.Errorf("unsafe socket directory: %w", err)
	}
	os.Remove(sock)
	// Create the socket as 0600 from the start: no window where it follows the
	// umask. Nothing else creates files at this point of startup.
	old := syscall.Umask(0o177)
	ln, err := net.Listen("unix", sock)
	syscall.Umask(old)
	if err != nil {
		eng.Close()
		return nil, err
	}
	// Only the owner may connect; the client also checks owner and mode.
	if err := os.Chmod(sock, 0o600); err != nil {
		ln.Close()
		eng.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{eng: eng, gov: govern.New(cfg), ln: ln, ctx: ctx, cancel: cancel,
		sem: make(chan struct{}, proto.MaxDecoding), live: make(chan struct{}, proto.MaxLive),
		conns: map[net.Conn]struct{}{}}, nil
}

// Serve accepts connections until Close or a shutdown request.
func (s *Server) Serve() error {
	for {
		// Past these limits new clients wait in the kernel's accept backlog.
		err := s.acquire(s.live)
		if err == nil {
			if err = s.acquire(s.sem); err != nil {
				<-s.live
			}
		}
		var c net.Conn
		if err == nil {
			c, err = s.ln.Accept()
		}
		if err != nil {
			s.wg.Wait()
			s.eng.Close()
			if errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.handle(c)
	}
}

// Close stops accepting, cancels queued and running jobs, and closes every
// open connection so Serve can return.
func (s *Server) Close() {
	s.cancel()
	s.ln.Close()
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
}

type counter struct {
	r io.Reader
	n int
}

func (c *counter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func (s *Server) acquire(ch chan struct{}) error {
	select {
	case ch <- struct{}{}:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *Server) reserve(n int, job bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.liveBytes+n > proto.MaxLiveBytes || job && s.jobs >= proto.MaxPendingJobs {
		return false
	}
	s.liveBytes += n
	if job {
		s.jobs++
	}
	return true
}

func (s *Server) release(n int, job bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.liveBytes -= n
	if job {
		s.jobs--
	}
}

func (s *Server) handle(c net.Conn) {
	defer s.wg.Done()
	defer func() { <-s.live }()
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		c.Close()
	}()
	// The slot bounds memory spent decoding requests. It is released once the
	// request is read, so long-running jobs do not hold up other clients.
	var req proto.Request
	c.SetReadDeadline(time.Now().Add(readTimeout))
	wire := &counter{r: io.LimitReader(c, proto.MaxRequestBytes)}
	err := json.NewDecoder(wire).Decode(&req)
	<-s.sem
	c.SetReadDeadline(time.Time{})
	if err != nil {
		json.NewEncoder(c).Encode(proto.Response{Code: proto.ExitUsage, Error: "bad or oversized request"})
		return
	}
	for _, v := range []string{req.Path, req.Agent, req.Message, req.Dir, req.Base, req.Op} {
		if len(v) > proto.MaxNameBytes {
			json.NewEncoder(c).Encode(proto.Response{Code: proto.ExitUsage, Error: "request field too long"})
			return
		}
	}
	// Every live request is charged its wire size (an upper bound on what it
	// holds once decoded) against one budget, so requests parked behind a
	// freeze or a queued job cannot pile up in RAM.
	n := wire.n
	if !s.reserve(n, req.Op == "run") {
		json.NewEncoder(c).Encode(proto.Response{Code: proto.ExitBusy, Error: "daemon is at its pending-request limit; retry later"})
		return
	}
	defer s.release(n, req.Op == "run")
	var resp proto.Response
	switch req.Op {
	case "run":
		resp = s.run(req, c)
	case "stats":
		resp = s.eng.Do(req)
		if resp.Info != nil {
			resp.Info["governor"] = s.gov.Stats()
			s.mu.Lock()
			resp.Info["live_request_bytes"], resp.Info["pending_jobs"] = s.liveBytes, s.jobs
			s.mu.Unlock()
		}
	case "shutdown":
		json.NewEncoder(c).Encode(proto.Response{})
		go s.Close()
		return
	default:
		resp = s.eng.Do(req)
	}
	json.NewEncoder(c).Encode(resp)
}

// run executes a command through the governor. Edits already acknowledged are
// on disk, so the job sees them. With Freeze, integration pauses for the
// duration so the job sees one stable version; otherwise the response says
// whether the tree moved underneath it.
func (s *Server) run(req proto.Request, c net.Conn) proto.Response {
	if len(req.Argv) == 0 {
		return proto.Response{Code: proto.ExitUsage, Error: "run needs a command"}
	}
	dir := req.Dir
	if dir == "" {
		dir = s.eng.Root()
	}
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(s.eng.Root(), dir)
	}
	// Cancel the job if the client goes away.
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	go func() {
		buf := make([]byte, 1)
		c.Read(buf)
		cancel()
	}()

	if req.Snapshot && (req.Share || req.Freeze) {
		return proto.Response{Code: proto.ExitUsage, Error: "--snapshot cannot be combined with --share or --freeze yet"}
	}
	// Sharing a result is only safe when every sharer saw the same files.
	if req.Share {
		req.Freeze = true
	}
	startOp := "gen"
	if req.Freeze {
		startOp = "freeze"
	}
	before := s.eng.Do(proto.Request{Op: startOp}).Info["gen"]
	job := govern.Job{
		Name: req.Message, Argv: req.Argv, Dir: dir, Env: req.Env,
		Share: req.Share, ShareKey: toString(before),
	}
	if req.Snapshot {
		job.Prepare = func(ctx context.Context) (string, func(), error) {
			d, gen, cleanup, err := s.snapshot(ctx, dir)
			before = gen // the version the job actually ran against
			return d, cleanup, err
		}
	}
	res := s.gov.Run(ctx, job)
	endOp := "gen"
	if req.Freeze {
		endOp = "unfreeze"
	}
	after := s.eng.Do(proto.Request{Op: endOp}).Info["gen"]

	resp := proto.Response{Code: res.ExitCode, Output: res.Output, Info: map[string]any{
		"queued_ms": res.QueuedMs, "prepare_ms": res.PrepareMs, "run_ms": res.RunMs,
		"coalesced": res.Coalesced, "gen": before, "tree_changed": !req.Snapshot && before != after,
	}}
	if res.Err != "" {
		resp.Error = res.Err
		if resp.Code == 0 {
			resp.Code = proto.ExitError
		}
	}
	return resp
}

func toString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
