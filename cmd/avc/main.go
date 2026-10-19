// Command avc is the client and daemon for Agent Version Control.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nt-neil/Agent-Version-Control/internal/govern"
	"github.com/nt-neil/Agent-Version-Control/internal/proto"
	"github.com/nt-neil/Agent-Version-Control/internal/server"
)

const usageText = `avc — version coordination for many AI agents on one working tree

  avc daemon [--root DIR] [--max-jobs N] [--workers N] [--nice N] [--max-load F] [--min-free-mb N]
  avc edit PATH --old STR --new STR [--token T]      exit 3 = conflict
  avc write PATH (--file SRC | --stdin) [--base SHA|absent] [--token T]
  avc cat PATH
  avc lease acquire PATH --agent ID [--ttl 30s]      prints token
  avc lease release PATH --token T
  avc run [--snapshot | --freeze | --share] [--name N] -- CMD...
  avc checkpoint -m MSG
  avc flush | stats | shutdown

Exit codes: 0 ok, 1 error, 2 usage, 3 conflict, 4 lease refused, 5 changed outside avc, 6 busy (retry).
The client finds the daemon by walking up from the current directory, or AVC_ROOT.`

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, usageText)
		return proto.ExitUsage
	}
	cmd, args := args[0], args[1:]
	switch cmd {
	case "daemon":
		return daemon(args)
	case "edit":
		fs, path, a := parse("edit", args, true)
		old := fs.String("old", "", "text to replace (must occur exactly once)")
		nw := fs.String("new", "", "replacement text")
		tok := fs.Uint64("token", 0, "lease token")
		if !finish(fs, path, a) {
			return proto.ExitUsage
		}
		return call(proto.Request{Op: "edit", Path: *path, Old: *old, New: *nw, Token: *tok}, false)
	case "write":
		fs, path, a := parse("write", args, true)
		src := fs.String("file", "", "read content from file")
		stdin := fs.Bool("stdin", false, "read content from stdin")
		base := fs.String("base", "", "expected sha256 of the current content, or 'absent'")
		tok := fs.Uint64("token", 0, "lease token")
		if !finish(fs, path, a) {
			return proto.ExitUsage
		}
		var data []byte
		var err error
		switch {
		case *src != "":
			data, err = os.ReadFile(*src)
		case *stdin:
			data, err = readAll(os.Stdin)
		default:
			err = errors.New("write needs --file or --stdin")
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "avc:", err)
			return proto.ExitUsage
		}
		return call(proto.Request{Op: "write", Path: *path, Content: data, Base: *base, Token: *tok}, false)
	case "cat":
		fs, path, a := parse("cat", args, true)
		if !finish(fs, path, a) {
			return proto.ExitUsage
		}
		return call(proto.Request{Op: "cat", Path: *path}, true)
	case "lease":
		if len(args) < 1 || (args[0] != "acquire" && args[0] != "release") {
			fmt.Fprintln(os.Stderr, "usage: avc lease acquire|release PATH ...")
			return proto.ExitUsage
		}
		sub := args[0]
		fs, path, a := parse("lease "+sub, args[1:], true)
		agent := fs.String("agent", "", "agent id")
		ttl := fs.Duration("ttl", 30*time.Second, "lease duration")
		tok := fs.Uint64("token", 0, "token to release")
		if !finish(fs, path, a) {
			return proto.ExitUsage
		}
		if sub == "acquire" {
			return call(proto.Request{Op: "lease", Path: *path, Agent: *agent, TTLms: ttl.Milliseconds()}, false)
		}
		return call(proto.Request{Op: "release", Path: *path, Token: *tok}, false)
	case "run":
		i := indexOf(args, "--")
		if i < 0 || i == len(args)-1 {
			fmt.Fprintln(os.Stderr, "usage: avc run [--snapshot | --freeze] [--share] [--name N] -- CMD...")
			return proto.ExitUsage
		}
		fs := flag.NewFlagSet("run", flag.ContinueOnError)
		share := fs.Bool("share", false, "let identical concurrent runs on the same tree version share one execution")
		freeze := fs.Bool("freeze", false, "pause integration while the command runs")
		snapshot := fs.Bool("snapshot", false, "run in a cloned snapshot of the tree (no pause for other agents)")
		name := fs.String("name", "", "label for stats")
		if fs.Parse(args[:i]) != nil {
			return proto.ExitUsage
		}
		wd, _ := os.Getwd()
		return call(proto.Request{Op: "run", Argv: args[i+1:], Dir: wd, Env: os.Environ(), Share: *share, Freeze: *freeze, Snapshot: *snapshot, Message: *name}, false)
	case "checkpoint":
		fs := flag.NewFlagSet("checkpoint", flag.ContinueOnError)
		msg := fs.String("m", "", "commit message")
		if fs.Parse(args) != nil {
			return proto.ExitUsage
		}
		return call(proto.Request{Op: "checkpoint", Message: *msg}, false)
	case "flush", "stats", "shutdown":
		return call(proto.Request{Op: cmd}, false)
	case "help", "-h", "--help":
		fmt.Println(usageText)
		return 0
	}
	fmt.Fprintln(os.Stderr, usageText)
	return proto.ExitUsage
}

// parse lets PATH appear before or after flags; finish fills it in.
func parse(name string, args []string, wantPath bool) (*flag.FlagSet, *string, []string) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	path := new(string)
	if wantPath && len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		*path, args = args[0], args[1:]
	}
	return fs, path, args
}

func finish(fs *flag.FlagSet, path *string, rest []string) bool {
	if err := fs.Parse(rest); err != nil {
		return false
	}
	if *path == "" && fs.NArg() > 0 {
		*path = fs.Arg(0)
	}
	if *path == "" {
		fmt.Fprintln(os.Stderr, "avc: missing PATH")
		return false
	}
	return true
}

func daemon(args []string) int {
	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)
	wd, _ := os.Getwd()
	root := fs.String("root", wd, "working tree to coordinate")
	maxJobs := fs.Int("max-jobs", 1, "expensive jobs allowed to run at once")
	workers := fs.Int("workers", 0, "worker budget hinted to each job (0 = NumCPU/2)")
	nice := fs.Int("nice", 10, "nice value for jobs (0 = unchanged)")
	maxLoad := fs.Float64("max-load", 0, "hold new jobs while the 1-minute load is above this (0 = NumCPU*1.5)")
	minFree := fs.Int("min-free-mb", 0, "hold new jobs while available memory is below this (0 = 1024)")
	if fs.Parse(args) != nil {
		return proto.ExitUsage
	}
	s, err := server.Listen(*root, govern.Config{MaxJobs: *maxJobs, Workers: *workers, Nice: *nice, MaxLoad: *maxLoad, MinFreeMemMB: *minFree})
	if err != nil {
		fmt.Fprintln(os.Stderr, "avc daemon:", err)
		return proto.ExitError
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; s.Close() }()
	fmt.Fprintln(os.Stderr, "avc daemon: serving", proto.SocketPath(*root))
	if err := s.Serve(); err != nil {
		fmt.Fprintln(os.Stderr, "avc daemon:", err)
		return proto.ExitError
	}
	return 0
}

func call(req proto.Request, raw bool) int {
	root := os.Getenv("AVC_ROOT")
	if root != "" {
		root = proto.Canonical(root)
	} else {
		wd, _ := os.Getwd()
		var ok bool
		if root, ok = proto.FindRoot(wd); !ok {
			fmt.Fprintln(os.Stderr, "avc: no daemon found (start one with `avc daemon` at the repo root)")
			return proto.ExitError
		}
	}
	if req.Op == "edit" || req.Op == "write" || req.Op == "cat" || req.Op == "lease" || req.Op == "release" {
		req.Agent = firstNonEmpty(req.Agent, os.Getenv("AVC_AGENT"))
		if wd, err := os.Getwd(); err == nil && !strings.HasPrefix(req.Path, "/") {
			req.Path = proto.Canonical(wd) + "/" + req.Path // engine relativizes against its root
		}
	}
	if err := proto.CheckSocket(proto.SocketPath(root)); err != nil {
		fmt.Fprintln(os.Stderr, "avc: refusing daemon socket:", err)
		return proto.ExitError
	}
	c, err := net.Dial("unix", proto.SocketPath(root))
	if err != nil {
		fmt.Fprintln(os.Stderr, "avc:", err)
		return proto.ExitError
	}
	defer c.Close()
	if err := json.NewEncoder(c).Encode(req); err != nil {
		fmt.Fprintln(os.Stderr, "avc:", err)
		return proto.ExitError
	}
	var resp proto.Response
	if err := json.NewDecoder(c).Decode(&resp); err != nil {
		if req.Op == "shutdown" {
			return 0
		}
		fmt.Fprintln(os.Stderr, "avc:", err)
		return proto.ExitError
	}
	switch {
	case req.Op == "run":
		os.Stdout.Write(resp.Output)
		if resp.Info["tree_changed"] == true {
			fmt.Fprintln(os.Stderr, "avc: warning: the tree changed while the command ran (use --snapshot or --freeze for a stable version)")
		}
	case raw:
		os.Stdout.Write(resp.Content)
	case resp.Token != 0:
		fmt.Println(resp.Token)
	case resp.Info != nil:
		b, _ := json.MarshalIndent(resp.Info, "", "  ")
		fmt.Println(string(b))
	case len(resp.Output) > 0:
		os.Stdout.Write(resp.Output)
	case resp.SHA != "":
		fmt.Println(resp.SHA)
	}
	if resp.Error != "" {
		fmt.Fprintln(os.Stderr, "avc:", resp.Error)
	}
	return resp.Code
}

func readAll(f *os.File) ([]byte, error) {
	var b strings.Builder
	buf := make([]byte, 64<<10)
	for {
		n, err := f.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			if err.Error() == "EOF" {
				return []byte(b.String()), nil
			}
			return nil, err
		}
	}
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
