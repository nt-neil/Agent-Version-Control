# Agent-Version-Control (`avc`)

## Why this exists

> 因為我用 AI agent 去做 coding 很容易會衝爆我的 Mac 的記憶體與 CPU。
>
> *I use AI agents to write code, and they keep maxing out my Mac's memory and CPU.*

On 2026-10-19 three agents each started a full test suite at the same time on a
10-core Mac. The load average hit 67, free memory fell to 86 MB, and each suite
went from about 10 minutes to 30–40.

Git works well for people. The problem is many agents working at once. Common
setups give each agent its own worktree, dependencies and build/test process,
so every extra agent adds another copy of all of them. I wanted something built
for agents: as fast as possible, with a lock you can turn on, so agent activity
doesn't keep loading up the machine. I also didn't want `.git`'s habit of piling
up objects and snapshots forever.

`avc` is that experiment. It is a small, dependency-free daemon that lets many
agents share **one** working tree. It also queues their expensive jobs so they
can't all start at once.

## What it does

| Problem | What `avc` does |
| --- | --- |
| One worktree / `node_modules` per agent | All agents edit **one shared tree** through a single daemon. Nothing is copied per agent. |
| Agents colliding on the same file | **Anchored edits** (`old → new`, the same shape as an editor "replace") are applied in arrival order. Edits to different parts of one file all land. Two edits to the same text: one wins, the other gets exit code `3` and can retry against fresh content. |
| Every edit paying for its own disk sync | **Group commit.** Everything that arrives together becomes one batch: one journal `fsync`, one write per touched file (not yet `fsync`ed; the journal covers it until a later flush), then every edit in the batch is acknowledged. |
| History piling up | The journal stores only the *after-image* of in-flight batches. It is emptied once those files are durable, so storage tracks current work and never grows with time. Git is only an export: `avc checkpoint` makes one commit when you want one. |
| Exclusive access when needed | File **leases** with TTL and fencing tokens. Once a file is leased, edits without the current token are refused (`4`), including old tokens from before a restart. |
| N agents × full test suite melting the machine | `avc run -- CMD` sends builds and tests through one **governor**. It allows `--max-jobs` at once (default **1**), holds new starts while load or free memory is past a threshold (capped at 60 s when nothing of ours is running), gives each job a worker budget (`AVC_WORKERS`, `MAKEFLAGS`, `GOMAXPROCS`, `JEST_WORKERS`) and runs jobs at `nice 10`. With `--share`, identical runs on the same tree version execute **once** and all callers get the result. |
| Tests needing a stable tree while 99 other agents keep editing | `avc run --snapshot` clones the tree once the job is admitted: APFS `clonefile` via `cp -c`, or reflinks on Linux. The job runs in the clone and the clone is deleted afterwards. Editing pauses only while the clone is made, and there are never more clones than `--max-jobs`. |

## Quick start

```sh
go build -o bin/avc ./cmd/avc          # Go 1.23+, no dependencies
export PATH="$PWD/bin:$PATH"           # or: go install ./cmd/avc
avc daemon --root /path/to/repo &      # one per repository

cd /path/to/repo
avc edit src/app.ts --old 'retries = 3' --new 'retries = 5'
avc cat src/app.ts
T=$(avc lease acquire src/db.ts --agent agent-7 --ttl 30s)
avc write src/db.ts --file /tmp/db.ts --token "$T"
avc lease release src/db.ts --token "$T"

avc run --snapshot -- npm test         # queued, budgeted, runs in a private clone
avc run --share -- npm run build       # identical concurrent builds run once
avc checkpoint -m "agents: batch 12"   # one git commit for everything so far
avc stats                              # JSON counters
```

Exit codes: `0` ok · `1` error · `2` usage / rejected path · `3` conflict (anchor
missing or ambiguous, `--base` mismatch) · `4` lease refused · `5` file changed
outside `avc` (the daemon reloaded it; retry) · `6` busy, a hard limit was hit
(retry). `avc run` passes through the command's own exit code.

## Contract and limits (read before relying on it)

- **Managed writes only.** Guarantees cover writes that go through `avc`. A
  process that writes files directly is not stopped by leases. Drift detection
  is best-effort: while a file is cached, a changed size or mtime gets `5`
  instead of an overwrite. Once the cache is dropped, the next edit simply
  reads the new content. Anchored edits then still apply only on top of what
  is really on disk, but an unconditional `write` without `--base` replaces
  it.
- **Durability.** An acknowledged edit is in the `fsync`ed journal. After a crash
  the daemon replays batches in order on startup. Replaying is idempotent
  because each record is the file's full new content. Durability is only as
  good as `fsync` on your OS, filesystem and disk. The tests cover replay and
  torn journal tails, but not real power loss or fault injection.
- **No exactly-once requests.** There is no request id. A client that loses
  the reply cannot tell whether the edit landed, so it must read the file back
  (`avc cat`) and check before retrying. Do not blindly resend: if the new text
  still contains the anchor, the edit would apply twice.
- **No multi-file transactions.** A batch that touches several files is replayed
  as a unit after a crash. While the daemon runs, though, another process can
  see some of those files updated before others.
- **No per-edit history.** Before `avc checkpoint` there is nothing to roll back
  to except the files themselves. This is the trade for bounded storage.
- **Stable versions for jobs.** A plain `avc run` sees every acknowledged edit,
  but agents keep editing while it runs. The CLI warns when that happened
  (`tree_changed`). `--snapshot` runs the job in a private clone taken at
  one accepted version. It needs a copy-on-write filesystem (APFS, or reflinks
  on Btrfs/XFS) and fails instead of doing a full copy. It copies everything
  except `.avc/`, `.git` and `node_modules` included (cheap when cloned), and
  gives up after 2 minutes. Clones are removed even if the job made them
  read-only. If one still cannot be removed, `--snapshot` is refused until the
  daemon restarts, and a restart that cannot clear `.avc/snaps` will not
  start. It cannot yet be combined with `--share` or
  `--freeze`. `--freeze`
  instead pauses managed edits for the whole run; paused edits wait in the
  queue and are not lost. `--share` implies `--freeze`, so
  sharers really did see the same files. Edits made outside `avc` during a run
  are not detected.
- **Hard limits** (`internal/proto`): 8 MiB per request (about 6 MiB of file
  content; larger files must be written outside `avc`), 32 requests read at
  once and 512 connections alive at once (the rest wait in the socket backlog),
  128 MiB of request data held by live connections, 64 queued or running
  `avc run` jobs (so edits always have room), 65,536 active leases (expired
  ones are purged when the table is full), 4 KiB path / agent / message
  fields, 4096 queued edits / 64 MiB while frozen, 256 MiB journal, and
  256 MiB file cache, which is dropped (never lost) past that size. Hitting a
  limit returns `6`. If a durable batch cannot be written into the tree, the
  daemon keeps the journal, refuses edits and applies the batch on restart. It
  never discards an acknowledged edit.
- **`--share` key.** Command, directory, the full effective environment and the
  tree version. Callers whose environments differ never share a result.
- **Soft resource limits.** The governor controls how many jobs *start* and
  hints a worker count. A single job that ignores the hint can still use every
  core. Use OS limits if you need a hard ceiling.
- **Case-insensitive volumes** (macOS default): `Foo.txt` and `foo.txt` are the
  same file for leases and edits. `.git` and `.avc` are reserved in any case.
  Paths through symlinks are refused.
- One daemon per repository, enforced by a lock file in `.avc/`. `.avc/` keeps
  itself out of git.
- **Local security.** The daemon socket is mode `0600`. Its directory (`.avc/`,
  or `/tmp/avc-<uid>/` for deep paths) must belong to you and must not be
  writable by group or others. The client refuses a socket or directory that
  is a symlink or belongs to another user, before sending anything. The daemon
  does not check the connecting process's UID itself; it relies on these file
  permissions.

## Benchmarks

Median of 3 rounds: 100 simulated agents edit one shared file, every result
checked for exact content. Machine: 10-core Apple silicon Mac that was also
running other work.

| | Total | P95 per agent | Commits | Disk |
| --- | ---: | ---: | ---: | ---: |
| native Git (one commit at the end) | 6.67 s | 6.11 s | 1 | 45 KB |
| Git worktree per agent | 15.60 s | 2.47 s | 200 | 1.99 MB |
| **avc** (one checkpoint) | **0.69 s** | **0.27 s** | 1 | 45 KB |

Three identical synthetic "test suites" (16 MiB, fixed hashing work) started
at once. `avc run --share` executed the job **once** instead of three times.
`avc run` (one at a time) lowered the peak. In this synthetic test, sampled
with `ps` every 50 ms, that meant about 60% less CPU and about a quarter lower
peak RSS.

See [`bench/`](bench/) and [`bench/RESULTS.md`](bench/RESULTS.md). The harness
compares native Git, Git worktrees, tuned Git and `avc` on 10–100 agents editing
one file, with correctness checks. It also has a resource scenario where several
"test suites" start at once, run directly or through `avc run`. The numbers are
measured on one machine. Read them with the caveats listed there; they are not
a claim of a general speed-up over Git.

## Layout

```
cmd/avc            CLI and daemon entry point
internal/engine    shared tree, batches, anchored edits, leases, checkpoint
internal/journal   bounded write-ahead log of batch after-images
internal/govern    job admission control, worker budgets, --share
internal/server    Unix-socket server
bench/             reproducible comparisons
```

## License

Apache 2.0. See [LICENSE](LICENSE). Contributions welcome; see
[CONTRIBUTING.md](CONTRIBUTING.md).
