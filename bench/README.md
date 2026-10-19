# Benchmarks

`bench` builds a throwaway fixture repository for every round and never touches
the repository it is run from. It also clears `GIT_*` variables and points
`AVC_ROOT` at the fixture. On exit or `SIGTERM` it kills only the processes it
started itself.

```sh
go build -o bin/avc ./cmd/avc
go run ./bench --avc ./bin/avc --agents 10 --rounds 1 --out ./bench/out   # start small
go run ./bench --avc ./bin/avc --agents 100 --rounds 3 --suite edits
go run ./bench --avc ./bin/avc --suite resources --rounds 3
```

Process-level acceptance checks (snapshot isolation, crash recovery, load gate,
shutdown with stalled clients):

```sh
python3 bench/acceptance_smoke.py
```

Run **one benchmark at a time** and start with few agents. The resource suite
deliberately loads the machine.

## Edit suite: N agents editing one shared file

Every agent replaces its own line in `shared.txt` at the fixture root (a disjoint edit to the
same file). At the end the harness checks the file's exact content: every line
must be present exactly once.

| Mode | What each agent does | History left behind |
| --- | --- | --- |
| `native` | Locked edit in one shared Git tree; `--commit-policy batch` (one commit at the end) or `per-agent` (one commit per edit) | per policy |
| `worktree` | Own `git worktree` + branch, commits, then everything is integrated into `main` | one commit per agent plus integration |
| `optimized` | Same as `native`, with Git tuning enabled where the machine supports it (`core.fsmonitor`, `core.untrackedCache`, split index). Each feature's actual state is written into the results; Scalar is **not** registered and is reported as not tested | per policy |
| `avc` | `avc edit` against one daemon, then a single `avc checkpoint` | one commit |

Reported per round: setup / work / integration / total time, tasks per second,
P50/P95/P99 latency per agent, logical disk bytes, commit count, integration
conflicts, plus `avc stats`. CPU and RSS are not measured for this suite (the
fields are `null`); only the resource suite samples them.

**These modes do not give equal guarantees.** They differ in how much history
they keep and in when an edit counts as durable, so the harness does not compute
a speed-up factor. Compare `avc` with `native --commit-policy batch` for "same
history", and with `per-agent` for "one commit per edit".

## Resource suite: several test suites starting at once

Each "suite" is a bounded worker: it allocates `--worker-mib` MiB, runs a fixed
number of SHA-256 passes (`--worker-hashes`) and lives at least `--worker-ms`.
Every *worker* does the same work, so differences between groups come from how
many workers actually run and when. `share` runs one worker where `direct` runs
three. Groups:

- **direct**: all workers start at once, as when agents run their tests directly.
- **scheduled**: every worker goes through `avc run` (default `--max-jobs 1`).
- **share**: identical runs go through `avc run --share`, so they should execute once.

Reported: total time, actual vs expected worker executions, peak concurrent
workers, CPU seconds and peak RSS.

## Reading the numbers

- Simulated agents, not real models, IDEs or language servers.
- One machine, a few rounds. Report hardware, OS, Git and Go versions with any numbers.
- `avc` is fast here partly because it writes less history. That trade-off is
  the point of the design, not a hidden cost; see "Contract and limits" in the
  top-level README.

Results: [RESULTS.md](RESULTS.md).
