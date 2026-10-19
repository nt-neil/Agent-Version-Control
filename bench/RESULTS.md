# Results

Raw data: [`results/batch-100/results.json`](results/batch-100/results.json) and
[`results.csv`](results/batch-100/results.csv). The table below shows the
median of 3 rounds; every round of every mode passed the exact-content check.

**Machine:** Apple silicon (arm64), 10 logical CPUs, macOS · Git 2.51.2 · Go 1.27.1 ·
2026-10-19. The machine was also running other agents (load average around 20–27),
so treat absolute times as indicative.

## Edit suite: 100 agents, one shared file, disjoint edits

| Mode | Total | Edit phase | Integration | P95 per agent | Tasks/s | Commits | Disk (logical) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| native Git, one commit at the end | 6.67 s | 6.45 s | 0.07 s | 6.11 s | 15.0 | 1 | 45 KB |
| tuned Git (fsmonitor, untracked cache, split index), one commit | 15.46 s | 9.02 s | 0.10 s | 8.59 s | 6.5 | 1 | 46 KB |
| Git worktree per agent + integration | 15.60 s | 2.49 s | 4.62 s | 2.47 s | 6.4 | 200 | 1.99 MB |
| **avc**, one `avc checkpoint` | **0.69 s** | **0.28 s** | 0.15 s | **0.27 s** | **145.9** | 1 | 45 KB |

Tuned Git: `fsmonitor` daemon started and `core.fsmonitor=true`, split index
and untracked cache enabled. Scalar was **not tested**, because it needs a global
registration we chose not to make.

How to read this:

- `avc`, native Git (batch) and tuned Git all end with **one commit** and the
  same file, so they leave the same history. Worktrees leave 200 commits and
  about 44× the disk.
- An `avc` edit is acknowledged once its batch is in an `fsync`ed journal. A
  native-Git edit here is a locked file write; the Git commit happens at the
  end. Durability points differ, so we do not quote a speed-up factor.
- Tuned Git was *slower* than plain Git on this tiny fixture. Its features pay
  off on large trees, not on 100 edits to one small file.
- Agents are simulated CLI clients, not real models or IDEs.

## Resource suite: three "test suites" starting at once

Each suite is a bounded synthetic job: 16 MiB buffer, 32 SHA-256 passes, at
least 300 ms long. The daemon ran with `--max-jobs 1`. Load and memory gates were
turned off (`--max-load 100000 --min-free-mb 1`) so unrelated machine load would
not delay jobs. This suite measures admission and sharing, not the adaptive gates.

| Group | Total | Executions | Peak RSS (process tree) | CPU seconds |
| --- | ---: | ---: | ---: | ---: |
| direct: all 3 start at once | 0.47 s | 3 of 3 | 67.8 MB | 1.01 |
| `avc run`: one at a time | 1.42 s | 3 of 3 | 50.3 MB | 1.05 |
| `avc run --share`: identical runs on the same tree version | 0.66 s | **1** of 1 | 49.5 MB | **0.39** |

How to read this:

- Queuing trades time for a lower peak. Running one at a time took longer
  (1.42 s vs 0.47 s) but cut peak memory by about a quarter. This is a small
  synthetic job. The load-67 incident in the README is a separate user
  report, not reproduced or measured here.
- `--share` ran the job once for all three callers, using about 60% less CPU
  than the direct group.
- RSS and CPU come from `ps` samples every 50 ms, so they are lower bounds, and
  RSS counts shared pages more than once. The harness did not sample swap,
  system load or physical disk I/O.

## Regression on the release candidate

> The performance numbers in this file were measured on binaries `8bd5759b…`
> (first table) and `6db3af66…` (regression). Later commits only tightened
> security and snapshot cleanup, and were checked with the acceptance suite
> below, not re-benchmarked.

After the review fixes (live-request budget, `--snapshot`, journal fail-closed
paths, shutdown), the `avc` mode and the resource suite ran again on one fixed
binary (sha256 `6db3af66…`): 3 rounds, all correct. Raw data is in
[`results/final-avc-100/`](results/final-avc-100/).

| | Total | P95 per agent | Tasks/s |
| --- | ---: | ---: | ---: |
| avc, 100 agents (median) | 0.46 s | 0.19 s | 217.6 |

| Resource group | Total | Executions | Peak RSS | CPU seconds |
| --- | ---: | ---: | ---: | ---: |
| direct | 0.37 s | 3 | 68.1 MB | 0.99 |
| `avc run` | 1.31 s | 3 | 50.4 MB | 1.13 |
| `avc run --share` | 0.47 s | 1 | 50.6 MB | 0.42 |

The pattern matches the first run: queuing lowered peak RSS by about a
quarter, and `--share` meant one execution and about 60% less CPU.

## Acceptance checks

`python3 bench/acceptance_smoke.py` uses `bin/avc` and throwaway fixtures in
`/tmp`. It was last run on the published binary (sha256 `2960db0e…`). The
script checks that the binary does not change during the run. It recorded the
following in
[`results/acceptance-smoke.json`](results/acceptance-smoke.json):

| Check | Result |
| --- | --- |
| `--snapshot` job while another agent edits | job read `v1`, the edit landed (`v2`), no snapshot left behind |
| Acknowledged write, then `SIGKILL` before the journal was emptied (3 rounds) | the edit was recovered on restart every time. Process crash only, not power loss |
| Load gate, forced by setting `--max-load` to 0.000001, with no job running | the first job waited instead of starting. This checks the gate's wiring, not how it judges a really busy machine |
| `shutdown` with 33 connections that sent half a JSON request | daemon exited in 9.37 ms |
| Daemon killed (`SIGKILL`) while a `--snapshot` job held a clone | the restart removed the leftover clone |
| Job left a read-only (`0555`) directory inside its snapshot | the clone was still removed |
| Clone made undeletable (macOS immutable flag) | later `--snapshot` runs and restarts were refused while managed edits kept working; everything recovered once the flag was cleared |
| `.avc` or `.avc/snaps` is a symlink to a directory outside the repo | the daemon refused to start before any I/O there; the outside files kept their content and modes |

## Not covered yet

- Real projects with `node_modules`, language servers and full test suites.
- Physical power loss and storage faults (failed `fsync` or writes) were not
  injected. Process-crash recovery is covered by the `SIGKILL` check above.
- More rounds, other machines, Linux.
