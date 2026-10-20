# Working on this repository (for AI agents)

This repo develops `avc` **with `avc`**, the way Git was developed with Git.
Every agent working here (Claude Code, Codex, others) follows these rules.

## 1. Start or find the daemon

```sh
go build -o bin/avc ./cmd/avc        # if bin/avc is missing or stale
bin/avc stats >/dev/null 2>&1 || bin/avc daemon --root . &
```

One daemon serves every agent; never start a second one (it will refuse).
Stop it only if you started it: `bin/avc shutdown`.

**Changing the daemon itself:** after rebuilding, the running daemon is still
the old binary. Restart it only when no other agent is mid-task:
`bin/avc shutdown`, then start it again.

## 2. Change files through avc, not directly

**Claude Code:** `.claude/settings.json` routes your Edit and Write tools
through avc. Use them normally while the daemon runs. A successful change comes
back as a blocked tool call that says "Applied through avc", which means it
landed. Do not retry it. `replace_all` and MultiEdit are refused; make one
unique edit at a time. The hook only acts in `acceptEdits` or
`bypassPermissions` mode, and never on `.claude/` or `.github/`. In other
modes your edits go through Claude Code's normal approval and land directly.

**Other agents and shell use:**

```sh
bin/avc edit PATH --old 'exact text' --new 'replacement'        # most changes
bin/avc append PATH --text '...'                                # add to the end; never conflicts
bin/avc write PATH --stdin --base absent < new-file             # new file
bin/avc write PATH --stdin --base "$(bin/avc cat PATH --sha)" < rewritten   # full rewrite
bin/avc cat PATH                                                # read the latest version
bin/avc rm PATH                                                 # delete a file
```

- `--old` must match exactly once. Include enough surrounding lines.
- Exit `3` (conflict): someone changed that text. Run `avc cat`, then redo the edit on the new content.
- Exit `5` (changed outside avc): someone bypassed avc. Re-read and redo. Then note it in `DOGFOOD.md`.
- Exit `6` (busy): wait a moment and retry.
- Lost the reply (timeout, crash)? Do **not** resend blindly. `avc cat` first and check.

For a multi-step change to one file that nobody else should touch meanwhile:

```sh
T=$(bin/avc lease acquire PATH --agent "$AVC_AGENT" --ttl 2m)
bin/avc edit PATH --old ... --new ... --token "$T"
bin/avc lease release PATH --token "$T"
```

## 3. Run builds and tests through avc

```sh
bin/avc run --light -- go vet ./...                # quick commands skip the load gate
bin/avc run --snapshot -- go test ./... -count=1   # runs on a stable copy; others keep editing
```

Never run test suites directly. Development comes first. Run only the tests
related to your change (`-run`) while developing. Run the full suite once your
batch of work is done, one suite at a time. `avc run` enforces the queue.

## 4. Commit with checkpoint

```sh
bin/avc checkpoint -m "area: what changed

Co-Authored-By: <your model> <noreply@...>"
```

`checkpoint` commits **everything** in the tree, other agents' finished edits
included. Agree on who checkpoints. When several agents work at once, one
coordinator does it. Push with plain `git push` as usual.

## 5. Improve avc yourself

This repository improves itself by using itself. The loop:

1. **Notice.** Each time avc gets in your way, add one line to `DOGFOOD.md`
   with `avc append`: the date, what you tried, what happened and what would
   have helped. Examples: a confusing message, a missing command, a retry
   loop, a step you had to do by hand, a bug.
2. **Fix it now.** Do not wait to be asked. Bugs, open `DOGFOOD.md` entries
   and review findings are yours to fix in the same session. Write a
   regression test, run the tests through `avc run`, mark the entry
   `**Fixed:** …`, `avc checkpoint`, `git push`.
3. **Ask only when the direction changes.** That means a new product
   direction or scope, an architecture change, work in another repository, or
   anything destructive. Everything else, just do and report.
