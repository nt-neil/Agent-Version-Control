# Dogfood log

Friction found while developing avc with avc. Newest last. Each entry gives the
date, what you tried, what happened and what would have helped.

- 2026-10-20 · Full rewrite with `--base` needs `avc cat PATH | shasum -a 256 | cut ...`. That is clumsy and easy to get wrong. `avc cat --sha` (print only the hash) would help. **Fixed:** `avc cat PATH --sha`.
- 2026-10-20 · Appending to the end of a file (like this log) needs an `--old` anchor on the last line. That anchor changes after every append, so two agents logging at once conflict. An `avc append PATH` op would help. **Fixed:** `avc append`.
- 2026-10-20 · Claude Code's own Edit/Write tools cannot go through avc. The agent has to use Bash, which loses Edit's diff view and the read-before-write safety. A Claude Code hook (or MCP tool) that routes Edit through `avc edit` would help. **Fixed:** `avc hook claude` in `.claude/settings.json`.

- 2026-10-20 · Light commands (`gofmt -l`, `go vet`) wait behind the load gate like full test suites: with load around 35, each waited the 60 s cap. A way to mark a job light (or gate only heavy ones) would help. **Fixed:** `avc run --light` skips the load gate and the main queue, with its own small cap.
- 2026-10-20 · There is no `avc rm`. A test file created through avc had to be deleted directly, behind the daemon's back. A delete op would help. **Fixed:** `avc rm`, journaled and replayed like any other change.
- 2026-10-20 · A file written through avc and then deleted directly (no `avc rm`) made every later flush fail, so `avc checkpoint` was stuck. **Fixed:** sync skips files deleted since they were written. The missing delete op is fixed too (`avc rm`).
- 2026-10-20 · After the hook applies an Edit through avc, Claude Code sees the file as "modified since read" and refuses the next Edit to the same file until it is read again. **Fixed:** the hook's success message now says to read the file again before editing it further.
