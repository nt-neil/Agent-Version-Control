# Dogfood log

Friction found while developing avc with avc. Newest last. Each entry gives the
date, what you tried, what happened and what would have helped.

- 2026-10-20 · Full rewrite with `--base` needs `avc cat PATH | shasum -a 256 | cut ...`. That is clumsy and easy to get wrong. `avc cat --sha` (print only the hash) would help.
- 2026-10-20 · Appending to the end of a file (like this log) needs an `--old` anchor on the last line. That anchor changes after every append, so two agents logging at once conflict. An `avc append PATH` op would help.
- 2026-10-20 · Claude Code's own Edit/Write tools cannot go through avc. The agent has to use Bash, which loses Edit's diff view and the read-before-write safety. A Claude Code hook (or MCP tool) that routes Edit through `avc edit` would help.

