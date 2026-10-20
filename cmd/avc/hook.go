package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nt-neil/Agent-Version-Control/internal/proto"
)

// hookClaude is a Claude Code PreToolUse hook. When an avc daemon serves the
// file being edited, it applies Edit and Write through avc and then denies the
// original tool call, telling Claude the change has already landed. Otherwise
// it stays silent and the tool runs normally.
//
// The hook runs before Claude Code's permission check, so it only applies
// changes the user has already pre-approved (acceptEdits or bypassPermissions
// mode). In any other mode the normal permission flow runs and the tool writes
// the file itself; avc picks the change up on its next read. Files that govern
// the agent or CI (.claude/, .github/) are never applied through the hook.
func hookClaude() int {
	var in struct {
		PermissionMode string `json:"permission_mode"`
		ToolName       string `json:"tool_name"`
		ToolInput      struct {
			FilePath   string `json:"file_path"`
			OldString  string `json:"old_string"`
			NewString  string `json:"new_string"`
			ReplaceAll bool   `json:"replace_all"`
			Content    string `json:"content"`
		} `json:"tool_input"`
	}
	if err := json.NewDecoder(io.LimitReader(os.Stdin, proto.MaxRequestBytes)).Decode(&in); err != nil {
		return 0 // not ours to judge; let the tool run
	}
	path := in.ToolInput.FilePath
	if path == "" || !filepath.IsAbs(path) {
		return 0
	}
	if in.PermissionMode != "acceptEdits" && in.PermissionMode != "bypassPermissions" {
		return 0 // the user approves each edit; do not act before they do
	}
	root, ok := proto.FindRoot(filepath.Dir(path))
	if !ok {
		return 0 // no daemon: plain editing
	}
	if rel, err := filepath.Rel(root, proto.Canonical(path)); err == nil {
		first := strings.SplitN(filepath.ToSlash(rel), "/", 2)[0]
		if strings.EqualFold(first, ".claude") || strings.EqualFold(first, ".github") {
			return 0 // agent and CI configuration keep Claude Code's own checks
		}
	}

	var req proto.Request
	switch in.ToolName {
	case "Edit":
		if in.ToolInput.ReplaceAll {
			return deny("avc: replace_all is not supported. Edit each occurrence with a unique old_string.")
		}
		req = proto.Request{Op: "edit", Path: path, Old: in.ToolInput.OldString, New: in.ToolInput.NewString}
	case "Write":
		req = proto.Request{Op: "write", Path: path, Content: []byte(in.ToolInput.Content)}
	case "MultiEdit":
		return deny("avc: MultiEdit is not applied atomically through avc. Use one Edit per change.")
	default:
		return 0
	}
	req.Agent = firstNonEmpty(os.Getenv("AVC_AGENT"), "claude-code")
	resp, err := send(root, req)
	if err != nil {
		return deny("avc: could not reach the daemon (" + err.Error() + "). Nothing was changed; retry, or ask the user to restart `avc daemon`.")
	}
	switch resp.Code {
	case proto.ExitOK:
		return deny(fmt.Sprintf("Applied through avc (batch %d): %s now contains your change. Do not retry this %s. Read the file again before editing it further.", resp.Seq, path, in.ToolName))
	case proto.ExitConflict:
		return deny("avc conflict, nothing was changed: " + resp.Error + ". Read the file again and redo the edit against its current content.")
	case proto.ExitDrift:
		return deny("avc: " + resp.Error + ". Nothing was changed. Read the file again and redo the edit.")
	case proto.ExitLease:
		return deny("avc: " + resp.Error + ". Nothing was changed. Another agent holds this file; work on something else or wait.")
	case proto.ExitBusy:
		return deny("avc is busy, nothing was changed: " + resp.Error + ". Retry shortly.")
	}
	return deny("avc refused the change, nothing was changed: " + resp.Error)
}

// deny blocks the original tool call and hands Claude the reason.
func deny(reason string) int {
	json.NewEncoder(os.Stdout).Encode(map[string]any{
		"hookSpecificOutput": map[string]any{
			"hookEventName":            "PreToolUse",
			"permissionDecision":       "deny",
			"permissionDecisionReason": reason,
		},
	})
	return 0
}
