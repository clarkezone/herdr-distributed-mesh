package herdr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrcompat"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
)

var (
	ErrWorkspacePrecondition  = errors.New("herdr: workspace precondition failed")
	ErrWorkspaceAmbiguous     = errors.New("herdr: workspace ambiguous")
	ErrWorkspaceUnavailable   = errors.New("herdr: workspace unavailable")
	ErrWorkspaceIndeterminate = errors.New("herdr: workspace outcome unknown")
)

// EnsureWorkspace finds or creates a workspace for an explicitly bound local
// checkout. It never focuses, starts commands, overrides env, or creates Git
// worktrees. All errors are sanitized. Every attempted create is terminal:
// uncertain responses must be quarantined by the caller's durable journal.
//
// Herdr has no atomic ensure operation. Callers must serialize local mutations;
// external concurrent clients can still create duplicates. The local filesystem
// and Herdr are trusted; path checks cannot close the filesystem/IPC TOCTOU gap.
func EnsureWorkspace(ctx context.Context, config Config, binding projects.Binding) (*pb.WorkspaceEnsureResult, error) {
	return ensureWorkspace(ctx, config, binding, dialLocal)
}

func ensureWorkspace(ctx context.Context, config Config, binding projects.Binding, dial dialFunc) (*pb.WorkspaceEnsureResult, error) {
	if binding.ValidatePath() != nil {
		return nil, ErrWorkspacePrecondition
	}
	if ctx.Err() != nil {
		return nil, ErrWorkspaceUnavailable
	}
	address, err := localAddress(config.SocketPath)
	if err != nil || config.RefreshInterval < 0 || config.RetryDelay < 0 || config.RequestTimeout < 0 {
		return nil, ErrWorkspaceUnavailable
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 3 * time.Second
	}
	config.SocketPath = address
	o := observer{config: config, dial: dial}
	pong, err := o.rpc(ctx, "ping", "pong")
	if err != nil {
		return nil, ErrWorkspaceUnavailable
	}
	_, protocol, err := versionProtocol(pong)
	if err != nil || !herdrcompat.SupportsProtocol(int64(protocol)) || ctx.Err() != nil {
		return nil, ErrWorkspaceUnavailable
	}
	list, err := o.rpc(ctx, "workspace.list", "workspace_list")
	if err != nil {
		return nil, ErrWorkspaceUnavailable
	}
	var workspaces []json.RawMessage
	if required(list, "workspaces", &workspaces) != nil || len(workspaces) > maxEntities {
		return nil, ErrWorkspaceUnavailable
	}
	seen := make(map[string]struct{}, len(workspaces))
	var matches []string
	for _, raw := range workspaces {
		id, checkout, err := workspaceIdentity(raw)
		if err != nil {
			return nil, ErrWorkspaceUnavailable
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, ErrWorkspaceUnavailable
		}
		seen[id] = struct{}{}
		if checkout != "" {
			match, err := workspaceMatches(checkout, binding.Path)
			if err != nil {
				return nil, ErrWorkspaceUnavailable
			}
			if match {
				matches = append(matches, id)
			}
		}
	}
	if binding.ValidatePath() != nil {
		return nil, ErrWorkspacePrecondition
	}
	if ctx.Err() != nil {
		return nil, ErrWorkspaceUnavailable
	}
	if len(matches) > 1 {
		return nil, ErrWorkspaceAmbiguous
	}
	result := &pb.WorkspaceEnsureResult{ProjectId: binding.ProjectID, BindingRevision: binding.Revision}
	if len(matches) == 1 {
		result.WorkspaceId = matches[0]
		return result, nil
	}
	if config.CheckSession != nil {
		if err := config.CheckSession(ctx); err != nil {
			return nil, err
		}
	}
	// workspace.create(cwd) opens a plain shell without checkout metadata.
	// worktree.open opens the existing checkout without creating a Git worktree
	// and gives subsequent ensure/observation calls a stable checkout identity.
	_, _, cleanup, created, err := o.request(ctx, "worktree.open", "worktree_opened", struct {
		Cwd   string `json:"cwd"`
		Path  string `json:"path"`
		Focus bool   `json:"focus"`
	}{Cwd: binding.Path, Path: binding.Path, Focus: false})
	if err != nil {
		return nil, ErrWorkspaceIndeterminate
	}
	cleanup()
	id, checkout, err := workspaceIdentity(created["workspace"])
	var alreadyOpen bool
	// Without confirmed checkout identity, later lists cannot recognize this
	// create and a new command could duplicate it instead of being quarantined.
	if err != nil || checkout == "" || required(created, "already_open", &alreadyOpen) != nil {
		return nil, ErrWorkspaceIndeterminate
	}
	if _, duplicate := seen[id]; duplicate {
		return nil, ErrWorkspaceIndeterminate
	}
	match, err := workspaceMatches(checkout, binding.Path)
	if err != nil || !match {
		return nil, ErrWorkspaceIndeterminate
	}
	if binding.ValidatePath() != nil || ctx.Err() != nil {
		return nil, ErrWorkspaceIndeterminate
	}
	result.WorkspaceId = id
	result.Created = !alreadyOpen
	return result, nil
}

func workspaceIdentity(raw json.RawMessage) (string, string, error) {
	var workspace object
	if decodeRequired(raw, &workspace) != nil {
		return "", "", ErrWorkspaceUnavailable
	}
	entity, err := sanitizeEntity(workspace, "workspace_id")
	if err != nil {
		return "", "", ErrWorkspaceUnavailable
	}
	var number, panes, tabs uint32
	var label, activeTab string
	if required(workspace, "number", &number) != nil ||
		required(workspace, "label", &label) != nil ||
		required(workspace, "pane_count", &panes) != nil ||
		required(workspace, "tab_count", &tabs) != nil ||
		required(workspace, "active_tab_id", &activeTab) != nil || !idPattern.MatchString(activeTab) {
		return "", "", ErrWorkspaceUnavailable
	}
	worktree, ok := workspace["worktree"]
	if !ok || bytes.Equal(bytes.TrimSpace(worktree), []byte("null")) {
		return entity.Id, "", nil
	}
	var tree object
	var checkout string
	if decodeRequired(worktree, &tree) != nil || required(tree, "checkout_path", &checkout) != nil ||
		!validWorkspacePath(checkout) {
		return "", "", ErrWorkspaceUnavailable
	}
	return entity.Id, checkout, nil
}

func validWorkspacePath(path string) bool {
	if len(path) == 0 || len(path) > 32768 || !utf8.ValidString(path) || !filepath.IsAbs(path) ||
		strings.HasPrefix(path, `\\`) || strings.HasPrefix(path, "//") {
		return false
	}
	for _, r := range path {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

func workspaceMatches(checkout, bound string) (bool, error) {
	canonical, err := filepath.EvalSymlinks(checkout)
	if err != nil || !validWorkspacePath(canonical) {
		return false, ErrWorkspaceUnavailable
	}
	// SameFile handles canonical aliases and Windows case differences without
	// incorrectly folding names in case-sensitive directories.
	first, err := os.Open(canonical)
	if err != nil {
		return false, ErrWorkspaceUnavailable
	}
	defer first.Close()
	firstInfo, err := first.Stat()
	if err != nil || !firstInfo.IsDir() {
		return false, ErrWorkspaceUnavailable
	}
	second, err := os.Open(bound)
	if err != nil {
		return false, ErrWorkspaceUnavailable
	}
	defer second.Close()
	secondInfo, err := second.Stat()
	if err != nil || !secondInfo.IsDir() {
		return false, ErrWorkspaceUnavailable
	}
	return os.SameFile(firstInfo, secondInfo), nil
}
