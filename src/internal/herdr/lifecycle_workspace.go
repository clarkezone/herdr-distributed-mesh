package herdr

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
)

// ResolveLifecycleWorkspace returns a node-private cwd for an explicitly
// selected workspace belonging to the current managed project binding. It
// never creates a workspace, focuses a pane, or interprets actor/grant fields.
func ResolveLifecycleWorkspace(ctx context.Context, config Config, workspaceID string, binding projects.Binding) (string, error) {
	return resolveLifecycleWorkspace(ctx, config, workspaceID, binding, dialLocal)
}

func resolveLifecycleWorkspace(ctx context.Context, config Config, workspaceID string, binding projects.Binding, dial dialFunc) (string, error) {
	if !idPattern.MatchString(workspaceID) || binding.ValidatePath() != nil {
		return "", ErrAgentPrecondition
	}
	o, err := lifecycleObserver(ctx, config, dial)
	if err != nil {
		return "", err
	}
	if err := o.lifecycleCheckSession(ctx); err != nil {
		return "", err
	}
	result, err := o.agentRequest(ctx, "workspace.get", "workspace_info", struct {
		WorkspaceID string `json:"workspace_id"`
	}{workspaceID})
	if err != nil {
		return "", ErrAgentUnavailable
	}
	id, checkout, err := workspaceIdentity(result["workspace"])
	if err != nil || id != workspaceID {
		return "", ErrAgentChanged
	}
	cwd := ""
	if checkout != "" {
		var workspace, tree object
		var repository string
		if required(result, "workspace", &workspace) != nil || required(workspace, "worktree", &tree) != nil ||
			required(tree, "repo_root", &repository) != nil {
			return "", ErrAgentPrecondition
		}
		match, err := workspaceMatches(repository, binding.Path)
		if err != nil || !match {
			return "", ErrAgentPrecondition
		}
		if _, err := lifecycleDirectoryInfo(checkout); err != nil {
			return "", ErrAgentPrecondition
		}
		cwd = checkout
	} else {
		list, err := o.rpc(ctx, "pane.list", "pane_list")
		if err != nil {
			return "", ErrAgentUnavailable
		}
		var panes []json.RawMessage
		if required(list, "panes", &panes) != nil || len(panes) > maxEntities {
			return "", ErrAgentUnavailable
		}
		for _, raw := range panes {
			var pane object
			var workspace, path string
			if decodeRequired(raw, &pane) != nil || required(pane, "workspace_id", &workspace) != nil {
				return "", ErrAgentUnavailable
			}
			if workspace != workspaceID {
				continue
			}
			rawPath, present := pane["cwd"]
			if !present || bytes.Equal(bytes.TrimSpace(rawPath), []byte("null")) {
				continue
			}
			if required(pane, "cwd", &path) != nil {
				return "", ErrAgentUnavailable
			}
			match, err := workspaceMatches(path, binding.Path)
			if err != nil || !match {
				return "", ErrAgentPrecondition
			}
			cwd = binding.Path
		}
	}
	if cwd == "" || binding.ValidatePath() != nil {
		return "", ErrAgentPrecondition
	}
	if err := o.lifecycleCheckSession(ctx); err != nil {
		return "", err
	}
	return cwd, nil
}
