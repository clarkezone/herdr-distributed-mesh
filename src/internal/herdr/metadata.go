package herdr

import (
	"bytes"
	"context"
	"strings"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

type ProjectResolver interface {
	ProjectForRepository(string) (string, error)
}

type projectObservation struct {
	projectID string
	err       error
}

func (o *observer) snapshot(ctx context.Context) (*pb.HerdrState, error) {
	result, err := o.rpc(ctx, "session.snapshot", "session_snapshot")
	if err != nil {
		return nil, err
	}
	return sanitizeSnapshotWithProjects(result["snapshot"], o.config.ProjectResolver)
}

func inheritWorkspaceProjects(state *pb.HerdrState) {
	projects := make(map[string]string, len(state.Workspaces))
	for _, workspace := range state.Workspaces {
		projects[workspace.Id] = workspace.ProjectId
	}
	for _, entities := range [][]*pb.HerdrEntity{state.Tabs, state.Panes, state.Agents} {
		for _, entity := range entities {
			entity.ProjectId = projects[entity.WorkspaceId]
		}
	}
}

func workspaceProject(entry object, resolver ProjectResolver, cache map[string]projectObservation) (string, error) {
	raw, exists := entry["worktree"]
	if !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", nil
	}
	var worktree object
	var root string
	if decodeRequired(raw, &worktree) != nil || required(worktree, "repo_root", &root) != nil || !validWorkspacePath(root) {
		return "", apiError("project_unavailable")
	}
	if observation, exists := cache[root]; exists {
		return observation.projectID, observation.err
	}
	projectID, err := resolver.ProjectForRepository(root)
	if projectID != "" && !idPattern.MatchString(projectID) {
		projectID, err = "", apiError("project_unavailable")
	}
	if err != nil {
		projectID = ""
	}
	cache[root] = projectObservation{projectID, err}
	return projectID, err
}

type entityObservation struct {
	provider         string
	interactiveReady *bool
	terminalID       string
	agentSessionID   string
}

func readEntityObservation(info object) (entityObservation, error) {
	var result entityObservation
	for _, field := range []struct {
		key string
		out *string
	}{
		{"terminal_id", &result.terminalID},
		{"agent", &result.provider},
	} {
		raw, exists := info[field.key]
		if !exists || (field.key == "agent" && bytes.Equal(bytes.TrimSpace(raw), []byte("null"))) {
			continue
		}
		if decodeRequired(raw, field.out) != nil || !idPattern.MatchString(*field.out) {
			return entityObservation{}, apiError("invalid_snapshot")
		}
	}
	if raw, exists := info["interactive_ready"]; exists {
		var ready bool
		if decodeRequired(raw, &ready) != nil {
			return entityObservation{}, apiError("invalid_snapshot")
		}
		result.interactiveReady = &ready
	}
	raw, exists := info["agent_session"]
	if !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return result, nil
	}
	var session object
	var kind, value, provider, source string
	if decodeRequired(raw, &session) != nil ||
		required(session, "kind", &kind) != nil || required(session, "value", &value) != nil ||
		required(session, "agent", &provider) != nil || required(session, "source", &source) != nil {
		return entityObservation{}, apiError("invalid_snapshot")
	}
	switch kind {
	case "id":
		expectedProvider := result.provider
		if expectedProvider == "" {
			expectedProvider = "unknown"
		}
		if !idPattern.MatchString(value) || strings.Contains(value, ":") || provider != expectedProvider {
			return entityObservation{}, apiError("invalid_snapshot")
		}
		result.agentSessionID = value
	case "path":
		// Native provider session-file references stay on the execution node.
	default:
		return entityObservation{}, apiError("invalid_snapshot")
	}
	return result, nil
}
