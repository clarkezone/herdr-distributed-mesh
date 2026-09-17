package herdr

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
)

type projectResolverFunc func(string) (string, error)

func (f projectResolverFunc) ProjectForRepository(path string) (string, error) { return f(path) }

func metadataSnapshot(t *testing.T, root string) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"version": "0.9.0", "protocol": 18,
		"workspaces": []any{
			map[string]any{"workspace_id": "w1", "focused": false, "agent_status": "working",
				"worktree": map[string]any{"repo_root": root, "checkout_path": root, "repo_key": "PRIVATE"}},
			map[string]any{"workspace_id": "w2", "focused": false, "agent_status": "idle",
				"worktree": map[string]any{"repo_root": root, "checkout_path": root, "repo_key": "PRIVATE"}},
		},
		"tabs": []any{}, "layouts": []any{},
		"panes": []any{map[string]any{"pane_id": "p1", "workspace_id": "w1", "tab_id": "t1",
			"focused": false, "agent_status": "working", "terminal_id": "term1", "agent": "copilot"}},
		"agents": []any{
			map[string]any{"pane_id": "p1", "workspace_id": "w1", "tab_id": "t1", "focused": false,
				"agent_status": "working", "terminal_id": "term1", "agent": "copilot", "interactive_ready": false,
				"cwd": root, "title": "PRIVATE", "agent_session": map[string]any{
					"kind": "id", "value": "provider-session", "agent": "copilot", "source": "native"}},
			map[string]any{"pane_id": "p2", "workspace_id": "orphan", "tab_id": "t2", "focused": false,
				"agent_status": "idle", "terminal_id": "term2", "agent": "copilot",
				"agent_session": map[string]any{"kind": "path", "value": root, "agent": "copilot", "source": "native"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSnapshotProjectsUseExplicitRepositoryIdentityAndStayPrivate(t *testing.T) {
	root := filepath.Join(t.TempDir(), "PRIVATE-repository")
	calls := 0
	state, err := sanitizeSnapshotWithProjects(metadataSnapshot(t, root), projectResolverFunc(func(path string) (string, error) {
		calls++
		if path != root {
			t.Fatal("project lookup used a name or wrong repository")
		}
		return "example-app", nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || state.Workspaces[0].ProjectId != "example-app" || state.Workspaces[1].ProjectId != "example-app" ||
		state.Panes[0].ProjectId != "example-app" || state.Agents[0].ProjectId != "example-app" || state.Agents[1].ProjectId != "" {
		t.Fatal("project lookup caching or same-workspace propagation failed")
	}
	agent := state.Agents[0]
	if agent.Provider != "copilot" || agent.TerminalId != "term1" || agent.ProviderSessionId != "provider-session" ||
		agent.InteractiveReady == nil || *agent.InteractiveReady {
		t.Fatal("public metadata or explicit false readiness was lost")
	}
	if state.Agents[1].ProviderSessionId != "" || state.Agents[1].InteractiveReady != nil {
		t.Fatal("private path or absent readiness was invented")
	}
	data, err := protojson.Marshal(state)
	if err != nil || strings.Contains(string(data), "PRIVATE") || strings.Contains(string(data), "repo_root") ||
		strings.Contains(string(data), "checkout_path") || strings.Contains(string(data), "cwd") {
		t.Fatalf("private source fields escaped projection: %v", err)
	}
}

func TestSnapshotProjectFailurePreservesOtherInventory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "PRIVATE-repository")
	for _, resolver := range []ProjectResolver{
		nil,
		projectResolverFunc(func(string) (string, error) { return "", nil }),
		projectResolverFunc(func(string) (string, error) { return "ignored", errors.New("PRIVATE") }),
		projectResolverFunc(func(string) (string, error) { return root, nil }),
	} {
		state, err := sanitizeSnapshotWithProjects(metadataSnapshot(t, root), resolver)
		if err != nil || state.Status != "ready" || len(state.Workspaces) != 2 || len(state.Agents) != 2 {
			t.Fatalf("optional mapping failure hid valid inventory: %v", err)
		}
		for _, entity := range state.Workspaces {
			if entity.ProjectId != "" {
				t.Fatal("unavailable or unsafe association was exposed")
			}
		}
	}
}
