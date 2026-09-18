package herdr

import (
	"context"
	"errors"
	"testing"
)

func TestLifecycleWorkspaceResolutionUsesSelectedCheckout(t *testing.T) {
	binding := workspaceBinding(t)
	workspace := workspaceInfo("ws:1", binding.Path)
	workspace["worktree"].(map[string]any)["repo_root"] = binding.Path
	steps := []step{agentPongStep(), agentReply("workspace.get", "workspace_info",
		map[string]any{"workspace": workspace}, map[string]any{"workspace_id": "ws:1"})}
	cwd, err := resolveLifecycleWorkspace(context.Background(), testConfig(), "ws:1", binding, agentDial(t, steps...))
	if err != nil || cwd != binding.Path {
		t.Fatalf("selected checkout not resolved: %v", err)
	}
}

func TestLifecycleWorkspaceResolutionRejectsOtherRepository(t *testing.T) {
	binding := workspaceBinding(t)
	workspace := workspaceInfo("ws:1", t.TempDir())
	workspace["worktree"].(map[string]any)["repo_root"] = t.TempDir()
	steps := []step{agentPongStep(), agentReply("workspace.get", "workspace_info",
		map[string]any{"workspace": workspace}, map[string]any{"workspace_id": "ws:1"})}
	if _, err := resolveLifecycleWorkspace(context.Background(), testConfig(), "ws:1", binding, agentDial(t, steps...)); !errors.Is(err, ErrAgentPrecondition) {
		t.Fatal("different repository accepted")
	}
}

func TestLifecycleWorkspaceWithoutWorktreeRequiresMatchingPaneCwd(t *testing.T) {
	binding := workspaceBinding(t)
	pane := lifecycleShell()
	pane["cwd"] = binding.Path
	steps := []step{agentPongStep(), lifecycleWorkspaceStep(), lifecycleListStep(pane)}
	if cwd, err := resolveLifecycleWorkspace(context.Background(), testConfig(), "ws:1", binding, agentDial(t, steps...)); err != nil || cwd != binding.Path {
		t.Fatalf("plain workspace rejected: %v", err)
	}

}

func TestLifecycleWorkspaceRejectsMalformedPaneCwd(t *testing.T) {
	binding := workspaceBinding(t)
	pane := lifecycleShell()
	pane["cwd"] = 42
	steps := []step{agentPongStep(), lifecycleWorkspaceStep(), lifecycleListStep(pane)}
	if _, err := resolveLifecycleWorkspace(context.Background(), testConfig(), "ws:1", binding, agentDial(t, steps...)); !errors.Is(err, ErrAgentUnavailable) {
		t.Fatal("malformed path metadata was ignored")
	}
}
