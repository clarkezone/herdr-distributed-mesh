//go:build windows

package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeWorktreeHerdr struct {
	path, checkout, destination, base, mode string
	creates                                 atomic.Int32
}

func (f *fakeWorktreeHerdr) serve(t *testing.T, conn net.Conn) {
	var request struct {
		ID     string                     `json:"id"`
		Method string                     `json:"method"`
		Params map[string]json.RawMessage `json:"params"`
	}
	if json.NewDecoder(conn).Decode(&request) != nil {
		return
	}
	var result any
	switch request.Method {
	case "ping":
		result = map[string]any{"type": "pong", "protocol": 18, "version": "0.7.5-preview"}
	case "events.subscribe":
		result = map[string]any{"type": "subscription_started"}
	case "session.snapshot":
		result = map[string]any{"type": "session_snapshot", "snapshot": map[string]any{"protocol": 18, "version": "0.7.5-preview", "workspaces": []any{}, "tabs": []any{}, "panes": []any{}, "agents": []any{}, "layouts": []any{}, "focused_workspace_id": nil}}
	case "worktree.list":
		var cwd string
		if len(request.Params) != 1 || json.Unmarshal(request.Params["cwd"], &cwd) != nil || cwd != f.checkout {
			t.Error("worktree listing escaped bound checkout")
			return
		}
		result = map[string]any{"type": "worktree_list", "source": map[string]any{"repo_key": "PRIVATE", "repo_name": "PRIVATE", "repo_root": f.checkout, "source_checkout_path": f.checkout},
			"worktrees": []any{map[string]any{"path": f.checkout, "branch": "main", "is_bare": false, "is_detached": false, "is_prunable": false, "is_linked_worktree": false, "label": "PRIVATE"}}}
	case "worktree.create":
		var cwd, path, branch, base string
		var focus bool
		if len(request.Params) != 5 || json.Unmarshal(request.Params["cwd"], &cwd) != nil || json.Unmarshal(request.Params["path"], &path) != nil ||
			json.Unmarshal(request.Params["branch"], &branch) != nil || json.Unmarshal(request.Params["base"], &base) != nil || json.Unmarshal(request.Params["focus"], &focus) != nil ||
			cwd != f.checkout || path != f.destination || branch != "task-one" || base != f.base || focus {
			t.Error("worktree creation escaped exact scoped contract")
			return
		}
		f.creates.Add(1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		output, err := exec.CommandContext(ctx, "git", "-C", f.checkout, "-c", "core.hooksPath=", "worktree", "add", "--quiet", "-b", branch, path, base).CombinedOutput()
		cancel()
		if err != nil {
			t.Errorf("isolated worktree creation: %v (%s)", err, output)
			return
		}
		if f.mode == "lost-reply" {
			return
		}
		workspace := map[string]any{"workspace_id": "w2", "number": 2, "label": "PRIVATE", "focused": false,
			"pane_count": 1, "tab_count": 1, "active_tab_id": "w2:t1", "agent_status": "idle",
			"worktree": map[string]any{"checkout_path": path, "repo_root": cwd, "repo_name": "PRIVATE", "repo_key": "PRIVATE", "is_linked_worktree": true}}
		worktree := map[string]any{"path": path, "branch": branch, "is_bare": false, "is_detached": false, "is_prunable": false, "is_linked_worktree": true, "label": "PRIVATE", "open_workspace_id": "w2"}
		if f.mode == "missing-metadata" {
			delete(workspace, "worktree")
		}
		if f.mode == "mismatched-metadata" {
			worktree["branch"] = "other"
		}
		result = map[string]any{"type": "worktree_created", "workspace": workspace, "worktree": worktree, "tab": map[string]any{"tab_id": "w2:t1"}, "root_pane": map[string]any{"pane_id": "w2:p1"}}
	default:
		t.Errorf("unexpected Herdr operation %q", request.Method)
		return
	}
	if json.NewEncoder(conn).Encode(map[string]any{"id": request.ID, "result": result}) != nil {
		return
	}
	if request.Method == "events.subscribe" {
		_, _ = io.Copy(io.Discard, conn)
	}
}

func worktreeGit(t *testing.T, checkout string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := append([]string{"-C", checkout, "-c", "core.hooksPath=", "-c", "commit.gpgsign=false"}, args...)
	output, err := exec.CommandContext(ctx, "git", command...).CombinedOutput()
	if err != nil {
		t.Fatalf("isolated Git operation: %v (%s)", err, output)
	}
	return strings.TrimSpace(string(output))
}

func newWorktreeFixture(t *testing.T, mode string) (*fakeWorktreeHerdr, *projects.Policy) {
	t.Helper()
	root := t.TempDir()
	checkout, outputRoot := filepath.Join(root, "checkout"), filepath.Join(root, "worktrees")
	for _, path := range []string{checkout, outputRoot} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	worktreeGit(t, checkout, "init", "--quiet", "-b", "main")
	worktreeGit(t, checkout, "-c", "user.name=Mesh Test", "-c", "user.email=mesh@example.invalid", "commit", "--quiet", "--allow-empty", "-m", "isolated worktree fixture")
	checkout, err := filepath.EvalSymlinks(checkout)
	if err != nil {
		t.Fatal(err)
	}
	outputRoot, err = filepath.EvalSymlinks(outputRoot)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeWorktreeHerdr{checkout: checkout, destination: filepath.Join(outputRoot, "task-one"), base: worktreeGit(t, checkout, "rev-parse", "HEAD"), mode: mode}
	raw, err := json.Marshal(map[string]any{"projects": []any{map[string]any{"project_id": "AgentFlow", "node_id": "node-1", "binding_revision": "r1", "actor_ids": []string{"client-1"},
		"path": checkout, "allow_worktrees": true, "worktree_root": outputRoot}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "policy.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	policy, err := projects.Load(path, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	fake.path = listenFakeHerdr(t, fake.serve)
	return fake, policy
}

func startWorktreeNode(t *testing.T, h *commandHarness, journal string, policy *projects.Policy, socket string) func() {
	t.Helper()
	stop := startWorkspaceNode(t, h, journal, policy, socket)
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 5*time.Second)
	defer cancel()
	for {
		list, err := pb.NewFleetClient(h.connection).ListNodes(ctx, &emptypb.Empty{})
		if err == nil && len(list.Nodes) == 1 && list.Nodes[0].WorktreeReady {
			return stop
		}
		select {
		case <-ctx.Done():
			t.Fatal("worktree readiness did not arrive")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestWorktreeCreationThroughRealNodeGitAndJournals(t *testing.T) {
	for _, mode := range []string{"known-result", "lost-reply", "missing-metadata", "mismatched-metadata"} {
		t.Run(mode, func(t *testing.T) {
			fake, policy := newWorktreeFixture(t, mode)
			root := t.TempDir()
			serverRoot, journal := filepath.Join(root, "server"), filepath.Join(root, "node", "journal.db")
			h := newCommandHarness(t, serverRoot)
			h.api.workspacePolicy = coordinatorWorktreePolicy(t)
			stop := startWorktreeNode(t, h, journal, policy, fake.path)
			ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 30*time.Second)
			defer cancel()
			request := worktreeRequest("first")
			request.WorktreeCreate.BaseCommit = fake.base
			request.Ttl = durationpb.New(30 * time.Second)
			first, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			want := pb.CommandStatus_COMMAND_STATUS_SUCCEEDED
			if mode != "known-result" {
				want = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE
			}
			record := awaitCommand(t, h, first.Command.CommandId, want)
			if fake.creates.Load() != 1 {
				t.Fatal("create was not attempted exactly once")
			}
			if want == pb.CommandStatus_COMMAND_STATUS_SUCCEEDED && (record.WorktreeCreate.GetWorkspaceId() != "w2" || record.WorktreeCreate.GetBaseCommit() != fake.base) {
				t.Fatal("typed worktree outcome missing")
			}
			if got := worktreeGit(t, fake.destination, "rev-parse", "HEAD"); got != fake.base {
				t.Fatal("worktree HEAD differs from requested exact commit")
			}
			if got := worktreeGit(t, fake.destination, "symbolic-ref", "--short", "HEAD"); got != "task-one" {
				t.Fatal("wrong worktree branch")
			}
			if got := worktreeGit(t, fake.checkout, "symbolic-ref", "--short", "HEAD"); got != "main" {
				t.Fatal("source branch changed")
			}
			if got := worktreeGit(t, fake.checkout, "rev-parse", "HEAD"); got != fake.base {
				t.Fatal("source HEAD changed")
			}
			raw, err := protojson.Marshal(record)
			if err != nil || strings.Contains(string(raw), "PRIVATE") || strings.Contains(string(raw), "checkout") {
				t.Fatal("local metadata leaked")
			}
			stop()
			h.stop()
			h = newCommandHarness(t, serverRoot)
			h.api.workspacePolicy = coordinatorWorktreePolicy(t)
			startWorktreeNode(t, h, journal, policy, fake.path)
			retry, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, request)
			if err != nil || retry.Command.CommandId != first.Command.CommandId || retry.Status != want {
				t.Fatal("restart lost immutable outcome", err)
			}
			second := proto.Clone(request).(*pb.SubmitCommandRequest)
			second.IdempotencyKey = "new-key"
			submitted, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, second)
			if err != nil {
				t.Fatal(err)
			}
			blocked := awaitCommand(t, h, submitted.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_REJECTED)
			detail := "precondition_failed"
			if mode != "known-result" {
				detail = "project_unresolved"
			}
			if blocked.Detail != detail {
				t.Fatalf("new key result %s, want %s", blocked.Detail, detail)
			}
			if mode != "known-result" {
				workspace, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, workspaceRequest("cross-type"))
				if err != nil {
					t.Fatal(err)
				}
				if record := awaitCommand(t, h, workspace.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_REJECTED); record.Detail != "project_unresolved" {
					t.Fatal("workspace bypassed worktree quarantine")
				}
			}
			if fake.creates.Load() != 1 {
				t.Fatal("new key/restart repeated worktree creation")
			}
		})
	}
}

func TestWorktreePreconditionsRejectWithoutEffects(t *testing.T) {
	for _, mode := range []string{"destination-exists", "branch-exists", "missing-commit"} {
		t.Run(mode, func(t *testing.T) {
			fake, policy := newWorktreeFixture(t, mode)
			switch mode {
			case "destination-exists":
				if err := os.Mkdir(fake.destination, 0700); err != nil {
					t.Fatal(err)
				}
			case "branch-exists":
				worktreeGit(t, fake.checkout, "branch", "task-one")
			}
			h := newCommandHarness(t, t.TempDir())
			h.api.workspacePolicy = coordinatorWorktreePolicy(t)
			startWorktreeNode(t, h, filepath.Join(t.TempDir(), "node", "journal.db"), policy, fake.path)
			request := worktreeRequest("reject")
			request.WorktreeCreate.BaseCommit = fake.base
			if mode == "missing-commit" {
				request.WorktreeCreate.BaseCommit = strings.Repeat("f", 40)
			}
			ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 10*time.Second)
			defer cancel()
			record, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if result := awaitCommand(t, h, record.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_REJECTED); result.Detail != "precondition_failed" {
				t.Fatal("precondition failed without safe rejection")
			}
			if fake.creates.Load() != 0 {
				t.Fatal("invalid precondition reached Herdr effect")
			}
		})
	}
}
