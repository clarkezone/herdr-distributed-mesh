//go:build windows

package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	winio "github.com/tailscale/go-winio"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"strings"
)

type fakeWorkspaceHerdr struct {
	path         string
	checkout     string
	created      atomic.Bool
	creates      atomic.Int32
	dropReply    bool
	omitMetadata bool
}

func newFakeWorkspaceHerdr(t *testing.T, checkout string, drop, omit bool) *fakeWorkspaceHerdr {
	t.Helper()
	fake := &fakeWorkspaceHerdr{checkout: checkout, dropReply: drop, omitMetadata: omit}
	fake.path = listenFakeHerdr(t, fake.serve)
	return fake
}

func listenFakeHerdr(t *testing.T, serve func(*testing.T, net.Conn)) string {
	t.Helper()
	path := `\\.\pipe\herdr-mesh-mutation-test-` + protocol.NewCommandID()
	listener, err := winio.ListenPipe(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	accepted := make(chan struct{})
	var workers sync.WaitGroup
	go func() {
		defer close(accepted)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Add(1)
			go func(conn net.Conn) {
				defer workers.Done()
				defer conn.Close()
				stop := context.AfterFunc(ctx, func() { conn.Close() })
				defer stop()
				conn.SetDeadline(time.Now().Add(15 * time.Second))
				serve(t, conn)
			}(connection)
		}
	}()
	t.Cleanup(func() { cancel(); listener.Close(); <-accepted; workers.Wait() })
	return path
}

func (f *fakeWorkspaceHerdr) workspace() map[string]any {
	return map[string]any{"workspace_id": "w1", "number": 1, "label": "PRIVATE LABEL", "focused": false,
		"pane_count": 1, "tab_count": 1, "active_tab_id": "w1:t1", "agent_status": "idle",
		"worktree": map[string]any{"checkout_path": f.checkout, "repo_root": f.checkout, "repo_name": "PRIVATE REPO", "repo_key": "private-key", "is_linked_worktree": false}}
}

func (f *fakeWorkspaceHerdr) serve(t *testing.T, conn net.Conn) {
	var request struct {
		ID     string                     `json:"id"`
		Method string                     `json:"method"`
		Params map[string]json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(conn).Decode(&request); err != nil {
		return
	}
	workspaces := []any{}
	if f.created.Load() {
		workspaces = append(workspaces, f.workspace())
	}
	var result any
	switch request.Method {
	case "ping":
		result = map[string]any{"type": "pong", "protocol": 18, "version": "0.7.5-preview"}
	case "events.subscribe":
		result = map[string]any{"type": "subscription_started"}
	case "session.snapshot":
		result = map[string]any{"type": "session_snapshot", "snapshot": map[string]any{
			"protocol": 18, "version": "0.7.5-preview", "workspaces": workspaces,
			"tabs": []any{}, "panes": []any{}, "agents": []any{}, "layouts": []any{}, "focused_workspace_id": nil}}
	case "workspace.list":
		result = map[string]any{"type": "workspace_list", "workspaces": workspaces}
	case "workspace.create":
		var cwd string
		var focus bool
		if len(request.Params) != 2 || json.Unmarshal(request.Params["cwd"], &cwd) != nil ||
			json.Unmarshal(request.Params["focus"], &focus) != nil || cwd != f.checkout || focus {
			t.Error("workspace creation escaped bound cwd/focus:false contract")
			return
		}
		f.creates.Add(1)
		f.created.Store(true)
		if f.dropReply {
			return
		}
		workspace := f.workspace()
		if f.omitMetadata {
			delete(workspace, "worktree")
		}
		result = map[string]any{"type": "workspace_created", "workspace": workspace,
			"tab": map[string]any{"tab_id": "w1:t1"}, "root_pane": map[string]any{"pane_id": "w1:p1"}}
	default:
		t.Errorf("unexpected Herdr method %q", request.Method)
		return
	}
	if err := json.NewEncoder(conn).Encode(map[string]any{"id": request.ID, "result": result}); err != nil {
		return
	}
	if request.Method == "events.subscribe" {
		_, _ = io.Copy(io.Discard, conn)
	}
}

func nodeWorkspacePolicy(t *testing.T, checkout string) *projects.Policy {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"projects": []any{map[string]any{
		"project_id": "AgentFlow", "node_id": "node-1", "binding_revision": "r1", "actor_ids": []string{"client-1"}, "path": checkout}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "node-policy.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	policy, err := projects.Load(path, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func startWorkspaceNode(t *testing.T, h *commandHarness, journal string, policy *projects.Policy, socket string) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(commandPeer(context.Background(), "node"))
	done := make(chan error, 1)
	go func() {
		_, err := node.RunSession(ctx, pb.NewNodeControlClient(h.connection), node.Options{
			InstanceID: "node-1", RequiredServerTag: node.DefaultRequiredServerTag, CommandJournalPath: journal,
			WorkspacePolicy: policy, HerdrSocket: socket, HeartbeatInterval: 50 * time.Millisecond,
			VerifyServerPeer: func(ctx context.Context, address string) error {
				if address != "bufconn" {
					return errors.New("unexpected integration coordinator peer")
				}
				return ctx.Err()
			}})
		done <- err
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("workspace node did not stop")
			}
		})
	}
	t.Cleanup(stop)
	wait, timeout := context.WithTimeout(commandPeer(context.Background(), "client"), 5*time.Second)
	defer timeout()
	for {
		list, err := pb.NewFleetClient(h.connection).ListNodes(wait, &emptypb.Empty{})
		if err == nil && len(list.Nodes) == 1 && list.Nodes[0].WorkspaceReady {
			return stop
		}
		select {
		case err := <-done:
			// Preserve the result for stop's single join.
			done <- err
			t.Fatalf("workspace node failed to start: %v", err)
		case <-wait.Done():
			t.Fatal("workspace readiness did not arrive")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestWorkspaceEnsureThroughRealNodeAndLocalIPC(t *testing.T) {
	for _, name := range []string{"known-result", "lost-result", "missing-metadata"} {
		t.Run(name, func(t *testing.T) {
			uncertain := name != "known-result"
			root := t.TempDir()
			checkout := filepath.Join(root, "checkout")
			if output, err := exec.Command("git", "init", "--quiet", checkout).CombinedOutput(); err != nil {
				t.Fatalf("initialize isolated checkout: %v (%s)", err, output)
			}
			checkout, err := filepath.EvalSymlinks(checkout)
			if err != nil {
				t.Fatal(err)
			}
			policy := nodeWorkspacePolicy(t, checkout)
			fake := newFakeWorkspaceHerdr(t, checkout, name == "lost-result", name == "missing-metadata")
			serverRoot := filepath.Join(root, "server")
			h := newCommandHarness(t, serverRoot)
			h.api.workspacePolicy = coordinatorWorkspacePolicy(t)
			journal := filepath.Join(root, "node", "journal.db")
			stop := startWorkspaceNode(t, h, journal, policy, fake.path)
			ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 5*time.Second)
			defer cancel()
			first, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, workspaceRequest("first"))
			if err != nil {
				t.Fatal(err)
			}
			want := pb.CommandStatus_COMMAND_STATUS_SUCCEEDED
			if uncertain {
				want = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE
			}
			record := awaitCommand(t, h, first.Command.CommandId, want)
			if fake.creates.Load() != 1 {
				t.Fatal("creation not attempted exactly once")
			}
			if !uncertain && (record.WorkspaceEnsure.GetWorkspaceId() != "w1" || !record.WorkspaceEnsure.GetCreated()) {
				t.Fatal("typed workspace creation result missing")
			}
			raw, err := protojson.Marshal(record)
			if err != nil || strings.Contains(string(raw), "PRIVATE") || strings.Contains(string(raw), `checkout`) {
				t.Fatal("local workspace metadata leaked")
			}
			stop()
			h.stop()
			h = newCommandHarness(t, serverRoot)
			h.api.workspacePolicy = coordinatorWorkspacePolicy(t)
			startWorkspaceNode(t, h, journal, policy, fake.path)
			awaitCommand(t, h, first.Command.CommandId, want)
			second, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, workspaceRequest("second-key"))
			if err != nil {
				t.Fatal(err)
			}
			if uncertain {
				blocked := awaitCommand(t, h, second.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_REJECTED)
				if blocked.Detail != "project_unresolved" {
					t.Fatal("uncertain project not quarantined")
				}
			} else {
				reused := awaitCommand(t, h, second.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
				if reused.WorkspaceEnsure.GetWorkspaceId() != "w1" || reused.WorkspaceEnsure.GetCreated() {
					t.Fatal("ensure did not reuse existing workspace")
				}
			}
			if fake.creates.Load() != 1 {
				t.Fatal("restart/new key duplicated workspace creation")
			}
		})
	}
}
