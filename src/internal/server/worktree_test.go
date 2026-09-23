package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func coordinatorWorktreePolicy(t *testing.T) *projects.Policy {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, []byte(`{"projects":[{"project_id":"AgentFlow","node_id":"node-1","binding_revision":"r1","actor_ids":["client-1"],"allow_worktrees":true}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	policy, err := projects.Load(path, "")
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func worktreeRequest(key string) *pb.SubmitCommandRequest {
	return &pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: key, CommandType: protocol.WorktreeCreateCommandType,
		WorktreeCreate: &pb.WorktreeCreate{ProjectId: "AgentFlow", BindingRevision: "r1", Name: "task-one", Branch: "task-one", BaseCommit: strings.Repeat("a", 40)}}
}

func readyWorktreeEntry(t *testing.T, h *commandHarness) *fleetEntry {
	t.Helper()
	entry := readyWorkspaceEntry(t, h)
	h.api.fleet.mu.Lock()
	entry.worktrees = true
	h.api.fleet.mu.Unlock()
	if err := h.api.fleet.heartbeat(entry, time.Now(), true, true, true); err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestWorktreeAdmissionRequiresOptInAuthorizationAndReadiness(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	h.api.workspacePolicy = coordinatorWorkspacePolicy(t)
	entry := readyWorktreeEntry(t, h)
	client := pb.NewFleetClient(h.connection)
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 5*time.Second)
	defer cancel()
	if _, err := client.SubmitCommand(ctx, worktreeRequest("old-policy")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("old policy enabled worktrees: %v", err)
	}
	h.api.workspacePolicy = coordinatorWorktreePolicy(t)
	for _, tc := range []struct {
		name, peer string
		change     func(*pb.SubmitCommandRequest)
	}{
		{"actor", "client2", func(*pb.SubmitCommandRequest) {}},
		{"revision", "client", func(r *pb.SubmitCommandRequest) { r.WorktreeCreate.BindingRevision = "r2" }},
		{"project", "client", func(r *pb.SubmitCommandRequest) { r.WorktreeCreate.ProjectId = "other" }},
	} {
		request := worktreeRequest(tc.name)
		tc.change(request)
		call, cancel := context.WithTimeout(commandPeer(context.Background(), tc.peer), time.Second)
		_, err := client.SubmitCommand(call, request)
		cancel()
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("%s admitted: %v", tc.name, err)
		}
	}
	first, err := client.SubmitCommand(ctx, worktreeRequest("stable"))
	if err != nil {
		t.Fatal(err)
	}
	h.api.fleet.mu.Lock()
	entry.view.WorktreeReady = false
	h.api.fleet.mu.Unlock()
	retry, err := client.SubmitCommand(ctx, worktreeRequest("stable"))
	if err != nil || retry.Command.CommandId != first.Command.CommandId {
		t.Fatal("readiness prevented historical retry", err)
	}
	if _, err := client.SubmitCommand(ctx, worktreeRequest("unready")); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("unready worktree admitted", err)
	}
	h.api.fleet.mu.Lock()
	entry.view.WorktreeReady = true
	entry.worktrees = false
	h.api.fleet.mu.Unlock()
	if _, err := client.SubmitCommand(ctx, worktreeRequest("capability")); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("unnegotiated capability admitted", err)
	}
	h.api.fleet.mu.Lock()
	entry.worktrees = true
	entry.view.HerdrReceivedAt.Seconds -= 31
	h.api.fleet.mu.Unlock()
	if _, err := client.SubmitCommand(ctx, worktreeRequest("stale")); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("stale worktree admitted", err)
	}
}

func TestWorktreeIdempotencyBindsEveryArgument(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	h.api.workspacePolicy = coordinatorWorktreePolicy(t)
	readyWorktreeEntry(t, h)
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 5*time.Second)
	defer cancel()
	client := pb.NewFleetClient(h.connection)
	request := worktreeRequest("locked")
	if _, err := client.SubmitCommand(ctx, request); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*pb.WorktreeCreate){
		func(v *pb.WorktreeCreate) { v.ProjectId = "other" }, func(v *pb.WorktreeCreate) { v.BindingRevision = "r2" },
		func(v *pb.WorktreeCreate) { v.Name = "other" }, func(v *pb.WorktreeCreate) { v.Branch = "other" }, func(v *pb.WorktreeCreate) { v.BaseCommit = strings.Repeat("b", 40) },
	} {
		copy := proto.Clone(request).(*pb.SubmitCommandRequest)
		change(copy.WorktreeCreate)
		if _, err := client.SubmitCommand(ctx, copy); status.Code(err) != codes.AlreadyExists {
			t.Fatal("retry changed immutable arguments", err)
		}
	}
}

func TestWorktreeDispatchRechecksBothPeers(t *testing.T) {
	for _, peer := range []string{"client-1", "stable-1"} {
		t.Run(peer, func(t *testing.T) {
			h := newCommandHarness(t, t.TempDir())
			h.api.workspacePolicy = coordinatorWorktreePolicy(t)
			entry := readyWorktreeEntry(t, h)
			identify := h.api.identifyPeer
			var revoked atomic.Bool
			h.api.identifyPeer = func(ctx context.Context) (transport.PeerIdentity, error) {
				identity, err := identify(ctx)
				if revoked.Load() && identity.StableID == peer {
					identity.Tags = nil
				}
				return identity, err
			}
			ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 5*time.Second)
			defer cancel()
			record, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, worktreeRequest("revoke"))
			if err != nil {
				t.Fatal(err)
			}
			queued := <-entry.outbound
			revoked.Store(true)
			if command, err := h.api.prepareCommand(entry, queued); err != nil || command != nil {
				t.Fatal("revoked worktree dispatched", err)
			}
			revoked.Store(false)
			rejected := awaitCommand(t, h, record.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_REJECTED)
			if rejected.Detail != "authorization_changed" || len(rejected.Audit) != 2 {
				t.Fatal("missing durable rejection")
			}
		})
	}
}

func TestWorktreeReadinessRequiresFreshNegotiationAndDoesNotRestore(t *testing.T) {
	var fleet fleetStore
	now := time.Now()
	entry, err := fleet.begin("node-1", "stable-1", true, now)
	if err != nil {
		t.Fatal(err)
	}
	entry.probes, entry.workspaces = true, true
	if err := fleet.update(entry, readyState(1), now); err != nil {
		t.Fatal(err)
	}
	if err := fleet.heartbeat(entry, now, true, true, true); err != nil {
		t.Fatal(err)
	}
	list, err := fleet.list(now)
	if err != nil || list.Nodes[0].WorktreeReady {
		t.Fatal("unnegotiated worktree ready", err)
	}
	entry.worktrees = true
	if err := fleet.heartbeat(entry, now, true, true, true); err != nil {
		t.Fatal(err)
	}
	list, err = fleet.list(now.Add(herdrStaleAfter - time.Nanosecond))
	if err != nil || !list.Nodes[0].WorktreeReady {
		t.Fatal("fresh worktree not ready", err)
	}
	expired, err := fleet.list(now.Add(herdrStaleAfter))
	if err != nil || expired.Nodes[0].WorktreeReady {
		t.Fatal("worktree freshness boundary missed", err)
	}
	var restored fleetStore
	if err := restored.restore(list.Nodes, now); err != nil {
		t.Fatal(err)
	}
	history, err := restored.list(now)
	if err != nil || history.Nodes[0].WorktreeReady {
		t.Fatal("restored history advertised worktree readiness", err)
	}
}

func TestWorktreeOutcomeIsBoundToNodeAndExactOperation(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	h.api.workspacePolicy = coordinatorWorktreePolicy(t)
	entry := readyWorktreeEntry(t, h)
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 5*time.Second)
	defer cancel()
	record, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, worktreeRequest("result-bound"))
	if err != nil {
		t.Fatal(err)
	}
	command, err := h.api.prepareCommand(entry, <-entry.outbound)
	if err != nil || command == nil {
		t.Fatal("worktree did not dispatch", err)
	}
	if err := h.api.bindNode("stable-2", "node-2"); err != nil {
		t.Fatal(err)
	}
	foreign, err := h.api.fleet.begin("node-2", "stable-2", true, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	result := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "worktree_created",
		WorktreeCreate: &pb.WorktreeCreateResult{ProjectId: "AgentFlow", BindingRevision: "r1", WorkspaceId: "w2", Name: "task-one", Branch: "task-one", BaseCommit: strings.Repeat("a", 40)}}
	if _, err := h.api.finishCommand(foreign, result); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("foreign node supplied worktree outcome", err)
	}
	mismatch := proto.Clone(result).(*pb.CommandResult)
	mismatch.WorktreeCreate.Branch = "other"
	if _, err := h.api.finishCommand(entry, mismatch); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("result changed requested branch", err)
	}
	mixed := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "workspace_created",
		WorkspaceEnsure: &pb.WorkspaceEnsureResult{ProjectId: "AgentFlow", BindingRevision: "r1", WorkspaceId: "w2", Created: true}}
	if _, err := h.api.finishCommand(entry, mixed); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("workspace result completed a worktree", err)
	}
	running := awaitCommand(t, h, record.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_RUNNING)
	if running.WorktreeCreate != nil || running.WorkspaceEnsure != nil {
		t.Fatal("rejected outcome corrupted journal")
	}
	if _, err := h.api.finishCommand(entry, result); err != nil {
		t.Fatal(err)
	}
	completed := awaitCommand(t, h, command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	if !proto.Equal(completed.WorktreeCreate, result.WorktreeCreate) {
		t.Fatal("typed outcome not durably retained")
	}
}
