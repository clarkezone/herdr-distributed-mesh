package server

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func coordinatorWorkspacePolicy(t *testing.T) *projects.Policy {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, []byte(`{"projects":[{"project_id":"AgentFlow","node_id":"node-1","binding_revision":"r1","actor_ids":["client-1"]}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	policy, err := projects.Load(path, "")
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func workspaceRequest(key string) *pb.SubmitCommandRequest {
	return &pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: key, CommandType: protocol.WorkspaceEnsureCommandType,
		WorkspaceEnsure: &pb.WorkspaceEnsure{ProjectId: "AgentFlow", BindingRevision: "r1"}}
}

func readyWorkspaceEntry(t *testing.T, h *commandHarness) *fleetEntry {
	t.Helper()
	if err := h.api.bindNode("stable-1", "node-1"); err != nil {
		t.Fatal(err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("test-peer", "node"))
	entry, err := h.api.fleet.beginSession(ctx, "node-1", "stable-1", true, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.api.fleet.mu.Lock()
	entry.probes, entry.workspaces = true, true
	h.api.fleet.mu.Unlock()
	if err := h.api.fleet.update(entry, readyState(1), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := h.api.fleet.heartbeat(entry, time.Now(), true, true); err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestWorkspaceAdmissionRequiresProjectActorRevisionAndFreshReadiness(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	h.api.workspacePolicy = coordinatorWorkspacePolicy(t)
	entry := readyWorkspaceEntry(t, h)
	client := pb.NewFleetClient(h.connection)
	policy := h.api.workspacePolicy
	h.api.workspacePolicy = nil
	deniedCtx, deniedCancel := context.WithTimeout(commandPeer(context.Background(), "client"), 3*time.Second)
	_, deniedErr := client.SubmitCommand(deniedCtx, workspaceRequest("policy-disabled"))
	deniedCancel()
	if status.Code(deniedErr) != codes.PermissionDenied {
		t.Fatalf("missing policy admitted workspace: %v", deniedErr)
	}
	h.api.workspacePolicy = policy
	for _, tc := range []struct {
		name, peer string
		change     func(*pb.SubmitCommandRequest)
	}{
		{"actor", "client2", func(*pb.SubmitCommandRequest) {}},
		{"project", "client", func(r *pb.SubmitCommandRequest) { r.WorkspaceEnsure.ProjectId = "other" }},
		{"revision", "client", func(r *pb.SubmitCommandRequest) { r.WorkspaceEnsure.BindingRevision = "r2" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := workspaceRequest(tc.name)
			tc.change(request)
			ctx, cancel := context.WithTimeout(commandPeer(context.Background(), tc.peer), 3*time.Second)
			defer cancel()
			if _, err := client.SubmitCommand(ctx, request); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("unauthorized mutation admitted: %v", err)
			}
		})
	}
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 3*time.Second)
	defer cancel()
	request := workspaceRequest("stable-key")
	first, err := client.SubmitCommand(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	h.api.fleet.mu.Lock()
	entry.view.WorkspaceReady = false
	h.api.fleet.mu.Unlock()
	retried, err := client.SubmitCommand(ctx, request)
	if err != nil || retried.Command.CommandId != first.Command.CommandId {
		t.Fatal("unready node prevented historical retry", err)
	}
	request.IdempotencyKey = "unready"
	if _, err := client.SubmitCommand(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unready workspace admitted: %v", err)
	}
	h.api.fleet.mu.Lock()
	entry.view.WorkspaceReady = true
	entry.view.HerdrReceivedAt.Seconds -= 31
	h.api.fleet.mu.Unlock()
	if _, err := client.SubmitCommand(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale workspace admitted: %v", err)
	}
}

func TestWorkspaceDispatchRechecksWhoIsAndAuditsRevocation(t *testing.T) {
	for _, revokedPeer := range []string{"client-1", "stable-1"} {
		t.Run(revokedPeer, func(t *testing.T) {
			h := newCommandHarness(t, t.TempDir())
			h.api.workspacePolicy = coordinatorWorkspacePolicy(t)
			entry := readyWorkspaceEntry(t, h)
			var revoked atomic.Bool
			identify := h.api.identifyPeer
			h.api.identifyPeer = func(ctx context.Context) (transport.PeerIdentity, error) {
				identity, err := identify(ctx)
				if revoked.Load() && identity.StableID == revokedPeer {
					identity.Tags = nil
				}
				return identity, err
			}
			ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 3*time.Second)
			defer cancel()
			record, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, workspaceRequest("revoke"))
			if err != nil {
				t.Fatal(err)
			}
			queued := <-entry.outbound
			revoked.Store(true)
			command, err := h.api.prepareCommand(entry, queued)
			if err != nil || command != nil {
				t.Fatal("revoked operation dispatched", err)
			}
			revoked.Store(false)
			rejected := awaitCommand(t, h, record.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_REJECTED)
			if rejected.Detail != "authorization_changed" || len(rejected.Audit) != 2 {
				t.Fatal("revocation missing durable audit")
			}
		})
	}
}

func TestWorkspaceRetryCannotChangeBinding(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	h.api.workspacePolicy = coordinatorWorkspacePolicy(t)
	readyWorkspaceEntry(t, h)
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 3*time.Second)
	defer cancel()
	client := pb.NewFleetClient(h.connection)
	request := workspaceRequest("binding-locked")
	if _, err := client.SubmitCommand(ctx, request); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"project", "revision"} {
		copy := proto.Clone(request).(*pb.SubmitCommandRequest)
		if field == "project" {
			copy.WorkspaceEnsure.ProjectId = "other"
		} else {
			copy.WorkspaceEnsure.BindingRevision = "r2"
		}
		if _, err := client.SubmitCommand(ctx, copy); status.Code(err) != codes.AlreadyExists {
			t.Fatalf("retry rebound original command: %v", err)
		}
	}
}

func TestWorkspaceReadinessExpiresAtBoundaryAndNeverSurvivesRestore(t *testing.T) {
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
	if err := fleet.heartbeat(entry, now, true, true); err != nil {
		t.Fatal(err)
	}
	fresh, err := fleet.list(now.Add(herdrStaleAfter - time.Nanosecond))
	if err != nil || !fresh.Nodes[0].WorkspaceReady {
		t.Fatal("fresh workspace not ready", err)
	}
	expired, err := fleet.list(now.Add(herdrStaleAfter))
	if err != nil || expired.Nodes[0].WorkspaceReady {
		t.Fatal("workspace freshness boundary not enforced", err)
	}
	var restored fleetStore
	if err := restored.restore(fresh.Nodes, now); err != nil {
		t.Fatal(err)
	}
	history, err := restored.list(now)
	if err != nil || history.Nodes[0].WorkspaceReady || history.Nodes[0].CommandReady || history.Nodes[0].Connected {
		t.Fatal("restored history advertised live mutation readiness", err)
	}
}
