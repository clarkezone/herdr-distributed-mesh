package node

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func managedCheckout(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "checkout")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("git", "-C", path, "init", "--quiet").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	return path
}

func managedHandler(t *testing.T) *commandHandler {
	t.Helper()
	journal := testJournal(t)
	h := &commandHandler{nodeID: "test-node", journal: journal, managedProjects: projects.NewManaged("test-node"),
		workspaceNegotiated: true, worktreeNegotiated: true, verifyCoordinator: allowCoordinator}
	if err := h.restoreProjects(context.Background(), journal, nil); err != nil {
		t.Fatal(err)
	}
	return h
}

func managedConfig(t *testing.T, generation uint64) *pb.ProjectConfig {
	return &pb.ProjectConfig{NodeInstanceId: "test-node", ProjectId: "project", Generation: generation, CheckoutPath: managedCheckout(t)}
}

func applyManaged(t *testing.T, h *commandHandler, config *pb.ProjectConfig) *pb.ProjectAck {
	t.Helper()
	ack, err := h.applyProject(context.Background(), config)
	if err != nil || ack.GetStatus() != "applied" {
		t.Fatalf("apply: %v %v", ack, err)
	}
	return ack
}

func managedCommand(generation uint64) *pb.Command {
	c := probe()
	c.CommandType = protocol.WorkspaceEnsureCommandType
	c.WorkspaceEnsure = &pb.WorkspaceEnsure{ProjectId: "project", BindingRevision: protocol.ProjectRevision(generation)}
	c.SubmittedRequest = &pb.SubmitCommandRequest{NodeInstanceId: c.TargetId, IdempotencyKey: c.IdempotencyKey,
		CommandType: c.CommandType, Ttl: c.Ttl, WorkspaceEnsure: &pb.WorkspaceEnsure{ProjectId: "project"}}
	return c
}

func TestManagedNodeDeliveryFencesAndPrivateCache(t *testing.T) {
	h := managedHandler(t)
	ctx := context.Background()
	first := managedConfig(t, 1)
	ack := applyManaged(t, h, first)
	repeated := applyManaged(t, h, first)
	if !proto.Equal(ack, repeated) {
		t.Fatal("repeated apply changed outcome")
	}
	bad := proto.Clone(first).(*pb.ProjectConfig)
	bad.CheckoutPath = managedCheckout(t)
	if _, err := h.applyProject(ctx, bad); status.Code(err) != codes.InvalidArgument {
		t.Fatal("same-generation retarget accepted", err)
	}
	bad.Generation = 2
	bad.CheckoutPath = filepath.Join(t.TempDir(), "missing")
	invalid, err := h.applyProject(ctx, bad)
	if err != nil || invalid.GetStatus() != "invalid" || invalid.CheckoutPath != "" || invalid.WorktreeRoot != "" {
		t.Fatalf("invalid: %v %v", invalid, err)
	}
	if _, err := h.resolveManaged("project", protocol.ProjectRevision(1)); err == nil {
		t.Fatal("old binding ready after invalid update")
	}
	if stale, err := h.applyProject(ctx, first); err != nil || stale != nil {
		t.Fatal("stale config not ignored", stale, err)
	}
	restored := &commandHandler{nodeID: h.nodeID, managedProjects: projects.NewManaged(h.nodeID), verifyCoordinator: allowCoordinator}
	if err := restored.restoreProjects(ctx, h.projectCache, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.resolveManaged("project", protocol.ProjectRevision(1)); err == nil {
		t.Fatal("cache became authority on restart")
	}
	if stale, err := restored.applyProject(ctx, first); err != nil || stale != nil {
		t.Fatal("restart lost generation fence")
	}
	next := managedConfig(t, 3)
	applyManaged(t, restored, next)
	if _, err := restored.resolveManaged("project", protocol.ProjectRevision(3)); err != nil {
		t.Fatal(err)
	}
}

func TestManagedNodeReconfigurationCannotRetargetAdmittedEffect(t *testing.T) {
	h := managedHandler(t)
	first := managedConfig(t, 1)
	ack := applyManaged(t, h, first)
	started, proceed := make(chan projects.Binding, 1), make(chan struct{})
	h.ensureWorkspace = func(ctx context.Context, _ herdr.Config, b projects.Binding) (*pb.WorkspaceEnsureResult, error) {
		started <- b
		select {
		case <-proceed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if b.Path != ack.CheckoutPath || b.WorktreeRoot != ack.WorktreeRoot || b.ValidatePath() != nil {
			t.Error("effect retargeted")
		}
		return workspaceSuccess(b, true), nil
	}
	c := managedCommand(1)
	type outcome struct {
		result *pb.CommandResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() { r, err := h.handle(context.Background(), c, time.Now()); done <- outcome{r, err} }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("mutation not admitted")
	}
	next := managedConfig(t, 2)
	applyManaged(t, h, next)
	close(proceed)
	got := <-done
	if got.err != nil || got.result.GetStatus() != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		t.Fatalf("frozen effect: %v %v", got.result, got.err)
	}
	h.ensureWorkspace = func(context.Context, herdr.Config, projects.Binding) (*pb.WorkspaceEnsureResult, error) {
		t.Fatal("historical retry reexecuted")
		return nil, nil
	}
	retry, err := h.handle(context.Background(), c, time.Now())
	if err != nil || !proto.Equal(retry, got.result) {
		t.Fatal("historical receipt changed", err)
	}
	stale := managedCommand(1)
	rejected, err := h.handle(context.Background(), stale, time.Now())
	if err != nil || rejected.GetStatus() != pb.CommandStatus_COMMAND_STATUS_REJECTED {
		t.Fatal("stale command executed", rejected, err)
	}
}

func TestManagedNodeOwnershipSurvivesNonemptyRootAndRestart(t *testing.T) {
	h := managedHandler(t)
	config := managedConfig(t, 1)
	ack := applyManaged(t, h, config)
	if err := os.WriteFile(filepath.Join(ack.WorktreeRoot, "retained-worktree"), []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	next := &commandHandler{nodeID: h.nodeID, managedProjects: projects.NewManaged(h.nodeID), verifyCoordinator: allowCoordinator}
	if err := next.restoreProjects(context.Background(), h.projectCache, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := next.resolveManaged("project", protocol.ProjectRevision(1)); err == nil {
		t.Fatal("restored ownership conferred readiness")
	}
	applyManaged(t, next, config)
	if data, err := os.ReadFile(filepath.Join(ack.WorktreeRoot, "retained-worktree")); err != nil || string(data) != "untouched" {
		t.Fatal("owned root modified")
	}
}
