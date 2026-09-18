package node

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func worktreeCommand() *pb.Command {
	command := probe()
	command.CommandType = protocol.WorktreeCreateCommandType
	command.WorktreeCreate = &pb.WorktreeCreate{
		ProjectId: "project", BindingRevision: "revision", Name: "feature",
		Branch: "feature-branch", BaseCommit: strings.Repeat("a", 40),
	}
	return command
}

func worktreePolicy(t *testing.T) *projects.Policy {
	t.Helper()
	checkout := t.TempDir()
	if err := os.Mkdir(filepath.Join(checkout, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"projects": []any{map[string]any{
		"project_id": "project", "node_id": "test-node", "binding_revision": "revision",
		"actor_ids": []string{"controller"}, "path": checkout,
		"allow_worktrees": true, "worktree_root": t.TempDir(),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	policy, err := projects.Load(path, "test-node")
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func worktreeSuccess(request *pb.WorktreeCreate) *pb.WorktreeCreateResult {
	return &pb.WorktreeCreateResult{
		ProjectId: request.ProjectId, BindingRevision: request.BindingRevision, WorkspaceId: "worktree-workspace",
		Name: request.Name, Branch: request.Branch, BaseCommit: request.BaseCommit,
	}
}

func TestWorktreeRequiresNegotiationBeforeClaim(t *testing.T) {
	handler := &commandHandler{nodeID: "test-node"}
	if _, err := handler.handle(context.Background(), worktreeCommand(), time.Now()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unnegotiated worktree reached journal: %v", err)
	}
}

func TestWorktreeInvalidInputNeverClaims(t *testing.T) {
	for name, mutate := range map[string]func(*pb.Command){
		"missing-request": func(c *pb.Command) { c.WorktreeCreate = nil },
		"mixed-types":     func(c *pb.Command) { c.WorkspaceEnsure = workspaceCommand().WorkspaceEnsure },
		"raw-payload":     func(c *pb.Command) { c.Payload = &structpb.Struct{} },
		"target":          func(c *pb.Command) { c.TargetId = "other-node" },
		"actor-role":      func(c *pb.Command) { c.Actor.Role = pb.Role_ROLE_NODE },
		"actor-hop":       func(c *pb.Command) { c.Actor.HopCount = 1 },
		"name-path":       func(c *pb.Command) { c.WorktreeCreate.Name = "../checkout" },
		"branch-ref":      func(c *pb.Command) { c.WorktreeCreate.Branch = "refs/heads/feature" },
		"base-ref":        func(c *pb.Command) { c.WorktreeCreate.BaseCommit = "HEAD" },
		"short-sha":       func(c *pb.Command) { c.WorktreeCreate.BaseCommit = strings.Repeat("a", 39) },
		"unknown":         func(c *pb.Command) { c.WorktreeCreate.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			command := worktreeCommand()
			mutate(command)
			handler := &commandHandler{nodeID: "test-node", worktreeNegotiated: true}
			if _, err := handler.handle(context.Background(), command, time.Now()); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid worktree reached journal: %v", err)
			}
		})
	}
}

func TestWorktreeAuthorizationAndExpiryAreDurable(t *testing.T) {
	policy := worktreePolicy(t)
	for _, name := range []string{"no-policy", "default-disabled", "actor", "revision", "project", "node", "absolute", "queued", "claim-delay"} {
		t.Run(name, func(t *testing.T) {
			claimed := false
			local := testJournal(t)
			if name == "node" {
				var err error
				local, err = state.OpenNodeJournal(context.Background(), filepath.Join(t.TempDir(), "node.db"), "other-node")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { local.Close() })
			}
			journal := observedJournal{commandJournal: local, afterClaim: func() {
				claimed = true
				if name == "claim-delay" {
					time.Sleep(30 * time.Millisecond)
				}
			}}
			handler := &commandHandler{nodeID: "test-node", journal: journal, workspacePolicy: policy, worktreeNegotiated: true,
				verifyCoordinator: func(context.Context) error {
					t.Error("unauthorized or expired command refreshed peer")
					return errors.New("unexpected verification")
				},
				createWorktree: func(context.Context, herdr.Config, projects.Binding, *pb.WorktreeCreate) (*pb.WorktreeCreateResult, error) {
					t.Error("unauthorized or expired command executed")
					return nil, herdr.ErrWorkspaceIndeterminate
				}}
			command := worktreeCommand()
			receivedAt := time.Now()
			detail := "project_not_authorized"
			switch name {
			case "no-policy":
				handler.workspacePolicy = nil
			case "default-disabled":
				handler.workspacePolicy = workspacePolicy(t)
			case "actor":
				command.Actor.ActorId = "other"
			case "revision":
				command.WorktreeCreate.BindingRevision = "old"
			case "project":
				command.WorktreeCreate.ProjectId = "other"
			case "node":
				handler.nodeID, command.TargetId = "other-node", "other-node"
			case "absolute":
				command.ExpiresAt = timestamppb.New(time.Now().Add(-time.Second))
				detail = "deadline_expired"
			case "queued":
				receivedAt = receivedAt.Add(-time.Minute)
				detail = "deadline_expired"
			case "claim-delay":
				command.Ttl = durationpb.New(10 * time.Millisecond)
				detail = "deadline_expired"
			}
			result, err := handler.handle(context.Background(), command, receivedAt)
			if err != nil || !claimed || result.GetDetail() != detail {
				t.Fatalf("claim=%v result=%v err=%v", claimed, result, err)
			}
			stored, again, err := journal.Claim(context.Background(), command)
			if err != nil || again || !proto.Equal(stored, result) {
				t.Fatalf("rejection not durable: %v %v", stored, err)
			}
		})
	}
}

func TestWorktreeTypedOutcomesAndReplay(t *testing.T) {
	policy := worktreePolicy(t)
	for _, test := range []struct {
		name   string
		err    error
		detail string
		status pb.CommandStatus
	}{
		{"created", nil, "worktree_created", pb.CommandStatus_COMMAND_STATUS_SUCCEEDED},
		{"created-sha256", nil, "worktree_created", pb.CommandStatus_COMMAND_STATUS_SUCCEEDED},
		{"precondition", herdr.ErrWorkspacePrecondition, "precondition_failed", pb.CommandStatus_COMMAND_STATUS_REJECTED},
		{"unavailable", herdr.ErrWorkspaceUnavailable, "herdr_unavailable", pb.CommandStatus_COMMAND_STATUS_REJECTED},
		{"partial-effect", herdr.ErrWorkspaceIndeterminate, "herdr_outcome_unknown", pb.CommandStatus_COMMAND_STATUS_INDETERMINATE},
		{"unknown", errors.New("private path and IPC payload"), "herdr_outcome_unknown", pb.CommandStatus_COMMAND_STATUS_INDETERMINATE},
		{"unexpected-ambiguity", herdr.ErrWorkspaceAmbiguous, "herdr_outcome_unknown", pb.CommandStatus_COMMAND_STATUS_INDETERMINATE},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := worktreeCommand()
			if test.name == "created-sha256" {
				command.WorktreeCreate.BaseCommit = strings.Repeat("b", 64)
			}
			original := proto.Clone(command).(*pb.Command)
			claimed, completed, calls := false, false, 0
			journal := observedJournal{commandJournal: testJournal(t), afterClaim: func() { claimed = true },
				onComplete: func(ctx context.Context, result *pb.CommandResult) {
					completed = true
					deadline, ok := ctx.Deadline()
					if ctx.Err() != nil || !ok || time.Until(deadline) > journalOperationTimeout {
						t.Error("completion lacks independent bounded journal context")
					}
				}}
			handler := &commandHandler{nodeID: "test-node", journal: journal, workspacePolicy: policy, worktreeNegotiated: true,
				verifyCoordinator: allowCoordinator,
				createWorktree: func(ctx context.Context, _ herdr.Config, binding projects.Binding, request *pb.WorktreeCreate) (*pb.WorktreeCreateResult, error) {
					calls++
					deadline, ok := ctx.Deadline()
					if !claimed || !ok || time.Until(deadline) <= journalOperationTimeout ||
						!binding.AllowWorktrees || binding.ProjectID != request.ProjectId || binding.Revision != request.BindingRevision ||
						!proto.Equal(request, original.WorktreeCreate) {
						t.Error("effect missing durable claim, deadline, binding, or full original request")
					}
					result := worktreeSuccess(request)
					request.Name = "executor-mutated"
					return result, test.err
				}}
			result, err := handler.handle(context.Background(), command, time.Now())
			if err != nil || !completed || result.GetDetail() != test.detail || result.GetStatus() != test.status {
				t.Fatalf("result=%v complete=%v err=%v", result, completed, err)
			}
			if !proto.Equal(command, original) {
				t.Fatal("executor changed original command")
			}
			if err := protocol.ValidateResultForCommand(result, command); err != nil {
				t.Fatal(err)
			}
			handler.workspacePolicy, handler.verifyCoordinator = nil, nil
			command.Ttl = durationpb.New(time.Nanosecond)
			replayed, err := handler.handle(context.Background(), command, time.Now().Add(-time.Hour))
			if err != nil || !proto.Equal(replayed, result) || calls != 1 {
				t.Fatalf("replay changed/effect repeated: %v %v calls=%d", replayed, err, calls)
			}
			if err := handler.acknowledge(context.Background(), &pb.CommandAck{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_FAILED}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("mismatched receipt accepted: %v", err)
			}
			if err := handler.acknowledge(context.Background(), &pb.CommandAck{CommandId: command.CommandId, Status: result.Status}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorktreePeerVerificationFailsClosedAndDurably(t *testing.T) {
	policy := worktreePolicy(t)
	for _, name := range []string{"missing", "no-peer", "revoked", "expired"} {
		t.Run(name, func(t *testing.T) {
			command := worktreeCommand()
			claimed := false
			journal := observedJournal{commandJournal: testJournal(t), afterClaim: func() { claimed = true }}
			handler := &commandHandler{nodeID: "test-node", journal: journal, workspacePolicy: policy, worktreeNegotiated: true,
				createWorktree: func(context.Context, herdr.Config, projects.Binding, *pb.WorktreeCreate) (*pb.WorktreeCreateResult, error) {
					t.Error("unverified worktree executed")
					return nil, herdr.ErrWorkspaceIndeterminate
				}}
			detail := "precondition_failed"
			switch name {
			case "no-peer":
				handler.verifyCoordinator = streamPeerVerifier(context.Background(), func(context.Context, string) error {
					t.Error("missing stream peer reached verifier")
					return nil
				})
			case "revoked":
				handler.verifyCoordinator = func(ctx context.Context) error {
					deadline, ok := ctx.Deadline()
					if !claimed || !ok || time.Until(deadline) > workspacePeerVerificationTimeout {
						t.Error("peer verification lacks claim or deadline")
					}
					return errors.New("tag revoked")
				}
			case "expired":
				command.Ttl = durationpb.New(30 * time.Millisecond)
				detail = "deadline_expired"
				handler.verifyCoordinator = func(ctx context.Context) error {
					<-ctx.Done()
					return nil
				}
			}
			result, err := handler.handle(context.Background(), command, time.Now())
			if err != nil || result.GetDetail() != detail {
				t.Fatalf("verification accepted: %v %v", result, err)
			}
			stored, again, err := journal.Claim(context.Background(), command)
			if err != nil || again || !proto.Equal(stored, result) {
				t.Fatalf("rejection not durable: %v %v", stored, err)
			}
		})
	}
}

func TestWorktreeDeadlineAndIndependentCompletion(t *testing.T) {
	policy := worktreePolicy(t)
	for _, absoluteFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "ttl", true: "absolute"}[absoluteFirst], func(t *testing.T) {
			journal := testJournal(t)
			command := worktreeCommand()
			receivedAt := time.Now()
			command.Ttl = durationpb.New(200 * time.Millisecond)
			want := receivedAt.Add(command.Ttl.AsDuration())
			if absoluteFirst {
				want = receivedAt.Add(100 * time.Millisecond)
				command.ExpiresAt = timestamppb.New(want)
			}
			completed := false
			handler := &commandHandler{nodeID: "test-node", workspacePolicy: policy, worktreeNegotiated: true,
				journal: observedJournal{commandJournal: journal, onComplete: func(ctx context.Context, _ *pb.CommandResult) {
					completed = true
					if ctx.Err() != nil {
						t.Error("journal completion inherited expired effect context")
					}
				}},
				verifyCoordinator: func(ctx context.Context) error {
					if got, ok := ctx.Deadline(); !ok || !got.Equal(want) {
						t.Errorf("verification deadline=%v want=%v", got, want)
					}
					return nil
				},
				createWorktree: func(ctx context.Context, _ herdr.Config, _ projects.Binding, _ *pb.WorktreeCreate) (*pb.WorktreeCreateResult, error) {
					if got, ok := ctx.Deadline(); !ok || !got.Equal(want) {
						t.Errorf("effect deadline=%v want=%v", got, want)
					}
					<-ctx.Done()
					return nil, herdr.ErrWorkspaceIndeterminate
				}}
			result, err := handler.handle(context.Background(), command, receivedAt)
			if err != nil || !completed || result.GetStatus() != pb.CommandStatus_COMMAND_STATUS_INDETERMINATE {
				t.Fatalf("partial effect not durably uncertain: %v %v", result, err)
			}
		})
	}
}

func TestWorktreeInvalidSuccessNeverCommits(t *testing.T) {
	for _, name := range []string{"nil", "project", "revision", "name", "branch", "base", "unknown"} {
		t.Run(name, func(t *testing.T) {
			handler := &commandHandler{nodeID: "test-node", workspacePolicy: worktreePolicy(t), worktreeNegotiated: true,
				journal: observedJournal{commandJournal: testJournal(t), onComplete: func(context.Context, *pb.CommandResult) {
					t.Error("invalid success committed")
				}}, verifyCoordinator: allowCoordinator,
				createWorktree: func(_ context.Context, _ herdr.Config, _ projects.Binding, request *pb.WorktreeCreate) (*pb.WorktreeCreateResult, error) {
					result := worktreeSuccess(request)
					switch name {
					case "nil":
						return nil, nil
					case "project":
						result.ProjectId = "other"
					case "revision":
						result.BindingRevision = "other"
					case "name":
						result.Name = "other"
					case "branch":
						result.Branch = "other"
					case "base":
						result.BaseCommit = strings.Repeat("b", 40)
					case "unknown":
						result.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
					}
					return result, nil
				}}
			if result, err := handler.handle(context.Background(), worktreeCommand(), time.Now()); result != nil || !isPermanentSessionError(err) {
				t.Fatalf("invalid success accepted: %v %v", result, err)
			}
		})
	}
}

func TestWorktreeTerminalReplayAfterJournalReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")
	journal, err := state.OpenNodeJournal(context.Background(), path, "test-node")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { journal.Close() })
	command := worktreeCommand()
	calls := 0
	handler := &commandHandler{nodeID: "test-node", journal: journal, workspacePolicy: worktreePolicy(t), worktreeNegotiated: true,
		verifyCoordinator: allowCoordinator,
		createWorktree: func(context.Context, herdr.Config, projects.Binding, *pb.WorktreeCreate) (*pb.WorktreeCreateResult, error) {
			calls++
			return nil, herdr.ErrWorkspaceIndeterminate
		}}
	result, err := handler.handle(context.Background(), command, time.Now())
	if err != nil || result.GetDetail() != "herdr_outcome_unknown" {
		t.Fatalf("partial effect: %v %v", result, err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = state.OpenNodeJournal(context.Background(), path, "test-node")
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	handler.journal, handler.workspacePolicy, handler.verifyCoordinator = journal, nil, nil
	replayed, err := handler.handle(context.Background(), command, time.Now())
	if err != nil || !proto.Equal(replayed, result) || calls != 1 {
		t.Fatalf("reopened result changed/reexecuted: %v %v calls=%d", replayed, err, calls)
	}
}

func TestMutationCrossTypeQuarantineReplaysWithoutAuthorizationOrEffect(t *testing.T) {
	for _, worktreeFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "workspace-first", true: "worktree-first"}[worktreeFirst], func(t *testing.T) {
			first, second := workspaceCommand(), worktreeCommand()
			if worktreeFirst {
				first, second = second, first
			}
			journal := testJournal(t)
			if _, claimed, err := journal.Claim(context.Background(), first); err != nil || !claimed {
				t.Fatalf("seed uncertain effect: %v", err)
			}
			second.Actor.ActorId = "different"
			handler := &commandHandler{nodeID: "test-node", journal: journal, workspaceNegotiated: true, worktreeNegotiated: true,
				verifyCoordinator: func(context.Context) error {
					t.Error("quarantine consulted peer authorization")
					return nil
				}}
			result, err := handler.handle(context.Background(), second, time.Now())
			if err != nil || result.GetDetail() != "project_unresolved" {
				t.Fatalf("cross-type quarantine failed: %v %v", result, err)
			}
			replayed, err := handler.handle(context.Background(), second, time.Now())
			if err != nil || !proto.Equal(replayed, result) {
				t.Fatalf("quarantine replay changed: %v %v", replayed, err)
			}
		})
	}
}
