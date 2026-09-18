package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func workspaceCommand() *pb.Command {
	command := probe()
	command.CommandType = protocol.WorkspaceEnsureCommandType
	command.WorkspaceEnsure = &pb.WorkspaceEnsure{ProjectId: "project", BindingRevision: "revision"}
	return command
}

func workspacePolicy(t *testing.T) *projects.Policy {
	t.Helper()
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"projects": []any{map[string]any{
		"project_id": "project", "node_id": "test-node", "binding_revision": "revision",
		"actor_ids": []string{"controller"}, "path": directory,
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

func workspaceSuccess(binding projects.Binding, created bool) *pb.WorkspaceEnsureResult {
	return &pb.WorkspaceEnsureResult{
		ProjectId: binding.ProjectID, BindingRevision: binding.Revision, WorkspaceId: "workspace", Created: created,
	}
}

func allowCoordinator(context.Context) error { return nil }

type observedJournal struct {
	commandJournal
	afterClaim       func()
	onComplete       func(context.Context, *pb.CommandResult)
	afterAcknowledge func(string)
}

func (j observedJournal) Claim(ctx context.Context, command *pb.Command) (*pb.CommandResult, bool, error) {
	result, claimed, err := j.commandJournal.Claim(ctx, command)
	if claimed && j.afterClaim != nil {
		j.afterClaim()
	}
	return result, claimed, err
}

func (j observedJournal) Complete(ctx context.Context, result *pb.CommandResult) error {
	if j.onComplete != nil {
		j.onComplete(ctx, result)
	}
	return j.commandJournal.Complete(ctx, result)
}

func (j observedJournal) Acknowledge(ctx context.Context, id string, status pb.CommandStatus) error {
	if err := j.commandJournal.Acknowledge(ctx, id, status); err != nil {
		return err
	}
	if j.afterAcknowledge != nil {
		j.afterAcknowledge(id)
	}
	return nil
}

func TestWorkspaceRequiresOptInAndNegotiation(t *testing.T) {
	handler := &commandHandler{nodeID: "test-node"}
	if _, err := handler.handle(context.Background(), workspaceCommand(), time.Now()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unnegotiated command reached journal: %v", err)
	}
	handler.journal = testJournal(t)
	handler.workspaceNegotiated = true
	handler.ensureWorkspace = func(context.Context, herdr.Config, projects.Binding) (*pb.WorkspaceEnsureResult, error) {
		t.Fatal("absent policy executed")
		return nil, nil
	}
	command := workspaceCommand()
	result, err := handler.handle(context.Background(), command, time.Now())
	if err != nil || result.GetDetail() != "project_not_authorized" {
		t.Fatalf("absent policy: %v %v", result, err)
	}
	stored, claimed, err := handler.journal.Claim(context.Background(), command)
	if err != nil || claimed || !proto.Equal(result, stored) {
		t.Fatalf("rejection was not durable: %v %v", stored, err)
	}
}

func TestWorkspaceOptionsFailBeforeNetwork(t *testing.T) {
	policy := workspacePolicy(t)
	for _, missing := range []string{"journal", "socket", "tag"} {
		t.Run(missing, func(t *testing.T) {
			options := Options{InstanceID: "test-node", WorkspacePolicy: policy,
				CommandJournalPath: filepath.Join(t.TempDir(), "node.db"), HerdrSocket: "unused",
				RequiredServerTag: DefaultRequiredServerTag}
			switch missing {
			case "journal":
				options.CommandJournalPath = ""
			case "socket":
				options.HerdrSocket = " "
			case "tag":
				options.RequiredServerTag = " "
			}
			if _, err := RunSession(context.Background(), nil, options); err == nil {
				t.Fatal("invalid session options accepted")
			}
			if err := Run(context.Background(), options); err == nil {
				t.Fatal("invalid production options accepted")
			}
		})
	}
}

func TestWorkspaceAuthorizationAndExpiryAfterDurableClaim(t *testing.T) {
	policy := workspacePolicy(t)
	for _, test := range []string{"actor", "revision", "project", "absolute", "queued", "claim-delay"} {
		t.Run(test, func(t *testing.T) {
			claimed := false
			journal := observedJournal{commandJournal: testJournal(t), afterClaim: func() {
				claimed = true
				if test == "claim-delay" {
					time.Sleep(30 * time.Millisecond)
				}
			}}
			handler := &commandHandler{nodeID: "test-node", journal: journal, workspacePolicy: policy, workspaceNegotiated: true,
				ensureWorkspace: func(context.Context, herdr.Config, projects.Binding) (*pb.WorkspaceEnsureResult, error) {
					t.Fatal("rejected or expired command executed")
					return nil, nil
				}}
			command := workspaceCommand()
			receivedAt := time.Now()
			detail := "project_not_authorized"
			switch test {
			case "actor":
				command.Actor.ActorId = "other"
			case "revision":
				command.WorkspaceEnsure.BindingRevision = "old"
			case "project":
				command.WorkspaceEnsure.ProjectId = "other"
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
				t.Fatalf("not durable: %v %v", stored, err)
			}
		})
	}
}

func TestWorkspaceTypedResultsAndSanitizedErrors(t *testing.T) {
	policy := workspacePolicy(t)
	for _, test := range []struct {
		name    string
		err     error
		created bool
		detail  string
		status  pb.CommandStatus
	}{
		{"created", nil, true, "workspace_created", pb.CommandStatus_COMMAND_STATUS_SUCCEEDED},
		{"present", nil, false, "workspace_present", pb.CommandStatus_COMMAND_STATUS_SUCCEEDED},
		{"precondition", herdr.ErrWorkspacePrecondition, false, "precondition_failed", pb.CommandStatus_COMMAND_STATUS_REJECTED},
		{"ambiguous", herdr.ErrWorkspaceAmbiguous, false, "ambiguous_workspace", pb.CommandStatus_COMMAND_STATUS_REJECTED},
		{"unavailable", herdr.ErrWorkspaceUnavailable, false, "herdr_unavailable", pb.CommandStatus_COMMAND_STATUS_REJECTED},
		{"indeterminate", herdr.ErrWorkspaceIndeterminate, false, "herdr_outcome_unknown", pb.CommandStatus_COMMAND_STATUS_INDETERMINATE},
		{"unexpected", errors.New("private checkout and raw IPC data"), false, "herdr_outcome_unknown", pb.CommandStatus_COMMAND_STATUS_INDETERMINATE},
	} {
		t.Run(test.name, func(t *testing.T) {
			claimed := false
			executions := 0
			journal := observedJournal{commandJournal: testJournal(t), afterClaim: func() { claimed = true }}
			handler := &commandHandler{nodeID: "test-node", journal: journal, workspacePolicy: policy, workspaceNegotiated: true,
				verifyCoordinator: allowCoordinator,
				ensureWorkspace: func(ctx context.Context, _ herdr.Config, binding projects.Binding) (*pb.WorkspaceEnsureResult, error) {
					executions++
					deadline, ok := ctx.Deadline()
					if !claimed || !ok || time.Until(deadline) <= 5*time.Second {
						t.Fatal("effect lacks durable claim or inherited the storage deadline")
					}
					return workspaceSuccess(binding, test.created), test.err
				}}
			command := workspaceCommand()
			result, err := handler.handle(context.Background(), command, time.Now())
			if err != nil || result.GetDetail() != test.detail || result.GetStatus() != test.status {
				t.Fatalf("result=%v err=%v", result, err)
			}
			if err := protocol.ValidateResultForCommand(result, command); err != nil {
				t.Fatal(err)
			}
			handler.workspacePolicy = nil
			command.Ttl = durationpb.New(time.Nanosecond)
			replayed, err := handler.handle(context.Background(), command, time.Now().Add(-time.Hour))
			if err != nil || !proto.Equal(replayed, result) || executions != 1 {
				t.Fatalf("known result changed without policy: %v %v count=%d", replayed, err, executions)
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

func TestWorkspaceExpiredEffectStillCommitsOutcome(t *testing.T) {
	completed := false
	journal := observedJournal{commandJournal: testJournal(t), onComplete: func(ctx context.Context, _ *pb.CommandResult) {
		completed = true
		if ctx.Err() != nil {
			t.Fatal("completion inherited expired mutation context")
		}
	}}
	handler := &commandHandler{nodeID: "test-node", journal: journal, workspacePolicy: workspacePolicy(t), workspaceNegotiated: true,
		verifyCoordinator: allowCoordinator,
		ensureWorkspace: func(ctx context.Context, _ herdr.Config, _ projects.Binding) (*pb.WorkspaceEnsureResult, error) {
			<-ctx.Done()
			return nil, herdr.ErrWorkspaceIndeterminate
		}}
	command := workspaceCommand()
	command.Ttl = durationpb.New(50 * time.Millisecond)
	result, err := handler.handle(context.Background(), command, time.Now())
	if err != nil || !completed || result.GetStatus() != pb.CommandStatus_COMMAND_STATUS_INDETERMINATE {
		t.Fatalf("expired effect not durably uncertain: %v %v", result, err)
	}
}

func TestWorkspaceExecutorUsesEarliestDeadline(t *testing.T) {
	for _, absoluteFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(absoluteFirst), func(t *testing.T) {
			command := workspaceCommand()
			receivedAt := time.Now().Add(-time.Second)
			want := receivedAt.Add(command.Ttl.AsDuration())
			if absoluteFirst {
				want = time.Now().Add(time.Second)
				command.ExpiresAt = timestamppb.New(want)
			}
			handler := &commandHandler{nodeID: "test-node", journal: testJournal(t),
				workspacePolicy: workspacePolicy(t), workspaceNegotiated: true,
				verifyCoordinator: allowCoordinator,
				ensureWorkspace: func(ctx context.Context, _ herdr.Config, binding projects.Binding) (*pb.WorkspaceEnsureResult, error) {
					got, ok := ctx.Deadline()
					if !ok || !got.Equal(want) {
						t.Fatalf("deadline=%v want=%v", got, want)
					}
					return workspaceSuccess(binding, false), nil
				}}
			if _, err := handler.handle(context.Background(), command, receivedAt); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type replayJournal struct {
	commandJournal
	result *pb.CommandResult
}

func (j replayJournal) Claim(context.Context, *pb.Command) (*pb.CommandResult, bool, error) {
	return j.result, false, nil
}

func TestWorkspaceStoredResultMustMatchCommand(t *testing.T) {
	command := workspaceCommand()
	for _, result := range []*pb.CommandResult{
		{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "pong"},
		{CommandId: protocol.NewCommandID(), Status: pb.CommandStatus_COMMAND_STATUS_REJECTED, Detail: "project_unresolved"},
		{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "workspace_present",
			WorkspaceEnsure: &pb.WorkspaceEnsureResult{ProjectId: "other", BindingRevision: "revision", WorkspaceId: "workspace"}},
	} {
		handler := &commandHandler{nodeID: "test-node", journal: replayJournal{result: result}, workspaceNegotiated: true}
		if value, err := handler.handle(context.Background(), command, time.Now()); value != nil || !isPermanentSessionError(err) {
			t.Fatalf("corrupt replay returned: %v %v", value, err)
		}
	}
}

func TestWorkspaceInvalidSuccessFailsBeforeCommit(t *testing.T) {
	for _, malformed := range []string{"nil", "binding", "unknown"} {
		t.Run(malformed, func(t *testing.T) {
			handler := &commandHandler{nodeID: "test-node", journal: observedJournal{commandJournal: testJournal(t),
				onComplete: func(context.Context, *pb.CommandResult) { t.Fatal("invalid outcome committed") }},
				workspacePolicy: workspacePolicy(t), workspaceNegotiated: true,
				verifyCoordinator: allowCoordinator,
				ensureWorkspace: func(_ context.Context, _ herdr.Config, binding projects.Binding) (*pb.WorkspaceEnsureResult, error) {
					value := workspaceSuccess(binding, true)
					switch malformed {
					case "nil":
						value = nil
					case "binding":
						value.ProjectId = "wrong"
					case "unknown":
						value.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
					}
					return value, nil
				}}
			if result, err := handler.handle(context.Background(), workspaceCommand(), time.Now()); result != nil || !isPermanentSessionError(err) {
				t.Fatalf("invalid outcome accepted: %v %v", result, err)
			}
		})
	}
}

func TestWorkspaceQuarantineReplaysWithoutExecution(t *testing.T) {
	journal := testJournal(t)
	first := workspaceCommand()
	if _, claimed, err := journal.Claim(context.Background(), first); err != nil || !claimed {
		t.Fatalf("seed uncertain project: %v", err)
	}
	command := workspaceCommand()
	command.Actor.ActorId = "different"
	command.WorkspaceEnsure.BindingRevision = "different"
	handler := &commandHandler{nodeID: "test-node", journal: journal, workspaceNegotiated: true,
		ensureWorkspace: func(context.Context, herdr.Config, projects.Binding) (*pb.WorkspaceEnsureResult, error) {
			t.Fatal("quarantined project executed")
			return nil, nil
		}}
	result, err := handler.handle(context.Background(), command, time.Now())
	if err != nil || result.GetDetail() != "project_unresolved" {
		t.Fatalf("quarantine changed: %v %v", result, err)
	}
}

func TestWorkspaceBaselineFreshness(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		status string
		age    time.Duration
		want   bool
	}{
		{"ready", 0, true}, {"ready", 30 * time.Second, true},
		{"ready", 30*time.Second + time.Nanosecond, false},
		{"ready", -time.Nanosecond, false}, {"unavailable", 0, false},
	} {
		state := &pb.HerdrState{Status: test.status, ObservedAt: timestamppb.New(now.Add(-test.age))}
		if got := workspaceBaselineReady(state, now); got != test.want {
			t.Fatalf("status=%s age=%v ready=%v", test.status, test.age, got)
		}
	}
	if workspaceBaselineReady(nil, now) || workspaceBaselineReady(&pb.HerdrState{Status: "ready"}, now) {
		t.Fatal("missing baseline accepted")
	}
}
