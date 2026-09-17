package control

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type lifecycleClientFixture struct {
	pb.FleetClient
	submit func(context.Context, *pb.SubmitCommandRequest) (*pb.CommandRecord, error)
	get    func(context.Context, *pb.GetCommandRequest) (*pb.CommandRecord, error)
}

func (f lifecycleClientFixture) SubmitCommand(ctx context.Context, r *pb.SubmitCommandRequest, _ ...grpc.CallOption) (*pb.CommandRecord, error) {
	return f.submit(ctx, r)
}

func (f lifecycleClientFixture) GetCommand(ctx context.Context, r *pb.GetCommandRequest, _ ...grpc.CallOption) (*pb.CommandRecord, error) {
	return f.get(ctx, r)
}

func lifecycleClientRecord(t *testing.T, request *pb.SubmitCommandRequest) *pb.CommandRecord {
	t.Helper()
	expires := time.Now().Add(request.Ttl.AsDuration())
	execution, err := protocol.LifecycleExecutionDeadline(expires, request.AgentStart.StartupTimeoutMs)
	if err != nil {
		t.Fatal(err)
	}
	start := proto.Clone(request.AgentStart).(*pb.AgentStart)
	start.BindingRevision, start.InitialPrompt = "managed:1", ""
	command := &pb.Command{CommandId: protocol.NewCommandID(), IdempotencyKey: request.IdempotencyKey, CommandType: request.CommandType,
		TargetId: request.NodeInstanceId, Ttl: request.Ttl, ExpiresAt: timestamppb.New(expires),
		ExecutionExpiresAt: timestamppb.New(execution), Actor: &pb.Actor{ActorId: "actor", Role: pb.Role_ROLE_CONTROLLER},
		SubmittedRequest: proto.Clone(request).(*pb.SubmitCommandRequest), AgentStart: start}
	return &pb.CommandRecord{Command: command, Status: pb.CommandStatus_COMMAND_STATUS_RUNNING, Detail: "dispatched",
		AgentLifecycle: &pb.AgentLifecycleReceipt{Handle: &pb.AgentLifecycleHandle{WorkspaceId: start.WorkspaceId,
			Target: &pb.AgentTarget{SessionIncarnation: strings.Repeat("a", 64)}}, PaneOutcome: "unknown",
			LaunchOutcome: "not_attempted", PromptOutcome: "not_attempted", StopOutcome: "not_attempted",
			Sequence: 1, Stages: []*pb.AgentLifecycleStage{{Stage: "create_pane", Before: true, Outcome: "unknown"}}}}
}

func TestLifecycleClientWaitCancellationWritesPartialReceiptWithoutResubmit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	submits := 0
	fixture := lifecycleClientFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		submits++
		record := lifecycleClientRecord(t, request)
		cancel()
		return record, nil
	}, get: func(context.Context, *pb.GetCommandRequest) (*pb.CommandRecord, error) {
		t.Fatal("canceled client should not poll")
		return nil, nil
	}}
	var output bytes.Buffer
	err := StartAgent(ctx, Options{FleetClient: fixture, RequiredServerTag: "server", Output: &output, JSON: true},
		"node", "fixed-key", &pb.AgentStart{ProjectId: "project", WorkspaceId: "ws:1", Name: "agent", Provider: "copilot"}, 10*time.Second)
	if !errors.Is(err, context.Canceled) || submits != 1 || !strings.Contains(err.Error(), "fixed-key") {
		t.Fatalf("cancellation lost recovery key or resubmitted: %v", err)
	}
	var record pb.CommandRecord
	if err := protojson.Unmarshal(output.Bytes(), &record); err != nil || record.AgentLifecycle.GetPaneOutcome() != "unknown" {
		t.Fatalf("client lost partial receipt: %v", err)
	}
}

func TestLifecycleClientRejectsChangedCheckpointIdentity(t *testing.T) {
	var first *pb.CommandRecord
	fixture := lifecycleClientFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		first = lifecycleClientRecord(t, request)
		return first, nil
	}, get: func(context.Context, *pb.GetCommandRequest) (*pb.CommandRecord, error) {
		next := proto.Clone(first).(*pb.CommandRecord)
		next.Command.AgentStart.Name = "different"
		return next, nil
	}}
	var output bytes.Buffer
	err := StartAgent(context.Background(), Options{FleetClient: fixture, RequiredServerTag: "server", Output: &output, JSON: true},
		"node", "fixed", &pb.AgentStart{ProjectId: "project", WorkspaceId: "ws:1", Name: "agent", Provider: "copilot"}, time.Second)
	if err == nil || !strings.Contains(err.Error(), "changed the lifecycle command identity") {
		t.Fatalf("server changed fingerprint while waiting: %v", err)
	}
}
