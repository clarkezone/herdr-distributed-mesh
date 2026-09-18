package server

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func namedPendingCommand(t *testing.T) *pb.Command {
	t.Helper()
	request, err := protocol.NormalizeCommandRequest(&pb.SubmitCommandRequest{NodeInstanceId: "node-1",
		IdempotencyKey: "original-key", CommandType: protocol.AgentStartCommandType, AgentStart: &pb.AgentStart{
			ProjectId: "demo", BindingRevision: "managed:1", WorkspaceId: "ws:1", Name: "smoke", Provider: "copilot",
			SessionName: "main", SessionIncarnation: strings.Repeat("b", 64), InitialPrompt: "private operator prompt"}})
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(request.Ttl.AsDuration())
	deadline, err := protocol.LifecycleExecutionDeadline(expires, request.AgentStart.StartupTimeoutMs)
	if err != nil {
		t.Fatal(err)
	}
	resolved := proto.Clone(request.AgentStart).(*pb.AgentStart)
	resolved.InitialPrompt = ""
	return &pb.Command{CommandId: protocol.NewCommandID(), TargetId: request.NodeInstanceId, IdempotencyKey: request.IdempotencyKey,
		CommandType: request.CommandType, Actor: &pb.Actor{ActorId: "client-1", Role: pb.Role_ROLE_CONTROLLER},
		Ttl: request.Ttl, ExpiresAt: timestamppb.New(expires), ExecutionExpiresAt: timestamppb.New(deadline),
		AgentStart: resolved, SubmittedRequest: request}
}

func TestNamedAgentRPCCrossClientDurabilityAndDuplicateGuard(t *testing.T) {
	root := t.TempDir()
	h := newCommandHarness(t, root)
	ctx := context.Background()
	if err := h.api.commands.Bind(ctx, "stable-1", "node-1"); err != nil {
		t.Fatal(err)
	}
	c := namedPendingCommand(t)
	if _, _, err := h.api.commands.CreateCommand(ctx, c, c.ExpiresAt.AsTime().Add(-c.Ttl.AsDuration())); err != nil {
		t.Fatal(err)
	}
	client := pb.NewFleetClient(h.connection)
	query := &pb.ResolveNamedAgentRequest{NodeInstanceId: "node-1", Name: "smoke", SessionName: "main"}
	got, err := client.ResolveNamedAgent(commandPeer(ctx, "client2"), query)
	redacted := proto.Clone(c).(*pb.Command)
	redacted.SubmittedRequest.AgentStart.InitialPrompt = ""
	if err != nil || !proto.Equal(got.GetRecord().GetCommand(), redacted) || !got.GetInitialPromptPresent() {
		t.Fatalf("cross-client original receipt: %v %v", got, err)
	}
	if strings.Contains(got.String(), "private operator prompt") {
		t.Fatal("named resolution leaked prompt contents")
	}
	other := proto.Clone(c.SubmittedRequest).(*pb.SubmitCommandRequest)
	other.IdempotencyKey = "new-key-cannot-bypass"
	if _, err := client.SubmitCommand(commandPeer(ctx, "client2"), other); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("duplicate name was not fenced: %v", err)
	}
	if _, err := client.SubmitCommand(commandPeer(ctx, "client"), c.SubmittedRequest); err != nil {
		t.Fatalf("exact original retry denied: %v", err)
	}
	if _, err := client.ResolveNamedAgent(commandPeer(ctx, "denied"), query); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unauthorized lookup: %v", err)
	}
	h.stop()
	h = newCommandHarness(t, root)
	client = pb.NewFleetClient(h.connection)
	got, err = client.ResolveNamedAgent(commandPeer(ctx, "client2"), query)
	if err != nil || !proto.Equal(got.GetRecord().GetCommand(), redacted) || !got.GetInitialPromptPresent() {
		t.Fatalf("restart changed original identity: %v %v", got, err)
	}
	query.Name = "unknown"
	if _, err := client.ResolveNamedAgent(commandPeer(ctx, "client2"), query); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown name guessed: %v", err)
	}
	query.Name = strings.Repeat("x", 1000)
	if _, err := client.ResolveNamedAgent(commandPeer(ctx, "client2"), query); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized lookup accepted: %v", err)
	}
}
