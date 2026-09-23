package protocol

import (
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func lifecycleTestCommand(t *testing.T) *pb.Command {
	t.Helper()
	request, err := NormalizeCommandRequest(&pb.SubmitCommandRequest{NodeInstanceId: "node", IdempotencyKey: "key",
		CommandType: AgentStartCommandType, AgentStart: &pb.AgentStart{ProjectId: "project", WorkspaceId: "ws:1",
			Name: "agent", Provider: "copilot", InitialPrompt: strings.Repeat("p", MaxAgentPromptBytes)}})
	if err != nil {
		t.Fatal(err)
	}
	start := proto.Clone(request.AgentStart).(*pb.AgentStart)
	start.InitialPrompt, start.BindingRevision = "", "managed:1"
	expires := time.Now().Add(DefaultCommandTTL)
	execution, err := LifecycleExecutionDeadline(expires, start.StartupTimeoutMs)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.Command{CommandId: NewCommandID(), IdempotencyKey: request.IdempotencyKey,
		CommandType: request.CommandType, TargetId: request.NodeInstanceId, Ttl: request.Ttl,
		Actor: &pb.Actor{ActorId: "actor", Role: pb.Role_ROLE_CONTROLLER}, ExpiresAt: timestamppb.New(expires),
		ExecutionExpiresAt: timestamppb.New(execution), SubmittedRequest: request, AgentStart: start}
}

func TestLifecycleOriginalPromptStoredOnce(t *testing.T) {
	command := lifecycleTestCommand(t)
	if err := ValidateCommand(command, "node"); err != nil {
		t.Fatal(err)
	}
	effective, err := EffectiveAgentStart(command)
	if err != nil || len(effective.InitialPrompt) != MaxAgentPromptBytes {
		t.Fatalf("effective prompt: %v", err)
	}
	payload, err := proto.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(payload), effective.InitialPrompt) != 1 {
		t.Fatal("prompt was duplicated in the journal body")
	}
	for _, mutate := range []func(*pb.Command){
		func(c *pb.Command) { c.AgentStart.InitialPrompt = "copy" },
		func(c *pb.Command) { c.AgentStart.Name = "changed" },
		func(c *pb.Command) { c.AgentStart.Provider = "claude" },
		func(c *pb.Command) { c.ExecutionExpiresAt = c.ExpiresAt },
		func(c *pb.Command) { c.SubmittedRequest = nil },
		func(c *pb.Command) { c.AgentStop = &pb.AgentStop{} },
	} {
		changed := proto.Clone(command).(*pb.Command)
		mutate(changed)
		if ValidateCommand(changed, "node") == nil {
			t.Fatal("changed fingerprint or unsafe execution bound accepted")
		}
	}
}

func TestLifecycleStopRequiresExplicitDefaultIncarnation(t *testing.T) {
	stop := &pb.AgentStop{WorkspaceId: "ws:1", TabId: "tab:1", Provider: "copilot",
		Target: &pb.AgentTarget{PaneId: "pane:1", TerminalId: "term:1"}}
	if ValidateAgentStop(stop) == nil {
		t.Fatal("unpinned default stop accepted")
	}
	stop.Target.SessionIncarnation = strings.Repeat("a", 64)
	if err := ValidateAgentStop(stop); err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleReceiptRejectsRetriesAndFalseSuccess(t *testing.T) {
	c := lifecycleTestCommand(t)
	r := &pb.AgentLifecycleReceipt{Handle: &pb.AgentLifecycleHandle{WorkspaceId: "ws:1",
		Target: &pb.AgentTarget{SessionIncarnation: strings.Repeat("a", 64)}},
		PaneOutcome: "unknown", LaunchOutcome: "not_attempted", PromptOutcome: "not_attempted", StopOutcome: "not_attempted",
		Sequence: 1, Stages: []*pb.AgentLifecycleStage{{Stage: "create_pane", Before: true, Outcome: "unknown"}}}
	if err := ValidateLifecycleReceipt(r, c); err != nil {
		t.Fatal(err)
	}
	result := &pb.CommandResult{CommandId: c.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED,
		Detail: "agent_started", AgentLifecycle: r}
	if ValidateLifecycleResult(result, c) == nil {
		t.Fatal("unconfirmed effect became success")
	}
	r.Stages = append(r.Stages, &pb.AgentLifecycleStage{Stage: "create_pane", Outcome: "not_attempted"},
		&pb.AgentLifecycleStage{Stage: "create_pane", Before: true, Outcome: "unknown"})
	r.Sequence = 3
	if ValidateLifecycleReceipt(r, c) == nil {
		t.Fatal("second attempt of a lifecycle effect was accepted")
	}
}
