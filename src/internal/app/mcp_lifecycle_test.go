package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshmcp"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func mcpLifecycleInputs(named bool) (meshmcp.StartAgentInput, meshmcp.StopAgentInput) {
	scope := meshmcp.SessionInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}}
	if named {
		scope.HerdrSessionID, scope.HerdrSessionIncarnation = "worker", strings.Repeat("b", 64)
	}
	start := meshmcp.StartAgentInput{
		EnsureWorkspaceInput: meshmcp.EnsureWorkspaceInput{
			WorkspaceInput:  meshmcp.WorkspaceInput{SessionInput: scope, ProjectID: "project-1"},
			BindingRevision: "revision-1", IdempotencyKey: "start-original",
		},
		WorkspaceID: "w1", Name: "agent-1", Provider: "copilot", InitialPrompt: "hello\nworld",
	}
	stop := meshmcp.StopAgentInput{SessionInput: scope,
		Target:      meshmcp.AgentTarget{PaneID: "w1:p1", TerminalID: "term-1"},
		WorkspaceID: "w1", TabID: "w1:t1", Provider: "copilot", IdempotencyKey: "stop-original",
	}
	if !named {
		stop.HerdrSessionIncarnation = strings.Repeat("a", 64)
	}
	return start, stop
}

func mcpLifecycleRecord(t *testing.T, request *pb.SubmitCommandRequest, commandStatus pb.CommandStatus) *pb.CommandRecord {
	t.Helper()
	submitted := proto.Clone(request).(*pb.SubmitCommandRequest)
	expires := time.Now().Add(request.Ttl.AsDuration())
	command := &pb.Command{
		CommandId: strings.Repeat("a", 32), CommandType: request.CommandType, TargetId: request.NodeInstanceId,
		IdempotencyKey: request.IdempotencyKey, Ttl: request.Ttl, Actor: &pb.Actor{ActorId: "controller", Role: pb.Role_ROLE_CONTROLLER},
		ExpiresAt: timestamppb.New(expires), SubmittedRequest: submitted,
	}
	handle := &pb.AgentLifecycleHandle{}
	if request.AgentStart != nil {
		command.AgentStart = proto.Clone(request.AgentStart).(*pb.AgentStart)
		command.AgentStart.InitialPrompt = ""
		if command.AgentStart.SessionIncarnation == "" {
			command.AgentStart.SessionIncarnation = strings.Repeat("a", 64)
		}
		deadline, err := protocol.LifecycleExecutionDeadline(expires, command.AgentStart.StartupTimeoutMs)
		if err != nil {
			t.Fatal(err)
		}
		command.ExecutionExpiresAt = timestamppb.New(deadline)
		handle.Target = &pb.AgentTarget{SessionName: command.AgentStart.SessionName, SessionIncarnation: command.AgentStart.SessionIncarnation}
		handle.WorkspaceId = command.AgentStart.WorkspaceId
	} else {
		command.AgentStop = proto.Clone(request.AgentStop).(*pb.AgentStop)
		handle.Target = proto.Clone(request.AgentStop.Target).(*pb.AgentTarget)
		handle.WorkspaceId, handle.TabId, handle.Provider = request.AgentStop.WorkspaceId, request.AgentStop.TabId, request.AgentStop.Provider
	}
	if err := protocol.ValidateCommand(command, request.NodeInstanceId); err != nil {
		t.Fatalf("invalid lifecycle fixture command: %v", err)
	}
	receipt := &pb.AgentLifecycleReceipt{Handle: handle, PaneOutcome: "not_attempted", LaunchOutcome: "not_attempted",
		PromptOutcome: "not_attempted", StopOutcome: "not_attempted"}
	apply := func(stage, outcome string) {
		receipt.Stages = append(receipt.Stages, &pb.AgentLifecycleStage{Stage: stage, Before: true, Outcome: "unknown"})
		if outcome != "unknown" {
			receipt.Stages = append(receipt.Stages, &pb.AgentLifecycleStage{Stage: stage, Outcome: outcome})
		}
		switch stage {
		case "create_pane":
			receipt.PaneOutcome = outcome
		case "launch":
			receipt.LaunchOutcome = outcome
		case "prompt":
			receipt.PromptOutcome = outcome
		case "close_pane":
			receipt.StopOutcome = outcome
		}
		receipt.Sequence = uint32(len(receipt.Stages))
	}
	detail := "precondition_failed"
	if commandStatus != pb.CommandStatus_COMMAND_STATUS_REJECTED {
		if request.AgentStart != nil {
			handle.Target.PaneId, handle.Target.TerminalId, handle.TabId = "w1:p1", "term-1", "w1:t1"
			apply("create_pane", "confirmed")
			if commandStatus == pb.CommandStatus_COMMAND_STATUS_FAILED {
				apply("launch", "not_attempted")
				detail = "lifecycle_partial"
			} else if commandStatus == pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
				handle.Provider = request.AgentStart.Provider
				apply("launch", "confirmed")
				if request.AgentStart.InitialPrompt != "" {
					apply("prompt", "confirmed")
				}
				receipt.ObservedStatus = "working"
				detail = "agent_started"
			} else {
				apply("launch", "unknown")
				detail = "lifecycle_uncertain"
			}
		} else {
			if commandStatus == pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
				apply("close_pane", "confirmed")
				detail = "agent_stopped"
			} else {
				apply("close_pane", "unknown")
				detail = "lifecycle_uncertain"
			}
		}
	}
	record := &pb.CommandRecord{Command: command, Status: commandStatus, Detail: detail, AgentLifecycle: receipt}
	if err := protocol.ValidateLifecycleReceipt(receipt, command); err != nil {
		t.Fatalf("invalid lifecycle fixture receipt: %v", err)
	}
	return record
}

func TestMCPLifecyclePublicServiceParityAndExactRetries(t *testing.T) {
	for _, named := range []bool{false, true} {
		t.Run(map[bool]string{false: "configured-default", true: "named"}[named], func(t *testing.T) {
			start, stop := mcpLifecycleInputs(named)
			for _, operation := range []meshmcp.Operation{meshmcp.StartAgent, meshmcp.StopAgent} {
				t.Run(string(operation), func(t *testing.T) {
					var requests []*pb.SubmitCommandRequest
					client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
						requests = append(requests, proto.Clone(request).(*pb.SubmitCommandRequest))
						return mcpLifecycleRecord(t, request, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED), nil
					}}
					var ordinary bytes.Buffer
					options := mcpTestOptions(client)
					options.JSON, options.Output = true, &ordinary
					var input any
					var err error
					if operation == meshmcp.StartAgent {
						input = start
						err = control.StartAgent(context.Background(), options, start.NodeInstanceID, start.IdempotencyKey, &pb.AgentStart{
							ProjectId: start.ProjectID, BindingRevision: start.BindingRevision, WorkspaceId: start.WorkspaceID,
							Name: start.Name, Provider: start.Provider, SessionName: start.HerdrSessionID,
							SessionIncarnation: start.HerdrSessionIncarnation, InitialPrompt: start.InitialPrompt,
						}, protocol.DefaultCommandTTL)
					} else {
						input = stop
						err = control.StopAgent(context.Background(), options, stop.NodeInstanceID, stop.IdempotencyKey, &pb.AgentStop{
							Target: &pb.AgentTarget{PaneId: stop.Target.PaneID, TerminalId: stop.Target.TerminalID,
								SessionName: stop.HerdrSessionID, SessionIncarnation: stop.HerdrSessionIncarnation},
							WorkspaceId: stop.WorkspaceID, TabId: stop.TabID, Provider: stop.Provider,
						}, protocol.DefaultCommandTTL)
					}

					if err != nil {
						t.Fatal(err)
					}
					var ordinaryRecord pb.CommandRecord
					if err := protojson.Unmarshal(ordinary.Bytes(), &ordinaryRecord); err != nil {
						t.Fatal(err)
					}
					session := mcpApplicationClient(t, mcpTestOptions(client))
					for range 2 {
						result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(operation), Arguments: input})
						if err != nil || result == nil || result.IsError {
							t.Fatalf("lifecycle MCP mapping failed: %v %v", result, err)
						}
						receipt := mcpObject(t, result.StructuredContent)["agent_lifecycle"]
						want, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(ordinaryRecord.AgentLifecycle)
						if err != nil || !reflect.DeepEqual(mcpObject(t, receipt), mcpObject(t, json.RawMessage(want))) {
							t.Fatalf("ordinary lifecycle receipt changed: %+v err=%v", receipt, err)
						}
						if operation == meshmcp.StartAgent {
							value := receipt.(map[string]any)
							if value["prompt_outcome"] != "confirmed" || value["observed_status"] != "working" {
								t.Fatalf("prompt acknowledgement was converted to task success: %+v", value)
							}
						}
					}
					if len(requests) != 3 || !proto.Equal(requests[0], requests[1]) || !proto.Equal(requests[1], requests[2]) {
						t.Fatalf("same-key requests diverged from CLI or retried with new identity: %+v", requests)
					}
					if operation == meshmcp.StartAgent && (requests[1].AgentStart.StartupTimeoutMs != 30000 || requests[1].Ttl.AsDuration() != 10*time.Second) {
						t.Fatalf("startup and dispatch budgets conflated: %+v", requests[1])
					}
				})
			}
		})
	}
}

func TestMCPLifecyclePartialReceiptsAreStructuredToolErrors(t *testing.T) {
	start, stop := mcpLifecycleInputs(true)
	for _, operation := range []meshmcp.Operation{meshmcp.StartAgent, meshmcp.StopAgent} {
		for _, commandStatus := range []pb.CommandStatus{
			pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, pb.CommandStatus_COMMAND_STATUS_REJECTED, pb.CommandStatus_COMMAND_STATUS_FAILED,
		} {
			if operation == meshmcp.StopAgent && commandStatus == pb.CommandStatus_COMMAND_STATUS_FAILED {
				continue
			}
			t.Run(string(operation)+"/"+commandStatus.String(), func(t *testing.T) {
				var submitted int
				var original *pb.CommandRecord
				client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
					submitted++
					original = mcpLifecycleRecord(t, request, commandStatus)
					return original, nil
				}}
				session := mcpApplicationClient(t, mcpTestOptions(client))
				var input any = start
				if operation == meshmcp.StopAgent {
					input = stop
				}
				result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(operation), Arguments: input})
				if err != nil || result == nil || !result.IsError || result.StructuredContent == nil || submitted != 1 {
					t.Fatalf("partial lifecycle result discarded or resubmitted: %v %v submissions=%d", result, err, submitted)
				}
				expected, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(original)
				if err != nil || !reflect.DeepEqual(mcpObject(t, json.RawMessage(expected)), mcpObject(t, result.StructuredContent)) {
					t.Fatalf("partial handles/stages/status were rewritten: %+v err=%v", result.StructuredContent, err)
				}
			})
		}
	}
}

func TestMCPLifecycleNodeUnavailableRetainsDurableReceipt(t *testing.T) {
	start, stop := mcpLifecycleInputs(true)
	for _, operation := range []meshmcp.Operation{meshmcp.StartAgent, meshmcp.StopAgent} {
		for _, detail := range []string{"node_disconnected", "server_restarted"} {
			t.Run(string(operation)+"/"+detail, func(t *testing.T) {
				var original *pb.CommandRecord
				submissions := 0
				client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
					submissions++
					original = mcpLifecycleRecord(t, request, pb.CommandStatus_COMMAND_STATUS_REJECTED)
					original.Status, original.Detail, original.AgentLifecycle = pb.CommandStatus_COMMAND_STATUS_NODE_UNAVAILABLE, detail, nil
					return original, nil
				}}
				session := mcpApplicationClient(t, mcpTestOptions(client))
				var input any = start
				if operation == meshmcp.StopAgent {
					input = stop
				}
				result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(operation), Arguments: input})
				if err != nil || result == nil || !result.IsError || result.StructuredContent == nil || submissions != 1 {
					t.Fatalf("node-unavailable receipt discarded or resubmitted: %v %v submissions=%d", result, err, submissions)
				}
				expected, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(original)
				if err != nil || !reflect.DeepEqual(mcpObject(t, json.RawMessage(expected)), mcpObject(t, result.StructuredContent)) {
					t.Fatalf("durable receipt was rewritten: %+v err=%v", result.StructuredContent, err)
				}
			})
		}
	}
}

func TestMCPLifecycleCancellationRetainsKnownRunningReceipt(t *testing.T) {
	start, _ := mcpLifecycleInputs(false)
	submitted := make(chan struct{})
	var latest *pb.CommandRecord
	client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		latest = mcpLifecycleRecord(t, request, pb.CommandStatus_COMMAND_STATUS_RUNNING)
		close(submitted)
		return latest, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		value any
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		value, err := invokeMCP(ctx, mcpTestOptions(client), meshmcp.StartAgent, start)
		done <- outcome{value, err}
	}()
	select {
	case <-submitted:
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle service never received start")
	}
	cancel()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) || result.value == nil {
			t.Fatalf("cancelled wait discarded known partial receipt: %v %v", result.value, result.err)
		}
		receipt := mcpObject(t, result.value)
		if receipt["status"] != "COMMAND_STATUS_RUNNING" ||
			receipt["agent_lifecycle"].(map[string]any)["launch_outcome"] != "unknown" {
			t.Fatalf("cancellation invented task/operation completion: %+v", receipt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation was not propagated")
	}
}

func TestMCPLifecycleMCPTimeoutKeepsPartialReceipt(t *testing.T) {
	start, _ := mcpLifecycleInputs(false)
	var submissions int
	client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		submissions++
		return mcpLifecycleRecord(t, request, pb.CommandStatus_COMMAND_STATUS_RUNNING), nil
	}}
	session := mcpApplicationClientWithLimits(t, mcpTestOptions(client), meshmcp.Options{CallTimeout: 50 * time.Millisecond})
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.StartAgent), Arguments: start})
	if err != nil || result == nil || !result.IsError || result.StructuredContent == nil || submissions != 1 {
		t.Fatalf("MCP deadline discarded known partial receipt or resubmitted: %v %v calls=%d", result, err, submissions)
	}
	receipt := mcpObject(t, result.StructuredContent)
	if receipt["status"] != "COMMAND_STATUS_RUNNING" || receipt["agent_lifecycle"].(map[string]any)["launch_outcome"] != "unknown" {
		t.Fatalf("MCP deadline invented terminal/task outcome: %+v", receipt)
	}
}

func TestMCPLifecycleValidationAndNoTargetDiscovery(t *testing.T) {
	start, stop := mcpLifecycleInputs(false)
	var submissions int
	client := &mcpFleetFixture{submit: func(context.Context, *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		submissions++
		return nil, errors.New("invalid input reached executor")
	}}
	for _, change := range []func(*meshmcp.StartAgentInput){
		func(in *meshmcp.StartAgentInput) { in.IdempotencyKey = "" },
		func(in *meshmcp.StartAgentInput) { in.Name = "" },
		func(in *meshmcp.StartAgentInput) { in.Provider = "arbitrary-executable" },
		func(in *meshmcp.StartAgentInput) { in.StartupTimeoutMS = 3000 },
		func(in *meshmcp.StartAgentInput) { in.StartupTimeoutMS = 300001 },
		func(in *meshmcp.StartAgentInput) { in.StartupTimeoutMS = -1 },
		func(in *meshmcp.StartAgentInput) { in.InitialPrompt = "\x1b[2J" },
		func(in *meshmcp.StartAgentInput) {
			in.InitialPrompt = strings.Repeat("x", protocol.MaxAgentPromptBytes+1)
		},
		func(in *meshmcp.StartAgentInput) { in.HerdrSessionID = "worker" },
	} {
		input := start
		change(&input)
		if value, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.StartAgent, input); err == nil || value != nil {
			t.Fatalf("invalid start accepted: %v %v", value, err)
		}
	}
	for _, change := range []func(*meshmcp.StopAgentInput){
		func(in *meshmcp.StopAgentInput) { in.IdempotencyKey = "" },
		func(in *meshmcp.StopAgentInput) { in.HerdrSessionIncarnation = "" },
		func(in *meshmcp.StopAgentInput) { in.Target.TerminalID = "" },
		func(in *meshmcp.StopAgentInput) { in.WorkspaceID = "" },
		func(in *meshmcp.StopAgentInput) { in.TabID = "" },
		func(in *meshmcp.StopAgentInput) { in.Provider = "" },
	} {
		input := stop
		change(&input)
		if value, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.StopAgent, input); err == nil || value != nil {
			t.Fatalf("invalid stop accepted: %v %v", value, err)
		}
	}
	if submissions != 0 {
		t.Fatalf("invalid lifecycle input submitted or rediscovered: %d", submissions)
	}
}

func TestMCPLifecycleSeparateBudgetsAndNoInitialPrompt(t *testing.T) {
	start, _ := mcpLifecycleInputs(false)
	start.InitialPrompt, start.StartupTimeoutMS = "", protocol.MaxAgentStartupTimeoutMs
	client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		if request.AgentStart.StartupTimeoutMs != 300000 || request.Ttl.AsDuration() != 10*time.Second ||
			request.AgentStart.SessionName != "" || request.AgentStart.SessionIncarnation != "" || request.AgentStart.InitialPrompt != "" {
			return nil, errors.New("separate startup/dispatch/default/prompt semantics changed")
		}
		return mcpLifecycleRecord(t, request, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED), nil
	}}
	result, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.StartAgent, start)
	if err != nil || result == nil {
		t.Fatalf("maximum startup budget failed: %v %v", result, err)
	}
	if mcpObject(t, result)["agent_lifecycle"].(map[string]any)["prompt_outcome"] != "not_attempted" {
		t.Fatal("empty initial prompt fabricated a submission")
	}
}

func TestMCPLifecycleRejectsFalseStageSuccess(t *testing.T) {
	start, _ := mcpLifecycleInputs(false)
	client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		record := mcpLifecycleRecord(t, request, pb.CommandStatus_COMMAND_STATUS_INDETERMINATE)
		record.Status, record.Detail = pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, "agent_started"
		return record, nil
	}}
	value, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.StartAgent, start)
	if err == nil || value != nil {
		t.Fatalf("unconfirmed launch accepted as success: %v %v", value, err)
	}
}
