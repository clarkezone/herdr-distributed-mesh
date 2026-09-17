package control

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type agentClientFixture struct {
	pb.FleetClient
	query  func(context.Context, *pb.AgentQueryRequest) (*pb.AgentQueryResult, error)
	submit func(context.Context, *pb.SubmitCommandRequest) (*pb.CommandRecord, error)
}

func (f agentClientFixture) QueryAgent(ctx context.Context, q *pb.AgentQueryRequest, _ ...grpc.CallOption) (*pb.AgentQueryResult, error) {
	return f.query(ctx, q)
}
func (f agentClientFixture) SubmitCommand(ctx context.Context, q *pb.SubmitCommandRequest, _ ...grpc.CallOption) (*pb.CommandRecord, error) {
	return f.submit(ctx, q)
}
func agentClientView() *pb.AgentView {
	return &pb.AgentView{Target: &pb.AgentTarget{PaneId: "w1:p1", TerminalId: "term_original", AgentSessionId: "session-original"}, WorkspaceId: "w1", TabId: "w1:t1", Provider: "copilot", Status: "idle"}
}

func TestAgentClientPinsDiscoveredTarget(t *testing.T) {
	for _, controlMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "prompt"}[controlMode], func(t *testing.T) {
			q := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ, Target: &pb.AgentTarget{PaneId: "w1:p1"}}
			lookup := proto.Clone(q).(*pb.AgentQueryRequest)
			lookup.Kind = pb.AgentQueryKind_AGENT_QUERY_KIND_GET
			calls := 0
			client := agentClientFixture{query: func(ctx context.Context, req *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
				calls++
				if calls == 1 && (req.Kind != pb.AgentQueryKind_AGENT_QUERY_KIND_GET || req.Target.TerminalId != "") {
					t.Fatal("missing discovery")
				}
				if calls == 2 && req.Target.TerminalId != "term_original" {
					t.Fatal("read re-resolved target")
				}
				result := &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: agentClientView()}
				if req.Kind == pb.AgentQueryKind_AGENT_QUERY_KIND_READ {
					result.Text = "visible response"
				}
				return result, nil
			}, submit: func(ctx context.Context, req *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
				calls++
				if !proto.Equal(req.AgentControl.Target, agentClientView().Target) || req.AgentControl.Text != "hello\nworld" || req.IdempotencyKey != "fixed-key" {
					t.Fatalf("submission changed: %v", req)
				}
				return &pb.CommandRecord{Command: &pb.Command{CommandId: protocol.NewCommandID(), AgentControl: req.AgentControl}, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED,
					AgentControl: &pb.AgentControlResult{Target: req.AgentControl.Target, ObservedStatus: "idle"}}, nil
			}}
			var action *pb.AgentControl
			if controlMode {
				action = &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, Text: "hello\nworld"}
			}
			var out bytes.Buffer
			if err := agentWithClient(context.Background(), Options{Output: &out, JSON: true}, client, q, lookup, action, "fixed-key", time.Second); err != nil {
				t.Fatal(err)
			}
			if calls != 2 || !json.Valid(out.Bytes()) || strings.Count(out.String(), "\n") != 1 {
				t.Fatalf("calls=%d output=%s", calls, &out)
			}
		})
	}
}

func TestAgentClientExactRetryDoesNotRediscover(t *testing.T) {
	q := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Target: agentClientView().Target}
	client := agentClientFixture{query: func(context.Context, *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		t.Fatal("retry rediscovered")
		return nil, nil
	},
		submit: func(_ context.Context, r *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
			return nil, status.Error(codes.AlreadyExists, "key target conflict")
		}}
	err := agentWithClient(context.Background(), Options{Output: &bytes.Buffer{}}, client, q, nil, &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT}, "fixed-key", time.Second)
	if err == nil || !strings.Contains(err.Error(), "fixed-key") || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("lost exact retry failure: %v", err)
	}
}

func TestAgentClientRejectsUnsafeOrFailedQueries(t *testing.T) {
	for _, test := range []struct {
		name   string
		result *pb.AgentQueryResult
	}{
		{"error", &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), ErrorCode: "timeout"}},
		{"unsafe", &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: agentClientView(), Text: "\x1b[2J"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := agentClientFixture{query: func(context.Context, *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) { return test.result, nil }}
			if _, err := queryAgent(context.Background(), client, &pb.AgentQueryRequest{Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ, Target: agentClientView().Target}); err == nil {
				t.Fatal("unsafe/failed query accepted")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := agentClientFixture{query: func(ctx context.Context, _ *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		return nil, ctx.Err()
	}}
	if _, err := queryAgent(ctx, client, &pb.AgentQueryRequest{}); err == nil {
		t.Fatal("cancellation lost")
	}
}

func TestNamedAgentDiscoveryAndExactRetry(t *testing.T) {
	for _, operation := range []string{"get", "read", "wait", "prompt", "input", "interrupt", "retry"} {
		t.Run(operation, func(t *testing.T) {
			view := agentClientView()
			view.Target.SessionName = "worker"
			view.Target.SessionIncarnation = strings.Repeat("a", 64)
			target := proto.Clone(view.Target).(*pb.AgentTarget)
			if operation != "retry" {
				target.SessionIncarnation = ""
			}
			selection := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_GET, Target: target}
			var action *pb.AgentControl
			switch operation {
			case "read":
				selection.Kind = pb.AgentQueryKind_AGENT_QUERY_KIND_READ
			case "wait":
				selection.Kind = pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT
			case "prompt":
				action = &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, Target: target, Text: "hello"}
			case "input":
				action = &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT, Target: target, Keys: []string{"enter"}}
			case "interrupt", "retry":
				action = &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT, Target: target}
			}
			original := proto.Clone(selection)
			var originalAction proto.Message
			if action != nil {
				originalAction = proto.Clone(action)
			}
			queries, submissions := 0, 0
			client := agentClientFixture{
				query: func(_ context.Context, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
					queries++
					if operation == "retry" {
						t.Fatal("fully pinned retry performed GET/preflight")
					}
					if queries == 1 && (request.Kind != pb.AgentQueryKind_AGENT_QUERY_KIND_GET || request.Target.SessionName != "worker" || request.Target.SessionIncarnation != "") {
						t.Fatalf("named unpinned terminal did not discover: %v", request)
					}
					if queries > 1 && !proto.Equal(request.Target, view.Target) {
						t.Fatalf("query not fully pinned: %v", request)
					}
					return &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: view}, nil
				},
				submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
					submissions++
					if !proto.Equal(request.AgentControl.Target, view.Target) || request.IdempotencyKey != "fixed-key" {
						t.Fatalf("submission changed identity: %v", request)
					}
					return &pb.CommandRecord{Command: &pb.Command{CommandId: protocol.NewCommandID()}, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED}, nil
				},
			}
			err := Agent(context.Background(), Options{RequiredServerTag: "tag:server", FleetClient: client, Output: &bytes.Buffer{}}, selection, action, "fixed-key", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			wantQueries := 1
			if operation == "retry" {
				wantQueries = 0
			} else if operation == "read" || operation == "wait" {
				wantQueries = 2
			}
			if queries != wantQueries || (submissions == 1) != (action != nil) {
				t.Fatalf("queries=%d submissions=%d", queries, submissions)
			}
			if !proto.Equal(selection, original) || (action != nil && !proto.Equal(action, originalAction)) {
				t.Fatal("caller request mutated")
			}
		})
	}
}
