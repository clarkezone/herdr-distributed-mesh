package control

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

func TestNamedAgentDiscoveryFailureNeverSubmits(t *testing.T) {
	for _, failure := range []string{"session_replaced", "session_unavailable", "wrong-session", "unpinned"} {
		t.Run(failure, func(t *testing.T) {
			target := &pb.AgentTarget{PaneId: "w1:p1", TerminalId: "term_original", SessionName: "worker"}
			client := agentClientFixture{
				query: func(context.Context, *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
					result := &pb.AgentQueryResult{QueryId: protocol.NewCommandID()}
					switch failure {
					case "wrong-session", "unpinned":
						result.Agent = agentClientView()
						result.Agent.Target.SessionName = "other"
						result.Agent.Target.SessionIncarnation = strings.Repeat("a", 64)
						if failure == "unpinned" {
							result.Agent.Target.SessionName = "worker"
							result.Agent.Target.SessionIncarnation = ""
						}
					default:
						result.ErrorCode = failure
					}
					return result, nil
				},
				submit: func(context.Context, *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
					t.Fatal("failed named discovery submitted an effect")
					return nil, nil
				},
			}
			err := Agent(context.Background(), Options{Output: &bytes.Buffer{}, RequiredServerTag: "tag:server", FleetClient: client},
				&pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_GET, Target: target},
				&pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT, Target: target}, "retry-key", time.Second)
			if err == nil {
				t.Fatal("unsafe discovery accepted")
			}
		})
	}
}

func TestSessionEnsureUncertaintyReturnsReceiptWithoutNewSubmission(t *testing.T) {
	var out bytes.Buffer
	calls := 0
	client := agentClientFixture{submit: func(context.Context, *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		calls++
		return &pb.CommandRecord{Command: &pb.Command{CommandId: protocol.NewCommandID()},
			Status: pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, Detail: "startup_uncertain"}, nil
	}}
	err := EnsureSession(context.Background(), Options{Output: &out, JSON: true, RequiredServerTag: "tag:server", FleetClient: client},
		"node-1", "same-key", "worker", time.Second)
	if err == nil || !strings.Contains(err.Error(), "INDETERMINATE") || !strings.Contains(out.String(), "startup_uncertain") || calls != 1 {
		t.Fatalf("uncertainty hidden or retried: calls=%d err=%v output=%s", calls, err, &out)
	}
}
