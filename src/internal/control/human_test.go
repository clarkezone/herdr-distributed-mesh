package control

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestCommandHumanStatusesAndRetryIdentity(t *testing.T) {
	for status, want := range map[pb.CommandStatus]string{
		pb.CommandStatus_COMMAND_STATUS_UNSPECIFIED:      "unknown (not reported)",
		pb.CommandStatus_COMMAND_STATUS_ACCEPTED:         "accepted (waiting to run)",
		pb.CommandStatus_COMMAND_STATUS_RUNNING:          "running",
		pb.CommandStatus_COMMAND_STATUS_SUCCEEDED:        "succeeded",
		pb.CommandStatus_COMMAND_STATUS_FAILED:           "failed",
		pb.CommandStatus_COMMAND_STATUS_TIMED_OUT:        "timed out",
		pb.CommandStatus_COMMAND_STATUS_REJECTED:         "rejected",
		pb.CommandStatus_COMMAND_STATUS_INDETERMINATE:    "outcome unknown",
		pb.CommandStatus_COMMAND_STATUS_NODE_UNAVAILABLE: "node unavailable",
		pb.CommandStatus(123):                            "unknown status (123)",
	} {
		record := &pb.CommandRecord{Command: &pb.Command{CommandId: "command-1", TargetId: "node-1", IdempotencyKey: "Exact_RETRY:123"}, Status: status}
		var out bytes.Buffer
		if err := writeCommand(Options{Output: &out}, record); err != nil {
			t.Fatal(err)
		}
		for _, text := range []string{"Command: command-1", "Status: " + want, "Target node: node-1", "Retry key: Exact_RETRY:123", "original request/target"} {
			if !strings.Contains(out.String(), text) {
				t.Fatalf("missing %q: %s", text, &out)
			}
		}
		if strings.Contains(out.String(), "COMMAND_STATUS_") || strings.Contains(out.String(), "Detail:") {
			t.Fatalf("raw enum or empty field: %s", &out)
		}
	}
}

func TestCommandHumanStageReceiptsAreNotTaskCompletion(t *testing.T) {
	record := &pb.CommandRecord{
		Command: &pb.Command{CommandId: "command-1", IdempotencyKey: "retry-key"},
		Status:  pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, Detail: "lifecycle_uncertain",
		AgentLifecycle: &pb.AgentLifecycleReceipt{
			Handle: &pb.AgentLifecycleHandle{WorkspaceId: "workspace-1", TabId: "tab-1", Provider: "copilot",
				Target: &pb.AgentTarget{PaneId: "pane-1", TerminalId: "terminal-1", SessionName: "dev", SessionIncarnation: strings.Repeat("a", 64)}},
			PaneOutcome: "confirmed", LaunchOutcome: "unknown", PromptOutcome: "not_attempted", StopOutcome: "not_attempted",
		},
	}
	var out bytes.Buffer
	if err := writeCommand(Options{Output: &out}, record); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"Agent pane: pane-1", "Terminal: terminal-1", "Session incarnation: " + strings.Repeat("a", 64),
		"Agent launch: unknown", "Initial prompt: not attempted", "not task completion", "do not retry with a new key"} {
		if !strings.Contains(out.String(), text) {
			t.Fatalf("missing %q: %s", text, &out)
		}
	}
	for _, code := range []string{"agent_started", "agent_prompt_sent", "agent_input_sent", "agent_interrupt_sent"} {
		if !strings.Contains(HumanDetail(code), "not task completion") {
			t.Fatalf("overstated %s", code)
		}
	}
}

func TestHumanViewsPreserveProtoJSON(t *testing.T) {
	agent := &pb.AgentQueryResult{Agent: agentClientView(), Text: "output", Truncated: true}
	project := projectFixture()
	sessions := &pb.SessionList{Sessions: []*pb.SessionView{{Name: "dev", Status: "stopped"}}}
	command := &pb.CommandRecord{Command: &pb.Command{CommandId: "command-1", IdempotencyKey: "retry"}, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED,
		Detail: "agent_input_sent", AgentControl: &pb.AgentControlResult{Target: agent.Agent.Target, StateChangeSeq: 77, ObservedStatus: "idle"}}
	for _, test := range []struct {
		name  string
		value proto.Message
		write func(Options) error
	}{
		{"command", command, func(o Options) error { return writeCommand(o, command) }},
		{"agent", agent, func(o Options) error { return writeAgent(o, pb.AgentQueryKind_AGENT_QUERY_KIND_GET, agent) }},
		{"project", project, func(o Options) error { return writeProject(o, project) }},
		{"sessions", sessions, func(o Options) error { return writeSessions(o, sessions) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := proto.Clone(test.value)
			for _, asJSON := range []bool{false, true} {
				var out bytes.Buffer
				if err := test.write(Options{Output: &out, JSON: asJSON}); err != nil {
					t.Fatal(err)
				}
				if asJSON {
					expected, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(before)
					if err != nil {
						t.Fatal(err)
					}
					var want, got any
					if json.Unmarshal(expected, &want) != nil || json.Unmarshal(out.Bytes(), &got) != nil || !reflect.DeepEqual(want, got) {
						t.Fatalf("JSON changed or contains prose: %s", &out)
					}
				}
			}
			if !proto.Equal(before, test.value) {
				t.Fatal("human formatting mutated wire data")
			}
			if err := test.write(Options{Output: failedWriter{}}); err == nil {
				t.Fatal("human writer failure suppressed")
			}
		})
	}
}

func TestInventoryHumanReadinessAndUnchangedJSON(t *testing.T) {
	list := inventoryFixture()
	var out bytes.Buffer
	options := Options{FleetClient: inventoryClientFixture{list: list}, Output: &out}
	if err := AgentInventory(context.Background(), options, InventoryFilter{}); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"Source node: node-1", "Agent pane: pane", "Session incarnation: " + strings.Repeat("a", 64),
		"Terminal: named-terminal", "Provider session: provider-session", "stale; not current readiness", "disconnected",
		"not ready for input", "unknown; query the agent", "Session manager: unavailable"} {
		if !strings.Contains(out.String(), text) {
			t.Fatalf("missing %q: %s", text, &out)
		}
	}
	out.Reset()
	options.JSON = true
	if err := AgentInventory(context.Background(), options, InventoryFilter{}); err != nil {
		t.Fatal(err)
	}
	expected, err := json.Marshal(projectAgentInventory(list, InventoryFilter{}))
	if err != nil || out.String() != string(expected)+"\n" {
		t.Fatalf("inventory JSON changed: %s", &out)
	}
}

func TestProjectAndSessionHumanRecovery(t *testing.T) {
	var out bytes.Buffer
	if err := writeProject(Options{Output: &out}, projectFixture()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "desired and applied generations differ") ||
		!strings.Contains(out.String(), "Desired checkout: C:\\desired") || !strings.Contains(out.String(), "Applied checkout: C:\\applied") {
		t.Fatalf("missing configuration mismatch: %s", &out)
	}
	out.Reset()
	list := &pb.SessionList{Sessions: []*pb.SessionView{{Name: "dev", Status: "unavailable", Stale: true, ErrorCode: "session_replaced"}}}
	if err := writeSessions(Options{Output: &out}, list); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "stale; not current readiness") ||
		!strings.Contains(out.String(), "Refresh its identity") || strings.Contains(out.String(), "Session incarnation:") {
		t.Fatalf("missing stale/mismatch guidance or empty field: %s", &out)
	}
}
