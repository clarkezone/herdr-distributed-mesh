package protocol

import (
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func agentTestTarget() *pb.AgentTarget {
	return &pb.AgentTarget{PaneId: "w1:p1", TerminalId: "term_123", AgentSessionId: "session-1"}
}
func agentTestView() *pb.AgentView {
	return &pb.AgentView{Target: agentTestTarget(), WorkspaceId: "w1", TabId: "w1:t1", Provider: "copilot", Status: "idle"}
}

func TestAgentControlBoundaries(t *testing.T) {
	for _, test := range []struct {
		name   string
		action pb.AgentControlAction
		text   string
		keys   []string
		valid  bool
	}{
		{"exact bytes", pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, strings.Repeat("x", 8192), nil, true},
		{"too large", pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, strings.Repeat("x", 8193), nil, false},
		{"UTF8 bytes", pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, strings.Repeat("\u00e9", 4096), nil, true},
		{"UTF8 excessive", pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, strings.Repeat("\u00e9", 4097), nil, false},
		{"empty", pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, " \r\n\t", nil, false},
		{"escape", pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, "hi\x1b[2J", nil, false},
		{"invalid UTF8", pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, "\xff", nil, false},
		{"multiline", pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, "hi\r\n\tthere", nil, true},
		{"mixed prompt", pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, "hello", []string{"enter"}, false},
		{"eight keys", pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT, "", []string{"a", " ", "up", "down", "enter", "esc", "ctrl+c", "tab"}, true},
		{"nine keys", pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT, "", strings.Fields("a b c d e f g h i"), false},
		{"no keys", pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT, "", nil, false},
		{"raw escape key", pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT, "", []string{"\x1b"}, false},
		{"unsupported key", pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT, "", []string{"ctrl+z"}, false},
		{"interrupt", pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT, "", nil, true},
		{"mixed interrupt", pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT, "", []string{"esc"}, false},
		{"unspecified", pb.AgentControlAction_AGENT_CONTROL_ACTION_UNSPECIFIED, "", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := &pb.AgentControl{Target: agentTestTarget(), Action: test.action, Text: test.text, Keys: test.keys}
			if err := ValidateAgentControl(input); (err == nil) != test.valid {
				t.Fatalf("valid=%t error=%v", test.valid, err)
			}
		})
	}
	if ValidateAgentTarget(&pb.AgentTarget{PaneId: "w1:p1"}, false) != nil || ValidateAgentTarget(&pb.AgentTarget{PaneId: "w1:p1"}, true) == nil {
		t.Fatal("terminal identity requirement lost")
	}
	for _, id := range []string{"", "../x", "a\nb", strings.Repeat("x", 129)} {
		if ValidateAgentTarget(&pb.AgentTarget{PaneId: id}, false) == nil {
			t.Fatalf("accepted pane %q", id)
		}
	}
}

func TestAgentQueryNormalizationAndLimits(t *testing.T) {
	base := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Target: agentTestTarget(), Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ}
	result, err := NormalizeAgentQuery(base)
	if err != nil || result.Lines != 100 || result.TimeoutMs != 30000 || base.Lines != 0 || base.TimeoutMs != 0 {
		t.Fatalf("defaults/cloning: %v %v", result, err)
	}
	result.Target.TerminalId = "other"
	if base.Target.TerminalId != "term_123" {
		t.Fatal("target aliased")
	}
	for _, test := range []struct {
		name   string
		mutate func(*pb.AgentQueryRequest)
	}{
		{"missing terminal", func(q *pb.AgentQueryRequest) { q.Target.TerminalId = "" }},
		{"excess lines", func(q *pb.AgentQueryRequest) { q.Lines = 1001 }},
		{"excess time", func(q *pb.AgentQueryRequest) { q.TimeoutMs = 300001 }},
		{"mixed read", func(q *pb.AgentQueryRequest) { q.Until = []string{"idle"} }},
		{"mixed get", func(q *pb.AgentQueryRequest) { q.Kind = pb.AgentQueryKind_AGENT_QUERY_KIND_GET; q.Lines = 1 }},
		{"mixed wait", func(q *pb.AgentQueryRequest) { q.Kind = pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT; q.Lines = 1 }},
		{"duplicate state", func(q *pb.AgentQueryRequest) {
			q.Kind = pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT
			q.Until = []string{"idle", "idle"}
		}},
		{"invalid state", func(q *pb.AgentQueryRequest) {
			q.Kind = pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT
			q.Until = []string{"success"}
		}},
		{"unknown kind", func(q *pb.AgentQueryRequest) { q.Kind = 99 }},
		{"unknown fields", func(q *pb.AgentQueryRequest) { q.ProtoReflect().SetUnknown([]byte{0x78, 1}) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := proto.Clone(base).(*pb.AgentQueryRequest)
			test.mutate(q)
			if _, err := NormalizeAgentQuery(q); err == nil {
				t.Fatal("invalid query accepted")
			}
		})
	}
	base.Lines = 1000
	base.TimeoutMs = 300000
	if _, err := NormalizeAgentQuery(base); err != nil {
		t.Fatal(err)
	}
	base.Kind = pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT
	base.Lines = 0
	wait, err := NormalizeAgentQuery(base)
	if err != nil || strings.Join(wait.Until, ",") != "idle,done,blocked" {
		t.Fatalf("wait defaults: %v %v", wait, err)
	}
	if err := ValidateAgentQuery(&pb.AgentQuery{QueryId: NewCommandID(), Request: wait, ExpiresAt: timestamppb.New(time.Now().Add(time.Second))}, "node-2"); err == nil {
		t.Fatal("wrong node accepted")
	}
}

func TestAgentOutputValidation(t *testing.T) {
	request := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ, Target: agentTestTarget()}
	base := &pb.AgentQueryResult{QueryId: NewCommandID(), Agent: agentTestView(), Text: strings.Repeat("x", MaxAgentOutputBytes)}
	if err := ValidateAgentQueryResult(base, request); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*pb.AgentQueryResult){
		func(r *pb.AgentQueryResult) { r.Text += "x" },
		func(r *pb.AgentQueryResult) { r.Text = "\xff" },
		func(r *pb.AgentQueryResult) { r.Text = "\x1b]0;title\x07" },
		func(r *pb.AgentQueryResult) { r.Text = "\u202eevil" },
		func(r *pb.AgentQueryResult) { r.Agent.Target.TerminalId = "replaced" },
		func(r *pb.AgentQueryResult) { r.Agent.Target.AgentSessionId = "replaced" },
		func(r *pb.AgentQueryResult) { r.ErrorCode = "timeout" },
		func(r *pb.AgentQueryResult) { r.QueryId = "bad" },
	} {
		result := proto.Clone(base).(*pb.AgentQueryResult)
		mutate(result)
		if err := ValidateAgentQueryResult(result, request); err == nil {
			t.Fatal("invalid result accepted")
		}
	}
	if got := SanitizeAgentText("a\r\n\tb\x1b\x07\u202e\u2066"); got != "a\n\tb" {
		t.Fatalf("unsafe sanitization %q", got)
	}
	request.Kind = pb.AgentQueryKind_AGENT_QUERY_KIND_GET
	if ValidateAgentQueryResult(base, request) == nil {
		t.Fatal("get leaked output")
	}
	base.Text = ""
	request.Kind = pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT
	request.Until = []string{"working"}
	if ValidateAgentQueryResult(base, request) == nil {
		t.Fatal("wait returned wrong state")
	}
	failure := &pb.AgentQueryResult{QueryId: NewCommandID(), ErrorCode: "timeout"}
	if err := ValidateAgentQueryResult(failure, request); err != nil {
		t.Fatal(err)
	}
	failure.ErrorCode = "arbitrary server text"
	if ValidateAgentQueryResult(failure, request) == nil {
		t.Fatal("unbounded error accepted")
	}
}

func TestAgentTypedCommandUnion(t *testing.T) {
	action := &pb.AgentControl{Target: agentTestTarget(), Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, Text: "hello"}
	for _, kind := range []string{ProbeCommandType, WorkspaceEnsureCommandType, WorktreeCreateCommandType} {
		if _, err := NormalizeCommandRequest(&pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "k", CommandType: kind, AgentControl: action}); err == nil {
			t.Fatalf("smuggled agent into %s", kind)
		}
	}
	result := &pb.CommandResult{CommandId: NewCommandID(), Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "agent_prompt_sent", AgentControl: &pb.AgentControlResult{Target: agentTestTarget(), ObservedStatus: "idle"}}
	if err := ValidateAgentControlResult(result); err != nil {
		t.Fatal(err)
	}
	command := &pb.Command{CommandId: result.CommandId, CommandType: AgentControlCommandType, AgentControl: action}
	if err := ValidateResultForCommand(result, command); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*pb.CommandResult){
		func(r *pb.CommandResult) { r.Detail = "agent_interrupt_sent" },
		func(r *pb.CommandResult) { r.AgentControl.Target.PaneId = "w2:p1" },
		func(r *pb.CommandResult) { r.AgentControl.Target.TerminalId = "new-terminal" },
		func(r *pb.CommandResult) { r.AgentControl.Target.AgentSessionId = "" },
		func(r *pb.CommandResult) { r.WorkspaceEnsure = &pb.WorkspaceEnsureResult{} },
	} {
		mismatch := proto.Clone(result).(*pb.CommandResult)
		mutate(mismatch)
		if ValidateResultForCommand(mismatch, command) == nil {
			t.Fatal("mismatched command result accepted")
		}
	}
	result.Status = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE
	result.Detail = "herdr_outcome_unknown"
	if ValidateAgentControlResult(result) == nil {
		t.Fatal("uncertain result carried success data")
	}
	result.AgentControl = nil
	if err := ValidateAgentControlResult(result); err != nil {
		t.Fatal(err)
	}
}
