package protocol

import (
	"encoding/hex"
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

var testIncarnation = strings.Repeat("a", 64)

func TestSessionSelectors(t *testing.T) {
	for _, name := range []string{"default", "worker-1", "a_b", strings.Repeat("x", 64)} {
		if ValidateSessionEnsure(&pb.SessionEnsure{Name: name}) != nil ||
			ValidateSessionSelector(name, testIncarnation, true) != nil ||
			ValidateSessionSelector(name, "", false) != nil {
			t.Fatalf("valid name rejected: %q", name)
		}
		if ValidateSessionSelector(name, "", true) == nil {
			t.Fatal("unpinned named mutation accepted")
		}
	}
	for _, name := range []string{"", "UPPER", "../worker", "worker.one", "con", "lpt9", strings.Repeat("x", 65), "é"} {
		if ValidateSessionEnsure(&pb.SessionEnsure{Name: name}) == nil {
			t.Fatalf("invalid ensure name accepted: %q", name)
		}
	}
	for _, incarnation := range []string{"a", strings.Repeat("A", 64), strings.Repeat("g", 64), strings.Repeat("a", 65)} {
		if ValidateSessionSelector("worker", incarnation, false) == nil {
			t.Fatal("invalid incarnation accepted")
		}
	}
	for _, incarnation := range []string{"", testIncarnation} {
		if ValidateSessionSelector("", incarnation, true) != nil {
			t.Fatal("configured default rejected")
		}
	}
	value := &pb.SessionEnsure{Name: "worker"}
	value.ProtoReflect().SetUnknown([]byte{0x10, 1})
	if ValidateSessionEnsure(value) == nil {
		t.Fatal("unknown ensure argument accepted")
	}
}

func TestNamedAgentSelectionAndResultIdentity(t *testing.T) {
	target := &pb.AgentTarget{PaneId: "pane", TerminalId: "terminal", SessionName: "worker"}
	query := &pb.AgentQueryRequest{NodeInstanceId: "node", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_GET, Target: target}
	if _, err := NormalizeAgentQuery(query); err != nil {
		t.Fatal(err)
	}
	control := &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT, Target: target}
	if ValidateAgentControl(control) == nil {
		t.Fatal("unpinned named mutation accepted")
	}
	for _, kind := range []pb.AgentQueryKind{pb.AgentQueryKind_AGENT_QUERY_KIND_READ, pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT} {
		query.Kind = kind
		if _, err := NormalizeAgentQuery(query); err == nil {
			t.Fatal("unpinned wire read/wait accepted")
		}
	}
	target.SessionIncarnation = testIncarnation
	if ValidateAgentControl(control) != nil {
		t.Fatal("pinned control rejected")
	}
	actual := proto.Clone(target).(*pb.AgentTarget)
	actual.SessionName = "different"
	if matchesAgentTarget(actual, target) || equalAgentTarget(actual, target) {
		t.Fatal("same pane in another session matched")
	}
	actual.SessionName, actual.SessionIncarnation = target.SessionName, strings.Repeat("b", 64)
	if matchesAgentTarget(actual, target) || equalAgentTarget(actual, target) {
		t.Fatal("replaced session matched")
	}
}

func TestSessionEnsureResultStrictAndExclusive(t *testing.T) {
	command := &pb.Command{CommandId: NewCommandID(), CommandType: SessionEnsureCommandType, SessionEnsure: &pb.SessionEnsure{Name: "worker"}}
	result := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED,
		Detail: "session_ready", SessionEnsure: &pb.SessionView{Name: "worker", Incarnation: testIncarnation, Status: "ready"}}
	if err := ValidateResultForCommand(result, command); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*pb.CommandResult){
		func(r *pb.CommandResult) { r.SessionEnsure.Name = "other" },
		func(r *pb.CommandResult) { r.SessionEnsure.Incarnation = "" },
		func(r *pb.CommandResult) { r.SessionEnsure.Status = "starting" },
		func(r *pb.CommandResult) { r.SessionEnsure.ErrorCode = "startup_pending" },
		func(r *pb.CommandResult) { r.WorkspaceEnsure = &pb.WorkspaceEnsureResult{} },
		func(r *pb.CommandResult) { r.SessionEnsure.Herdr = &pb.HerdrState{} },
	} {
		bad := proto.Clone(result).(*pb.CommandResult)
		change(bad)
		if ValidateResultForCommand(bad, command) == nil {
			t.Fatal("invalid success accepted")
		}
	}
	if ValidateProbeResult(result) == nil || ValidateWorkspaceResult(result) == nil ||
		ValidateWorktreeResult(result) == nil || ValidateAgentControlResult(result) == nil {
		t.Fatal("session result accepted as another result type")
	}
	for _, detail := range []string{"session_manager_unavailable", "session_unavailable", "session_replaced",
		"session_start_failed", "unsupported_protocol", "session_default_unmanaged", "session_capacity", "precondition_failed"} {
		result.Status, result.Detail, result.SessionEnsure = pb.CommandStatus_COMMAND_STATUS_REJECTED, detail, nil
		if ValidateResultForCommand(result, command) != nil {
			t.Fatal("safe session error rejected", detail)
		}
	}
	result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, "startup_uncertain"
	if ValidateResultForCommand(result, command) != nil {
		t.Fatal("startup uncertainty rejected")
	}
	result.Detail = `failed opening C:\private\socket`
	if ValidateCommandResult(result) == nil {
		t.Fatal("raw diagnostic accepted")
	}
	request := &pb.SubmitCommandRequest{NodeInstanceId: "node", IdempotencyKey: "key",
		CommandType: SessionEnsureCommandType, SessionEnsure: command.SessionEnsure}
	if _, err := NormalizeCommandRequest(request); err != nil {
		t.Fatal(err)
	}
	request.AgentControl = &pb.AgentControl{}
	if _, err := NormalizeCommandRequest(request); err == nil {
		t.Fatal("mixed session command accepted")
	}
}

func TestLegacySessionlessWireIdentity(t *testing.T) {
	for _, test := range []struct {
		value proto.Message
		wire  string
	}{
		{&pb.WorkspaceEnsure{ProjectId: "p", BindingRevision: "r"}, "0a0170120172"},
		{&pb.WorktreeCreate{ProjectId: "p", BindingRevision: "r", Name: "n", Branch: "b", BaseCommit: "c"}, "0a01701201721a016e2201622a0163"},
		{&pb.AgentTarget{PaneId: "p", TerminalId: "t", AgentSessionId: "s"}, "0a01701201741a0173"},
	} {
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(test.value)
		if err != nil || hex.EncodeToString(data) != test.wire {
			t.Fatalf("legacy bytes changed: %x, %v", data, err)
		}
	}

}

func TestSessionQualifiedWorkspaceAndWorktreeResults(t *testing.T) {
	for _, worktree := range []bool{false, true} {
		command := &pb.Command{CommandId: NewCommandID(), CommandType: WorkspaceEnsureCommandType,
			WorkspaceEnsure: &pb.WorkspaceEnsure{ProjectId: "project", BindingRevision: "revision", SessionName: "worker"}}
		result := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED,
			Detail: "workspace_present", WorkspaceEnsure: &pb.WorkspaceEnsureResult{
				ProjectId: "project", BindingRevision: "revision", WorkspaceId: "workspace", SessionName: "worker", SessionIncarnation: testIncarnation}}
		if worktree {
			command.CommandType, command.WorkspaceEnsure = WorktreeCreateCommandType, nil
			command.WorktreeCreate = &pb.WorktreeCreate{ProjectId: "project", BindingRevision: "revision", Name: "task", Branch: "task",
				BaseCommit: strings.Repeat("a", 40), SessionName: "worker"}
			result.Detail, result.WorkspaceEnsure = "worktree_created", nil
			result.WorktreeCreate = &pb.WorktreeCreateResult{ProjectId: "project", BindingRevision: "revision", WorkspaceId: "workspace",
				Name: "task", Branch: "task", BaseCommit: strings.Repeat("a", 40), SessionName: "worker", SessionIncarnation: testIncarnation}
		}
		if err := ValidateResultForCommand(result, command); err != nil {
			t.Fatal(err)
		}
		for _, selectors := range [][2]string{{"worker", ""}, {"other", testIncarnation}, {"", testIncarnation}, {"worker", "bad"}} {
			bad := proto.Clone(result).(*pb.CommandResult)
			if worktree {
				bad.WorktreeCreate.SessionName, bad.WorktreeCreate.SessionIncarnation = selectors[0], selectors[1]
			} else {
				bad.WorkspaceEnsure.SessionName, bad.WorkspaceEnsure.SessionIncarnation = selectors[0], selectors[1]
			}
			if ValidateResultForCommand(bad, command) == nil {
				t.Fatal("missing actual incarnation or mismatched result session accepted")
			}
		}
		if worktree {
			command.WorktreeCreate.SessionIncarnation = strings.Repeat("b", 64)
		} else {
			command.WorkspaceEnsure.SessionIncarnation = strings.Repeat("b", 64)
		}
		if ValidateResultForCommand(result, command) == nil {
			t.Fatal("expected incarnation ignored")
		}
	}
}
