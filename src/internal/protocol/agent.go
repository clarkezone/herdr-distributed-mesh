package protocol

import (
	"errors"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

const (
	AgentControlCommandType = "agent.control.v1"
	AgentControlCapability  = "herdr.agent-control.v1"
	MaxAgentPromptBytes     = 8192
	MaxAgentOutputBytes     = 65536
	MaxAgentQueryTimeout    = 5 * time.Minute
)

func validateCommandBody(kind string, workspace *pb.WorkspaceEnsure, worktree *pb.WorktreeCreate, agent *pb.AgentControl) error {
	switch kind {
	case ProbeCommandType:
		if workspace == nil && worktree == nil && agent == nil {
			return nil
		}
	case WorkspaceEnsureCommandType:
		if worktree == nil && agent == nil {
			return ValidateWorkspaceEnsure(workspace)
		}
	case WorktreeCreateCommandType:
		if workspace == nil && agent == nil {
			return ValidateWorktreeCreate(worktree)
		}
	case AgentControlCommandType:
		if workspace == nil && worktree == nil {
			return ValidateAgentControl(agent)
		}
	}
	return errors.New("unsupported or mixed command arguments")
}

func ValidateAgentTarget(target *pb.AgentTarget, requireTerminal bool) error {
	if target == nil || len(target.ProtoReflect().GetUnknown()) != 0 || !commandToken.MatchString(target.PaneId) ||
		(requireTerminal && target.TerminalId == "") || (target.TerminalId != "" && !commandToken.MatchString(target.TerminalId)) ||
		(target.AgentSessionId != "" && !commandToken.MatchString(target.AgentSessionId)) {
		return errors.New("agent target requires bounded pane and terminal identifiers")
	}
	return nil
}

func validAgentStatus(value string) bool {
	return slices.Contains([]string{"idle", "working", "blocked", "done", "unknown"}, value)
}

func ValidateAgentView(value *pb.AgentView) error {
	if value == nil || len(value.ProtoReflect().GetUnknown()) != 0 || ValidateAgentTarget(value.Target, true) != nil ||
		!commandToken.MatchString(value.WorkspaceId) || !commandToken.MatchString(value.TabId) ||
		!commandToken.MatchString(value.Provider) || !validAgentStatus(value.Status) {
		return errors.New("invalid agent projection")
	}
	return nil
}

func ValidateAgentControl(value *pb.AgentControl) error {
	if value == nil || len(value.ProtoReflect().GetUnknown()) != 0 || ValidateAgentTarget(value.Target, true) != nil {
		return errors.New("invalid agent control target")
	}
	return ValidateAgentControlInput(value.Action, value.Text, value.Keys)
}

func ValidateAgentControlInput(action pb.AgentControlAction, text string, keys []string) error {
	switch action {
	case pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT:
		if len(keys) != 0 || strings.TrimSpace(text) == "" || len(text) > MaxAgentPromptBytes || !utf8.ValidString(text) {
			return errors.New("prompt must contain 1..8192 UTF-8 bytes and no key actions")
		}
		for _, r := range text {
			if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
				return errors.New("prompt contains terminal control characters")
			}
		}
	case pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT:
		if text != "" || len(keys) == 0 || len(keys) > 8 {
			return errors.New("input requires 1..8 explicit keys and no prompt text")
		}
		for _, key := range keys {
			if !ValidAgentKey(key) {
				return errors.New("unsupported agent key")
			}
		}
	case pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT:
		if text != "" || len(keys) != 0 {
			return errors.New("interrupt must not carry arbitrary input")
		}
	default:
		return errors.New("unsupported agent control action")
	}
	return nil
}

func ValidAgentKey(key string) bool {
	if len(key) == 1 && key[0] >= 32 && key[0] <= 126 {
		return true
	}
	return slices.Contains([]string{"enter", "esc", "tab", "shift+tab", "up", "down", "left", "right", "backspace", "delete", "home", "end", "ctrl+c"}, key)
}

func NormalizeAgentQuery(request *pb.AgentQueryRequest) (*pb.AgentQueryRequest, error) {
	value, err := NormalizeAgentSelection(request)
	if err != nil {
		return nil, err
	}
	if err := ValidateAgentTarget(value.Target, value.Kind != pb.AgentQueryKind_AGENT_QUERY_KIND_GET); err != nil {
		return nil, err
	}
	return value, nil
}

// NormalizeAgentSelection validates client options before resolving a pane.
// Wire requests must instead use NormalizeAgentQuery, which requires pinned targets.
func NormalizeAgentSelection(request *pb.AgentQueryRequest) (*pb.AgentQueryRequest, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || !commandToken.MatchString(request.NodeInstanceId) {
		return nil, errors.New("agent query requires a bounded node ID")
	}
	if err := ValidateAgentTarget(request.Target, false); err != nil {
		return nil, err
	}
	copy := proto.Clone(request).(*pb.AgentQueryRequest)
	if copy.TimeoutMs == 0 {
		copy.TimeoutMs = 30000
	}
	if copy.TimeoutMs > uint32(MaxAgentQueryTimeout/time.Millisecond) {
		return nil, errors.New("agent query timeout must not exceed five minutes")
	}
	switch copy.Kind {
	case pb.AgentQueryKind_AGENT_QUERY_KIND_GET:
		if copy.Lines != 0 || len(copy.Until) != 0 {
			return nil, errors.New("get query cannot carry read/wait arguments")
		}
	case pb.AgentQueryKind_AGENT_QUERY_KIND_READ:
		if len(copy.Until) != 0 || copy.Lines > 1000 {
			return nil, errors.New("read requires at most 1000 lines and no wait states")
		}
		if copy.Lines == 0 {
			copy.Lines = 100
		}
	case pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT:
		if copy.Lines != 0 {
			return nil, errors.New("wait query cannot carry read arguments")
		}
		if len(copy.Until) == 0 {
			copy.Until = []string{"idle", "done", "blocked"}
		}
		if len(copy.Until) > 5 {
			return nil, errors.New("too many wait states")
		}
		seen := map[string]bool{}
		for _, state := range copy.Until {
			if !validAgentStatus(state) || seen[state] {
				return nil, errors.New("invalid or duplicate wait state")
			}
			seen[state] = true
		}
	default:
		return nil, errors.New("unsupported agent query")
	}
	return copy, nil
}

func ValidateAgentQuery(query *pb.AgentQuery, nodeID string) error {
	if query == nil || len(query.ProtoReflect().GetUnknown()) != 0 || !ValidCommandID(query.QueryId) ||
		query.Request.GetNodeInstanceId() != nodeID || query.ExpiresAt == nil || query.ExpiresAt.CheckValid() != nil ||
		len(query.ExpiresAt.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("invalid agent query envelope")
	}
	_, err := NormalizeAgentQuery(query.Request)
	return err
}

func SanitizeAgentText(text string) string {
	return strings.Map(func(r rune) rune {
		if (unicode.IsControl(r) && r != '\n' && r != '\t') || (r >= 0x202a && r <= 0x202e) || (r >= 0x2066 && r <= 0x2069) {
			return -1
		}
		return r
	}, text)
}

func ValidateAgentQueryResult(result *pb.AgentQueryResult, request *pb.AgentQueryRequest) error {
	if result == nil || request == nil || !ValidCommandID(result.QueryId) || len(result.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("invalid agent query result")
	}
	if result.ErrorCode != "" {
		if result.Agent != nil || result.Text != "" || result.Truncated || !slices.Contains([]string{
			"target_unavailable", "target_changed", "agent_busy", "agent_blocked", "herdr_unavailable", "timeout", "unsupported", "invalid_request", "overloaded", "canceled",
		}, result.ErrorCode) {
			return errors.New("invalid agent query error")
		}
		return nil
	}
	if ValidateAgentView(result.Agent) != nil || !matchesAgentTarget(result.Agent.Target, request.Target) {
		return errors.New("agent query target mismatch")
	}
	if len(result.Text) > MaxAgentOutputBytes || !utf8.ValidString(result.Text) || SanitizeAgentText(result.Text) != result.Text {
		return errors.New("invalid or excessive agent output")
	}
	if request.Kind != pb.AgentQueryKind_AGENT_QUERY_KIND_READ && (result.Text != "" || result.Truncated) {
		return errors.New("non-read query returned terminal output")
	}
	if request.Kind == pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT {
		normalized, err := NormalizeAgentQuery(request)
		if err != nil || !slices.Contains(normalized.Until, result.Agent.Status) {
			return errors.New("wait returned an unmatched state")
		}
	}
	return nil
}

func matchesAgentTarget(actual, want *pb.AgentTarget) bool {
	return actual != nil && want != nil && actual.PaneId == want.PaneId &&
		(want.TerminalId == "" || actual.TerminalId == want.TerminalId) &&
		(want.AgentSessionId == "" || actual.AgentSessionId == want.AgentSessionId)
}

func equalAgentTarget(a, b *pb.AgentTarget) bool { return proto.Equal(a, b) }

func AgentControlSuccessDetail(action pb.AgentControlAction) string {
	switch action {
	case pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT:
		return "agent_prompt_sent"
	case pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT:
		return "agent_input_sent"
	case pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT:
		return "agent_interrupt_sent"
	default:
		return ""
	}
}

func ValidateAgentControlResult(result *pb.CommandResult) error {
	if result == nil || !ValidCommandID(result.CommandId) || len(result.ProtoReflect().GetUnknown()) != 0 ||
		result.Payload != nil || result.WorkspaceEnsure != nil || result.WorktreeCreate != nil {
		return errors.New("invalid agent command result")
	}
	if result.Status == pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		value := result.AgentControl
		if value == nil || len(value.ProtoReflect().GetUnknown()) != 0 || ValidateAgentTarget(value.Target, true) != nil ||
			!validAgentStatus(value.ObservedStatus) || !slices.Contains([]string{"agent_prompt_sent", "agent_input_sent", "agent_interrupt_sent"}, result.Detail) {
			return errors.New("invalid typed agent acknowledgement")
		}
		return nil
	}
	if result.AgentControl != nil {
		return errors.New("unsuccessful agent command contains success data")
	}
	switch result.Status {
	case pb.CommandStatus_COMMAND_STATUS_REJECTED:
		if slices.Contains([]string{"journal_full", "precondition_failed", "agent_busy", "agent_blocked", "target_changed", "herdr_unavailable", "unsupported", "authorization_changed"}, result.Detail) {
			return nil
		}
	case pb.CommandStatus_COMMAND_STATUS_TIMED_OUT:
		if result.Detail == "deadline_expired" {
			return nil
		}
	case pb.CommandStatus_COMMAND_STATUS_INDETERMINATE:
		if result.Detail == "node_restarted" || result.Detail == "herdr_outcome_unknown" {
			return nil
		}
	}
	return errors.New("invalid agent command outcome")
}
