package control

import (
	"fmt"
	"strings"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

type humanView struct{ strings.Builder }

func (v *humanView) field(label, value string) {
	if value != "" {
		fmt.Fprintf(&v.Builder, "%s: %s\n", label, value)
	}
}

func readable(value string) string {
	return strings.ReplaceAll(value, "_", " ")
}

// HumanCommandStatus formats a command status for human output, not wire data.
func HumanCommandStatus(status pb.CommandStatus) string {
	switch status {
	case pb.CommandStatus_COMMAND_STATUS_UNSPECIFIED:
		return "unknown (not reported)"
	case pb.CommandStatus_COMMAND_STATUS_ACCEPTED:
		return "accepted (waiting to run)"
	case pb.CommandStatus_COMMAND_STATUS_RUNNING:
		return "running"
	case pb.CommandStatus_COMMAND_STATUS_SUCCEEDED:
		return "succeeded"
	case pb.CommandStatus_COMMAND_STATUS_FAILED:
		return "failed"
	case pb.CommandStatus_COMMAND_STATUS_TIMED_OUT:
		return "timed out"
	case pb.CommandStatus_COMMAND_STATUS_REJECTED:
		return "rejected"
	case pb.CommandStatus_COMMAND_STATUS_INDETERMINATE:
		return "outcome unknown"
	case pb.CommandStatus_COMMAND_STATUS_NODE_UNAVAILABLE:
		return "node unavailable"
	default:
		return fmt.Sprintf("unknown status (%d)", status)
	}
}

// HumanDetail explains a detail code while preserving its recovery implications.
func HumanDetail(code string) string {
	var meaning string
	switch code {
	case "":
		return ""
	case "pong":
		meaning = "Node answered the connectivity probe."
	case "workspace_created":
		meaning = "Workspace created."
	case "workspace_present":
		meaning = "Existing workspace reused."
	case "worktree_created":
		meaning = "Worktree created."
	case "session_ready":
		meaning = "Herdr session is ready."
	case "agent_started":
		meaning = "Agent startup confirmed; this is not task completion."
	case "agent_stopped":
		meaning = "Agent stop confirmed."
	case "agent_prompt_sent", "agent_input_sent", "agent_interrupt_sent":
		meaning = "Input delivery confirmed; this is not task completion."
	case "deadline_expired":
		meaning = "Command deadline expired. Inspect this command before submitting another operation."
	case "node_restarted", "server_restarted", "node_disconnected", "herdr_outcome_unknown", "startup_uncertain", "lifecycle_uncertain", "persistence_failed":
		meaning = "Effects may already have occurred. Inspect this command and its original target; do not retry with a new key."
	case "lifecycle_partial", "lifecycle_unresolved":
		meaning = "Agent lifecycle is incomplete. Inspect the stage receipts and original target before further changes."
	case "target_changed", "session_replaced", "precondition_failed":
		meaning = "The target no longer matches the request. Refresh its identity before a new operation; keep the original identity for exact retries."
	case "session_manager_unavailable", "session_unavailable", "session_start_failed", "herdr_unavailable":
		meaning = "Herdr is unavailable. Check the execution node and its session status."
	case "unsupported", "unsupported_protocol":
		meaning = "This operation is not supported by the connected runtime. Check compatible node and Herdr versions."
	case "session_default_unmanaged":
		meaning = "The default session is not managed. Select a named managed session."
	case "session_capacity", "journal_full":
		meaning = "The node has reached its capacity. Inspect active sessions and commands before trying again."
	case "agent_busy", "agent_blocked":
		meaning = "The agent cannot accept this operation now. Read its output and resolve any pending interaction."
	case "project_unresolved", "project_not_authorized":
		meaning = "The project is not available for this operation. Inspect its registration and applied binding on the target node."
	case "invalid_path":
		meaning = "Check that the registered checkout and worktree paths exist and are accessible on the execution node."
	case "configuration_conflict":
		meaning = "The project configuration conflicts with an existing binding. Compare desired and applied paths on the execution node."
	case "ambiguous_workspace":
		meaning = "More than one workspace matches. Inspect the project workspaces and select an explicit target."
	case "authorization_changed":
		meaning = "Authorization changed. Check the coordinator and node role tags and tailnet policy."
	default:
		meaning = readable(code) + ". Inspect the target state and command receipt before further changes."
	}
	return meaning + " (" + code + ")"
}

func (v *humanView) target(target *pb.AgentTarget) {
	v.field("Agent pane", target.GetPaneId())
	v.field("Terminal", target.GetTerminalId())
	v.field("Provider session", target.GetAgentSessionId())
	v.session(target.GetSessionName(), target.GetSessionIncarnation())
}

func (v *humanView) session(name, incarnation string) {
	v.field("Session", name)
	v.field("Session incarnation", incarnation)
}

func (v *humanView) freshness(stale bool) {
	if stale {
		v.field("Observation", "stale; not current readiness. Check the node connection and refresh before acting.")
	} else {
		v.field("Observation", "latest reported snapshot; not a live readiness check")
	}
}

func readyText(ready bool) string {
	if ready {
		return "ready for input (not task completion)"
	}
	return "not ready for input; inspect agent status and output"
}
