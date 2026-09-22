package herdr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrcompat"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

var (
	ErrAgentUnavailable   = errors.New("herdr: agent unavailable")
	ErrAgentPrecondition  = errors.New("herdr: agent precondition failed")
	ErrAgentBusy          = errors.New("herdr: agent busy")
	ErrAgentBlocked       = errors.New("herdr: agent blocked")
	ErrAgentIndeterminate = errors.New("herdr: agent outcome unknown")
	ErrAgentUnsupported   = errors.New("herdr: agent operation unsupported")
	ErrAgentChanged       = errors.New("herdr: agent identity changed")
)

// ControlAgent delivers input to an existing, freshly verified agent. A prompt
// acknowledgement means input was delivered, not that a task completed. Any
// failure after attempting delivery is indeterminate and must not be retried.
//
// Supported native protocols have no expected-terminal CAS: verification cannot
// atomically prevent replacement by another local client during delivery.
// Provider session identity is pinned only when specified by the caller.
// Copilot interrupt delivers two Esc keys in one request. Its acknowledgement
// does not establish that generation has stopped; other providers are unsupported.
func ControlAgent(ctx context.Context, config Config, command *pb.AgentControl) (*pb.AgentControlResult, error) {
	return controlAgent(ctx, config, command, dialLocal)
}

func controlAgent(ctx context.Context, config Config, command *pb.AgentControl, dial dialFunc) (*pb.AgentControlResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if protocol.ValidateAgentControl(command) != nil {
		return nil, ErrAgentPrecondition
	}
	o, err := agentObserver(ctx, config, dial)
	if err != nil {
		return nil, err
	}
	before, err := o.getAgent(ctx, command.Target)
	if err != nil {
		return nil, err
	}
	if before.Provider == "unknown" && command.Action != pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT {
		return nil, ErrAgentPrecondition
	}
	var method, resultType string
	var params any
	switch command.Action {
	case pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT:
		switch before.Status {
		case "working":
			return nil, ErrAgentBusy
		case "blocked":
			return nil, ErrAgentBlocked
		case "idle", "done":
		default:
			return nil, ErrAgentPrecondition
		}
		if !before.InteractiveReady || before.LaunchPending {
			return nil, ErrAgentPrecondition
		}
		method, resultType = "agent.prompt", "agent_prompted"
		params = struct {
			Target string `json:"target"`
			Text   string `json:"text"`
		}{command.Target.PaneId, command.Text}
	case pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT, pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT:
		keys := command.Keys
		if command.Action == pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT {
			if before.Provider != "copilot" {
				return nil, ErrAgentUnsupported
			}
			// Live Copilot requires a second Esc; a single Esc can report idle
			// while generation continues. Deliver both without retry or wait.
			keys = []string{"esc", "esc"}
		}
		method, resultType = "agent.send_keys", "ok"
		params = struct {
			Target string   `json:"target"`
			Keys   []string `json:"keys"`
		}{command.Target.PaneId, keys}
	default:
		return nil, ErrAgentPrecondition
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.CheckSession != nil {
		if err := config.CheckSession(ctx); err != nil {
			return nil, err
		}
	}
	ack, err := o.agentRequest(ctx, method, resultType, params)
	if err != nil {
		return nil, agentFailure(ctx, ErrAgentIndeterminate)
	}
	observed := before
	if command.Action == pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT {
		observed, err = parseAgentInfo(ack["agent"])
		if err != nil || !sameAgent(before, observed, command.Target) {
			return nil, agentFailure(ctx, ErrAgentIndeterminate)
		}
	} else {
		// The generic keys acknowledgement carries no agent identity or state.
		observed, err = o.getAgent(ctx, command.Target)
		if err != nil || !sameAgent(before, observed, command.Target) {
			return nil, agentFailure(ctx, ErrAgentIndeterminate)
		}
	}
	if ctx.Err() != nil {
		return nil, agentFailure(ctx, ErrAgentIndeterminate)
	}
	return &pb.AgentControlResult{
		Target: &pb.AgentTarget{
			PaneId: command.Target.PaneId, TerminalId: command.Target.TerminalId,
			AgentSessionId: command.Target.AgentSessionId,
		},
		ObservedStatus: observed.Status,
		StateChangeSeq: observed.StateChangeSeq,
	}, nil
}

func agentObserver(ctx context.Context, config Config, dial dialFunc) (*observer, error) {
	address, err := localAddress(config.SocketPath)
	if err != nil || config.RefreshInterval < 0 || config.RetryDelay < 0 || config.RequestTimeout < 0 {
		return nil, agentFailure(ctx, ErrAgentUnavailable)
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 3 * time.Second
	}
	config.SocketPath = address
	o := &observer{config: config, dial: dial}
	pong, err := o.rpc(ctx, "ping", "pong")
	if err != nil {
		return nil, agentFailure(ctx, ErrAgentUnavailable)
	}
	_, version, err := versionProtocol(pong)
	if err != nil {
		return nil, agentFailure(ctx, ErrAgentUnavailable)
	}
	if !herdrcompat.SupportsProtocol(int64(version)) {
		return nil, agentFailure(ctx, ErrAgentUnsupported)
	}
	return o, nil
}

func agentFailure(ctx context.Context, sentinel error) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(sentinel, err)
	}
	// A transport deadline can fire just before the context timer is scheduled.
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return errors.Join(sentinel, context.DeadlineExceeded)
	}
	return sentinel
}

func (o *observer) agentRequest(ctx context.Context, method, resultType string, params any) (object, error) {
	_, _, cleanup, result, err := o.request(ctx, method, resultType, params)
	if err != nil {
		return nil, err
	}
	cleanup()
	return result, nil
}

func (o *observer) getAgent(ctx context.Context, target *pb.AgentTarget) (*pb.AgentView, error) {
	result, err := o.agentRequest(ctx, "agent.get", "agent_info", struct {
		Target string `json:"target"`
	}{target.PaneId})
	if err != nil {
		return nil, agentFailure(ctx, ErrAgentUnavailable)
	}
	view, err := parseAgentInfo(result["agent"])
	if err != nil {
		return nil, agentFailure(ctx, ErrAgentUnavailable)
	}
	if view.Target.PaneId != target.PaneId ||
		(target.TerminalId != "" && view.Target.TerminalId != target.TerminalId) ||
		(target.AgentSessionId != "" && view.Target.AgentSessionId != target.AgentSessionId) {
		return nil, agentFailure(ctx, ErrAgentChanged)
	}
	if ctx.Err() != nil {
		return nil, agentFailure(ctx, ErrAgentUnavailable)
	}
	return view, nil
}

func sameAgent(before, after *pb.AgentView, expected *pb.AgentTarget) bool {
	return before != nil && after != nil &&
		before.Target.PaneId == after.Target.PaneId &&
		before.Target.TerminalId == after.Target.TerminalId &&
		(expected.AgentSessionId == "" || after.Target.AgentSessionId == expected.AgentSessionId) &&
		before.Provider == after.Provider &&
		before.WorkspaceId == after.WorkspaceId && before.TabId == after.TabId
}

func parseAgentInfo(raw json.RawMessage) (*pb.AgentView, error) {
	var info object
	if decodeRequired(raw, &info) != nil {
		return nil, ErrAgentUnavailable
	}
	// An absent/null provider is not sufficient authority for input delivery.
	view := &pb.AgentView{Target: &pb.AgentTarget{}, Provider: "unknown"}
	var err error
	view.DisplayName, view.Directory, err = displayMetadata(info, "agents")
	if err != nil {
		return nil, ErrAgentUnavailable
	}
	var focused bool
	for _, field := range []struct {
		key string
		out any
	}{
		{"pane_id", &view.Target.PaneId}, {"terminal_id", &view.Target.TerminalId},
		{"workspace_id", &view.WorkspaceId}, {"tab_id", &view.TabId},
		{"agent_status", &view.Status}, {"revision", &view.Revision}, {"focused", &focused},
	} {
		if required(info, field.key, field.out) != nil {
			return nil, ErrAgentUnavailable
		}
	}
	for _, field := range []struct {
		key string
		out any
	}{
		{"interactive_ready", &view.InteractiveReady},
		{"launch_pending", &view.LaunchPending}, {"state_change_seq", &view.StateChangeSeq},
	} {
		if raw, exists := info[field.key]; exists && decodeRequired(raw, field.out) != nil {
			return nil, ErrAgentUnavailable
		}
	}
	if raw, exists := info["agent"]; exists && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if decodeRequired(raw, &view.Provider) != nil {
			return nil, ErrAgentUnavailable
		}
	}
	if raw, exists := info["agent_session"]; exists && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		var session object
		var kind, value, provider, source string
		if decodeRequired(raw, &session) != nil ||
			required(session, "kind", &kind) != nil || required(session, "value", &value) != nil ||
			required(session, "agent", &provider) != nil || required(session, "source", &source) != nil {
			return nil, ErrAgentUnavailable
		}
		switch kind {
		case "id":
			// A colon can turn even an otherwise bounded token into a URI.
			if !idPattern.MatchString(value) || strings.Contains(value, ":") || provider != view.Provider {
				return nil, ErrAgentUnavailable
			}
			view.Target.AgentSessionId = value
		case "path":
			// Local session file references never cross the adapter boundary.
		default:
			return nil, ErrAgentUnavailable
		}
	}
	if protocol.ValidateAgentView(view) != nil {
		return nil, ErrAgentUnavailable
	}
	return view, nil
}
