package herdr

import (
	"context"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

const maxAgentTextBytes = protocol.MaxAgentOutputBytes

// QueryAgent returns an ephemeral observation, never a semantic task-success
// claim. READ and WAIT verify terminal identity before and after the query;
// Herdr's target-only API cannot make these checks atomic with local replacement.
// No query output is logged or persisted. QueryId is assigned by the caller.
func QueryAgent(ctx context.Context, config Config, query *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
	return queryAgent(ctx, config, query, dialLocal)
}

func queryAgent(ctx context.Context, config Config, query *pb.AgentQueryRequest, dial dialFunc) (*pb.AgentQueryResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	query, err := protocol.NormalizeAgentQuery(query)
	if err != nil || query == nil || query.TimeoutMs < 1 ||
		time.Duration(query.TimeoutMs)*time.Millisecond > protocol.MaxAgentQueryTimeout {
		return nil, ErrAgentPrecondition
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(query.TimeoutMs)*time.Millisecond)
	defer cancel()
	o, err := agentObserver(ctx, config, dial)
	if err != nil {
		return nil, err
	}
	before, err := o.getAgent(ctx, query.Target)
	if err != nil {
		return nil, err
	}
	result := &pb.AgentQueryResult{Agent: before}
	switch query.Kind {
	case pb.AgentQueryKind_AGENT_QUERY_KIND_GET:
		return result, nil
	case pb.AgentQueryKind_AGENT_QUERY_KIND_READ:
		read, err := o.agentRequest(ctx, "agent.read", "pane_read", struct {
			Target    string `json:"target"`
			Source    string `json:"source"`
			Lines     uint32 `json:"lines"`
			StripANSI bool   `json:"strip_ansi"`
			Format    string `json:"format"`
		}{query.Target.PaneId, "recent_unwrapped", query.Lines, true, "text"})
		if err != nil {
			return nil, agentFailure(ctx, ErrAgentUnavailable)
		}
		result.Text, result.Truncated, err = parseAgentRead(read, before, query.Lines)
		if err != nil {
			return nil, agentFailure(ctx, err)
		}
	case pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT:
		// WAIT gets the query's remaining budget, not the observer's default
		// three-second request timeout. The outer context also bounds dialing.
		deadline, _ := ctx.Deadline()
		remaining := time.Until(deadline)
		if remaining < time.Millisecond {
			return nil, agentFailure(ctx, ErrAgentUnavailable)
		}
		o.config.RequestTimeout = remaining
		waited, err := o.agentRequest(ctx, "agent.wait", "agent_info", struct {
			Target    string   `json:"target"`
			Until     []string `json:"until"`
			TimeoutMs uint32   `json:"timeout_ms"`
		}{query.Target.PaneId, query.Until, uint32(remaining / time.Millisecond)})
		if err != nil {
			return nil, agentFailure(ctx, ErrAgentUnavailable)
		}
		// Protocol 18 WAIT returns the same agent_info response as GET.
		observed, err := parseAgentInfo(waited["agent"])
		if err != nil {
			return nil, agentFailure(ctx, ErrAgentUnavailable)
		}
		if !sameAgent(before, observed, query.Target) {
			return nil, agentFailure(ctx, ErrAgentChanged)
		}
		if !slices.Contains(query.Until, observed.Status) {
			return nil, agentFailure(ctx, ErrAgentUnavailable)
		}
		result.Agent = observed
	default:
		return nil, ErrAgentPrecondition
	}
	after, err := o.getAgent(ctx, query.Target)
	if err != nil {
		return nil, err
	}
	if !sameAgent(before, after, query.Target) {
		return nil, agentFailure(ctx, ErrAgentChanged)
	}
	if query.Kind == pb.AgentQueryKind_AGENT_QUERY_KIND_READ {
		result.Agent = after
	}
	return result, nil
}

func parseAgentRead(result object, before *pb.AgentView, lines uint32) (string, bool, error) {
	var read object
	var pane, workspace, tab, source, format, text string
	var revision uint64
	var truncated bool
	if required(result, "read", &read) != nil {
		return "", false, ErrAgentUnavailable
	}
	for _, field := range []struct {
		key string
		out any
	}{
		{"pane_id", &pane}, {"workspace_id", &workspace}, {"tab_id", &tab},
		{"source", &source}, {"format", &format}, {"text", &text},
		{"revision", &revision}, {"truncated", &truncated},
	} {
		if required(read, field.key, field.out) != nil {
			return "", false, ErrAgentUnavailable
		}
	}
	if pane != before.Target.PaneId || workspace != before.WorkspaceId || tab != before.TabId {
		return "", false, ErrAgentChanged
	}
	if source != "recent_unwrapped" || format != "text" || !utf8.ValidString(text) || lines < 1 || lines > 1000 {
		return "", false, ErrAgentUnavailable
	}
	text, shortened := boundedAgentText(text, int(lines))
	return text, truncated || shortened, nil
}

func boundedAgentText(text string, lines int) (string, bool) {
	var out strings.Builder
	out.Grow(min(len(text), maxAgentTextBytes))
	line := 1
	shortened := false
	// Herdr is asked to strip ANSI. Independently remove terminal controls,
	// including complete CSI/string sequences if the server leaves any behind.
	const (
		plain = iota
		escape
		csi
		controlString
		stringEscape
	)
	state := plain
	for _, r := range text {
		switch state {
		case escape:
			switch r {
			case '[':
				state = csi
			case ']', 'P', 'X', '^', '_':
				state = controlString
			default:
				if r >= 0x30 && r <= 0x7e {
					state = plain
				}
			}
			continue
		case csi:
			if r >= 0x40 && r <= 0x7e {
				state = plain
			}
			continue
		case controlString:
			if r == 0x07 || r == 0x9c {
				state = plain
			} else if r == 0x1b {
				state = stringEscape
			}
			continue
		case stringEscape:
			if r == '\\' || r == 0x9c || r == 0x07 {
				state = plain
			} else if r != 0x1b {
				state = controlString
			}
			continue
		}
		switch r {
		case 0x1b:
			state = escape
			continue
		case 0x9b:
			state = csi
			continue
		case 0x90, 0x98, 0x9d, 0x9e, 0x9f:
			state = controlString
			continue
		}
		if (r < 0x20 && r != '\n' && r != '\t') || (r >= 0x7f && r <= 0x9f) {
			continue
		}
		if line > lines || out.Len()+utf8.RuneLen(r) > maxAgentTextBytes {
			shortened = true
			break
		}
		out.WriteRune(r)
		if r == '\n' {
			line++
		}
	}
	return protocol.SanitizeAgentText(out.String()), shortened
}
