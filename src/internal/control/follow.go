package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// FollowAgent polls bounded snapshots using one transport connection and one
// pinned target. Cancellation ends observation, never the remote task.
func FollowAgent(ctx context.Context, options Options, query *pb.AgentQueryRequest, interval time.Duration) error {
	if options.RequiredServerTag == "" || options.Output == nil {
		return errors.New("agent following requires an expected server tag and output")
	}
	if interval < 250*time.Millisecond || interval > 30*time.Second {
		return errors.New("follow interval must be 250ms..30s")
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("agent following requires an overall deadline")
	}
	selection, err := protocol.NormalizeAgentSelection(query)
	if err != nil {
		return err
	}
	if selection.Kind != pb.AgentQueryKind_AGENT_QUERY_KIND_READ {
		return errors.New("only agent read supports following")
	}
	return withFleet(ctx, options, func(client pb.FleetClient, _ transport.SelfStatus) error {
		return followAgentWithClient(ctx, options, client, selection, interval)
	})
}

type followEvent struct {
	Type       string          `json:"type"`
	ObservedAt string          `json:"observed_at"`
	ErrorCode  string          `json:"error_code,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
}

func writeFollowEvent(options Options, kind, code string, result *pb.AgentQueryResult) error {
	event := followEvent{Type: kind, ErrorCode: code, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if result != nil {
		data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(result)
		if err != nil {
			return err
		}
		event.Data = data
	}
	if options.JSON {
		return json.NewEncoder(options.Output).Encode(event)
	}
	if _, err := fmt.Fprintf(options.Output, "[%s %s", event.ObservedAt, kind); err != nil {
		return err
	}
	if code != "" {
		if _, err := fmt.Fprintf(options.Output, " %s", code); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(options.Output, "]"); err != nil {
		return err
	}
	if result != nil {
		if _, err := fmt.Fprintln(options.Output, result.Text); err != nil {
			return err
		}
		if result.Truncated {
			_, err := fmt.Fprintln(options.Output, "[snapshot truncated]")
			return err
		}
	}
	return nil
}

func followAgentWithClient(ctx context.Context, options Options, client pb.FleetClient, selection *pb.AgentQueryRequest, interval time.Duration) error {
	selection = proto.Clone(selection).(*pb.AgentQueryRequest)
	if selection.Target.TerminalId == "" ||
		(selection.Target.SessionName != "" && selection.Target.SessionIncarnation == "") {
		lookup := proto.Clone(selection).(*pb.AgentQueryRequest)
		lookup.Kind, lookup.Lines = pb.AgentQueryKind_AGENT_QUERY_KIND_GET, 0
		result, err := queryAgent(ctx, client, lookup)
		if err != nil {
			return err
		}
		selection.Target = proto.Clone(result.Agent.Target).(*pb.AgentTarget)
	}
	request, err := protocol.NormalizeAgentQuery(selection)
	if err != nil {
		return err
	}
	var previous *pb.AgentQueryResult
	var gap string
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		poll, cancel := context.WithTimeout(ctx, time.Duration(request.TimeoutMs)*time.Millisecond)
		result, callErr := client.QueryAgent(poll, request)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		code := ""
		transient := false
		if callErr != nil {
			switch status.Code(callErr) {
			case codes.Unavailable:
				code, transient = "disconnected", true
			case codes.DeadlineExceeded:
				code, transient = "timeout", true
			case codes.ResourceExhausted:
				code, transient = "overloaded", true
			default:
				if err := writeFollowEvent(options, "error", "query_failed", nil); err != nil {
					return err
				}
				return fmt.Errorf("follow agent: %w", callErr)
			}
		} else {
			if err := protocol.ValidateAgentQueryResult(result, request); err != nil {
				if writeErr := writeFollowEvent(options, "error", "invalid_response", nil); writeErr != nil {
					return writeErr
				}
				return fmt.Errorf("follow agent: %w", err)
			}
			code = result.ErrorCode
			switch code {
			case "herdr_unavailable", "session_unavailable", "session_manager_unavailable", "timeout", "overloaded":
				transient = true
			}
		}
		if code != "" {
			kind := "gap"
			if !transient {
				kind = "error"
			}
			if code != gap || !transient {
				if err := writeFollowEvent(options, kind, code, nil); err != nil {
					return err
				}
			}
			if !transient {
				return fmt.Errorf("follow agent: %s", code)
			}
			gap = code
		} else {
			if gap != "" {
				if err := writeFollowEvent(options, "resumed", "", nil); err != nil {
					return err
				}
			}
			if gap != "" || previous == nil || previous.Text != result.Text ||
				previous.Truncated != result.Truncated || !proto.Equal(previous.Agent, result.Agent) {
				if err := writeFollowEvent(options, "snapshot", "", result); err != nil {
					return err
				}
			}
			gap = ""
			previous = result
			request.Target = proto.Clone(result.Agent.Target).(*pb.AgentTarget)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
