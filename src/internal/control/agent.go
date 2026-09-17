package control

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Agent resolves a pane to an explicit terminal before reading or sending input.
// Supplying the original target bypasses discovery for durable command retries.
func Agent(ctx context.Context, options Options, query *pb.AgentQueryRequest, action *pb.AgentControl, key string, ttl time.Duration) error {
	if options.RequiredServerTag == "" {
		return errors.New("agent operations require an expected server tag")
	}
	if query == nil {
		return errors.New("agent selection is required")
	}
	selection, err := protocol.NormalizeAgentSelection(query)
	if err != nil {
		return err
	}
	lookup, err := protocol.NormalizeAgentQuery(&pb.AgentQueryRequest{NodeInstanceId: selection.NodeInstanceId, Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_GET,
		Target: selection.Target, TimeoutMs: selection.TimeoutMs})
	if err != nil {
		return err
	}
	var control *pb.AgentControl
	if action != nil {
		control = proto.Clone(action).(*pb.AgentControl)
		if !proto.Equal(control.Target, selection.Target) || len(control.ProtoReflect().GetUnknown()) != 0 {
			return errors.New("agent control must match the selected target")
		}
		if err := protocol.ValidateAgentControlInput(control.Action, control.Text, control.Keys); err != nil {
			return err
		}
		if key == "" {
			key = protocol.NewCommandID()
		}
		if !protocol.ValidIdempotencyKey(key) || ttl <= 0 || ttl > protocol.MaxCommandTTL {
			return errors.New("agent input requires a bounded idempotency key and a TTL in (0,30s]")
		}
		log.Printf("command type=%s idempotency_key=%s target_node=%s", protocol.AgentControlCommandType, key, selection.NodeInstanceId)
	}
	return withFleet(ctx, options, func(client pb.FleetClient, _ transport.SelfStatus) error {
		return agentWithClient(ctx, options, client, selection, lookup, control, key, ttl)
	})
}

func agentWithClient(ctx context.Context, options Options, client pb.FleetClient, selection, lookup *pb.AgentQueryRequest, control *pb.AgentControl, key string, ttl time.Duration) error {
	if selection.Target.TerminalId == "" || (selection.Target.SessionName != "" && selection.Target.SessionIncarnation == "") {
		result, err := queryAgent(ctx, client, lookup)
		if err != nil {
			return err
		}
		selection.Target = proto.Clone(result.Agent.Target).(*pb.AgentTarget)
		if control == nil && selection.Kind == pb.AgentQueryKind_AGENT_QUERY_KIND_GET {
			return writeAgent(options, selection.Kind, result)
		}
	}
	if control == nil {
		normalized, err := protocol.NormalizeAgentQuery(selection)
		if err != nil {
			return err
		}
		result, err := queryAgent(ctx, client, normalized)
		if err != nil {
			return err
		}
		return writeAgent(options, selection.Kind, result)
	}
	control.Target = selection.Target
	request, err := protocol.NormalizeCommandRequest(&pb.SubmitCommandRequest{NodeInstanceId: selection.NodeInstanceId,
		CommandType: protocol.AgentControlCommandType, IdempotencyKey: key, Ttl: durationpb.New(ttl), AgentControl: control})
	if err != nil {
		return err
	}
	log.Printf("agent target pane=%s terminal=%s agent_session=%s session=%s session_incarnation=%s", control.Target.PaneId, control.Target.TerminalId, control.Target.AgentSessionId, control.Target.SessionName, control.Target.SessionIncarnation)
	return submitAndWaitWithClient(ctx, options, client, request)
}

func queryAgent(ctx context.Context, client pb.FleetClient, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
	result, err := retryUnavailable(ctx, func() (*pb.AgentQueryResult, error) { return client.QueryAgent(ctx, request) })
	if err != nil {
		return nil, fmt.Errorf("query agent: %w", err)
	}
	if err := protocol.ValidateAgentQueryResult(result, request); err != nil {
		return nil, fmt.Errorf("invalid server agent response: %w", err)
	}
	if result.ErrorCode != "" {
		return nil, fmt.Errorf("agent query: %s", result.ErrorCode)
	}
	return result, nil
}

func writeAgent(options Options, kind pb.AgentQueryKind, result *pb.AgentQueryResult) error {
	if options.JSON {
		data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(result)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(options.Output, string(data))
		return err
	}
	if kind == pb.AgentQueryKind_AGENT_QUERY_KIND_READ {
		_, err := fmt.Fprintln(options.Output, result.Text)
		if result.Truncated {
			log.Printf("agent output truncated by the requested output limit")
		}
		return err
	}
	value := result.Agent
	_, err := fmt.Fprintf(options.Output, "agent=%s terminal=%s provider=%s status=%s ready=%t workspace=%s tab=%s state_change_seq=%d session=%s session_incarnation=%s agent_session=%s\n",
		value.Target.PaneId, value.Target.TerminalId, value.Provider, value.Status, value.InteractiveReady, value.WorkspaceId, value.TabId, value.StateChangeSeq, value.Target.SessionName, value.Target.SessionIncarnation, value.Target.AgentSessionId)
	return err
}
