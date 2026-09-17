package control

import (
	"context"
	"errors"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/types/known/durationpb"
)

// StartAgent reports readiness and optional prompt submission, not task success.
// Retrying the identical key/request retrieves its receipt, never relaunches.
func StartAgent(ctx context.Context, options Options, nodeID, key string, start *pb.AgentStart, ttl time.Duration) error {
	return lifecycleCommand(ctx, options, &pb.SubmitCommandRequest{NodeInstanceId: nodeID,
		IdempotencyKey: key, CommandType: protocol.AgentStartCommandType, AgentStart: start, Ttl: durationpb.New(ttl)})
}

// StopAgent requires the caller's full pinned handle; it never discovers or
// infers selectors and never stops the enclosing workspace or native session.
func StopAgent(ctx context.Context, options Options, nodeID, key string, stop *pb.AgentStop, ttl time.Duration) error {
	return lifecycleCommand(ctx, options, &pb.SubmitCommandRequest{NodeInstanceId: nodeID,
		IdempotencyKey: key, CommandType: protocol.AgentStopCommandType, AgentStop: stop, Ttl: durationpb.New(ttl)})
}

func lifecycleCommand(ctx context.Context, options Options, request *pb.SubmitCommandRequest) error {
	if options.RequiredServerTag == "" {
		return errors.New("lifecycle commands require an expected server tag")
	}
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = protocol.NewCommandID()
	}
	normalized, err := protocol.NormalizeCommandRequest(request)
	if err != nil {
		return err
	}
	return submitAndWait(ctx, options, normalized)
}
