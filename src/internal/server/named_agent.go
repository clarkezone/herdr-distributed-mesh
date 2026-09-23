package server

import (
	"context"
	"errors"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *service) ResolveNamedAgent(ctx context.Context, request *pb.ResolveNamedAgentRequest) (*pb.NamedAgentRecord, error) {
	if s.requiredCommandTag == "" {
		return nil, status.Error(codes.PermissionDenied, "command authorization is not configured")
	}
	if _, err := s.authorizePeer(ctx, s.requiredCommandTag); err != nil {
		return nil, err
	}
	if s.commands == nil {
		return nil, status.Error(codes.Unimplemented, "command journals are not configured")
	}
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 ||
		!protocol.ValidIdempotencyKey(request.NodeInstanceId) || !protocol.ValidIdempotencyKey(request.Name) ||
		protocol.ValidateSessionSelector(request.SessionName, "", false) != nil {
		return nil, status.Error(codes.InvalidArgument, "valid node, agent name and session are required")
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if s.fleet.storageErr != nil {
		return nil, storageUnavailable()
	}
	op, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	record, err := s.commands.FindNamedAgent(op, request.NodeInstanceId, request.SessionName, request.Name)
	if errors.Is(err, state.ErrNamedAgentAmbiguous) {
		return nil, status.Error(codes.FailedPrecondition, "agent name is ambiguous; refusing to select a newer pane")
	}
	if err != nil {
		return nil, s.commandErrorLocked(err)
	}
	projection := proto.Clone(record).(*pb.CommandRecord)
	promptPresent := projection.Command.GetSubmittedRequest().GetAgentStart().GetInitialPrompt() != ""
	if projection.Command.SubmittedRequest != nil && projection.Command.SubmittedRequest.AgentStart != nil {
		projection.Command.SubmittedRequest.AgentStart.InitialPrompt = ""
	}
	return &pb.NamedAgentRecord{Record: projection, InitialPromptPresent: promptPresent}, nil
}
