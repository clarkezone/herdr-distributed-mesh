package server

import (
	"context"
	"errors"
	"log"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *service) SubmitCommand(ctx context.Context, request *agentflowv1.SubmitCommandRequest) (*agentflowv1.CommandRecord, error) {
	if s.requiredCommandTag == "" {
		return nil, status.Error(codes.PermissionDenied, "command authorization is not configured")
	}
	identity, err := s.authorizePeer(ctx, s.requiredCommandTag)
	if err != nil {
		log.Printf("command admission denied: unauthorized_peer")
		return nil, err
	}
	if s.commands == nil {
		return nil, status.Error(codes.Unimplemented, "command journals are not configured")
	}
	if request != nil && request.CommandType != protocol.ProbeCommandType {
		log.Printf("command admission denied actor=%s reason=unsupported_command", identity.StableID)
		return nil, status.Error(codes.PermissionDenied, "only read-only node ping is permitted")
	}
	request, err = protocol.NormalizeProbeRequest(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if s.fleet.storageErr != nil {
		return nil, storageUnavailable()
	}
	op, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	existing, err := s.commands.FindCommand(op, identity.StableID, request.IdempotencyKey)
	if err == nil {
		if existing.Command.TargetId != request.NodeInstanceId || existing.Command.CommandType != request.CommandType ||
			!proto.Equal(existing.Command.Ttl, request.Ttl) {
			return nil, status.Error(codes.AlreadyExists, "idempotency key was used for a different request")
		}
		return existing, nil
	}
	if !errors.Is(err, state.ErrCommandNotFound) {
		return nil, s.commandErrorLocked(err)
	}
	entry := s.fleet.nodes[request.NodeInstanceId]
	if entry == nil || !s.fleet.current(entry) || !entry.view.Connected || !entry.probes || !entry.view.CommandReady {
		log.Printf("command admission denied actor=%s reason=node_not_ready", identity.StableID)
		return nil, status.Error(codes.FailedPrecondition, "target node has no ready probe session")
	}
	if len(entry.outbound) >= cap(entry.outbound) {
		return nil, status.Error(codes.ResourceExhausted, "node command queue is full")
	}
	now := time.Now()
	command := &agentflowv1.Command{
		CommandId: protocol.NewCommandID(), IdempotencyKey: request.IdempotencyKey, CommandType: request.CommandType,
		TargetId: request.NodeInstanceId, Ttl: request.Ttl, ExpiresAt: timestamppb.New(now.Add(request.Ttl.AsDuration())),
		Actor: &agentflowv1.Actor{ActorId: identity.StableID, Role: agentflowv1.Role_ROLE_CONTROLLER},
	}
	record, created, err := s.commands.CreateCommand(op, command, now)
	if err != nil {
		return nil, s.commandErrorLocked(err)
	}
	if created {
		select {
		case entry.outbound <- &agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_Command{Command: record.Command}}:
		default:
			return nil, s.fleet.failLocked(errors.New("command queue capacity invariant failed"))
		}
	}
	return record, nil
}

func (s *service) GetCommand(ctx context.Context, request *agentflowv1.GetCommandRequest) (*agentflowv1.CommandRecord, error) {
	if s.requiredCommandTag == "" {
		return nil, status.Error(codes.PermissionDenied, "command authorization is not configured")
	}
	identity, err := s.authorizePeer(ctx, s.requiredCommandTag)
	if err != nil {
		return nil, err
	}
	if s.commands == nil {
		return nil, status.Error(codes.Unimplemented, "command journals are not configured")
	}
	if request == nil || !protocol.ValidCommandID(request.CommandId) || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "a valid command ID is required")
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if s.fleet.storageErr != nil {
		return nil, storageUnavailable()
	}
	op, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	record, err := s.commands.GetCommand(op, identity.StableID, request.CommandId)
	if err != nil {
		return nil, s.commandErrorLocked(err)
	}
	return record, nil
}

func (s *service) commandErrorLocked(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, state.ErrCommandNotFound):
		return status.Error(codes.NotFound, "command not found")
	case errors.Is(err, state.ErrCommandConflict), errors.Is(err, state.ErrIdentityConflict):
		return status.Error(codes.FailedPrecondition, "command identity or outcome conflict")
	case errors.Is(err, state.ErrCommandCapacity):
		return status.Error(codes.ResourceExhausted, "command journal capacity reached; records were not discarded")
	default:
		return s.fleet.failLocked(err)
	}
}

func (s *service) prepareCommand(entry *fleetEntry, pending *agentflowv1.Command) (*agentflowv1.Command, error) {
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if !s.fleet.current(entry) {
		return nil, status.Error(codes.Aborted, "node stream superseded")
	}
	if s.fleet.storageErr != nil {
		return nil, storageUnavailable()
	}
	if pending == nil || s.commands == nil || !entry.probes {
		return nil, status.Error(codes.FailedPrecondition, "probe dispatch is not available")
	}
	op, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	record, dispatch, err := s.commands.DispatchCommand(op, entry.view.InstanceId, pending.CommandId, time.Now())
	if err != nil {
		return nil, s.commandErrorLocked(err)
	}
	if !dispatch {
		return nil, nil
	}
	command := proto.Clone(record.Command).(*agentflowv1.Command)
	remaining := time.Until(command.ExpiresAt.AsTime())
	if remaining <= 0 {
		_, err := s.commands.FinishCommand(op, entry.view.InstanceId, entry.view.TailscaleStableId,
			&agentflowv1.CommandResult{CommandId: command.CommandId, Status: agentflowv1.CommandStatus_COMMAND_STATUS_TIMED_OUT, Detail: "deadline_expired"}, time.Now())
		if err != nil {
			return nil, s.commandErrorLocked(err)
		}
		return nil, nil
	}
	command.Ttl = durationpb.New(remaining)
	return command, nil
}

func (s *service) finishCommand(entry *fleetEntry, result *agentflowv1.CommandResult) (*agentflowv1.CommandAck, error) {
	if err := protocol.ValidateProbeResult(result); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if !s.fleet.current(entry) {
		return nil, status.Error(codes.Aborted, "node stream superseded")
	}
	if s.fleet.storageErr != nil {
		return nil, storageUnavailable()
	}
	op, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.commands.FinishCommand(op, entry.view.InstanceId, entry.view.TailscaleStableId, result, time.Now())
	if err != nil {
		return nil, s.commandErrorLocked(err)
	}
	// Receipt acknowledges the node's stored result, not a reconciled coordinator status.
	return &agentflowv1.CommandAck{CommandId: result.CommandId, Status: result.Status}, nil
}
