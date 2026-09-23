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

type queuedCommand struct {
	command      *agentflowv1.Command
	actorContext context.Context
}

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
	if request != nil && request.CommandType != protocol.ProbeCommandType && request.CommandType != protocol.WorkspaceEnsureCommandType && request.CommandType != protocol.WorktreeCreateCommandType && request.CommandType != protocol.AgentControlCommandType && request.CommandType != protocol.SessionEnsureCommandType && !protocol.IsLifecycleCommand(request.CommandType) {
		log.Printf("command admission denied actor=%s reason=unsupported_command", identity.StableID)
		return nil, status.Error(codes.PermissionDenied, "unsupported command type")
	}
	request, err = protocol.NormalizeCommandRequest(request)
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
		if original := existing.Command.SubmittedRequest; original != nil {
			if !proto.Equal(original, request) {
				return nil, status.Error(codes.AlreadyExists, "idempotency key was used for a different request")
			}
			return existing, nil
		}
		if existing.Command.TargetId != request.NodeInstanceId || existing.Command.CommandType != request.CommandType ||
			!proto.Equal(existing.Command.Ttl, request.Ttl) || !proto.Equal(existing.Command.WorkspaceEnsure, request.WorkspaceEnsure) ||
			!proto.Equal(existing.Command.WorktreeCreate, request.WorktreeCreate) || !proto.Equal(existing.Command.AgentControl, request.AgentControl) ||
			!proto.Equal(existing.Command.SessionEnsure, request.SessionEnsure) {
			return nil, status.Error(codes.AlreadyExists, "idempotency key was used for a different request")
		}
		return existing, nil
	}
	if !errors.Is(err, state.ErrCommandNotFound) {
		return nil, s.commandErrorLocked(err)
	}
	entry := s.fleet.nodes[request.NodeInstanceId]
	name, incarnation := requestSession(request)
	if entry == nil || !s.fleet.current(entry) || !entry.view.Connected {
		log.Printf("command admission denied actor=%s reason=node_not_ready", identity.StableID)
		return nil, status.Error(codes.FailedPrecondition, "target node has no ready probe session")
	}
	if request.CommandType == protocol.SessionEnsureCommandType {
		if !s.sessionsConfigured() || !sessionManagerReady(entry, time.Now()) {
			return nil, status.Error(codes.FailedPrecondition, "target node has no ready session manager")
		}
	} else if name != "" {
		if !mutationReady(entry, request.CommandType, time.Now(), name, incarnation) {
			return nil, status.Error(codes.FailedPrecondition, "selected session is not ready")
		}
	} else if !entry.probes || !entry.view.CommandReady {
		return nil, status.Error(codes.FailedPrecondition, "target node has no ready probe session")
	}
	var submitted *agentflowv1.SubmitCommandRequest
	// Agent controls are already fully resolved and use their exact typed body
	// as retry identity. Duplicating an 8 KiB prompt would exceed the journal.
	if request.CommandType != protocol.AgentControlCommandType &&
		(name != "" || incarnation != "" || protocol.IsLifecycleCommand(request.CommandType)) {
		submitted = proto.Clone(request).(*agentflowv1.SubmitCommandRequest)
	}
	if projectID, revision := protocol.RequestProject(request); projectID != "" {
		if entry.projects || s.workspacePolicy == nil || request.AgentStart != nil {
			record, err := s.managedProjectReadyLocked(op, entry, projectID, revision)
			if err != nil {
				return nil, err
			}
			submitted = proto.Clone(request).(*agentflowv1.SubmitCommandRequest)
			revision = protocol.ProjectRevision(record.Desired.Generation)
			if request.AgentStart != nil {
				request.AgentStart.BindingRevision = revision
			}
			if request.WorkspaceEnsure != nil {
				request.WorkspaceEnsure.BindingRevision = revision
			}
			if w := request.WorktreeCreate; w != nil {
				w.BindingRevision = revision
				if w.Branch == "" {
					w.Branch = w.Name
				}
			}
		} else {
			binding, err := s.workspacePolicy.Resolve(request.NodeInstanceId, projectID, revision, identity.StableID)
			if err != nil || (request.CommandType == protocol.WorktreeCreateCommandType && !binding.AllowWorktrees) {
				return nil, status.Error(codes.PermissionDenied, "project operation is not authorized")
			}
		}
		if !mutationReady(entry, request.CommandType, time.Now(), name, incarnation) {
			return nil, status.Error(codes.FailedPrecondition, "target node has no fresh session for this operation")
		}
	}
	if (request.CommandType == protocol.AgentControlCommandType || protocol.IsLifecycleCommand(request.CommandType)) &&
		(!s.agentConfigured() || !mutationReady(entry, request.CommandType, time.Now(), name, incarnation)) {
		return nil, status.Error(codes.FailedPrecondition, "target node has no ready agent session")
	}
	if name != "" && incarnation == "" && request.CommandType != protocol.SessionEnsureCommandType {
		// Resolve against the same locked inventory used for admission, but
		// retain the operator's unpinned request as the exact retry identity.
		selected := findSession(entry.view, name)
		if selected == nil || !protocol.ValidSessionIncarnation(selected.Incarnation) {
			return nil, status.Error(codes.FailedPrecondition, "selected session has no current incarnation")
		}
		if request.WorkspaceEnsure != nil {
			request.WorkspaceEnsure.SessionIncarnation = selected.Incarnation
		}
		if request.WorktreeCreate != nil {
			request.WorktreeCreate.SessionIncarnation = selected.Incarnation
		}
		if request.AgentStart != nil {
			request.AgentStart.SessionIncarnation = selected.Incarnation
		}
	}
	if len(entry.outbound) >= cap(entry.outbound) {
		return nil, status.Error(codes.ResourceExhausted, "node command queue is full")
	}
	now := time.Now()
	command := &agentflowv1.Command{
		CommandId: protocol.NewCommandID(), IdempotencyKey: request.IdempotencyKey, CommandType: request.CommandType,
		TargetId: request.NodeInstanceId, Ttl: request.Ttl, ExpiresAt: timestamppb.New(now.Add(request.Ttl.AsDuration())),
		Actor:            &agentflowv1.Actor{ActorId: identity.StableID, Role: agentflowv1.Role_ROLE_CONTROLLER},
		WorkspaceEnsure:  request.WorkspaceEnsure,
		WorktreeCreate:   request.WorktreeCreate,
		AgentControl:     request.AgentControl,
		SessionEnsure:    request.SessionEnsure,
		AgentStart:       request.AgentStart,
		AgentStop:        request.AgentStop,
		SubmittedRequest: submitted,
	}
	if command.AgentStart != nil {
		command.AgentStart.InitialPrompt = ""
		deadline, err := protocol.LifecycleExecutionDeadline(command.ExpiresAt.AsTime(), command.AgentStart.StartupTimeoutMs)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		command.ExecutionExpiresAt = timestamppb.New(deadline)
	}
	if err := protocol.ValidateCommand(command, command.TargetId); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	record, created, err := s.commands.CreateCommand(op, command, now)
	if err != nil {
		return nil, s.commandErrorLocked(err)
	}
	if created {
		select {
		case entry.outbound <- queuedCommand{command: record.Command, actorContext: context.WithoutCancel(ctx)}:
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
	_, err := s.authorizePeer(ctx, s.requiredCommandTag)
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
	record, err := s.commands.GetOperatorCommand(op, request.CommandId)
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
	case errors.Is(err, state.ErrLifecycleUnresolved):
		return status.Error(codes.FailedPrecondition, "native target has unresolved effects; use its original receipt or explicitly pinned stop/interrupt")
	case errors.Is(err, state.ErrNamedAgentExists):
		return status.Error(codes.FailedPrecondition, "agent name already has a durable start; resolve its original receipt instead of launching again")
	case errors.Is(err, state.ErrCommandCapacity):
		return status.Error(codes.ResourceExhausted, "command journal capacity reached; records were not discarded")
	default:
		return s.fleet.failLocked(err)
	}
}

func (s *service) prepareCommand(entry *fleetEntry, queued queuedCommand) (*agentflowv1.Command, error) {
	pending := queued.command
	authorized := true
	projectID, _ := protocol.CommandProject(pending)
	mutation := projectID != "" || pending.GetCommandType() == protocol.AgentControlCommandType || pending.GetCommandType() == protocol.SessionEnsureCommandType || protocol.IsLifecycleCommand(pending.GetCommandType())
	if mutation {
		authorized = s.refreshMutationAuthorization(entry, queued)
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if !s.fleet.current(entry) {
		return nil, status.Error(codes.Aborted, "node stream superseded")
	}
	if s.fleet.storageErr != nil {
		return nil, storageUnavailable()
	}
	if pending == nil || s.commands == nil || (!entry.probes && !(pending.CommandType == protocol.SessionEnsureCommandType && entry.sessions)) {
		return nil, status.Error(codes.FailedPrecondition, "probe dispatch is not available")
	}
	op, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if projectID != "" && pending.SubmittedRequest != nil && (entry.projects || s.workspacePolicy == nil) {
		project, revision := protocol.CommandProject(pending)
		if _, err := s.managedProjectReadyLocked(op, entry, project, revision); err != nil {
			if s.fleet.storageErr != nil {
				return nil, err
			}
			authorized = false
		}
	}
	name, incarnation := protocol.CommandSession(pending)
	if mutation && (!authorized || !mutationReady(entry, pending.CommandType, time.Now(), name, incarnation)) {
		if _, err := s.commands.RejectCommand(op, entry.view.InstanceId, pending.CommandId, "authorization_changed", time.Now()); err != nil {
			return nil, s.commandErrorLocked(err)
		}
		return nil, nil
	}
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
	if err := protocol.ValidateCommandResult(result); err != nil {
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
