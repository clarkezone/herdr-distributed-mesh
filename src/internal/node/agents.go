package node

import (
	"context"
	"errors"
	"sync"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const maxAgentQueries = 8

func (h *commandHandler) agentReady() bool {
	return h.agentNegotiated && h.journal != nil && h.verifyCoordinator != nil && workspaceBaselineReady(h.agentBaseline.Load(), time.Now())
}

func (h *commandHandler) selectedAgentReady(name string) bool {
	if name == "" {
		return h.agentReady()
	}
	return h.agentNegotiated && h.sessionsReady()
}

func (h *commandHandler) agentMutation(ctx context.Context, command *pb.Command, receivedAt time.Time) *pb.CommandResult {
	result := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_REJECTED, Detail: "precondition_failed"}
	name, expected := protocol.CommandSession(command)
	if !h.selectedAgentReady(name) {
		return result
	}
	deadline := command.ExpiresAt.AsTime()
	if remaining := receivedAt.Add(command.Ttl.AsDuration()); remaining.Before(deadline) {
		deadline = remaining
	}
	effect, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	verification, stop := context.WithTimeout(effect, workspacePeerVerificationTimeout)
	err := h.verifyCoordinator(verification)
	verificationErr := verification.Err()
	stop()
	if effect.Err() != nil {
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_TIMED_OUT, "deadline_expired"
		return result
	}
	if err != nil || verificationErr != nil {
		result.Detail = "authorization_changed"
		return result
	}
	if !h.selectedAgentReady(name) {
		return result
	}
	config, incarnation, err := h.resolveSession(effect, name, expected)
	if h.lifecycleScopeAvailable(name) && err == nil {
		config, incarnation, err = h.resolveLifecycleSession(effect, name, expected)
	}
	if err != nil {
		result.Detail = sessionError(err)
		return result
	}
	if h.lifecycleScopeAvailable(name) {
		journal, ok := h.journal.(lifecycleJournal)
		if !ok {
			result.Detail = "precondition_failed"
			return result
		}
		check := config.CheckSession
		config.CheckSession = func(ctx context.Context) error {
			if err := check(ctx); err != nil {
				return err
			}
			return journal.PinAgentCommand(ctx, command.CommandId, incarnation)
		}
		if err := config.CheckSession(effect); err != nil {
			result.Detail = lifecycleErrorDetail(err)
			return result
		}
	}
	execute := h.controlAgent
	if execute == nil {
		execute = herdr.ControlAgent
	}
	value, err := execute(effect, config, proto.Clone(command.AgentControl).(*pb.AgentControl))
	if err == nil {
		if incarnation != "" {
			if _, _, refreshErr := h.resolveSession(effect, name, incarnation); refreshErr != nil {
				result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, "herdr_outcome_unknown"
				return result
			}
		}
		if value == nil || value.Target == nil {
			result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, "herdr_outcome_unknown"
			return result
		}
		value.Target.SessionName, value.Target.SessionIncarnation = name, incarnation
		result.Status, result.AgentControl = pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, value
		result.Detail = protocol.AgentControlSuccessDetail(command.AgentControl.Action)
		return result
	}
	var sessionErr sessionFailure
	switch {
	case errors.Is(err, herdr.ErrAgentIndeterminate):
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, "herdr_outcome_unknown"
	case errors.Is(err, state.ErrLifecycleUnresolved):
		result.Detail = "lifecycle_unresolved"
	case errors.As(err, &sessionErr):
		result.Detail = sessionError(err)
	case errors.Is(err, context.DeadlineExceeded):
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_TIMED_OUT, "deadline_expired"
	case errors.Is(err, herdr.ErrAgentPrecondition):
		result.Detail = "precondition_failed"
	case errors.Is(err, herdr.ErrAgentBusy):
		result.Detail = "agent_busy"
	case errors.Is(err, herdr.ErrAgentBlocked):
		result.Detail = "agent_blocked"
	case errors.Is(err, herdr.ErrAgentChanged):
		result.Detail = "target_changed"
	case errors.Is(err, herdr.ErrAgentUnavailable):
		result.Detail = "herdr_unavailable"
	case errors.Is(err, herdr.ErrAgentUnsupported):
		result.Detail = "unsupported"
	default:
		// Once IPC starts an unclassified failure cannot prove absence of effects.
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, "herdr_outcome_unknown"
	}
	return result
}

type agentQueries struct {
	ctx     context.Context
	options Options
	handler *commandHandler
	mu      sync.Mutex
	active  map[string]context.CancelFunc
	workers sync.WaitGroup
	results chan *pb.AgentQueryResult
}

func newAgentQueries(ctx context.Context, options Options, handler *commandHandler) *agentQueries {
	return &agentQueries{ctx: ctx, options: options, handler: handler, active: make(map[string]context.CancelFunc), results: make(chan *pb.AgentQueryResult, maxAgentQueries)}
}

func (q *agentQueries) join() { q.workers.Wait() }

func (q *agentQueries) cancelQuery(request *pb.AgentQueryCancel) error {
	if request == nil || !protocol.ValidCommandID(request.QueryId) || len(request.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "invalid query cancellation")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if cancel := q.active[request.QueryId]; cancel != nil {
		cancel()
	}
	return nil
}

func (q *agentQueries) start(query *pb.AgentQuery) error {
	if err := protocol.ValidateAgentQuery(query, q.options.InstanceID); err != nil {
		return status.Error(codes.InvalidArgument, "invalid agent query")
	}
	request, err := protocol.NormalizeAgentQuery(query.Request)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid agent query")
	}
	query = proto.Clone(query).(*pb.AgentQuery)
	query.Request = request
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.active[query.QueryId] != nil {
		return status.Error(codes.InvalidArgument, "duplicate agent query")
	}
	if len(q.active) >= maxAgentQueries {
		select {
		case q.results <- &pb.AgentQueryResult{QueryId: query.QueryId, ErrorCode: "overloaded"}:
			return nil
		default:
			return status.Error(codes.ResourceExhausted, "agent query response queue full")
		}
	}
	deadline := query.ExpiresAt.AsTime()
	timeout := time.Duration(query.Request.TimeoutMs) * time.Millisecond
	if requested := time.Now().Add(timeout); requested.Before(deadline) {
		deadline = requested
	}
	ctx, cancel := context.WithDeadline(q.ctx, deadline)
	q.active[query.QueryId] = cancel
	q.workers.Add(1)
	go func() {
		defer q.workers.Done()
		defer cancel()
		defer func() { q.mu.Lock(); delete(q.active, query.QueryId); q.mu.Unlock() }()
		result := &pb.AgentQueryResult{QueryId: query.QueryId}
		if !q.handler.selectedAgentReady(query.Request.Target.SessionName) {
			result.ErrorCode = "herdr_unavailable"
		} else if ctx.Err() != nil {
			result.ErrorCode = agentQueryError(ctx.Err())
		} else {
			execute := q.options.QueryAgent
			if execute == nil {
				execute = herdr.QueryAgent
			}
			target := query.Request.Target
			config, incarnation, sessionErr := q.handler.resolveSession(ctx, target.SessionName, target.SessionIncarnation)
			if q.handler.lifecycleScopeAvailable(target.SessionName) && sessionErr == nil {
				config, incarnation, sessionErr = q.handler.resolveLifecycleSession(ctx, target.SessionName, target.SessionIncarnation)
			}
			var value *pb.AgentQueryResult
			var err error
			if sessionErr == nil {
				value, err = execute(ctx, config, proto.Clone(query.Request).(*pb.AgentQueryRequest))
				if err == nil && incarnation != "" {
					_, _, sessionErr = q.handler.resolveSession(ctx, target.SessionName, incarnation)
				}
			}
			if sessionErr != nil {
				result.ErrorCode = sessionError(sessionErr)
			} else if err != nil {
				result.ErrorCode = agentQueryError(err)
			} else if value == nil {
				result.ErrorCode = "invalid_request"
			} else {
				result = proto.Clone(value).(*pb.AgentQueryResult)
				result.QueryId = query.QueryId
				if result.Agent != nil && result.Agent.Target != nil {
					result.Agent.Target.SessionName, result.Agent.Target.SessionIncarnation = target.SessionName, incarnation
				}
				if err := protocol.ValidateAgentQueryResult(result, query.Request); err != nil {
					result = &pb.AgentQueryResult{QueryId: query.QueryId, ErrorCode: "invalid_request"}
				}
			}
		}
		if ctx.Err() != nil {
			result = &pb.AgentQueryResult{QueryId: query.QueryId, ErrorCode: agentQueryError(ctx.Err())}
		}
		select {
		case q.results <- result:
		case <-q.ctx.Done():
		}
	}()
	return nil
}

func agentQueryError(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, herdr.ErrAgentChanged):
		return "target_changed"
	case errors.Is(err, herdr.ErrAgentPrecondition):
		return "target_unavailable"
	case errors.Is(err, herdr.ErrAgentBusy):
		return "agent_busy"
	case errors.Is(err, herdr.ErrAgentBlocked):
		return "agent_blocked"
	case errors.Is(err, herdr.ErrAgentUnsupported):
		return "unsupported"
	default:
		return "herdr_unavailable"
	}
}
