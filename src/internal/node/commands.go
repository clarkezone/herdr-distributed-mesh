package node

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const DefaultRequiredServerTag = "tag:herdr-mesh-server"

const journalOperationTimeout = 5 * time.Second
const maxEphemeralResults = 64

type commandJournal interface {
	Recover(context.Context) error
	Claim(context.Context, *pb.Command) (*pb.CommandResult, bool, error)
	Complete(context.Context, *pb.CommandResult) error
	PendingResults(context.Context) ([]*pb.CommandResult, error)
	Acknowledge(context.Context, string, pb.CommandStatus) error
}

type journalError struct{ err error }

func (e *journalError) Error() string { return e.err.Error() }
func (e *journalError) Unwrap() error { return e.err }

func storageError(operation string, err error) error {
	return &journalError{fmt.Errorf("node command journal %s: %w", operation, err)}
}

func openJournal(ctx context.Context, options Options) (*state.NodeJournal, error) {
	if options.EnableAgentControl && (options.CommandJournalPath == "" || strings.TrimSpace(options.HerdrSocket) == "" || strings.TrimSpace(options.RequiredServerTag) == "") {
		return nil, errors.New("agent control requires a command journal, Herdr socket, and required server tag")
	}
	if options.WorkspacePolicy != nil && (options.CommandJournalPath == "" || strings.TrimSpace(options.HerdrSocket) == "" || strings.TrimSpace(options.RequiredServerTag) == "") {
		return nil, errors.New("workspace policy requires a command journal, Herdr socket, and required server tag")
	}
	if options.CommandJournalPath == "" {
		return nil, nil
	}
	if strings.TrimSpace(options.RequiredServerTag) == "" {
		return nil, errors.New("required server tag must not be empty when probes are enabled")
	}
	ctx, cancel := context.WithTimeout(ctx, journalOperationTimeout)
	defer cancel()
	journal, err := state.OpenNodeJournal(ctx, options.CommandJournalPath, options.InstanceID)
	if err != nil {
		return nil, storageError("open", err)
	}
	if err := journal.Recover(ctx); err != nil {
		return nil, errors.Join(storageError("recover", err), journal.Close())
	}
	return journal, nil
}

// RunSession runs one session using an already authenticated client. Callers must
// authorize the actual peer on every dial (production uses DialGRPCWithPeerTag).
// It owns, recovers, and closes the requested journal for this session.
// Workspace, worktree, and agent effects additionally require Options.VerifyServerPeer;
// without one they fail closed. Run installs the production WhoIs verifier.
func RunSession(ctx context.Context, client pb.NodeControlClient, options Options) (registered bool, err error) {
	journal, err := openJournal(ctx, options)
	if err != nil {
		return false, err
	}
	if journal == nil {
		return runSessionWithJournal(ctx, client, options, nil, nil)
	}
	defer func() {
		if closeErr := journal.Close(); closeErr != nil {
			err = errors.Join(err, storageError("close", closeErr))
		}
	}()
	return runSessionWithJournal(ctx, client, options, journal, nil)
}

type commandHandler struct {
	journal             commandJournal
	nodeID              string
	execute             func()
	ephemeral           map[string]*pb.Command
	ephemeralMu         sync.Mutex
	workspacePolicy     *projects.Policy
	workspaceNegotiated bool
	worktreeNegotiated  bool
	herdrConfig         herdr.Config
	ensureWorkspace     workspaceExecutor
	createWorktree      worktreeExecutor
	verifyCoordinator   func(context.Context) error
	agentNegotiated     bool
	agentBaseline       atomic.Pointer[pb.HerdrState]
	controlAgent        func(context.Context, herdr.Config, *pb.AgentControl) (*pb.AgentControlResult, error)
}

type workspaceExecutor func(context.Context, herdr.Config, projects.Binding) (*pb.WorkspaceEnsureResult, error)
type worktreeExecutor func(context.Context, herdr.Config, projects.Binding, *pb.WorktreeCreate) (*pb.WorktreeCreateResult, error)

func (h *commandHandler) handle(ctx context.Context, command *pb.Command, receivedAt time.Time) (*pb.CommandResult, error) {
	if err := protocol.ValidateCommand(command, h.nodeID); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid command: %v", err)
	}
	if command.CommandType == protocol.WorkspaceEnsureCommandType && !h.workspaceNegotiated {
		return nil, status.Error(codes.FailedPrecondition, "workspace capability was not negotiated")
	}
	if command.CommandType == protocol.WorktreeCreateCommandType && !h.worktreeNegotiated {
		return nil, status.Error(codes.FailedPrecondition, "worktree capability was not negotiated")
	}
	if command.CommandType == protocol.AgentControlCommandType && !h.agentNegotiated {
		return nil, status.Error(codes.FailedPrecondition, "agent capability was not negotiated")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	operation, cancel := context.WithTimeout(ctx, journalOperationTimeout)
	result, claimed, err := h.journal.Claim(operation, command)
	operationErr := operation.Err()
	cancel()
	if errors.Is(err, state.ErrCommandConflict) {
		return nil, status.Error(codes.InvalidArgument, "conflicting duplicate command")
	}
	if errors.Is(err, state.ErrCommandCapacity) {
		h.ephemeralMu.Lock()
		defer h.ephemeralMu.Unlock()
		if previous := h.ephemeral[command.CommandId]; previous != nil {
			// The journal's duplicate fingerprint deliberately excludes remaining TTL.
			copy := proto.Clone(previous).(*pb.Command)
			copy.Ttl = command.Ttl
			if !proto.Equal(copy, command) {
				return nil, status.Error(codes.InvalidArgument, "conflicting rejected duplicate command")
			}
		} else {
			if len(h.ephemeral) >= maxEphemeralResults {
				return nil, status.Error(codes.ResourceExhausted, "too many unacknowledged journal_full results; reconnect required")
			}
			if h.ephemeral == nil {
				h.ephemeral = make(map[string]*pb.Command)
			}
			h.ephemeral[command.CommandId] = proto.Clone(command).(*pb.Command)
		}
		return &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_REJECTED, Detail: "journal_full"}, nil
	}
	if err != nil {
		return nil, storageError("claim", err)
	}
	if !claimed {
		if err := protocol.ValidateResultForCommand(result, command); err != nil {
			return nil, storageError("replay", fmt.Errorf("stored outcome is not terminal: %w", err))
		}
		return result, nil
	}
	if operationErr != nil {
		return nil, storageError("interrupted claim", operationErr)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result = &pb.CommandResult{CommandId: command.CommandId}
	now := time.Now()
	if !now.Before(command.ExpiresAt.AsTime()) || !now.Before(receivedAt.Add(command.Ttl.AsDuration())) {
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_TIMED_OUT, "deadline_expired"
	} else if command.CommandType == protocol.AgentControlCommandType {
		result = h.agentMutation(ctx, command, receivedAt)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	} else if command.CommandType == protocol.WorkspaceEnsureCommandType || command.CommandType == protocol.WorktreeCreateCommandType {
		result = h.mutation(ctx, command, receivedAt)
		// Cancellation can race a remote mutation. Leave the durable intent for
		// recovery rather than commit a potentially misleading executor result.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	} else {
		if h.execute != nil {
			h.execute()
		}
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, "pong"
	}
	if err := protocol.ValidateResultForCommand(result, command); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid command completion")
	}
	completion, stop := context.WithTimeout(context.WithoutCancel(ctx), journalOperationTimeout)
	defer stop()
	if err := h.journal.Complete(completion, result); err != nil {
		if errors.Is(err, state.ErrCommandConflict) {
			return nil, status.Error(codes.InvalidArgument, "conflicting command completion")
		}
		return nil, storageError("complete", err)
	}
	return result, nil
}

func (h *commandHandler) mutation(ctx context.Context, command *pb.Command, receivedAt time.Time) *pb.CommandResult {
	result := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_REJECTED, Detail: "project_not_authorized"}
	if h.workspacePolicy == nil {
		return result
	}
	projectID, revision := protocol.CommandProject(command)
	binding, err := h.workspacePolicy.Resolve(h.nodeID, projectID, revision, command.Actor.ActorId)
	if err != nil {
		return result
	}
	if command.CommandType == protocol.WorktreeCreateCommandType && !binding.AllowWorktrees {
		return result
	}
	deadline := command.ExpiresAt.AsTime()
	if remaining := receivedAt.Add(command.Ttl.AsDuration()); remaining.Before(deadline) {
		deadline = remaining
	}
	effect, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if effect.Err() != nil {
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_TIMED_OUT, "deadline_expired"
		return result
	}
	if h.verifyCoordinator == nil {
		result.Detail = "precondition_failed"
		return result
	}
	verification, stop := context.WithTimeout(effect, workspacePeerVerificationTimeout)
	err = h.verifyCoordinator(verification)
	verificationErr := verification.Err()
	stop()
	if effect.Err() != nil {
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_TIMED_OUT, "deadline_expired"
		return result
	}
	if err != nil || verificationErr != nil {
		result.Detail = "precondition_failed"
		return result
	}
	if command.CommandType == protocol.WorktreeCreateCommandType {
		execute := h.createWorktree
		if execute == nil {
			execute = herdr.CreateWorktree
		}
		value, effectErr := execute(effect, h.herdrConfig, binding, proto.Clone(command.WorktreeCreate).(*pb.WorktreeCreate))
		err = effectErr
		if err == nil {
			result.Status, result.Detail, result.WorktreeCreate = pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, "worktree_created", value
		}
	} else {
		execute := h.ensureWorkspace
		if execute == nil {
			execute = herdr.EnsureWorkspace
		}
		value, effectErr := execute(effect, h.herdrConfig, binding)
		err = effectErr
		if err == nil {
			result.Status, result.Detail, result.WorkspaceEnsure = pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, "workspace_present", value
			if value.GetCreated() {
				result.Detail = "workspace_created"
			}
		}
	}
	if err == nil {
		return result
	}
	switch {
	case errors.Is(err, herdr.ErrWorkspacePrecondition):
		result.Detail = "precondition_failed"
	case command.CommandType == protocol.WorkspaceEnsureCommandType && errors.Is(err, herdr.ErrWorkspaceAmbiguous):
		result.Detail = "ambiguous_workspace"
	case errors.Is(err, herdr.ErrWorkspaceUnavailable):
		result.Detail = "herdr_unavailable"
	default:
		// Unknown failures are conservatively uncertain, never exposed verbatim.
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, "herdr_outcome_unknown"
	}
	return result
}

func (h *commandHandler) acknowledge(ctx context.Context, ack *pb.CommandAck) error {
	if ack == nil || !protocol.ValidCommandID(ack.CommandId) || len(ack.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "invalid command acknowledgement")
	}
	h.ephemeralMu.Lock()
	if h.ephemeral[ack.CommandId] != nil {
		if ack.Status != pb.CommandStatus_COMMAND_STATUS_REJECTED {
			h.ephemeralMu.Unlock()
			return status.Error(codes.InvalidArgument, "conflicting journal_full acknowledgement")
		}
		delete(h.ephemeral, ack.CommandId)
		h.ephemeralMu.Unlock()
		return nil
	}
	h.ephemeralMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, journalOperationTimeout)
	defer cancel()
	err := h.journal.Acknowledge(ctx, ack.CommandId, ack.Status)
	if errors.Is(err, state.ErrCommandNotFound) || errors.Is(err, state.ErrCommandConflict) {
		return status.Errorf(codes.InvalidArgument, "invalid command acknowledgement: %v", err)
	}
	if err != nil {
		return storageError("acknowledge", err)
	}
	return nil
}
