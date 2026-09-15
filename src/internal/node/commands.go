package node

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
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
	journal   commandJournal
	nodeID    string
	execute   func()
	ephemeral map[string]*pb.Command
}

func (h *commandHandler) handle(ctx context.Context, command *pb.Command, receivedAt time.Time) (*pb.CommandResult, error) {
	if err := protocol.ValidateProbeCommand(command, h.nodeID); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid probe: %v", err)
	}
	operation, cancel := context.WithTimeout(ctx, journalOperationTimeout)
	defer cancel()
	result, claimed, err := h.journal.Claim(operation, command)
	if errors.Is(err, state.ErrCommandConflict) {
		return nil, status.Error(codes.InvalidArgument, "conflicting duplicate command")
	}
	if errors.Is(err, state.ErrCommandCapacity) {
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
		if err := protocol.ValidateProbeResult(result); err != nil {
			return nil, storageError("replay", fmt.Errorf("stored outcome is not terminal: %w", err))
		}
		if result.CommandId != command.CommandId {
			return nil, storageError("replay", errors.New("stored outcome has a different command ID"))
		}
		return result, nil
	}
	if operation.Err() != nil {
		return nil, storageError("interrupted claim", operation.Err())
	}
	result = &pb.CommandResult{CommandId: command.CommandId}
	now := time.Now()
	if !now.Before(command.ExpiresAt.AsTime()) || !now.Before(receivedAt.Add(command.Ttl.AsDuration())) {
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_TIMED_OUT, "deadline_expired"
	} else {
		if h.execute != nil {
			h.execute()
		}
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, "pong"
	}
	if err := h.journal.Complete(operation, result); err != nil {
		return nil, storageError("complete", err)
	}
	return result, nil
}

func (h *commandHandler) acknowledge(ctx context.Context, ack *pb.CommandAck) error {
	if ack == nil || !protocol.ValidCommandID(ack.CommandId) || len(ack.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "invalid command acknowledgement")
	}
	if h.ephemeral[ack.CommandId] != nil {
		if ack.Status != pb.CommandStatus_COMMAND_STATUS_REJECTED {
			return status.Error(codes.InvalidArgument, "conflicting journal_full acknowledgement")
		}
		delete(h.ephemeral, ack.CommandId)
		return nil
	}
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
