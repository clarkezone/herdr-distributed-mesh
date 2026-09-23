package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
)

var ErrLifecycleUnresolved = errors.New("native lifecycle target has unresolved effects")

func readLifecycle(ctx context.Context, tx *sql.Tx, command *pb.Command) (*pb.AgentLifecycleReceipt, error) {
	var payload []byte
	var sequence uint32
	err := tx.QueryRowContext(ctx, "SELECT sequence, receipt FROM node_lifecycle WHERE command_id = ?", command.CommandId).Scan(&sequence, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	value := new(pb.AgentLifecycleReceipt)
	if err := decodeCommandMessage(payload, value); err != nil {
		return nil, err
	}
	if sequence != value.Sequence {
		return nil, errors.New("lifecycle sequence index mismatch")
	}
	if err := protocol.ValidateLifecycleReceipt(value, command); err != nil {
		return nil, err
	}
	return value, nil
}

func lifecycleIncarnation(c *pb.Command, r *pb.AgentLifecycleReceipt) string {
	if r != nil {
		return r.Handle.Target.SessionIncarnation
	}
	_, incarnation := protocol.CommandSession(c)
	return incarnation
}

// Unresolved legacy default selectors are deliberately conservative: spelling
// an alias cannot establish that two unknown native incarnations differ.
func lifecycleConflict(prior *pb.Command, receipt *pb.AgentLifecycleReceipt, status pb.CommandStatus, current *pb.Command, incarnation string) bool {
	if prior.CommandId == current.CommandId || prior.TargetId != current.TargetId ||
		(status != statusAccepted && status != statusRunning && status != statusIndeterminate && !protocol.LifecycleHasUnknown(receipt)) {
		return false
	}
	oldIncarnation := lifecycleIncarnation(prior, receipt)
	if incarnation == "" {
		incarnation = lifecycleIncarnation(current, nil)
	}
	if oldIncarnation != "" && incarnation != "" && oldIncarnation != incarnation {
		return false
	}
	if current.AgentStart != nil && prior.AgentStart != nil {
		return current.AgentStart.WorkspaceId == prior.AgentStart.WorkspaceId
	}
	if control := current.AgentControl; control != nil && control.Action != pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT {
		if old := prior.AgentControl; old != nil && old.Action != pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT {
			return old.Target.TerminalId == control.Target.TerminalId
		}
		if prior.AgentStart != nil {
			return receipt == nil || receipt.Handle.Target.TerminalId == "" ||
				receipt.Handle.Target.TerminalId == control.Target.TerminalId
		}
		if prior.AgentStop != nil {
			return prior.AgentStop.Target.TerminalId == control.Target.TerminalId
		}
	}
	return false
}

func checkCoordinatorLifecycleFence(ctx context.Context, tx *sql.Tx, command *pb.Command) error {
	if command.AgentStart == nil && command.AgentControl == nil {
		return nil
	}
	records, err := readCommands(ctx, tx, "WHERE c.target_id = ? AND c.status IN (?, ?, ?)",
		[]any{command.TargetId, statusAccepted, statusRunning, statusIndeterminate})
	if err != nil {
		return err
	}
	for _, prior := range records {
		if lifecycleConflict(prior.Command, prior.AgentLifecycle, prior.Status, command, "") {
			return ErrLifecycleUnresolved
		}
	}
	return nil
}

func checkNodeLifecycleFence(ctx context.Context, tx *sql.Tx, node string, command *pb.Command, incarnation string) error {
	if command.AgentStart == nil && command.AgentControl == nil {
		return nil
	}
	if incarnation == "" {
		incarnation = lifecycleIncarnation(command, nil)
	}
	entries, err := readNodeEntries(ctx, tx, node, "WHERE status IN (?, ?)", []any{statusRunning, statusIndeterminate})
	if err != nil {
		return err
	}
	for _, prior := range entries {
		var receipt *pb.AgentLifecycleReceipt
		if protocol.IsLifecycleCommand(prior.command.CommandType) {
			receipt, err = readLifecycle(ctx, tx, prior.command)
			if err != nil {
				return err
			}
		}
		scope := incarnation
		var pinned string
		err = tx.QueryRowContext(ctx, "SELECT incarnation FROM node_command_scopes WHERE command_id = ?", prior.command.CommandId).Scan(&pinned)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if pinned != "" && scope != "" && pinned != scope {
			continue
		}
		status := statusRunning
		if prior.result != nil {
			status = prior.result.Status
		}
		if lifecycleConflict(prior.command, receipt, status, command, scope) {
			return ErrLifecycleUnresolved
		}
	}
	return nil
}

func pinCommandScope(ctx context.Context, tx *sql.Tx, node string, c *pb.Command, incarnation, workspace, terminal string) error {
	if !protocol.ValidSessionIncarnation(incarnation) {
		return ErrCommandConflict
	}
	_, expected := protocol.CommandSession(c)
	if expected != "" && expected != incarnation {
		return ErrCommandConflict
	}
	workspaceKey, terminalKey := "", ""
	var err error
	if workspace != "" {
		workspaceKey, err = lifecycleResourceKey(node, incarnation, "workspace", workspace)
		if err != nil {
			return err
		}
	}
	if terminal != "" {
		terminalKey, err = lifecycleResourceKey(node, incarnation, "terminal", terminal)
		if err != nil {
			return err
		}
	}
	var oldIncarnation, oldWorkspace, oldTerminal string
	err = tx.QueryRowContext(ctx, "SELECT incarnation, workspace_key, terminal_key FROM node_command_scopes WHERE command_id = ?", c.CommandId).
		Scan(&oldIncarnation, &oldWorkspace, &oldTerminal)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && (oldIncarnation != incarnation || oldWorkspace != "" && oldWorkspace != workspaceKey || oldTerminal != "" && oldTerminal != terminalKey) {
		return ErrCommandConflict
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO node_command_scopes(command_id, incarnation, workspace_key, terminal_key)
		VALUES (?, ?, ?, ?) ON CONFLICT(command_id) DO UPDATE SET workspace_key=excluded.workspace_key, terminal_key=excluded.terminal_key`,
		c.CommandId, incarnation, workspaceKey, terminalKey)
	return err
}

// PinAgentCommand fences ordinary prompt/input against lifecycle uncertainty.
// Interrupt is deliberately allowed, but must still use this exact incarnation.
func (j *NodeJournal) PinAgentCommand(ctx context.Context, commandID, incarnation string) error {
	if err := j.store.enter(ctx); err != nil {
		return err
	}
	defer j.store.leave()
	return j.store.transaction(ctx, func(tx *sql.Tx) error {
		entry, err := readNodeEntry(ctx, tx, j.nodeID, commandID)
		if err != nil {
			return err
		}
		if entry.result != nil || entry.command.AgentControl == nil {
			return ErrCommandConflict
		}
		if err := checkNodeLifecycleFence(ctx, tx, j.nodeID, entry.command, incarnation); err != nil {
			return err
		}
		return pinCommandScope(ctx, tx, j.nodeID, entry.command, incarnation, "", entry.command.AgentControl.Target.TerminalId)
	})
}

func (j *NodeJournal) SaveLifecycle(ctx context.Context, commandID string, receipt *pb.AgentLifecycleReceipt) error {
	if err := j.store.enter(ctx); err != nil {
		return err
	}
	defer j.store.leave()
	return j.store.transaction(ctx, func(tx *sql.Tx) error {
		entry, err := readNodeEntry(ctx, tx, j.nodeID, commandID)
		if err != nil {
			return err
		}
		if entry.result != nil {
			return ErrCommandConflict
		}
		old, err := readLifecycle(ctx, tx, entry.command)
		if err != nil {
			return err
		}
		if err := protocol.ValidateLifecycleAdvance(old, receipt, entry.command); err != nil {
			return err
		}
		if err := checkNodeLifecycleFence(ctx, tx, j.nodeID, entry.command, receipt.Handle.Target.SessionIncarnation); err != nil {
			return err
		}
		if err := pinCommandScope(ctx, tx, j.nodeID, entry.command, receipt.Handle.Target.SessionIncarnation,
			receipt.Handle.WorkspaceId, receipt.Handle.Target.TerminalId); err != nil {
			return err
		}
		payload, err := marshalCommandMessage(receipt)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO node_lifecycle(command_id, sequence, receipt) VALUES (?, ?, ?)
			ON CONFLICT(command_id) DO UPDATE SET sequence=excluded.sequence, receipt=excluded.receipt`, commandID, receipt.Sequence, payload)
		return err
	})
}

func (j *NodeJournal) GetLifecycle(ctx context.Context, commandID string) (*pb.AgentLifecycleReceipt, error) {
	if err := j.store.enter(ctx); err != nil {
		return nil, err
	}
	defer j.store.leave()
	var receipt *pb.AgentLifecycleReceipt
	err := j.store.transaction(ctx, func(tx *sql.Tx) error {
		entry, err := readNodeEntry(ctx, tx, j.nodeID, commandID)
		if err != nil {
			return err
		}
		receipt, err = readLifecycle(ctx, tx, entry.command)
		return err
	})
	return receipt, err
}

func recoveredLifecycleResult(ctx context.Context, tx *sql.Tx, command *pb.Command) (*pb.CommandResult, error) {
	result := interruptedCommandResult(command)
	if !protocol.IsLifecycleCommand(command.CommandType) {
		return result, nil
	}
	receipt, err := readLifecycle(ctx, tx, command)
	if err != nil {
		return nil, err
	}
	result.AgentLifecycle = receipt
	switch {
	case protocol.LifecycleComplete(receipt, command):
		result.Status, result.Detail = statusSucceeded, "agent_started"
		if command.AgentStop != nil {
			result.Detail = "agent_stopped"
		}
	case protocol.LifecycleHasUnknown(receipt):
		result.Status, result.Detail = statusIndeterminate, "lifecycle_uncertain"
	case protocol.LifecycleHasEffect(receipt):
		result.Status, result.Detail = statusFailed, "lifecycle_partial"
	default:
		result.Status = statusRejected
	}
	return result, nil
}

func (s *Store) SaveCommandProgress(ctx context.Context, nodeID string, progress *pb.CommandProgress) error {
	if progress == nil || len(progress.ProtoReflect().GetUnknown()) != 0 || !protocol.ValidCommandID(progress.CommandId) {
		return ErrCommandConflict
	}
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.leave()
	return s.transaction(ctx, func(tx *sql.Tx) error {
		record, err := readCommand(ctx, tx, "WHERE c.command_id = ?", progress.CommandId)
		if err != nil {
			return err
		}
		if record.Command.TargetId != nodeID {
			return ErrCommandConflict
		}
		// Progress and completion use independent bounded worker channels.
		// A delayed prefix may arrive after the final cumulative receipt.
		if record.AgentLifecycle != nil && progress.AgentLifecycle != nil &&
			progress.AgentLifecycle.Sequence <= record.AgentLifecycle.Sequence {
			if err := protocol.ValidateLifecycleAdvance(progress.AgentLifecycle, record.AgentLifecycle, record.Command); err != nil {
				return fmt.Errorf("%w: %v", ErrCommandConflict, err)
			}
			return nil
		}
		if record.Status != statusRunning && record.Status != statusIndeterminate {
			return ErrCommandConflict
		}
		if err := protocol.ValidateLifecycleAdvance(record.AgentLifecycle, progress.AgentLifecycle, record.Command); err != nil {
			return fmt.Errorf("%w: %v", ErrCommandConflict, err)
		}
		record.AgentLifecycle = proto.Clone(progress.AgentLifecycle).(*pb.AgentLifecycleReceipt)
		if err := validateCommandRecord(record); err != nil {
			return err
		}
		payload, err := marshalCommandMessage(record)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE commands SET record = ? WHERE command_id = ?", payload, progress.CommandId)
		return err
	})
}
