package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrCommandNotFound = errors.New("command not found")
	ErrCommandConflict = errors.New("command conflict")
	ErrCommandCapacity = errors.New("command journal capacity reached")
)

const (
	maxCommands     = 4096
	maxCommandBytes = 16 * 1024
	maxCommandAudit = 16

	statusAccepted      = pb.CommandStatus_COMMAND_STATUS_ACCEPTED
	statusRunning       = pb.CommandStatus_COMMAND_STATUS_RUNNING
	statusSucceeded     = pb.CommandStatus_COMMAND_STATUS_SUCCEEDED
	statusTimedOut      = pb.CommandStatus_COMMAND_STATUS_TIMED_OUT
	statusRejected      = pb.CommandStatus_COMMAND_STATUS_REJECTED
	statusIndeterminate = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE
	statusUnavailable   = pb.CommandStatus_COMMAND_STATUS_NODE_UNAVAILABLE
)

func commandTime(now time.Time) (*timestamppb.Timestamp, error) {
	stamp := timestamppb.New(now)
	if err := stamp.CheckValid(); err != nil {
		return nil, errors.New("invalid command journal timestamp")
	}
	return stamp, nil
}

func marshalCommandMessage(message proto.Message) ([]byte, error) {
	if proto.Size(message) > maxCommandBytes || unknownFields(message.ProtoReflect()) {
		return nil, errors.New("command journal message is oversized or contains unknown fields")
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode command journal message: %w", err)
	}
	if len(payload) == 0 || len(payload) > maxCommandBytes {
		return nil, errors.New("command journal message exceeds payload bounds")
	}
	return payload, nil
}

func decodeCommandMessage(payload []byte, message proto.Message) error {
	if len(payload) == 0 || len(payload) > maxCommandBytes {
		return errors.New("stored command journal payload exceeds bounds")
	}
	if err := (proto.UnmarshalOptions{RecursionLimit: 32}).Unmarshal(payload, message); err != nil {
		return fmt.Errorf("decode command journal message: %w", err)
	}
	if unknownFields(message.ProtoReflect()) {
		return errors.New("stored command journal message contains unknown fields")
	}
	return nil
}

func validateCommand(command *pb.Command, target string) error {
	if command == nil || proto.Size(command) > maxCommandBytes {
		return errors.New("command is nil or oversized")
	}
	return protocol.ValidateCommand(command, target)
}

func projectMutation(commandType string) bool {
	return commandType == protocol.WorkspaceEnsureCommandType || commandType == protocol.WorktreeCreateCommandType
}

func validCommandOutcome(commandType string, status pb.CommandStatus, detail string) bool {
	switch status {
	case statusAccepted:
		return detail == "accepted"
	case statusRunning:
		return detail == "dispatched"
	case statusSucceeded:
		if commandType == protocol.WorktreeCreateCommandType {
			return detail == "worktree_created"
		}
		if commandType == protocol.WorkspaceEnsureCommandType {
			return detail == "workspace_created" || detail == "workspace_present"
		}
		return detail == "pong"
	case statusTimedOut:
		return detail == "deadline_expired"
	case statusRejected:
		if projectMutation(commandType) {
			switch detail {
			case "journal_full", "project_unresolved", "project_not_authorized", "precondition_failed",
				"ambiguous_workspace", "herdr_unavailable", "authorization_changed":
				return true
			}
			return false
		}
		return detail == "journal_full"
	case statusIndeterminate:
		return detail == "node_restarted" || detail == "server_restarted" ||
			detail == "node_disconnected" || detail == "deadline_expired" ||
			(projectMutation(commandType) && detail == "herdr_outcome_unknown")
	case statusUnavailable:
		return detail == "node_disconnected" || detail == "server_restarted"
	}
	return false
}

func validCommandTransition(commandType string, from, to pb.CommandStatus, detail string) bool {
	switch from {
	case statusAccepted:
		return to == statusRunning || to == statusTimedOut || to == statusUnavailable ||
			(projectMutation(commandType) && to == statusRejected && detail == "authorization_changed")
	case statusRunning:
		return to == statusSucceeded || to == statusTimedOut || to == statusRejected || to == statusIndeterminate
	case statusIndeterminate:
		return to == statusSucceeded || to == statusTimedOut || to == statusRejected
	}
	return false
}

func validateCommandRecord(record *pb.CommandRecord) error {
	if record == nil || record.Command == nil {
		return errors.New("stored command record is missing its command")
	}
	if err := validateCommand(record.Command, record.Command.TargetId); err != nil {
		return err
	}
	if record.Status == statusSucceeded {
		if err := protocol.ValidateResultForCommand(recordResult(record), record.Command); err != nil {
			return err
		}
	} else if record.WorkspaceEnsure != nil || record.WorktreeCreate != nil {
		return errors.New("stored unsuccessful command contains mutation success data")
	}
	if record.CreatedAt == nil || record.CreatedAt.CheckValid() != nil ||
		record.UpdatedAt == nil || record.UpdatedAt.CheckValid() != nil ||
		len(record.Audit) == 0 || len(record.Audit) > maxCommandAudit ||
		unknownFields(record.ProtoReflect()) {
		return errors.New("stored command record has invalid metadata or audit bounds")
	}
	created := record.CreatedAt.AsTime()
	deadline := record.Command.ExpiresAt.AsTime()
	if !deadline.After(created) || deadline.After(created.Add(record.Command.Ttl.AsDuration())) {
		return errors.New("stored command deadline does not match original TTL")
	}
	var previous *pb.CommandAudit
	for _, event := range record.Audit {
		if event == nil || event.OccurredAt == nil || event.OccurredAt.CheckValid() != nil ||
			!validCommandOutcome(record.Command.CommandType, event.Status, event.Detail) {
			return errors.New("stored command audit event is invalid")
		}
		if previous == nil {
			if event.Status != statusAccepted || !proto.Equal(event.OccurredAt, record.CreatedAt) {
				return errors.New("stored command admission audit is invalid")
			}
		} else if event.OccurredAt.AsTime().Before(previous.OccurredAt.AsTime()) ||
			!validCommandTransition(record.Command.CommandType, previous.Status, event.Status, event.Detail) {
			return errors.New("stored command audit order or transition is invalid")
		}
		previous = event
	}
	if previous.Status != record.Status || previous.Detail != record.Detail || !proto.Equal(previous.OccurredAt, record.UpdatedAt) {
		return errors.New("stored command outcome disagrees with audit")
	}
	return nil
}

func recordResult(record *pb.CommandRecord) *pb.CommandResult {
	return &pb.CommandResult{
		CommandId: record.Command.CommandId, Status: record.Status, Detail: record.Detail,
		WorkspaceEnsure: record.WorkspaceEnsure,
		WorktreeCreate:  record.WorktreeCreate,
	}
}

// Filters are package-owned SQL fragments only; all caller values are parameters.
func readCommands(ctx context.Context, tx *sql.Tx, filter string, args []any) (records []*pb.CommandRecord, err error) {
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM commands").Scan(&count); err != nil {
		return nil, err
	}
	if count > maxCommands {
		return nil, errors.New("stored coordinator command count exceeds limit")
	}
	query := `SELECT c.command_id, c.actor_id, c.idempotency_key, c.target_id,
		c.expires_seconds, c.expires_nanos, c.status,
		CASE WHEN typeof(c.record) = 'blob' AND length(c.record) BETWEEN 1 AND 16384 THEN c.record ELSE NULL END,
		b.instance_id
		FROM commands c LEFT JOIN bindings b ON b.instance_id = c.target_id ` + filter +
		" ORDER BY c.command_id LIMIT 4097"
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	records = make([]*pb.CommandRecord, 0)
	for rows.Next() {
		var id, actor, key, target string
		var seconds int64
		var nanos int32
		var status pb.CommandStatus
		var payload []byte
		var binding sql.NullString
		if err := rows.Scan(&id, &actor, &key, &target, &seconds, &nanos, &status, &payload, &binding); err != nil {
			return nil, err
		}
		if len(records) == maxCommands {
			return nil, errors.New("stored coordinator command count exceeds limit")
		}
		record := new(pb.CommandRecord)
		if err := decodeCommandMessage(payload, record); err != nil {
			return nil, err
		}
		if err := validateCommandRecord(record); err != nil {
			return nil, err
		}
		command := record.Command
		if id != command.CommandId || actor != command.Actor.ActorId || key != command.IdempotencyKey ||
			target != command.TargetId || seconds != command.ExpiresAt.Seconds || nanos != command.ExpiresAt.Nanos ||
			status != record.Status || !binding.Valid || binding.String != target {
			return nil, errors.New("stored command record disagrees with indexed fields or binding")
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func readCommand(ctx context.Context, tx *sql.Tx, filter string, args ...any) (*pb.CommandRecord, error) {
	records, err := readCommands(ctx, tx, filter, args)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, ErrCommandNotFound
	}
	if len(records) != 1 {
		return nil, errors.New("command lookup is not unique")
	}
	return records[0], nil
}

func (s *Store) findCommand(ctx context.Context, actorID, value, filter string) (*pb.CommandRecord, error) {
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	defer s.leave()
	if emptyID(actorID) || emptyID(value) {
		return nil, errors.New("command actor and lookup ID must not be empty")
	}
	var record *pb.CommandRecord
	err := s.transaction(ctx, func(tx *sql.Tx) (err error) {
		record, err = readCommand(ctx, tx, filter, actorID, value)
		return err
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Store) FindCommand(ctx context.Context, actorID, idempotencyKey string) (*pb.CommandRecord, error) {
	return s.findCommand(ctx, actorID, idempotencyKey, "WHERE c.actor_id = ? AND c.idempotency_key = ?")
}

func (s *Store) GetCommand(ctx context.Context, actorID, commandID string) (*pb.CommandRecord, error) {
	return s.findCommand(ctx, actorID, commandID, "WHERE c.actor_id = ? AND c.command_id = ?")
}

func matchingRequest(original, proposed *pb.Command) bool {
	a := proto.Clone(original).(*pb.Command)
	b := proto.Clone(proposed).(*pb.Command)
	a.CommandId, b.CommandId = "", ""
	a.ExpiresAt, b.ExpiresAt = nil, nil
	return proto.Equal(a, b)
}

func (s *Store) CreateCommand(ctx context.Context, command *pb.Command, now time.Time) (*pb.CommandRecord, bool, error) {
	if err := s.enter(ctx); err != nil {
		return nil, false, err
	}
	defer s.leave()
	if err := validateCommand(command, command.GetTargetId()); err != nil {
		return nil, false, err
	}
	stamp, err := commandTime(now)
	if err != nil {
		return nil, false, err
	}
	var record *pb.CommandRecord
	created := false
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		existing, err := readCommand(ctx, tx, "WHERE c.actor_id = ? AND c.idempotency_key = ?", command.Actor.ActorId, command.IdempotencyKey)
		if err == nil {
			if !matchingRequest(existing.Command, command) {
				return ErrCommandConflict
			}
			record = existing
			return nil
		}
		if !errors.Is(err, ErrCommandNotFound) {
			return err
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM commands WHERE command_id = ?", command.CommandId).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return ErrCommandConflict
		}
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM bindings WHERE instance_id = ?", command.TargetId).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return ErrIdentityConflict
		}
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM commands").Scan(&count); err != nil {
			return err
		}
		if count >= maxCommands {
			return ErrCommandCapacity
		}
		record = &pb.CommandRecord{
			Command: proto.Clone(command).(*pb.Command), Status: statusAccepted, Detail: "accepted",
			CreatedAt: stamp, UpdatedAt: proto.Clone(stamp).(*timestamppb.Timestamp),
			Audit: []*pb.CommandAudit{{Status: statusAccepted, Detail: "accepted", OccurredAt: proto.Clone(stamp).(*timestamppb.Timestamp)}},
		}
		if err := validateCommandRecord(record); err != nil {
			return err
		}
		payload, err := marshalCommandMessage(record)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO commands
			(command_id, actor_id, idempotency_key, target_id, expires_seconds, expires_nanos, status, record)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, command.CommandId, command.Actor.ActorId, command.IdempotencyKey,
			command.TargetId, command.ExpiresAt.Seconds, command.ExpiresAt.Nanos, record.Status, payload)
		created = err == nil
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return record, created, nil
}

func transitionCommand(ctx context.Context, tx *sql.Tx, record *pb.CommandRecord, status pb.CommandStatus, detail string, now time.Time) error {
	if !validCommandTransition(record.Command.CommandType, record.Status, status, detail) ||
		!validCommandOutcome(record.Command.CommandType, status, detail) {
		return ErrCommandConflict
	}
	if len(record.Audit) >= maxCommandAudit {
		return errors.New("command audit capacity reached")
	}
	stamp, err := commandTime(now)
	if err != nil {
		return err
	}
	record.Status, record.Detail, record.UpdatedAt = status, detail, stamp
	record.Audit = append(record.Audit, &pb.CommandAudit{Status: status, Detail: detail, OccurredAt: proto.Clone(stamp).(*timestamppb.Timestamp)})
	if err := validateCommandRecord(record); err != nil {
		return err
	}
	payload, err := marshalCommandMessage(record)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE commands SET status = ?, record = ? WHERE command_id = ?", status, payload, record.Command.CommandId)
	return err
}

// RejectCommand revokes only accepted project mutation admissions before dispatch.
// Already changed records are returned without rewriting their outcome or audit.
func (s *Store) RejectCommand(ctx context.Context, nodeID, commandID, detail string, now time.Time) (*pb.CommandRecord, error) {
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	defer s.leave()
	if emptyID(nodeID) || emptyID(commandID) || detail != "authorization_changed" {
		return nil, errors.New("invalid pre-dispatch authorization rejection")
	}
	if _, err := commandTime(now); err != nil {
		return nil, err
	}
	var record *pb.CommandRecord
	err := s.transaction(ctx, func(tx *sql.Tx) (err error) {
		record, err = readCommand(ctx, tx, "WHERE c.command_id = ?", commandID)
		if err != nil {
			return err
		}
		if record.Command.TargetId != nodeID {
			return ErrIdentityConflict
		}
		if !projectMutation(record.Command.CommandType) {
			return ErrCommandConflict
		}
		if record.Status != statusAccepted {
			return nil
		}
		return transitionCommand(ctx, tx, record, statusRejected, detail, now)
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Store) DispatchCommand(ctx context.Context, nodeID, commandID string, now time.Time) (*pb.CommandRecord, bool, error) {
	if err := s.enter(ctx); err != nil {
		return nil, false, err
	}
	defer s.leave()
	if emptyID(nodeID) || emptyID(commandID) {
		return nil, false, errors.New("dispatch target and command ID must not be empty")
	}
	if _, err := commandTime(now); err != nil {
		return nil, false, err
	}
	var record *pb.CommandRecord
	dispatched := false
	err := s.transaction(ctx, func(tx *sql.Tx) (err error) {
		record, err = readCommand(ctx, tx, "WHERE c.command_id = ?", commandID)
		if err != nil {
			return err
		}
		if record.Command.TargetId != nodeID {
			return ErrIdentityConflict
		}
		if record.Status != statusAccepted {
			return nil
		}
		if !now.Before(record.Command.ExpiresAt.AsTime()) {
			return transitionCommand(ctx, tx, record, statusTimedOut, "deadline_expired", now)
		}
		if err := transitionCommand(ctx, tx, record, statusRunning, "dispatched", now); err != nil {
			return err
		}
		dispatched = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return record, dispatched, nil
}

// FinishCommand returns the authoritative record, which can differ from an
// acknowledged uncertainty replay. Wire ACKs acknowledge result.Status, not
// record.Status; otherwise the node cannot clear its retained replay.
func (s *Store) FinishCommand(ctx context.Context, nodeID, stableID string, result *pb.CommandResult, now time.Time) (*pb.CommandRecord, error) {
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	defer s.leave()
	if emptyID(nodeID) || emptyID(stableID) {
		return nil, errors.New("result node and stable IDs must not be empty")
	}
	if err := protocol.ValidateCommandResult(result); err != nil {
		return nil, err
	}
	if _, err := commandTime(now); err != nil {
		return nil, err
	}
	var record *pb.CommandRecord
	err := s.transaction(ctx, func(tx *sql.Tx) (err error) {
		record, err = readCommand(ctx, tx, "WHERE c.command_id = ?", result.CommandId)
		if err != nil {
			return err
		}
		var bound string
		err = tx.QueryRowContext(ctx, "SELECT stable_id FROM bindings WHERE instance_id = ?", nodeID).Scan(&bound)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && (bound != stableID || nodeID != record.Command.TargetId)) {
			return ErrIdentityConflict
		}
		if err != nil {
			return err
		}
		if err := protocol.ValidateResultForCommand(result, record.Command); err != nil {
			return fmt.Errorf("%w: %v", ErrCommandConflict, err)
		}
		if result.Status == statusIndeterminate && protocol.IsTerminalCommand(record.Status) {
			return nil
		}
		if proto.Equal(recordResult(record), result) {
			return nil
		}
		if record.Status != statusRunning && record.Status != statusIndeterminate {
			return ErrCommandConflict
		}
		if result.WorkspaceEnsure != nil {
			record.WorkspaceEnsure = proto.Clone(result.WorkspaceEnsure).(*pb.WorkspaceEnsureResult)
		}
		if result.WorktreeCreate != nil {
			record.WorktreeCreate = proto.Clone(result.WorktreeCreate).(*pb.WorktreeCreateResult)
		}
		return transitionCommand(ctx, tx, record, result.Status, result.Detail, now)
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Store) changeActiveCommands(ctx context.Context, now time.Time, filter string, args []any, acceptedStatus pb.CommandStatus, detail string) error {
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.leave()
	if _, err := commandTime(now); err != nil {
		return err
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		records, err := readCommands(ctx, tx, filter, args)
		if err != nil {
			return err
		}
		for _, record := range records {
			status := acceptedStatus
			if record.Status == statusRunning {
				status = statusIndeterminate
			}
			if err := transitionCommand(ctx, tx, record, status, detail, now); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) LoseNodeCommands(ctx context.Context, nodeID string, now time.Time) error {
	if emptyID(nodeID) {
		return errors.New("lost node ID must not be empty")
	}
	return s.changeActiveCommands(ctx, now, "WHERE c.target_id = ? AND c.status IN (?, ?)",
		[]any{nodeID, statusAccepted, statusRunning}, statusUnavailable, "node_disconnected")
}

func (s *Store) ExpireCommands(ctx context.Context, now time.Time) error {
	stamp, err := commandTime(now)
	if err != nil {
		return err
	}
	return s.changeActiveCommands(ctx, now,
		"WHERE c.status IN (?, ?) AND (c.expires_seconds < ? OR (c.expires_seconds = ? AND c.expires_nanos <= ?))",
		[]any{statusAccepted, statusRunning, stamp.Seconds, stamp.Seconds, stamp.Nanos}, statusTimedOut, "deadline_expired")
}

func (s *Store) RecoverCommands(ctx context.Context, now time.Time) error {
	return s.changeActiveCommands(ctx, now, "WHERE c.status IN (?, ?)", []any{statusAccepted, statusRunning},
		statusUnavailable, "server_restarted")
}
