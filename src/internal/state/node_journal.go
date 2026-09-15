package state

import (
	"context"
	"database/sql"
	"errors"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// NodeJournal retains execution intents and result tombstones. Opening it does
// not recover or execute commands; callers explicitly Recover before connecting.
type NodeJournal struct {
	store  *Store
	nodeID string
}

func OpenNodeJournal(ctx context.Context, path, nodeInstanceID string) (*NodeJournal, error) {
	store, err := openStore(ctx, path, nodeInstanceID, nodeKind)
	if err != nil {
		return nil, err
	}
	return &NodeJournal{store: store, nodeID: nodeInstanceID}, nil
}

type nodeCommandEntry struct {
	command   *pb.Command
	result    *pb.CommandResult
	delivered bool
}

func readNodeEntries(ctx context.Context, tx *sql.Tx, nodeID, filter string, args []any) (entries []*nodeCommandEntry, err error) {
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM node_commands").Scan(&count); err != nil {
		return nil, err
	}
	if count > maxCommands {
		return nil, errors.New("stored node command count exceeds limit")
	}
	rows, err := tx.QueryContext(ctx, `SELECT command_id, status, delivered, result IS NULL,
		CASE WHEN typeof(command) = 'blob' AND length(command) BETWEEN 1 AND 16384 THEN command ELSE NULL END,
		CASE WHEN typeof(result) = 'blob' AND length(result) BETWEEN 1 AND 16384 THEN result ELSE NULL END
		FROM node_commands `+filter+" ORDER BY command_id LIMIT 4097", args...)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	entries = make([]*nodeCommandEntry, 0)
	for rows.Next() {
		var id string
		var status pb.CommandStatus
		var delivered int
		var resultNull bool
		var commandBytes, resultBytes []byte
		if err := rows.Scan(&id, &status, &delivered, &resultNull, &commandBytes, &resultBytes); err != nil {
			return nil, err
		}
		if len(entries) == maxCommands {
			return nil, errors.New("stored node command count exceeds limit")
		}
		entry := &nodeCommandEntry{command: new(pb.Command), delivered: delivered == 1}
		if err := decodeCommandMessage(commandBytes, entry.command); err != nil {
			return nil, err
		}
		if err := validateCommand(entry.command, nodeID); err != nil {
			return nil, err
		}
		if id != entry.command.CommandId || (delivered != 0 && delivered != 1) {
			return nil, errors.New("stored node command disagrees with indexed fields")
		}
		if status == statusRunning {
			if !resultNull || entry.delivered {
				return nil, errors.New("running node command has a result or acknowledgement")
			}
		} else {
			entry.result = new(pb.CommandResult)
			if err := decodeCommandMessage(resultBytes, entry.result); err != nil {
				return nil, err
			}
			if err := protocol.ValidateProbeResult(entry.result); err != nil {
				return nil, err
			}
			if entry.result.CommandId != id || entry.result.Status != status {
				return nil, errors.New("stored node result disagrees with command identity or status")
			}
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func readNodeEntry(ctx context.Context, tx *sql.Tx, nodeID, commandID string) (*nodeCommandEntry, error) {
	entries, err := readNodeEntries(ctx, tx, nodeID, "WHERE command_id = ?", []any{commandID})
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, ErrCommandNotFound
	}
	if len(entries) != 1 {
		return nil, errors.New("node command lookup is not unique")
	}
	return entries[0], nil
}

func matchingNodeCommand(original, proposed *pb.Command) bool {
	a := proto.Clone(original).(*pb.Command)
	b := proto.Clone(proposed).(*pb.Command)
	a.Ttl, b.Ttl = nil, nil
	return proto.Equal(a, b)
}

func writeNodeResult(ctx context.Context, tx *sql.Tx, result *pb.CommandResult) error {
	if err := protocol.ValidateProbeResult(result); err != nil {
		return err
	}
	payload, err := marshalCommandMessage(result)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE node_commands SET status = ?, result = ?, delivered = 0 WHERE command_id = ?",
		result.Status, payload, result.CommandId)
	return err
}

func interruptedNodeResult(id string) *pb.CommandResult {
	return &pb.CommandResult{CommandId: id, Status: statusIndeterminate, Detail: "node_restarted"}
}

// Claim returns nil,true only after committing the execution intent. An existing
// RUNNING intent is durably marked indeterminate, never granted to an executor.
// Remaining wire TTL is not identity; the fixed absolute expiry is.
func (j *NodeJournal) Claim(ctx context.Context, command *pb.Command) (*pb.CommandResult, bool, error) {
	if err := j.store.enter(ctx); err != nil {
		return nil, false, err
	}
	defer j.store.leave()
	if err := validateCommand(command, j.nodeID); err != nil {
		return nil, false, err
	}
	var result *pb.CommandResult
	claimed := false
	err := j.store.transaction(ctx, func(tx *sql.Tx) error {
		entry, err := readNodeEntry(ctx, tx, j.nodeID, command.CommandId)
		if err == nil {
			if !matchingNodeCommand(entry.command, command) {
				return ErrCommandConflict
			}
			if entry.result != nil {
				result = entry.result
				return nil
			}
			result = interruptedNodeResult(command.CommandId)
			return writeNodeResult(ctx, tx, result)
		}
		if !errors.Is(err, ErrCommandNotFound) {
			return err
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM node_commands").Scan(&count); err != nil {
			return err
		}
		if count >= maxCommands {
			return ErrCommandCapacity
		}
		payload, err := marshalCommandMessage(command)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO node_commands(command_id, status, delivered, command) VALUES (?, ?, 0, ?)",
			command.CommandId, statusRunning, payload)
		claimed = err == nil
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return result, claimed, nil
}

// Complete commits a result before delivery. Repeating a completed result does
// not clear an acknowledgement; a different terminal result is never accepted.
func (j *NodeJournal) Complete(ctx context.Context, result *pb.CommandResult) error {
	if err := j.store.enter(ctx); err != nil {
		return err
	}
	defer j.store.leave()
	if err := protocol.ValidateProbeResult(result); err != nil {
		return err
	}
	return j.store.transaction(ctx, func(tx *sql.Tx) error {
		entry, err := readNodeEntry(ctx, tx, j.nodeID, result.CommandId)
		if err != nil {
			return err
		}
		if entry.result != nil {
			if !proto.Equal(entry.result, result) {
				return ErrCommandConflict
			}
			return nil
		}
		return writeNodeResult(ctx, tx, result)
	})
}

func (j *NodeJournal) PendingResults(ctx context.Context) ([]*pb.CommandResult, error) {
	if err := j.store.enter(ctx); err != nil {
		return nil, err
	}
	defer j.store.leave()
	results := make([]*pb.CommandResult, 0)
	err := j.store.transaction(ctx, func(tx *sql.Tx) error {
		entries, err := readNodeEntries(ctx, tx, j.nodeID, "WHERE delivered = 0 AND status != ?", []any{statusRunning})
		if err != nil {
			return err
		}
		for _, entry := range entries {
			results = append(results, entry.result)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (j *NodeJournal) Acknowledge(ctx context.Context, id string, status pb.CommandStatus) error {
	if err := j.store.enter(ctx); err != nil {
		return err
	}
	defer j.store.leave()
	if emptyID(id) {
		return errors.New("acknowledged command ID must not be empty")
	}
	return j.store.transaction(ctx, func(tx *sql.Tx) error {
		entry, err := readNodeEntry(ctx, tx, j.nodeID, id)
		if err != nil {
			return err
		}
		if entry.result == nil || entry.result.Status != status {
			return ErrCommandConflict
		}
		if entry.delivered {
			return nil
		}
		_, err = tx.ExecContext(ctx, "UPDATE node_commands SET delivered = 1 WHERE command_id = ?", id)
		return err
	})
}

func (j *NodeJournal) Recover(ctx context.Context) error {
	if err := j.store.enter(ctx); err != nil {
		return err
	}
	defer j.store.leave()
	return j.store.transaction(ctx, func(tx *sql.Tx) error {
		entries, err := readNodeEntries(ctx, tx, j.nodeID, "WHERE status = ?", []any{statusRunning})
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := writeNodeResult(ctx, tx, interruptedNodeResult(entry.command.CommandId)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (j *NodeJournal) Close() error { return j.store.Close() }
