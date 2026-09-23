package state

import (
	"context"
	"database/sql"
	"errors"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

var ErrNamedAgentAmbiguous = errors.New("multiple original starts match the agent name")
var ErrNamedAgentExists = errors.New("agent name already has a durable start")

func checkNamedAgentClaim(ctx context.Context, tx *sql.Tx, command *pb.Command) error {
	if command.AgentStart == nil {
		return nil
	}
	records, err := readCommands(ctx, tx, "WHERE c.target_id = ?", []any{command.TargetId})
	if err != nil {
		return err
	}
	for _, record := range records {
		start := record.Command.AgentStart
		if start != nil && start.Name == command.AgentStart.Name && start.SessionName == command.AgentStart.SessionName {
			return ErrNamedAgentExists
		}
	}
	return nil
}

// FindNamedAgent deliberately searches across actors and all generations. A
// replacement incarnation must never redirect an operator to a newer pane.
func (s *Store) FindNamedAgent(ctx context.Context, node, session, name string) (*pb.CommandRecord, error) {
	if !protocol.ValidIdempotencyKey(node) || !protocol.ValidIdempotencyKey(name) ||
		protocol.ValidateSessionSelector(session, "", false) != nil {
		return nil, errors.New("invalid named agent selector")
	}
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	defer s.leave()
	var found *pb.CommandRecord
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		records, err := readCommands(ctx, tx, "WHERE c.target_id = ?", []any{node})
		if err != nil {
			return err
		}
		for _, record := range records {
			start := record.Command.AgentStart
			if start == nil || start.Name != name || start.SessionName != session {
				continue
			}
			if found != nil {
				return ErrNamedAgentAmbiguous
			}
			found = record
		}
		if found == nil {
			return ErrCommandNotFound
		}
		return nil
	})
	return found, err
}
