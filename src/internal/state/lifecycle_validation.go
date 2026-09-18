package state

import (
	"context"
	"database/sql"
	"errors"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// Validate sidecar contents before accepting a journal, including completed
// commands that recovery will not visit. A corrupt incarnation must never
// quietly remove a canonical-resource fence.
func validateNodeLifecycleData(ctx context.Context, tx *sql.Tx, node string, entries []*nodeCommandEntry) error {
	var checkpointCount, scopeCount int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM node_lifecycle").Scan(&checkpointCount); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM node_command_scopes").Scan(&scopeCount); err != nil {
		return err
	}
	seenCheckpoints, seenScopes := 0, 0
	for _, entry := range entries {
		c := entry.command
		receipt, err := readLifecycle(ctx, tx, c)
		if err != nil {
			return err
		}
		if receipt != nil {
			seenCheckpoints++
			if entry.result != nil && !proto.Equal(entry.result.AgentLifecycle, receipt) {
				return errors.New("terminal lifecycle result disagrees with its checkpoint")
			}
		} else if entry.result.GetAgentLifecycle() != nil {
			return errors.New("terminal lifecycle result is missing its checkpoint")
		}
		var incarnation, workspaceKey, terminalKey string
		err = tx.QueryRowContext(ctx, "SELECT incarnation, workspace_key, terminal_key FROM node_command_scopes WHERE command_id = ?", c.CommandId).
			Scan(&incarnation, &workspaceKey, &terminalKey)
		if errors.Is(err, sql.ErrNoRows) {
			if receipt != nil {
				return errors.New("lifecycle checkpoint is missing its native scope")
			}
			continue
		}
		if err != nil {
			return err
		}
		seenScopes++
		_, expected := protocol.CommandSession(c)
		if !protocol.ValidSessionIncarnation(incarnation) || expected != "" && expected != incarnation {
			return errors.New("invalid retained native incarnation")
		}
		var workspace, terminal string
		switch {
		case receipt != nil:
			if receipt.Handle.Target.SessionIncarnation != incarnation {
				return errors.New("checkpoint and scope incarnation disagree")
			}
			workspace, terminal = receipt.Handle.WorkspaceId, receipt.Handle.Target.TerminalId
		case c.AgentControl != nil:
			terminal = c.AgentControl.Target.TerminalId
		default:
			return errors.New("unexpected native scope")
		}
		wantWorkspace, wantTerminal := "", ""
		if workspace != "" {
			wantWorkspace, err = lifecycleResourceKey(node, incarnation, "workspace", workspace)
			if err != nil {
				return err
			}
		}
		if terminal != "" {
			wantTerminal, err = lifecycleResourceKey(node, incarnation, "terminal", terminal)
			if err != nil {
				return err
			}
		}
		if workspaceKey != wantWorkspace || terminalKey != wantTerminal {
			return errors.New("canonical resource scope hash mismatch")
		}
	}
	if checkpointCount != seenCheckpoints || scopeCount != seenScopes {
		return errors.New("orphan lifecycle checkpoint or native scope")
	}
	return nil
}
