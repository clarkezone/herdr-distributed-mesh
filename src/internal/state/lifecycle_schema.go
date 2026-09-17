package state

import (
	"context"
	"database/sql"
	"strings"
)

var lifecycleCommandsSchema = strings.Replace(commandsSchema, "(1, 2, 3, 5, 6, 7, 8)", "(1, 2, 3, 4, 5, 6, 7, 8)", 1)
var lifecycleNodeCommandsSchema = strings.Replace(nodeCommandsSchema, "(2, 3, 5, 6, 7)", "(2, 3, 4, 5, 6, 7)", 1)

const nodeLifecycleSchema = `CREATE TABLE node_lifecycle (
	command_id TEXT PRIMARY KEY NOT NULL REFERENCES node_commands(command_id) ON DELETE CASCADE,
	sequence INTEGER NOT NULL CHECK (sequence BETWEEN 0 AND 8),
	receipt BLOB NOT NULL CHECK (length(receipt) BETWEEN 1 AND 4096)
) STRICT`

const nodeCommandScopesSchema = `CREATE TABLE node_command_scopes (
	command_id TEXT PRIMARY KEY NOT NULL REFERENCES node_commands(command_id) ON DELETE CASCADE,
	incarnation TEXT NOT NULL CHECK (length(incarnation) = 64),
	workspace_key TEXT NOT NULL CHECK (length(workspace_key) IN (0, 64)),
	terminal_key TEXT NOT NULL CHECK (length(terminal_key) IN (0, 64))
) STRICT`

// Rebuild the constrained table without decoding/re-encoding retained blobs.
// Indexes are recreated only after dropping their renamed original table.
func migrateLifecycleSchema(ctx context.Context, tx *sql.Tx, kind databaseKind) error {
	statements := []string{
		"ALTER TABLE commands RENAME TO lifecycle_previous_commands",
		lifecycleCommandsSchema,
		`INSERT INTO commands SELECT command_id, actor_id, idempotency_key, target_id,
			expires_seconds, expires_nanos, status, record FROM lifecycle_previous_commands`,
		"DROP TABLE lifecycle_previous_commands",
		commandTargetIndex, commandExpiryIndex,
		"PRAGMA user_version = 7",
	}
	if kind == nodeKind {
		statements = []string{
			"ALTER TABLE node_commands RENAME TO lifecycle_previous_commands",
			lifecycleNodeCommandsSchema,
			`INSERT INTO node_commands SELECT command_id, status, delivered, command, result
				FROM lifecycle_previous_commands`,
			"DROP TABLE lifecycle_previous_commands",
			nodePendingIndex, nodeLifecycleSchema, nodeCommandScopesSchema,
			"PRAGMA user_version = 6",
		}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
