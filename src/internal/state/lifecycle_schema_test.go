package state

import (
	"context"
	"database/sql"
	"testing"
)

// Historical fixtures must contain the actual pre-lifecycle CHECK definitions,
// not merely label a current-schema database with an older version number.
func removeLifecycleSchema(t *testing.T, s *Store, kind databaseKind) {
	t.Helper()
	ctx := context.Background()
	requireOK(t, s.transaction(ctx, func(tx *sql.Tx) error {
		statements := []string{
			"ALTER TABLE commands RENAME TO lifecycle_current_commands",
			commandsSchema,
			"INSERT INTO commands SELECT * FROM lifecycle_current_commands",
			"DROP TABLE lifecycle_current_commands",
			commandTargetIndex, commandExpiryIndex,
		}
		if kind == nodeKind {
			statements = []string{
				"DROP TABLE node_lifecycle",
				"DROP TABLE node_command_scopes",
				"ALTER TABLE node_commands RENAME TO lifecycle_current_commands",
				nodeCommandsSchema,
				"INSERT INTO node_commands SELECT * FROM lifecycle_current_commands",
				"DROP TABLE lifecycle_current_commands",
				nodePendingIndex,
			}
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	}))
}

func TestLifecycleSchemaRequiresCheckpointAndScopeTables(t *testing.T) {
	for _, table := range []string{"node_lifecycle", "node_command_scopes"} {
		t.Run(table, func(t *testing.T) {
			ctx := context.Background()
			path := testPath(t)
			j := openTestNodeJournal(t, path)
			_, err := j.store.conn.ExecContext(ctx, "DROP TABLE "+table)
			requireOK(t, err)
			requireOK(t, j.Close())
			reopened, err := OpenNodeJournal(ctx, path, "node")
			if reopened != nil {
				requireOK(t, reopened.Close())
			}
			if err == nil {
				t.Fatal("missing lifecycle journal structure was accepted")
			}
		})
	}
}
