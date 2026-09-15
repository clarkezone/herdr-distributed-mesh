package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

type databaseKind int

const (
	coordinatorKind databaseKind = 0x484d434f
	nodeKind        databaseKind = 0x484d4e4a
)

const metadataSchema = `CREATE TABLE metadata (
	singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
	server_instance_id TEXT NOT NULL CHECK (length(server_instance_id) > 0)
) STRICT`

const bindingsSchema = `CREATE TABLE bindings (
	stable_id TEXT PRIMARY KEY NOT NULL CHECK (length(stable_id) > 0),
	instance_id TEXT NOT NULL UNIQUE CHECK (length(instance_id) > 0)
) STRICT`

const latestNodesSchema = `CREATE TABLE latest_nodes (
	instance_id TEXT PRIMARY KEY NOT NULL REFERENCES bindings(instance_id),
	payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 266240)
) STRICT`

const commandsSchema = `CREATE TABLE commands (
	command_id TEXT PRIMARY KEY NOT NULL,
	actor_id TEXT NOT NULL,
	idempotency_key TEXT NOT NULL,
	target_id TEXT NOT NULL REFERENCES bindings(instance_id),
	expires_seconds INTEGER NOT NULL,
	expires_nanos INTEGER NOT NULL CHECK (expires_nanos BETWEEN 0 AND 999999999),
	status INTEGER NOT NULL CHECK (status IN (1, 2, 3, 5, 6, 7, 8)),
	record BLOB NOT NULL CHECK (length(record) BETWEEN 1 AND 16384),
	UNIQUE (actor_id, idempotency_key)
) STRICT`

const commandTargetIndex = `CREATE INDEX commands_target_status ON commands(target_id, status)`
const commandExpiryIndex = `CREATE INDEX commands_status_expiry ON commands(status, expires_seconds, expires_nanos)`

const nodeMetadataSchema = `CREATE TABLE node_metadata (
	singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
	node_instance_id TEXT NOT NULL CHECK (length(node_instance_id) > 0)
) STRICT`

const nodeCommandsSchema = `CREATE TABLE node_commands (
	command_id TEXT PRIMARY KEY NOT NULL,
	status INTEGER NOT NULL CHECK (status IN (2, 3, 5, 6, 7)),
	delivered INTEGER NOT NULL CHECK (delivered IN (0, 1)),
	command BLOB NOT NULL CHECK (length(command) BETWEEN 1 AND 16384),
	result BLOB CHECK (length(result) BETWEEN 1 AND 16384),
	CHECK ((status = 2 AND result IS NULL AND delivered = 0) OR (status != 2 AND result IS NOT NULL))
) STRICT`

const nodePendingIndex = `CREATE INDEX node_commands_pending ON node_commands(delivered, status)`

// Schema versions name exact schemas owned by this package, not arbitrary
// application tables. Validate their definitions before permitting migration.
func normalizeSchema(statement string) string {
	return strings.ToLower(strings.Join(strings.Fields(statement), " "))
}

func (s *Store) requireSchema(ctx context.Context, name, kind, definition string) error {
	var actual string
	err := s.conn.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?", kind, name).Scan(&actual)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("state database is corrupt: required %s %s is missing", kind, name)
	}
	if err != nil {
		return fmt.Errorf("inspect state schema: %w", err)
	}
	if normalizeSchema(actual) != normalizeSchema(definition) {
		return fmt.Errorf("state database is corrupt: required %s %s has an unexpected definition", kind, name)
	}
	return nil
}

func (s *Store) inspectSchema(ctx context.Context, ownerID string, created bool, kind databaseKind) (int, error) {
	var version, application int
	if err := s.conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return 0, err
	}
	if err := s.conn.QueryRowContext(ctx, "PRAGMA application_id").Scan(&application); err != nil {
		return 0, err
	}
	maxVersion := 3
	if kind == nodeKind {
		maxVersion = 2
	}
	if version < 0 || version > maxVersion {
		return 0, fmt.Errorf("unsupported state schema version %d", version)
	}
	if version == 0 {
		if !created {
			return 0, errors.New("existing state database has no initialized schema; refusing to initialize or reset it")
		}
		var objects int
		if err := s.conn.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").Scan(&objects); err != nil {
			return 0, err
		}
		if application != 0 || objects != 0 {
			return 0, errors.New("unversioned database is not empty")
		}
		return 0, nil
	}
	legacyCoordinator := kind == coordinatorKind && version == 1 && application == 0
	if application != int(kind) && !legacyCoordinator {
		return 0, errors.New("state database kind does not match requested journal")
	}
	schemas := []struct{ name, kind, sql string }{
		{"metadata", "table", metadataSchema},
		{"bindings", "table", bindingsSchema},
		{"latest_nodes", "table", latestNodesSchema},
	}
	identityQuery := "SELECT server_instance_id FROM metadata WHERE singleton = 1"
	countQuery := "SELECT count(*) FROM metadata"
	if kind == nodeKind {
		schemas = []struct{ name, kind, sql string }{
			{"node_metadata", "table", nodeMetadataSchema},
			{"node_commands", "table", nodeCommandsSchema},
			{"node_commands_pending", "index", nodePendingIndex},
		}
		identityQuery = "SELECT node_instance_id FROM node_metadata WHERE singleton = 1"
		countQuery = "SELECT count(*) FROM node_metadata"
	} else if version >= 2 {
		schemas = append(schemas,
			struct{ name, kind, sql string }{"commands", "table", commandsSchema},
			struct{ name, kind, sql string }{"commands_target_status", "index", commandTargetIndex},
			struct{ name, kind, sql string }{"commands_status_expiry", "index", commandExpiryIndex})
	}
	for _, schema := range schemas {
		if err := s.requireSchema(ctx, schema.name, schema.kind, schema.sql); err != nil {
			return 0, err
		}
	}
	var identity string
	if err := s.conn.QueryRowContext(ctx, identityQuery).Scan(&identity); err != nil {
		return 0, fmt.Errorf("state database server identity is missing or invalid: %w", err)
	}
	if emptyID(identity) || identity != ownerID {
		return 0, fmt.Errorf("%w: database belongs to another owner", ErrIdentityConflict)
	}
	var count int
	if err := s.conn.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
		return 0, err
	}
	if count != 1 {
		return 0, errors.New("invalid database owner metadata")
	}
	rows, err := s.conn.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return 0, err
	}
	invalid := rows.Next()
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return 0, err
	}
	if invalid {
		return 0, errors.New("database foreign key check failed")
	}
	return version, nil
}

func initializeSchema(ctx context.Context, tx *sql.Tx, ownerID string, version int, kind databaseKind) error {
	if kind == nodeKind {
		if version == 0 {
			for _, statement := range []string{nodeMetadataSchema, nodeCommandsSchema, nodePendingIndex,
				"PRAGMA application_id = 1213025866", "PRAGMA user_version = 1"} {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO node_metadata(singleton, node_instance_id) VALUES (1, ?)", ownerID); err != nil {
				return err
			}
		}
		entries, err := readNodeEntries(ctx, tx, ownerID, "", nil)
		if err != nil {
			return err
		}
		if version == 1 {
			for _, entry := range entries {
				if err := protocol.ValidateProbeCommand(entry.command, ownerID); err != nil {
					return fmt.Errorf("invalid legacy node command: %w", err)
				}
			}
		}
		// The version fences probe-only binaries without rewriting retained bytes.
		if version < 2 {
			_, err = tx.ExecContext(ctx, "PRAGMA user_version = 2")
		}
		return err
	}
	if version > 0 && version < 3 {
		if err := validateLegacyBindings(ctx, tx); err != nil {
			return err
		}
		if _, err := readFleet(ctx, tx); err != nil {
			return err
		}
	}
	if version == 0 {
		for _, statement := range []string{metadataSchema, bindingsSchema, latestNodesSchema} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO metadata(singleton, server_instance_id) VALUES (1, ?)", ownerID); err != nil {
			return err
		}
	}
	if version < 2 {
		for _, statement := range []string{commandsSchema, commandTargetIndex, commandExpiryIndex,
			"PRAGMA application_id = 1213023055", "PRAGMA user_version = 2"} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
	}
	records, err := readCommands(ctx, tx, "", nil)
	if err != nil {
		return err
	}
	if version > 0 && version < 3 {
		for _, record := range records {
			if err := protocol.ValidateProbeCommand(record.Command, record.Command.TargetId); err != nil {
				return fmt.Errorf("invalid legacy coordinator command: %w", err)
			}
		}
	}
	if version < 3 {
		_, err = tx.ExecContext(ctx, "PRAGMA user_version = 3")
	}
	return err
}

func validateLegacyBindings(ctx context.Context, tx *sql.Tx) (err error) {
	rows, err := tx.QueryContext(ctx, "SELECT stable_id, instance_id FROM bindings")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var stableID, instanceID string
		if err := rows.Scan(&stableID, &instanceID); err != nil {
			return err
		}
		if emptyID(stableID) || emptyID(instanceID) {
			return errors.New("legacy database contains an empty identity binding")
		}
	}
	return rows.Err()
}
