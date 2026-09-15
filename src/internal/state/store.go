// Package state persists coordinator identity bindings and bounded fleet snapshots.
package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	_ "modernc.org/sqlite"
)

var (
	ErrIdentityConflict = errors.New("state identity conflict")
	ErrLocked           = errors.New("state is owned by another store")
)

const (
	maxNodeBytes = 260 * 1024
	maxNodes     = 128
)

type Binding struct {
	StableID   string
	InstanceID string
}

// Store owns one pinned SQLite connection and a kernel-held file lock.
// Operations and Close serialize; waiting operations remain context-cancellable.
type Store struct {
	db       *sql.DB
	conn     *sql.Conn
	lock     *os.File
	gate     chan struct{}
	closed   bool
	closeErr error
}

// Open accepts a filesystem path, not a SQLite URI. The exact parent directory
// must be dedicated to coordinator state: its permissions are made private.
// The startup context does not own the returned store's lifetime.
func Open(ctx context.Context, path string, serverInstanceID string) (_ *Store, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if emptyID(serverInstanceID) {
		return nil, errors.New("server instance ID must not be empty")
	}
	if strings.TrimSpace(path) == "" || strings.Contains(path, ":memory:") ||
		strings.Contains(strings.TrimPrefix(path, filepath.VolumeName(path)), ":") {
		return nil, errors.New("state path must be a durable filesystem path, not a SQLite URI")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve state path: %w", err)
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	info, err := os.Lstat(parent)
	if err != nil {
		return nil, fmt.Errorf("inspect state directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("state parent must be a directory, not a symbolic link")
	}
	if err := protectPath(parent, true); err != nil {
		return nil, fmt.Errorf("protect state directory: %w", err)
	}
	// Canonicalizing ancestor links ensures aliases use the same ownership file.
	parent, err = filepath.EvalSymlinks(parent)
	if err != nil {
		return nil, fmt.Errorf("resolve state directory: %w", err)
	}
	path = filepath.Join(parent, filepath.Base(path))
	if err := checkRegular(path + ".lock"); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open state ownership lock: %w", err)
	}
	s := &Store{lock: lock, gate: make(chan struct{}, 1)}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}()
	if err := lockFile(lock); err != nil {
		return nil, fmt.Errorf("acquire state ownership: %w", err)
	}
	if err := protectPath(path+".lock", false); err != nil {
		return nil, fmt.Errorf("protect state lock: %w", err)
	}
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		name := path + suffix
		if err := checkRegular(name); err != nil {
			return nil, err
		}
		if _, statErr := os.Lstat(name); statErr == nil {
			if err := protectPath(name, false); err != nil {
				return nil, fmt.Errorf("protect state file: %w", err)
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return nil, fmt.Errorf("inspect state file: %w", statErr)
		}
	}
	// Precreate the database with private permissions; SQLite sidecars inherit
	// its mode on Unix and the directory's inheritable DACL on Windows.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	created := err == nil
	if errors.Is(err, os.ErrExist) {
		file, err = os.OpenFile(path, os.O_RDWR, 0600)
	}
	if err != nil {
		return nil, fmt.Errorf("open state database file: %w", err)
	}
	if !created {
		info, statErr := file.Stat()
		if statErr != nil {
			return nil, errors.Join(fmt.Errorf("inspect state database file: %w", statErr), file.Close())
		}
		if info.Size() == 0 {
			return nil, errors.Join(errors.New("existing state database is empty; refusing to initialize or reset it"), file.Close())
		}
	}
	if err := errors.Join(protectPath(path, false), file.Close()); err != nil {
		return nil, fmt.Errorf("protect state database: %w", err)
	}
	uriPath := filepath.ToSlash(path)
	if !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	s.db, err = sql.Open("sqlite", (&url.URL{Scheme: "file", Path: uriPath}).String())
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)
	s.conn, err = s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect state database: %w", err)
	}
	if err := s.initialize(ctx, serverInstanceID, created); err != nil {
		return nil, fmt.Errorf("initialize state database: %w", err)
	}
	return s, nil
}

func checkRegular(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect state file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("state files must be regular files, not symbolic links")
	}
	return nil
}

func (s *Store) initialize(ctx context.Context, serverID string, created bool) error {
	if _, err := s.conn.ExecContext(ctx, "PRAGMA busy_timeout = 3000"); err != nil {
		return err
	}
	var check string
	if err := s.conn.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&check); err != nil {
		return fmt.Errorf("check database integrity: %w", err)
	}
	if check != "ok" {
		return errors.New("database integrity check failed")
	}
	var version int
	if err := s.conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version != 0 && version != 1 {
		return fmt.Errorf("unsupported state schema version %d", version)
	}
	if version == 0 && !created {
		return errors.New("existing state database has no initialized schema; refusing to initialize or reset it")
	}
	if version == 1 {
		for _, table := range []string{"metadata", "bindings", "latest_nodes"} {
			var count int
			if err := s.conn.QueryRowContext(ctx,
				"SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ? AND tbl_name = ?",
				table, table).Scan(&count); err != nil {
				return fmt.Errorf("inspect state schema: %w", err)
			}
			if count != 1 {
				return fmt.Errorf("state database is corrupt: required table %s is missing", table)
			}
		}
		var identity string
		if err := s.conn.QueryRowContext(ctx, "SELECT server_instance_id FROM metadata WHERE singleton = 1").Scan(&identity); err != nil {
			return fmt.Errorf("state database server identity is missing or invalid: %w", err)
		}
		if identity != serverID {
			return fmt.Errorf("%w: database belongs to another server", ErrIdentityConflict)
		}
	} else {
		var tables int
		if err := s.conn.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").Scan(&tables); err != nil {
			return err
		}
		if tables != 0 {
			return errors.New("unversioned database is not empty")
		}
	}
	for _, setting := range []struct {
		sql  string
		want string
	}{
		{"PRAGMA journal_mode = WAL", "wal"},
		{"PRAGMA synchronous = FULL", ""},
		{"PRAGMA foreign_keys = ON", ""},
		{"PRAGMA wal_autocheckpoint = 1000", "1000"},
		{"PRAGMA journal_size_limit = 8388608", "8388608"},
	} {
		if setting.want == "" {
			if _, err := s.conn.ExecContext(ctx, setting.sql); err != nil {
				return err
			}
		} else {
			var got string
			if err := s.conn.QueryRowContext(ctx, setting.sql).Scan(&got); err != nil {
				return err
			}
			if got != setting.want {
				return errors.New("SQLite refused required durability configuration")
			}
		}
	}
	for _, setting := range []struct {
		sql  string
		want int
	}{
		{"PRAGMA synchronous", 2},
		{"PRAGMA foreign_keys", 1},
		{"PRAGMA busy_timeout", 3000},
	} {
		var got int
		if err := s.conn.QueryRowContext(ctx, setting.sql).Scan(&got); err != nil {
			return err
		}
		if got != setting.want {
			return errors.New("SQLite refused required connection configuration")
		}
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		if version == 0 {
			for _, statement := range []string{
				`CREATE TABLE metadata (
					singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
					server_instance_id TEXT NOT NULL CHECK (length(server_instance_id) > 0)
				) STRICT`,
				`CREATE TABLE bindings (
					stable_id TEXT PRIMARY KEY NOT NULL CHECK (length(stable_id) > 0),
					instance_id TEXT NOT NULL UNIQUE CHECK (length(instance_id) > 0)
				) STRICT`,
				`CREATE TABLE latest_nodes (
					instance_id TEXT PRIMARY KEY NOT NULL REFERENCES bindings(instance_id),
					payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 266240)
				) STRICT`,
				"PRAGMA user_version = 1",
			} {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO metadata(singleton, server_instance_id) VALUES (1, ?)", serverID); err != nil {
				return err
			}
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM metadata").Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return errors.New("invalid server metadata")
		}
		rows, err := tx.QueryContext(ctx, "PRAGMA foreign_key_check")
		if err != nil {
			return err
		}
		invalid := rows.Next()
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		if invalid {
			return errors.New("database foreign key check failed")
		}
		return nil
	})
}

func emptyID(id string) bool { return strings.TrimSpace(id) == "" }

func (s *Store) enter(ctx context.Context) error {
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		s.leave()
		return err
	}
	if s.closed {
		s.leave()
		return errors.New("state store is closed")
	}
	return nil
}

func (s *Store) leave() { <-s.gate }

func (s *Store) transaction(ctx context.Context, fn func(*sql.Tx) error) (err error) {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func bind(ctx context.Context, tx *sql.Tx, binding Binding) error {
	if emptyID(binding.StableID) || emptyID(binding.InstanceID) {
		return errors.New("binding stable and instance IDs must not be empty")
	}
	var conflicts int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM bindings
		WHERE (stable_id = ? AND instance_id != ?) OR (instance_id = ? AND stable_id != ?)`,
		binding.StableID, binding.InstanceID, binding.InstanceID, binding.StableID).Scan(&conflicts); err != nil {
		return err
	}
	if conflicts != 0 {
		return ErrIdentityConflict
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO bindings(stable_id, instance_id) VALUES (?, ?) ON CONFLICT(stable_id) DO NOTHING",
		binding.StableID, binding.InstanceID)
	return err
}

// ImportBindings commits all bindings or none, including conflicts within input.
func (s *Store) ImportBindings(ctx context.Context, bindings []Binding) error {
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.leave()
	return s.transaction(ctx, func(tx *sql.Tx) error {
		for _, binding := range bindings {
			if err := bind(ctx, tx, binding); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) Bind(ctx context.Context, stableID, instanceID string) error {
	return s.ImportBindings(ctx, []Binding{{StableID: stableID, InstanceID: instanceID}})
}

func validateNode(view *agentflowv1.NodeView) error {
	if view == nil || emptyID(view.InstanceId) || emptyID(view.TailscaleStableId) {
		return errors.New("node and its instance and stable IDs must not be empty")
	}
	if view.LastSeen == nil || view.LastSeen.CheckValid() != nil {
		return errors.New("node last-seen timestamp is missing or invalid")
	}
	if view.HerdrReceivedAt != nil && view.HerdrReceivedAt.CheckValid() != nil {
		return errors.New("node Herdr receipt timestamp is invalid")
	}
	if unknownFields(view.ProtoReflect()) {
		return errors.New("node contains unknown protobuf fields")
	}
	return nil
}

func unknownFields(message protoreflect.Message) bool {
	if len(message.GetUnknown()) != 0 {
		return true
	}
	unknown := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap() && field.MapValue().Message() != nil:
			value.Map().Range(func(_ protoreflect.MapKey, value protoreflect.Value) bool {
				unknown = unknownFields(value.Message())
				return !unknown
			})
		case field.IsList() && field.Message() != nil:
			for i := 0; i < value.List().Len() && !unknown; i++ {
				unknown = unknownFields(value.List().Get(i).Message())
			}
		case !field.IsMap() && !field.IsList() && field.Message() != nil:
			unknown = unknownFields(value.Message())
		}
		return !unknown
	})
	return unknown
}

// SaveNode requires an existing matching binding. It does not implicitly bind.
func (s *Store) SaveNode(ctx context.Context, view *agentflowv1.NodeView) error {
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.leave()
	if view == nil || proto.Size(view) > maxNodeBytes {
		return errors.New("node is nil or exceeds the 260 KiB payload limit")
	}
	if err := validateNode(view); err != nil {
		return err
	}
	payload, err := proto.Marshal(view)
	if err != nil {
		return fmt.Errorf("encode node: %w", err)
	}
	if len(payload) > maxNodeBytes {
		return errors.New("node exceeds the 260 KiB payload limit")
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		var stableID string
		err := tx.QueryRowContext(ctx, "SELECT stable_id FROM bindings WHERE instance_id = ?", view.InstanceId).Scan(&stableID)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && stableID != view.TailscaleStableId) {
			return ErrIdentityConflict
		}
		if err != nil {
			return err
		}
		var count, exists int
		if err := tx.QueryRowContext(ctx, "SELECT count(*), coalesce(sum(instance_id = ?), 0) FROM latest_nodes", view.InstanceId).Scan(&count, &exists); err != nil {
			return err
		}
		if count > maxNodes || (count == maxNodes && exists == 0) {
			return errors.New("fleet exceeds the 128-node limit")
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO latest_nodes(instance_id, payload) VALUES (?, ?)
			ON CONFLICT(instance_id) DO UPDATE SET payload = excluded.payload`, view.InstanceId, payload)
		return err
	})
}

// DeleteNodes removes snapshots only. Identity bindings are permanent.
func (s *Store) DeleteNodes(ctx context.Context, instanceIDs []string) error {
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.leave()
	return s.transaction(ctx, func(tx *sql.Tx) error {
		for _, id := range instanceIDs {
			if emptyID(id) {
				return errors.New("deleted node instance ID must not be empty")
			}
			if _, err := tx.ExecContext(ctx, "DELETE FROM latest_nodes WHERE instance_id = ?", id); err != nil {
				return err
			}
		}
		return nil
	})
}

// LoadFleet returns historical snapshots, not current connection status.
// The caller must validate domain state and restore nodes as disconnected/stale
// before publishing them, applying its retention and aggregate fleet budget.
func (s *Store) LoadFleet(ctx context.Context) (fleet []*agentflowv1.NodeView, err error) {
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	defer s.leave()
	err = s.transaction(ctx, func(tx *sql.Tx) (err error) {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM latest_nodes").Scan(&count); err != nil {
			return err
		}
		if count > maxNodes {
			return errors.New("stored fleet exceeds the 128-node limit")
		}
		// CASE bounds the blob before the driver allocates a Go byte slice.
		rows, err := tx.QueryContext(ctx, `SELECT n.instance_id, b.stable_id,
			CASE WHEN typeof(n.payload) = 'blob' AND length(n.payload) BETWEEN 1 AND ?
				THEN n.payload ELSE NULL END
			FROM latest_nodes n LEFT JOIN bindings b ON b.instance_id = n.instance_id
			ORDER BY n.instance_id LIMIT ?`, maxNodeBytes, maxNodes+1)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, rows.Close()) }()
		fleet = make([]*agentflowv1.NodeView, 0, count)
		for rows.Next() {
			var instanceID string
			var stableID sql.NullString
			var payload []byte
			if err := rows.Scan(&instanceID, &stableID, &payload); err != nil {
				return err
			}
			if len(fleet) == maxNodes || !stableID.Valid || len(payload) == 0 || len(payload) > maxNodeBytes {
				return errors.New("stored node has invalid binding or payload bounds")
			}
			view := new(agentflowv1.NodeView)
			if err := (proto.UnmarshalOptions{RecursionLimit: 64}).Unmarshal(payload, view); err != nil {
				return fmt.Errorf("decode stored node: %w", err)
			}
			if err := validateNode(view); err != nil {
				return err
			}
			if instanceID != view.InstanceId || stableID.String != view.TailscaleStableId {
				return fmt.Errorf("%w: stored node does not match its binding", ErrIdentityConflict)
			}
			fleet = append(fleet, view)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return fleet, nil
}

// Close waits for active operations, closes SQLite before releasing ownership,
// and is idempotent. The ownership file intentionally remains on disk.
func (s *Store) Close() error {
	s.gate <- struct{}{}
	defer s.leave()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	if s.conn != nil {
		s.closeErr = errors.Join(s.closeErr, s.conn.Close())
	}
	if s.db != nil {
		s.closeErr = errors.Join(s.closeErr, s.db.Close())
	}
	if s.lock != nil {
		// Closing the handle releases the OS lock, including after a failed open.
		s.closeErr = errors.Join(s.closeErr, s.lock.Close())
	}
	return s.closeErr
}
