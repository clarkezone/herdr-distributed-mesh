package state

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

var (
	ErrMaintenancePath         = errors.New("maintenance: unsafe, missing, or inaccessible local state path")
	ErrMaintenanceIntegrity    = errors.New("maintenance: integrity, schema, or identity validation failed; state was not repaired")
	ErrMaintenanceBounds       = errors.New("maintenance: operation exceeds bounded file, byte, or depth limits")
	ErrMaintenanceChanged      = errors.New("maintenance: source changed during inspection or copy")
	ErrMaintenanceBackup       = errors.New("maintenance: incomplete, altered, or unsupported backup")
	ErrMaintenanceLockProtocol = errors.New("maintenance: full-role backup requires an activated runtime locking contract; successfully start an upgraded role, then stop it")
)

const (
	maintenanceRoleLock             = "role-state.lock"
	maintenanceLockVersion          = "herdr-mesh-role-lock-v1\n"
	maintenanceLockPending          = "herdr-mesh-role-lock-pending-v1\n"
	maintenanceLockActivation       = "active\n"
	maintenanceMaxFiles             = 4096
	maintenanceMaxBytes       int64 = 512 << 20
	maintenanceMaxDepth             = 16
	maintenanceTimeout              = 2 * time.Minute
)

type MaintenanceCapacity struct {
	Used      int  `json:"used"`
	Limit     int  `json:"limit"`
	Remaining int  `json:"remaining"`
	Full      bool `json:"full"`
}

// Reports intentionally contain no command IDs, payloads, peers, or paths.
type MaintenanceReport struct {
	Role             string              `json:"role"`
	SchemaVersion    int                 `json:"schema_version"`
	Integrity        string              `json:"integrity"`
	Commands         MaintenanceCapacity `json:"commands"`
	Accepted         int                 `json:"accepted"`
	Running          int                 `json:"running"`
	Indeterminate    int                 `json:"indeterminate"`
	PendingReceipts  int                 `json:"pending_receipts"`
	PruningSupported bool                `json:"pruning_supported"`
	Recovery         string              `json:"recovery"`
}

// RoleStateLock is a runtime lifetime guard, not a journal or execution lease.
// Every server/node process, including transport-only nodes, must hold it from
// before identity/tsnet access until all state writers have stopped.
type RoleStateLock struct {
	mu            sync.Mutex
	file          *os.File
	pendingMarker string
	active        bool
}

// Activate confirms participation only AFTER startup has acquired tsnet's
// identity lock and every other state-writer lock. Acquiring the role guard
// alone cannot exclude an older transport-only process that ignores it.
func (g *RoleStateLock) Activate() error {
	if g == nil {
		return ErrMaintenanceLockProtocol
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.file == nil {
		return ErrMaintenanceLockProtocol
	}
	if g.active {
		return nil
	}
	n, err := g.file.WriteAt([]byte(maintenanceLockActivation), int64(len(g.pendingMarker)))
	if err != nil || n != len(maintenanceLockActivation) {
		return ErrMaintenancePath
	}
	if err := g.file.Sync(); err != nil {
		return ErrMaintenancePath
	}
	g.active = true
	return nil
}

func (g *RoleStateLock) Close() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.file == nil {
		return nil
	}
	err := g.file.Close()
	g.file = nil
	if err != nil {
		return ErrMaintenancePath
	}
	return nil
}

// AcquireRoleState uses an existing local role directory. Startup callers create
// their directory first. New or interrupted markers remain pending until
// Activate succeeds after actual writer ownership. Maintenance never activates.
func AcquireRoleState(ctx context.Context, stateDir, role string) (*RoleStateLock, error) {
	if role != "server" && role != "node" && role != "client" {
		return nil, errors.New("maintenance: runtime lock role must be server, node, or client")
	}
	root, err := runtimeDirectory(stateDir)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path := filepath.Join(root, maintenanceRoleLock)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	created := err == nil
	if errors.Is(err, os.ErrExist) {
		file, err = runtimeOpenRegular(path, true)
	}
	if err != nil {
		return nil, ErrMaintenancePath
	}
	fail := func(err error) (*RoleStateLock, error) { _ = file.Close(); return nil, err }
	if err := lockFile(file); err != nil {
		return fail(maintenanceFailure(ctx, err, ErrMaintenancePath))
	}
	if created {
		if protectPath(path, false) != nil {
			return fail(ErrMaintenancePath)
		}
	}
	value, err := io.ReadAll(io.LimitReader(file, 128))
	if err != nil {
		return fail(ErrMaintenancePath)
	}
	active, pending := maintenanceMarkerState(string(value), role)
	if !active && !pending {
		return fail(ErrMaintenanceLockProtocol)
	}
	marker := maintenanceLockPending + role + "\n"
	if pending {
		// Normalize interrupted creation/activation while retaining the same
		// locked inode. A partial activation never authorizes maintenance.
		if err := file.Truncate(0); err != nil {
			return fail(ErrMaintenancePath)
		}
		if n, err := file.WriteAt([]byte(marker), 0); err != nil || n != len(marker) {
			return fail(ErrMaintenancePath)
		}
		if err := file.Sync(); err != nil {
			return fail(ErrMaintenancePath)
		}
		if err := maintenanceSyncDirectory(root); err != nil {
			return fail(ErrMaintenancePath)
		}
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	return &RoleStateLock{file: file, pendingMarker: marker, active: active}, nil
}

func maintenanceMarkerState(value, role string) (active, pending bool) {
	marker := maintenanceLockPending + role + "\n"
	if value == maintenanceLockVersion+role+"\n" || value == marker+maintenanceLockActivation {
		return true, false
	}
	if strings.HasPrefix(marker, value) ||
		(strings.HasPrefix(value, marker) && strings.HasPrefix(maintenanceLockActivation, strings.TrimPrefix(value, marker))) {
		return false, true
	}
	return false, false
}

func maintenanceRole(role string) (databaseKind, string, error) {
	switch role {
	case "server":
		return coordinatorKind, filepath.Join("coordinator", "mesh.db"), nil
	case "node":
		return nodeKind, filepath.Join("commands", "journal.db"), nil
	default:
		return 0, "", errors.New("maintenance: role must be server or node")
	}
}

func maintenanceFailure(ctx context.Context, err, fallback error) error {
	if errors.Is(err, ErrLocked) {
		return ErrLocked
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fallback
}

type maintenanceOwner struct {
	root     string
	database string
	role     string
	kind     databaseKind
	locks    map[string]*os.File
}

func (g *maintenanceOwner) close() error {
	var err error
	for _, file := range g.locks {
		if file.Close() != nil {
			err = ErrMaintenancePath
		}
	}
	return err
}

func maintenanceOwn(ctx context.Context, stateDir, role string, requireRoleLock bool) (_ *maintenanceOwner, err error) {
	kind, relative, err := maintenanceRole(role)
	if err != nil {
		return nil, err
	}
	root, err := maintenanceDirectory(stateDir)
	if err != nil {
		return nil, err
	}
	g := &maintenanceOwner{root: root, database: filepath.Join(root, relative), role: role, kind: kind, locks: make(map[string]*os.File)}
	defer func() {
		if err != nil {
			err = errors.Join(err, g.close())
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path := filepath.Join(root, maintenanceRoleLock)
	roleActive := false
	info, statErr := os.Lstat(path)
	if statErr == nil {
		if !info.Mode().IsRegular() {
			return nil, ErrMaintenancePath
		}
		file, err := maintenanceOpenRegular(path, true)
		if err != nil {
			return nil, err
		}
		g.locks[path] = file
		if err := lockFile(file); err != nil {
			return nil, maintenanceFailure(ctx, err, ErrMaintenancePath)
		}
		value, err := io.ReadAll(io.LimitReader(file, 128))
		if err != nil {
			return nil, ErrMaintenanceLockProtocol
		}
		var pending bool
		roleActive, pending = maintenanceMarkerState(string(value), role)
		if !roleActive && !pending {
			return nil, ErrMaintenanceLockProtocol
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, ErrMaintenancePath
	}
	if _, err := maintenanceDirectory(filepath.Dir(g.database)); err != nil {
		return nil, err
	}
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		path := g.database + suffix
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) && suffix != "" {
			continue
		}
		file, err := maintenanceOpenRegular(path, false)
		if err != nil {
			return nil, err
		}
		if file.Close() != nil {
			return nil, ErrMaintenancePath
		}
	}
	path = g.database + ".lock"
	file, err := maintenanceOpenRegular(path, true)
	if err != nil {
		return nil, err
	}
	g.locks[path] = file
	if err := lockFile(file); err != nil {
		return nil, maintenanceFailure(ctx, err, ErrMaintenancePath)
	}
	if requireRoleLock && !roleActive {
		return nil, ErrMaintenanceLockProtocol
	}
	return g, nil
}

// InspectMaintenance takes existing ownership locks and opens SQLite read-only.
// It does not migrate, recover running claims, checkpoint, prune, or execute.
func InspectMaintenance(ctx context.Context, stateDir, role string) (_ *MaintenanceReport, err error) {
	ctx, cancel := context.WithTimeout(ctx, maintenanceTimeout)
	defer cancel()
	g, err := maintenanceOwn(ctx, stateDir, role, false)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, g.close()) }()
	return g.inspect(ctx)
}

func (g *maintenanceOwner) inspect(ctx context.Context) (_ *MaintenanceReport, err error) {
	file, err := maintenanceOpenRegular(filepath.Join(g.root, "instance-id"), false)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, 256))
	closeErr := file.Close()
	id := strings.TrimSpace(string(data))
	if readErr != nil || closeErr != nil || len(data) >= 256 || !protocol.ValidIdempotencyKey(id) {
		return nil, ErrMaintenanceIntegrity
	}
	info, err := os.Stat(g.database)
	if err != nil || info.Size() == 0 || info.Size() > maintenanceMaxBytes {
		return nil, ErrMaintenanceBounds
	}
	uriPath := filepath.ToSlash(g.database)
	if !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	uri := (&url.URL{Scheme: "file", Path: uriPath, RawQuery: "mode=ro"}).String()
	db, err := sql.Open("sqlite", uri)
	if err != nil {
		return nil, ErrMaintenanceIntegrity
	}
	defer func() {
		if db.Close() != nil {
			err = errors.Join(err, ErrMaintenanceIntegrity)
		}
	}()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
	}
	defer func() {
		if conn.Close() != nil {
			err = errors.Join(err, ErrMaintenanceIntegrity)
		}
	}()
	for _, query := range []string{"PRAGMA query_only = ON", "PRAGMA busy_timeout = 1000"} {
		if _, err := conn.ExecContext(ctx, query); err != nil {
			return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
		}
	}
	var integrity string
	if err := conn.QueryRowContext(ctx, "PRAGMA integrity_check(1)").Scan(&integrity); err != nil || integrity != "ok" {
		return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
	}
	store := &Store{conn: conn}
	version, err := store.inspectSchema(ctx, id, false, g.kind)
	if err != nil {
		return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
	}
	report := &MaintenanceReport{Role: g.role, SchemaVersion: version, Integrity: "ok",
		Recovery: "operator_review_required", Commands: MaintenanceCapacity{Limit: maxCommands}}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
	}
	defer tx.Rollback()
	if g.kind == nodeKind {
		entries, err := readNodeEntries(ctx, tx, id, "", nil)
		if err != nil {
			return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
		}
		report.Commands.Used = len(entries)
		for _, entry := range entries {
			if entry.result == nil {
				report.Running++
			} else {
				if entry.result.Status == statusIndeterminate {
					report.Indeterminate++
				}
				if !entry.delivered {
					report.PendingReceipts++
				}
			}
		}
		if version >= 5 {
			if _, err := readNodeProjects(ctx, tx, id); err != nil {
				return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
			}
		}
	} else {
		if _, err := readFleet(ctx, tx); err != nil {
			return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
		}
		if err := validateLegacyBindings(ctx, tx); err != nil {
			return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
		}
		if version >= 2 {
			records, err := readCommands(ctx, tx, "", nil)
			if err != nil {
				return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
			}
			report.Commands.Used = len(records)
			for _, record := range records {
				switch record.Status {
				case statusAccepted:
					report.Accepted++
				case statusRunning:
					report.Running++
				case statusIndeterminate:
					report.Indeterminate++
				}
			}
		}
		if version >= 6 {
			if _, err := readProjects(ctx, tx, "", ""); err != nil {
				return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, maintenanceFailure(ctx, err, ErrMaintenanceIntegrity)
	}
	report.Commands.Remaining = report.Commands.Limit - report.Commands.Used
	report.Commands.Full = report.Commands.Remaining == 0
	return report, nil
}
