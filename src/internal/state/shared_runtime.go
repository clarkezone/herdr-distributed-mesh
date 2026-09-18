package state

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// RoleStateInUse probes existing kernel-held ownership without creating,
// normalizing, activating, or otherwise changing any state or permissions.
func RoleStateInUse(ctx context.Context, stateDir, role string) (_ bool, result error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if (role != "server" && role != "node" && role != "client") || stateDir == "" || !maintenancePathSyntaxSafe(stateDir) {
		return false, ErrMaintenancePath
	}
	root, err := filepath.Abs(stateDir)
	if err != nil {
		return false, ErrMaintenancePath
	}
	if _, err := os.Lstat(root); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, ErrMaintenancePath
	}
	if _, err := maintenanceDirectory(root); err != nil {
		return false, err
	}
	path := filepath.Join(root, maintenanceRoleLock)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, ErrMaintenancePath
	}
	file, err := maintenanceOpenRegular(path, false)
	if err != nil {
		return false, err
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	if err := lockFile(file); errors.Is(err, ErrLocked) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	content, err := io.ReadAll(io.LimitReader(file, 129))
	if err != nil {
		return false, err
	}
	active, pending := maintenanceMarkerState(string(content), role)
	if !active && !pending {
		return false, ErrMaintenanceLockProtocol
	}
	return false, nil
}

// DisableFullRoleBackup retains runtime ownership while withdrawing the legacy
// full-role restore contract. Borrowed transports and combined managed layouts
// must not claim that this directory independently owns the network identity.
func (g *RoleStateLock) DisableFullRoleBackup() error {
	if g == nil {
		return ErrMaintenanceLockProtocol
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.file == nil {
		return ErrMaintenanceLockProtocol
	}
	if err := g.file.Truncate(0); err != nil {
		return ErrMaintenancePath
	}
	if n, err := g.file.WriteAt([]byte(g.pendingMarker), 0); err != nil || n != len(g.pendingMarker) {
		return ErrMaintenancePath
	}
	if err := g.file.Sync(); err != nil {
		return ErrMaintenancePath
	}
	g.active = false
	return nil
}
