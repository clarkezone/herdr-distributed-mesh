package state

import (
	"context"
	"os"
	"path/filepath"
	"strings"
)

// PrepareRoleState creates a role directory, but never activates its backup
// contract. The caller activates only after acquiring all runtime writer locks.
func PrepareRoleState(ctx context.Context, stateDir, role string) (*RoleStateLock, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(stateDir) == "" || (role != "server" && role != "node" && role != "client") {
		return nil, ErrMaintenancePath
	}
	// Abs cleans traversal and can turn foreign UNC syntax into a local path.
	if !maintenancePathSyntaxSafe(stateDir) {
		return nil, ErrMaintenancePath
	}
	root, err := filepath.Abs(stateDir)
	if err != nil || !maintenanceLocalPath(root) || filepath.Dir(root) == root {
		return nil, ErrMaintenancePath
	}
	_, statErr := os.Lstat(root)
	created := os.IsNotExist(statErr)
	if statErr != nil && !created {
		return nil, ErrMaintenancePath
	}
	if !created {
		if _, err := runtimeDirectory(root); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, ErrMaintenancePath
	}
	if created {
		if err := protectPath(root, true); err != nil {
			return nil, ErrMaintenancePath
		}
	}
	return AcquireRoleState(ctx, root, role)
}

func runtimeDirectory(path string) (string, error) {
	if !maintenanceLocalPath(path) {
		return "", ErrMaintenancePath
	}
	path = filepath.Clean(path)
	if filepath.Dir(path) == path {
		return "", ErrMaintenancePath
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || !maintenanceOrdinary(path, info) {
		return "", ErrMaintenancePath
	}
	return path, nil
}
