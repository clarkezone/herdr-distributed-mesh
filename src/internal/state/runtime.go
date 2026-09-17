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
	root, err := filepath.Abs(stateDir)
	if err != nil || !maintenanceLocalPath(root) || filepath.Dir(root) == root {
		return nil, ErrMaintenancePath
	}
	_, statErr := os.Lstat(root)
	created := os.IsNotExist(statErr)
	if statErr != nil && !created {
		return nil, ErrMaintenancePath
	}
	parent := root
	for {
		if info, err := os.Lstat(parent); err == nil {
			if filepath.Dir(parent) == parent {
				if !info.IsDir() || !maintenanceOrdinary(parent, info) {
					return nil, ErrMaintenancePath
				}
			} else {
				if _, err := maintenanceDirectory(parent); err != nil {
					return nil, err
				}
			}
			break
		} else if !os.IsNotExist(err) || filepath.Dir(parent) == parent {
			return nil, ErrMaintenancePath
		}
		parent = filepath.Dir(parent)
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
