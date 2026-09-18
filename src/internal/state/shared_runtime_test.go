package state

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestSharedTransportWithdrawsEvenEstablishedFullRoleBackupContract(t *testing.T) {
	ctx := context.Background()
	for _, role := range []string{"server", "node"} {
		t.Run(role, func(t *testing.T) {
			root, _ := maintenanceFixture(t, role, true)
			guard, err := AcquireRoleState(ctx, root, role)
			requireOK(t, err)
			requireOK(t, guard.DisableFullRoleBackup())
			destination := filepath.Join(maintenanceTempDir(t), "backup")
			if _, err := BackupMaintenance(ctx, root, role, destination); !errors.Is(err, ErrLocked) {
				t.Fatalf("shared role lost runtime ownership: %v", err)
			}
			requireOK(t, guard.Close())
			if _, err := BackupMaintenance(ctx, root, role, destination); !errors.Is(err, ErrMaintenanceLockProtocol) {
				t.Fatalf("shared role advertised a legacy full identity backup: %v", err)
			}
			// A later independently owned legacy runtime may explicitly restore
			// its contract after acquiring its own transport and journal locks.
			legacy, err := AcquireRoleState(ctx, root, role)
			requireOK(t, err)
			requireOK(t, legacy.Activate())
			requireOK(t, legacy.Close())
			if _, err := BackupMaintenance(ctx, root, role, destination); err != nil {
				t.Fatalf("legacy full-role backup behavior changed: %v", err)
			}
		})
	}
}
