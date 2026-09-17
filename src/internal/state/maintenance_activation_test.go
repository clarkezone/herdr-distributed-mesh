package state

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMaintenanceAbortedStartupDoesNotActivateBackupContract(t *testing.T) {
	ctx := context.Background()
	root, _ := maintenanceFixture(t, "node", false)
	destination := filepath.Join(t.TempDir(), "backup")
	first, err := AcquireRoleState(ctx, root, "node")
	requireOK(t, err)
	if _, err := BackupMaintenance(ctx, root, "node", destination); !errors.Is(err, ErrLocked) {
		t.Fatalf("acquired runtime guard did not deny maintenance: %v", err)
	}
	requireOK(t, first.Close()) // Simulate tsnet startup failing against an older writer.
	if _, err := BackupMaintenance(ctx, root, "node", destination); !errors.Is(err, ErrMaintenanceLockProtocol) {
		t.Fatalf("aborted startup advertised writer participation: %v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pending contract published a backup")
	}
	marker, err := os.ReadFile(filepath.Join(root, maintenanceRoleLock))
	requireOK(t, err)
	if active, pending := maintenanceMarkerState(string(marker), "node"); active || !pending {
		t.Fatal("aborted startup marker was not explicitly pending")
	}
	if err := first.Activate(); !errors.Is(err, ErrMaintenanceLockProtocol) {
		t.Fatal("closed guard activated participation")
	}

	next, err := AcquireRoleState(ctx, root, "node")
	requireOK(t, err)
	requireOK(t, next.Activate()) // Runtime calls this only after all actual writer locks are acquired.
	requireOK(t, next.Activate())
	if _, err := BackupMaintenance(ctx, root, "node", destination); !errors.Is(err, ErrLocked) {
		t.Fatalf("active runtime guard did not deny maintenance: %v", err)
	}
	requireOK(t, next.Close())
	report, err := BackupMaintenance(ctx, root, "node", destination)
	requireOK(t, err)
	if !report.Verified {
		t.Fatal("successful stopped runtime contract did not permit backup")
	}
}

func TestMaintenancePendingMarkersRecoverWithoutImplicitActivation(t *testing.T) {
	ctx := context.Background()
	pending := maintenanceLockPending + "node\n"
	for _, value := range []string{"", maintenanceLockPending[:8], pending, pending + "act", pending + "active"} {
		t.Run(value, func(t *testing.T) {
			root, _ := maintenanceFixture(t, "node", false)
			path := filepath.Join(root, maintenanceRoleLock)
			requireOK(t, os.WriteFile(path, []byte(value), 0600))
			guard, err := AcquireRoleState(ctx, root, "node")
			requireOK(t, err)
			requireOK(t, guard.Close())
			actual, err := os.ReadFile(path)
			requireOK(t, err)
			if string(actual) != pending {
				t.Fatal("interrupted marker was not normalized to pending")
			}
			if _, err := BackupMaintenance(ctx, root, "node", filepath.Join(t.TempDir(), "backup")); !errors.Is(err, ErrMaintenanceLockProtocol) {
				t.Fatalf("recovered pending marker admitted backup: %v", err)
			}
			guard, err = AcquireRoleState(ctx, root, "node")
			requireOK(t, err)
			requireOK(t, guard.Activate())
			requireOK(t, guard.Close())
			if _, err := BackupMaintenance(ctx, root, "node", filepath.Join(t.TempDir(), "backup")); err != nil {
				t.Fatalf("recovered marker could not subsequently activate: %v", err)
			}
		})
	}
}

func TestMaintenanceEstablishedMarkerCompatibility(t *testing.T) {
	ctx := context.Background()
	for _, active := range []string{
		maintenanceLockVersion + "node\n",
		maintenanceLockPending + "node\n" + maintenanceLockActivation,
	} {
		root, _ := maintenanceFixture(t, "node", false)
		path := filepath.Join(root, maintenanceRoleLock)
		requireOK(t, os.WriteFile(path, []byte(active), 0600))
		guard, err := AcquireRoleState(ctx, root, "node")
		requireOK(t, err)
		requireOK(t, guard.Activate())
		requireOK(t, guard.Close())
		actual, err := os.ReadFile(path)
		requireOK(t, err)
		if !bytes.Equal(actual, []byte(active)) {
			t.Fatal("established marker was rewritten")
		}
		if _, err := BackupMaintenance(ctx, root, "node", filepath.Join(t.TempDir(), "backup")); err != nil {
			t.Fatalf("established participation stopped being compatible: %v", err)
		}
	}
}
