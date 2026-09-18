package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

func maintenanceTempDir(t *testing.T) string {
	t.Helper()
	// macOS temp roots can contain /var, which aliases /private/var.
	root, err := filepath.EvalSymlinks(t.TempDir())
	requireOK(t, err)
	return root
}

func maintenanceFixture(t *testing.T, role string, guarded bool) (string, string) {
	t.Helper()
	root := maintenanceTempDir(t)
	requireOK(t, os.WriteFile(filepath.Join(root, "instance-id"), []byte(role+"\n"), 0600))
	requireOK(t, os.Mkdir(filepath.Join(root, "tsnet"), 0700))
	requireOK(t, os.WriteFile(filepath.Join(root, "tsnet", "tailscaled.state"), []byte("PRIVATE-ENROLLMENT-MATERIAL"), 0600))
	kind, relative, err := maintenanceRole(role)
	requireOK(t, err)
	path := filepath.Join(root, relative)
	s, err := openStore(context.Background(), path, role, kind)
	requireOK(t, err)
	requireOK(t, s.Close())
	if guarded {
		guard, err := AcquireRoleState(context.Background(), root, role)
		requireOK(t, err)
		requireOK(t, guard.Activate())
		requireOK(t, guard.Close())
	}
	return root, path
}

func TestMaintenanceFixturesCanonicalizeTemporaryRoot(t *testing.T) {
	parent := maintenanceTempDir(t)
	target := maintenanceTempDir(t)
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(name, alias)
	}
	// A fresh subtest allocates its temp root under the aliased temp directory.
	t.Run("canonical-fixture", func(t *testing.T) {
		root, _ := maintenanceFixture(t, "node", true)
		canonical, err := filepath.EvalSymlinks(root)
		requireOK(t, err)
		if root != canonical {
			t.Fatalf("fixture retained a directory alias: %q", root)
		}
		_, err = InspectMaintenance(context.Background(), root, "node")
		requireOK(t, err)
		destination := filepath.Join(maintenanceTempDir(t), "backup")
		_, err = BackupMaintenance(context.Background(), root, "node", destination)
		requireOK(t, err)
	})
	if _, err := maintenanceDirectory(alias); !errors.Is(err, ErrMaintenancePath) {
		t.Fatalf("production accepted a directory alias: %v", err)
	}
}

func TestMaintenanceInspectPreservesRunningClaimsAndRedacts(t *testing.T) {
	ctx := context.Background()
	root, path := maintenanceFixture(t, "node", false)
	j, err := OpenNodeJournal(ctx, path, "node")
	requireOK(t, err)
	first := claimTestCommand(t, j, 1)
	requireOK(t, j.Complete(ctx, probeResult(first, statusSucceeded, "pong")))
	requireOK(t, j.Acknowledge(ctx, first.CommandId, statusSucceeded))
	second := claimTestCommand(t, j, 2)
	requireOK(t, j.Close())
	before, err := os.ReadFile(path)
	requireOK(t, err)
	report, err := InspectMaintenance(ctx, root, "node")
	requireOK(t, err)
	if report.Commands.Used != 2 || report.Commands.Remaining != maxCommands-2 ||
		report.Running != 1 || report.Indeterminate != 0 || report.PendingReceipts != 0 ||
		report.PruningSupported || report.Integrity != "ok" {
		t.Fatalf("incorrect aggregate inspection: %+v", report)
	}
	after, err := os.ReadFile(path)
	requireOK(t, err)
	if !bytes.Equal(before, after) {
		t.Fatal("inspection rewrote/migrated/recovered the journal")
	}
	data, err := json.Marshal(report)
	requireOK(t, err)
	for _, private := range []string{root, first.CommandId, first.IdempotencyKey, second.CommandId, "PRIVATE-ENROLLMENT-MATERIAL"} {
		if strings.Contains(string(data), private) {
			t.Fatal("private state escaped inspection")
		}
	}
	j, err = OpenNodeJournal(ctx, path, "node")
	requireOK(t, err)
	defer j.Close()
	retry, claimed, err := j.Claim(ctx, first)
	requireOK(t, err)
	if claimed || retry.GetDetail() != "pong" {
		t.Fatal("inspection destroyed tombstone")
	}
	if got := rowCount(t, j.store, "SELECT count(*) FROM node_commands WHERE status = 2 AND result IS NULL"); got != 1 {
		t.Fatal("inspection forced running claim to a terminal outcome")
	}
}

func TestMaintenanceLocksAndRuntimeProtocolFailClosed(t *testing.T) {
	ctx := context.Background()
	root, path := maintenanceFixture(t, "node", true)
	guard, err := AcquireRoleState(ctx, root, "node")
	requireOK(t, err)
	for _, run := range []func() error{
		func() error { _, err := InspectMaintenance(ctx, root, "node"); return err },
		func() error {
			_, err := BackupMaintenance(ctx, root, "node", filepath.Join(maintenanceTempDir(t), "backup"))
			return err
		},
	} {
		if err := run(); !errors.Is(err, ErrLocked) {
			t.Fatalf("live role lock not rejected: %v", err)
		}
	}
	requireOK(t, guard.Close())
	j, err := OpenNodeJournal(ctx, path, "node")
	requireOK(t, err)
	if _, err := InspectMaintenance(ctx, root, "node"); !errors.Is(err, ErrLocked) {
		t.Fatalf("live journal lock not rejected: %v", err)
	}
	requireOK(t, j.Close())
	legacy, _ := maintenanceFixture(t, "node", false)
	if _, err := BackupMaintenance(ctx, legacy, "node", filepath.Join(maintenanceTempDir(t), "backup")); !errors.Is(err, ErrMaintenanceLockProtocol) {
		t.Fatalf("journal lock alone authorized whole-role copy: %v", err)
	}
	if _, err := os.Stat(filepath.Join(legacy, maintenanceRoleLock)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("maintenance invented runtime lock capability")
	}
}

func TestMaintenanceBackupPreservesCompleteRoleAndVerifies(t *testing.T) {
	for _, role := range []string{"node", "server"} {
		t.Run(role, func(t *testing.T) {
			ctx := context.Background()
			root, path := maintenanceFixture(t, role, true)
			if role == "node" {
				j, err := OpenNodeJournal(ctx, path, role)
				requireOK(t, err)
				c := claimTestCommand(t, j, 1)
				requireOK(t, j.Complete(ctx, probeResult(c, statusSucceeded, "pong")))
				claimTestCommand(t, j, 2)
				requireOK(t, j.Close())
			} else {
				s, err := Open(ctx, path, role)
				requireOK(t, err)
				requireOK(t, s.Bind(ctx, "stable", "node"))
				createTestCommand(t, s, 1)
				requireOK(t, s.Close())
			}
			// Unknown state files are retained rather than an allowlist silently
			// omitting credentials or future retry/session evidence.
			requireOK(t, os.WriteFile(filepath.Join(root, "future-private-state"), []byte("PRIVATE-FUTURE-EVIDENCE"), 0600))
			destination := filepath.Join(maintenanceTempDir(t), "backup")
			report, err := BackupMaintenance(ctx, root, role, destination)
			requireOK(t, err)
			if !report.Verified || report.Destination != destination || report.Source.Commands.Used == 0 {
				t.Fatal("incomplete backup report")
			}
			entries, _, err := maintenanceTree(ctx, root)
			requireOK(t, err)
			for _, entry := range entries {
				if entry.Directory {
					continue
				}
				source, err := os.ReadFile(filepath.Join(root, entry.Path))
				requireOK(t, err)
				copied, err := os.ReadFile(filepath.Join(destination, "state", entry.Path))
				requireOK(t, err)
				if !bytes.Equal(source, copied) {
					t.Fatal("role file not preserved byte-for-byte")
				}
			}
			verified, err := VerifyMaintenanceBackup(ctx, destination, role)
			requireOK(t, err)
			if !verified.Verified || verified.Source != report.Source {
				t.Fatal("verification lost source report")
			}
			if _, err := BackupMaintenance(ctx, root, role, destination); !errors.Is(err, ErrMaintenancePath) {
				t.Fatal("backup overwrote destination")
			}
			requireOK(t, os.WriteFile(filepath.Join(destination, "state", "future-private-state"), []byte("altered"), 0600))
			if _, err := VerifyMaintenanceBackup(ctx, destination, role); !errors.Is(err, ErrMaintenanceBackup) {
				t.Fatalf("modified backup passed: %v", err)
			}
		})
	}
}

func TestMaintenanceCapacityDoesNotDiscardAnyIdentity(t *testing.T) {
	ctx := context.Background()
	root, path := maintenanceFixture(t, "node", false)
	j, err := OpenNodeJournal(ctx, path, "node")
	requireOK(t, err)
	requireOK(t, j.store.transaction(ctx, func(tx *sql.Tx) error {
		for i := 1; i <= maxCommands; i++ {
			c := probeCommand(i, "actor", fmt.Sprintf("key%d", i), "node", commandTestTime)
			payload, err := marshalCommandMessage(c)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO node_commands(command_id, status, delivered, command) VALUES (?, ?, 0, ?)", c.CommandId, statusRunning, payload); err != nil {
				return err
			}
		}
		return nil
	}))
	requireOK(t, j.Close())
	report, err := InspectMaintenance(ctx, root, "node")
	requireOK(t, err)
	if !report.Commands.Full || report.Commands.Remaining != 0 || report.Commands.Used != maxCommands || report.Running != maxCommands {
		t.Fatalf("capacity threshold wrong: %+v", report)
	}
	j, err = OpenNodeJournal(ctx, path, "node")
	requireOK(t, err)
	defer j.Close()
	if got := rowCount(t, j.store, "SELECT count(*) FROM node_commands"); got != maxCommands {
		t.Fatal("capacity diagnostics removed receipts")
	}
	newCommand := probeCommand(maxCommands+1, "actor", protocol.NewCommandID(), "node", commandTestTime)
	if _, _, err := j.Claim(ctx, newCommand); !errors.Is(err, ErrCommandCapacity) {
		t.Fatal("inspection silently freed retry slots")
	}
}

func TestMaintenanceRejectsCorruptionIdentityMismatchAndUnsafePaths(t *testing.T) {
	ctx := context.Background()
	for _, damage := range []string{"corrupt", "identity", "missing", "cancel"} {
		t.Run(damage, func(t *testing.T) {
			root, path := maintenanceFixture(t, "node", false)
			switch damage {
			case "corrupt":
				requireOK(t, os.WriteFile(path, []byte("not SQLite"), 0600))
			case "identity":
				requireOK(t, os.WriteFile(filepath.Join(root, "instance-id"), []byte("other"), 0600))
			case "missing":
				requireOK(t, os.Remove(path))
			}
			op := ctx
			if damage == "cancel" {
				var cancel context.CancelFunc
				op, cancel = context.WithCancel(ctx)
				cancel()
			}
			if report, err := InspectMaintenance(op, root, "node"); err == nil || report != nil {
				t.Fatal("invalid state appeared healthy")
			}
		})
	}
	root, _ := maintenanceFixture(t, "node", true)
	for _, destination := range []string{root, filepath.Join(root, "nested"), filepath.Join(root, "tsnet", "nested"), "relative"} {
		if _, err := BackupMaintenance(ctx, root, "node", destination); !errors.Is(err, ErrMaintenancePath) {
			t.Fatalf("unsafe destination accepted: %v", err)
		}
	}
	link := filepath.Join(maintenanceTempDir(t), "alias")
	if err := os.Symlink(root, link); err == nil {
		if _, err := InspectMaintenance(ctx, link, "node"); !errors.Is(err, ErrMaintenancePath) {
			t.Fatal("linked source accepted")
		}
		if _, err := BackupMaintenance(ctx, root, "node", filepath.Join(link, "backup")); !errors.Is(err, ErrMaintenancePath) {
			t.Fatal("linked destination ancestor accepted")
		}
	}
	original := filepath.Join(root, "tsnet", "tailscaled.state")
	hardlink := filepath.Join(maintenanceTempDir(t), "alias")
	if err := os.Link(original, hardlink); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	destination := filepath.Join(maintenanceTempDir(t), "backup")
	if _, err := BackupMaintenance(ctx, root, "node", destination); err == nil {
		t.Fatal("hardlinked sensitive state copied")
	}
	if _, err := os.Stat(filepath.Join(destination, "COMPLETE")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed backup was marked complete")
	}
}

func TestMaintenanceBackupBoundsAndIncompleteArtifacts(t *testing.T) {
	root, _ := maintenanceFixture(t, "node", true)
	deep := root
	for i := 0; i <= maintenanceMaxDepth; i++ {
		deep = filepath.Join(deep, "deep")
		requireOK(t, os.Mkdir(deep, 0700))
	}
	if _, err := BackupMaintenance(context.Background(), root, "node", filepath.Join(maintenanceTempDir(t), "backup")); !errors.Is(err, ErrMaintenanceBounds) {
		t.Fatalf("depth bound not enforced: %v", err)
	}
	incomplete := maintenanceTempDir(t)
	requireOK(t, os.WriteFile(filepath.Join(incomplete, "manifest.json"), []byte("{}"), 0600))
	if _, err := VerifyMaintenanceBackup(context.Background(), incomplete, "node"); !errors.Is(err, ErrMaintenanceBackup) {
		t.Fatal("incomplete artifact passed")
	}
}

func TestMaintenanceBackupRequiresTransportIdentityAndMatchingRole(t *testing.T) {
	ctx := context.Background()
	root, _ := maintenanceFixture(t, "node", true)
	if _, err := AcquireRoleState(ctx, root, "client"); !errors.Is(err, ErrMaintenanceLockProtocol) {
		t.Fatalf("role-state lock silently switched role: %v", err)
	}
	requireOK(t, os.Remove(filepath.Join(root, "tsnet", "tailscaled.state")))
	destination := filepath.Join(maintenanceTempDir(t), "backup")
	if _, err := BackupMaintenance(ctx, root, "node", destination); !errors.Is(err, ErrMaintenancePath) {
		t.Fatalf("missing transport identity admitted full-role backup: %v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing identity caused output publication")
	}
}

func TestMaintenanceEntryDetectsChangedSourceAndCancellation(t *testing.T) {
	root := maintenanceTempDir(t)
	path := filepath.Join(root, "state")
	requireOK(t, os.WriteFile(path, []byte("original"), 0600))
	entries, _, err := maintenanceTree(context.Background(), root)
	requireOK(t, err)
	requireOK(t, os.WriteFile(path, []byte("changed length"), 0600))
	if _, err := maintenanceReadEntry(context.Background(), root, entries[0], nil, nil); !errors.Is(err, ErrMaintenanceChanged) {
		t.Fatalf("source mutation not detected: %v", err)
	}
	entries, _, err = maintenanceTree(context.Background(), root)
	requireOK(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := maintenanceReadEntry(ctx, root, entries[0], nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("chunked read ignored cancellation: %v", err)
	}
}
