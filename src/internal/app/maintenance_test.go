package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

func maintenanceAppTempDir(t *testing.T) string {
	t.Helper()
	// Production deliberately rejects aliases, including macOS's /var.
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func maintenanceAppFixture(t *testing.T) string {
	t.Helper()
	root := maintenanceAppTempDir(t)
	if err := os.WriteFile(filepath.Join(root, "instance-id"), []byte("node\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "tsnet"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "tsnet", "tailscaled.state"), []byte("PRIVATE-ENROLLMENT"), 0600); err != nil {
		t.Fatal(err)
	}
	j, err := state.OpenNodeJournal(context.Background(), filepath.Join(root, "commands", "journal.db"), "node")
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestMaintenanceRouterInspectionIsAggregateAndLocal(t *testing.T) {
	root := maintenanceAppFixture(t)
	for _, jsonOutput := range []bool{false, true} {
		var out, stderr bytes.Buffer
		args := []string{"inspect", "-role", "node", "-state-dir", root}
		if jsonOutput {
			args = append(args, "-json")
		}
		if err := RunMaintenance(context.Background(), args, IO{Out: &out, Err: &stderr}); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), root) || strings.Contains(out.String(), "PRIVATE") {
			t.Fatal("local state escaped aggregate output")
		}
		if jsonOutput {
			var report state.MaintenanceReport
			if err := json.Unmarshal(out.Bytes(), &report); err != nil || report.Integrity != "ok" || report.Commands.Used != 0 {
				t.Fatal("invalid JSON inspection", err)
			}
		} else if !strings.Contains(out.String(), "No pruning") {
			t.Fatal("capacity safety omitted")
		}
	}
}

func TestMaintenanceRouterBackupProtocolAndVerification(t *testing.T) {
	root := maintenanceAppFixture(t)
	destination := filepath.Join(maintenanceAppTempDir(t), "backup")
	var out, stderr bytes.Buffer
	io := IO{Out: &out, Err: &stderr}
	args := []string{"backup", "-role", "node", "-state-dir", root, "-destination", destination, "-json"}
	if err := RunMaintenance(context.Background(), args, io); !errors.Is(err, state.ErrMaintenanceLockProtocol) || out.Len() != 0 {
		t.Fatalf("unguarded runtime admitted backup: %v", err)
	}
	guard, err := state.AcquireRoleState(context.Background(), root, "node")
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.Activate(); err != nil {
		t.Fatal(err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	if err := RunMaintenance(context.Background(), args, io); err != nil {
		t.Fatal(err)
	}
	var result state.MaintenanceBackupReport
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || !result.Verified || result.Destination != destination {
		t.Fatal("incomplete backup output", err)
	}
	out.Reset()
	if err := RunMaintenance(context.Background(), []string{"verify-backup", "-role", "node", "-state-dir", destination}, io); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "does not authorize restoring stale") {
		t.Fatal("restore risk omitted")
	}
}

func TestMaintenanceRouterRejectsUnsupportedMutationsAndFlags(t *testing.T) {
	for _, args := range [][]string{
		nil, {"prune"}, {"restore"}, {"sql"}, {"inspect"}, {"inspect", "-role", "client", "-state-dir", "x"},
		{"inspect", "-role", "node", "-state-dir", "x", "-timeout", "0s"},
		{"inspect", "-role", "node", "-state-dir", "x", "-timeout", "3m"},
		{"inspect", "-role", "node", "-state-dir", "x", "-destination", "y"},
		{"backup", "-role", "node", "-state-dir", "x"},
		{"inspect", "-role", "node", "-state-dir", "x", "extra"},
	} {
		var out, stderr bytes.Buffer
		if err := RunMaintenance(context.Background(), args, IO{Out: &out, Err: &stderr}); err == nil || out.Len() != 0 {
			t.Fatalf("invalid invocation accepted: %v", args)
		}
	}
}
