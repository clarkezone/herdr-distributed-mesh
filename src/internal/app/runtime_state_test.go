package app

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

func TestRuntimeRolesLockBeforeIdentityAndTransport(t *testing.T) {
	for _, test := range []struct {
		name string
		role string
		args []string
	}{
		{"server", "server", []string{"server"}},
		{"node", "node", []string{"node", "-server", "unused:50052"}},
		{"controller", "client", []string{"ctl", "nodes", "-server", "unused:50052"}},
		{"dashboard", "client", []string{"dashboard", "-server", "unused:50052", "-listen", "127.0.0.1:0"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := maintenanceAppTempDir(t)
			guard, err := state.PrepareRoleState(context.Background(), root, test.role)
			if err != nil {
				t.Fatal(err)
			}
			defer guard.Close()
			t.Setenv("HERDR_RUNTIME_LOCK_TEST_NO_KEY", "")
			args := append(test.args, "-state-dir", root, "-auth-key-env", "HERDR_RUNTIME_LOCK_TEST_NO_KEY")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err = Run(ctx, args, IO{Out: io.Discard, Err: io.Discard})
			if !errors.Is(err, state.ErrLocked) {
				t.Fatalf("role bypassed exclusive state guard: %v", err)
			}
			if _, err := os.Stat(filepath.Join(root, "instance-id")); !os.IsNotExist(err) {
				t.Fatal("blocked startup accessed identity before acquiring its guard")
			}
		})
	}
}

func TestMaintenanceTopLevelRouteUsesOfflineLock(t *testing.T) {
	root := maintenanceAppTempDir(t)
	guard, err := state.PrepareRoleState(context.Background(), root, "node")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	err = Run(context.Background(), []string{"maintenance", "inspect", "-role", "node", "-state-dir", root},
		IO{Out: io.Discard, Err: io.Discard})
	if !errors.Is(err, state.ErrLocked) {
		t.Fatalf("maintenance router did not use the offline lock: %v", err)
	}
}
