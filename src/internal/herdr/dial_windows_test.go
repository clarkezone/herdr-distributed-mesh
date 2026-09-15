//go:build windows

package herdr

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

func TestWindowsLocalPipeMapping(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{`C:\Users\test\AppData\Roaming\herdr\herdr.sock`, `\\.\pipe\C:\Users\test\AppData\Roaming\herdr\herdr.sock`},
		{`\\.\pipe\C:\Users\test\herdr.sock`, `\\.\pipe\C:\Users\test\herdr.sock`},
		{`\\.\pipe\herdr.sock`, `\\.\pipe\herdr.sock`},
		{`\\.\PIPE\herdr.sock`, `\\.\pipe\herdr.sock`},
	} {
		got, err := localAddress(tc.input)
		if err != nil || got != tc.want {
			t.Errorf("local mapping failed: %v", err)
		}
	}
	for _, path := range []string{
		"", `\\remote\pipe\herdr`, `\\localhost\pipe\herdr`, `\\?\pipe\herdr`, `\\?\C:\herdr.sock`,
		`//remote/pipe/herdr`, `\\.\pipe\`, `relative.sock`, `C:relative.sock`, `\herdr.sock`,
		"C:\\herdr\x00.sock", "C:\\herdr\n.sock",
	} {
		if _, err := localAddress(path); err == nil {
			t.Error("non-local or malformed path accepted")
		}
	}
}

// TestLiveWindowsHerdr is opt-in and only observes the existing application.
// Never include raw replies, paths, or session contents in test diagnostics.
func TestLiveWindowsHerdr(t *testing.T) {
	path := os.Getenv("HERDR_MESH_TEST_SOCKET")
	if path == "" {
		t.Skip("HERDR_MESH_TEST_SOCKET is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	complete := errors.New("live observation complete")
	count := 0
	err := Observe(ctx, Config{
		SocketPath: path, RefreshInterval: 300 * time.Millisecond, RequestTimeout: 3 * time.Second,
	}, func(state *agentflowv1.HerdrState) error {
		count++
		checkState(t, state, uint64(count), "ready")
		if state.Status != "ready" {
			t.Errorf("live observer unavailable: %s", state.ErrorCode)
			return complete
		}
		if count == 2 {
			return complete
		}
		return nil
	})
	if err != complete || count != 2 {
		t.Fatal("live observer did not produce two ready reconciliations")
	}
}
