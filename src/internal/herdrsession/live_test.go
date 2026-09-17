package herdrsession

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// This opt-in compatibility test creates only its own named headless session;
// it never attaches a frontend or starts a provider.
func TestLiveNamedHeadlessEnsure(t *testing.T) {
	if os.Getenv("HERDR_MESH_SESSION_TEST") != "1" {
		t.Skip("set HERDR_MESH_SESSION_TEST=1 with compatible Herdr installed")
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	name := "mesh-test-" + hex.EncodeToString(nonce[:])
	config := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(config, []byte("[general]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HERDR_CONFIG_PATH", config)
	m, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	native := func(args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, m.runner.(commandRunner).executable, args...)
		configureCommand(command)
		if err := command.Run(); err != nil {
			t.Errorf("disposable session lifecycle command failed: %v", err)
		}
	}
	t.Cleanup(func() {
		native("session", "stop", name, "--json")
		native("session", "delete", name, "--json")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	first, err := m.Ensure(ctx, name)
	if err != nil || first.Status != "ready" || first.Incarnation == "" {
		t.Fatalf("headless ensure = %+v, %v", first, err)
	}
	second, err := m.Ensure(ctx, name)
	if err != nil || first.Incarnation != second.Incarnation {
		t.Fatalf("re-ensure did not reuse original server: %v", err)
	}
	sessions, err := m.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, session := range sessions {
		if session.Name == name && session.Incarnation == first.Incarnation {
			found = true
		}
	}
	if !found {
		t.Fatal("ready named session missing from discovery")
	}
	native("session", "stop", name, "--json")
	deadline := time.Now().Add(10 * time.Second)
	for {
		stopped, err := m.Status(ctx, name)
		if err == nil && stopped.Status == "stopped" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("disposable session did not stop")
		}
		time.Sleep(100 * time.Millisecond)
	}
	restarted, err := m.Ensure(ctx, name)
	if err != nil || restarted.Status != "ready" || restarted.Incarnation == first.Incarnation {
		t.Fatalf("restart did not produce new incarnation: %v", err)
	}
}
