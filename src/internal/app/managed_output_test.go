package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

func savedController(t *testing.T, local meshlocal.Status) string {
	t.Helper()
	dir := maintenanceAppTempDir(t)
	if err := meshlocal.Save(dir, meshlocal.Config{Version: 1, Name: "desktop", Tailnet: "example.com", Coordinator: true}); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(local)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "status.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestControllerHelpShowsSavedJoinCommandWithoutNetwork(t *testing.T) {
	for _, status := range []meshlocal.Status{
		{State: "ready", DNSName: "herdr-mesh-desktop-1.tail123.ts.net", Server: "herdr-mesh-desktop-1.tail123.ts.net:50052"},
		{State: "stopped", DNSName: "herdr-mesh-desktop-1.tail123.ts.net", Server: "herdr-mesh-desktop-1.tail123.ts.net:5443"},
	} {
		dir := savedController(t, status)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var output bytes.Buffer
		if err := Run(ctx, []string{"--state-dir", dir, "help"}, IO{Out: &output, Err: &output}); err != nil {
			t.Fatal(err)
		}
		want := "herdr-mesh join --server herdr-mesh-desktop-1.tail123.ts.net"
		if status.State == "stopped" {
			want += ":5443"
		}
		if !strings.Contains(output.String(), want+"\n") {
			t.Fatalf("missing exact saved endpoint: %s", &output)
		}
		if !strings.Contains(output.String(), "http://herdr-mesh-desktop-1.tail123.ts.net:8787/") {
			t.Fatalf("missing hosted dashboard endpoint: %s", &output)
		}
		for _, unwanted := range []string{"Background runtime only", "No registry", ":50052\n"} {
			if strings.Contains(output.String(), unwanted) {
				t.Fatalf("unwanted help text %q", unwanted)
			}
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 2 {
			t.Fatalf("help changed saved state: %v, %v", entries, err)
		}
	}
}

func TestControllerHelpDoesNotInventEndpoint(t *testing.T) {
	for _, local := range []meshlocal.Status{
		{State: "login_required"},
		{State: "ready", DNSName: "herdr-mesh-desktop.tail123.ts.net", Server: "different.tail123.ts.net:50052"},
	} {
		dir := savedController(t, local)
		var output bytes.Buffer
		err := printControllerInstructions(dir, &output)
		if local.DNSName == "" {
			if err != nil || !strings.Contains(output.String(), "herdr-mesh status") {
				t.Fatalf("missing setup guidance: %v, %s", err, &output)
			}
		} else if err == nil {
			t.Fatal("mismatched controller address advertised")
		}
		if strings.Contains(output.String(), "join --server") {
			t.Fatal("invented a join endpoint")
		}
	}
}

func TestConnectionGuidancePreservesCauseAndExplainsSignIn(t *testing.T) {
	dir := savedController(t, meshlocal.Status{State: "login_required", AuthURL: "https://login.tailscale.com/a/test"})
	ctx, err := meshlocal.WithStateDir(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("private transport diagnostic")
	stopped := explainManagedConnection(ctx, cause)
	if !errors.Is(stopped, cause) || !strings.Contains(stopped.Error(), "herdr-mesh start") ||
		strings.Contains(stopped.Error(), cause.Error()) {
		t.Fatalf("unhelpful stopped error: %v", stopped)
	}
	guard, err := state.PrepareRoleState(ctx, dir, "client")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	waiting := explainManagedConnection(ctx, cause)
	if !strings.Contains(waiting.Error(), "Tailscale sign-in") || !errors.Is(waiting, cause) {
		t.Fatalf("unhelpful login error: %v", waiting)
	}
	var output bytes.Buffer
	if err := printLocalStatus(&output, dir, meshlocal.Status{State: "login_required", AuthURL: "https://untrusted.example/a/test"}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "untrusted") || strings.Contains(output.String(), "daemon=") {
		t.Fatalf("unsafe or cryptic status: %s", &output)
	}
}

func TestHelpBeforeSetupDoesNotCreateState(t *testing.T) {
	dir := filepath.Join(maintenanceAppTempDir(t), "missing")
	for _, help := range []string{"help", "--help", "-h"} {
		if err := Run(context.Background(), []string{"--state-dir", dir, help}, IO{Out: io.Discard, Err: io.Discard}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("help created state")
	}
}

func TestWorkerHelpDoesNotAdvertiseController(t *testing.T) {
	dir := maintenanceAppTempDir(t)
	cfg := meshlocal.Config{Version: 1, Name: "worker", Server: "controller.tail123.ts.net:50052"}
	if err := meshlocal.Save(dir, cfg); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := printControllerInstructions(dir, &output); err != nil || output.Len() != 0 {
		t.Fatalf("worker advertised as a controller: %v %s", err, &output)
	}
}

func TestStoppedStatusKeepsJSONContractAndShowsHumanJoinInstructions(t *testing.T) {
	dir := savedController(t, meshlocal.Status{State: "ready", DNSName: "controller.tail123.ts.net", Server: "controller.tail123.ts.net:50052"})
	ctx, err := meshlocal.WithStateDir(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	handled, err := runManagedCommands(ctx, []string{"status", "--json"}, IO{Out: &output, Err: io.Discard})
	if !handled || err == nil {
		t.Fatalf("stopped status must report failure: %v", err)
	}
	var result struct {
		Daemon meshlocal.Status `json:"daemon"`
		Error  string           `json:"server_error"`
	}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatalf("non-JSON output: %v %s", err, &output)
	}
	if result.Daemon.State != "ready" || !strings.Contains(result.Error, "connect to managed daemon") {
		t.Fatalf("machine contract changed: %+v", result)
	}
	output.Reset()
	_, err = runManagedCommands(ctx, []string{"status"}, IO{Out: &output, Err: io.Discard})
	if err == nil || !strings.Contains(output.String(), "Mesh: Stopped") ||
		!strings.Contains(output.String(), "herdr-mesh join --server controller.tail123.ts.net\n") {
		t.Fatalf("unhelpful human status: %v %s", err, &output)
	}
}
