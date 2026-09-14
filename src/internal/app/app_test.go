package app

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunHelp(t *testing.T) {
	var output bytes.Buffer
	err := Run(context.Background(), []string{"help"}, IO{Out: &output, Err: &output})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !strings.Contains(output.String(), "herdr-mesh server") {
		t.Fatalf("help output = %q", output.String())
	}
}

func TestRunRejectsUnknownCommand(t *testing.T) {
	err := Run(context.Background(), []string{"unknown"}, IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}})
	if err == nil {
		t.Fatal("Run() error = nil, want error")
	}
}

func TestRunNodeRejectsNonPositiveReconnectDelay(t *testing.T) {
	err := Run(
		context.Background(),
		[]string{"node", "-server", "server.test:50052", "-reconnect-delay", "0s"},
		IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}},
	)
	if err == nil {
		t.Fatal("Run() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "reconnect-delay") {
		t.Fatalf("Run() error = %q, want reconnect-delay validation", err)
	}
}

func TestRunNodeRejectsReconnectMaximumBelowInitialDelay(t *testing.T) {
	err := Run(
		context.Background(),
		[]string{
			"node",
			"-server", "server.test:50052",
			"-reconnect-delay", "10s",
			"-reconnect-max-delay", "5s",
		},
		IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}},
	)
	if err == nil {
		t.Fatal("Run() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "reconnect-max-delay") {
		t.Fatalf("Run() error = %q, want reconnect-max-delay validation", err)
	}
}
