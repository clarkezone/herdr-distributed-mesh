package app

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestSetupTopLevelRouteHelpNeedsNoRuntime(t *testing.T) {
	var out, diagnostics bytes.Buffer
	err := Run(context.Background(), []string{"setup", "tailnet", "-h"}, IO{Out: &out, Err: &diagnostics})
	if err != nil || !strings.Contains(out.String(), "api-token-env") || !strings.Contains(out.String(), "apply") {
		t.Fatalf("embedded setup help is not routed: %v", err)
	}

	for _, args := range [][]string{{"setup"}, {"setup", "unknown"}, {"setup", "tailnet", "-unknown"}} {
		if err := Run(context.Background(), args, IO{Out: &out, Err: &diagnostics}); err == nil {
			t.Fatal("invalid setup route was accepted")
		}
	}
}

func TestBootstrapTopLevelRouteHelpNeedsNoRuntime(t *testing.T) {
	var out, diagnostics bytes.Buffer
	err := Run(context.Background(), []string{"bootstrap", "-h"}, IO{Out: &out, Err: &diagnostics})
	if err != nil || !strings.Contains(out.String(), "ssh-host") || !strings.Contains(out.String(), "herdr-executable") {
		t.Fatalf("embedded bootstrap help is not routed: %v", err)
	}
	if err := Run(context.Background(), []string{"bootstrap", "-unknown"}, IO{Out: &out, Err: &diagnostics}); err == nil {
		t.Fatal("invalid bootstrap flags were accepted")
	}
}
