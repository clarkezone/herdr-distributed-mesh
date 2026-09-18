package app

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"strings"
	"testing"
)

func TestRunRoutesMCPHelpWithoutNetwork(t *testing.T) {
	var output, diagnostics bytes.Buffer
	err := Run(context.Background(), []string{"mcp", "-help"}, IO{Out: &output, Err: &diagnostics})
	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(diagnostics.String(), "-call-timeout") {
		t.Fatalf("MCP help was not routed: %v %s", err, diagnostics.String())
	}
	if output.Len() != 0 {
		t.Fatal("CLI diagnostics contaminated MCP protocol stdout")
	}
	output.Reset()
	if err := Run(context.Background(), []string{"help"}, IO{Out: &output, Err: &diagnostics}); err != nil ||
		!strings.Contains(output.String(), "herdr-mesh mcp") {
		t.Fatalf("top-level usage omitted MCP: %v %s", err, output.String())
	}
}
