package app

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestDashboardRejectsUnsafeConfiguration(t *testing.T) {
	for _, args := range [][]string{
		{"dashboard"},
		{"dashboard", "-server", "server:50052", "-listen", "0.0.0.0:8787"},
		{"dashboard", "-server", "server:50052", "-listen", "localhost:8787"},
		{"dashboard", "-server", "server:50052", "unexpected"},
	} {
		if err := Run(context.Background(), args, IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}); err == nil {
			t.Fatalf("accepted invalid configuration: %v", args)
		}
	}
}

func TestHelpIncludesDashboard(t *testing.T) {
	var output bytes.Buffer
	if err := Run(context.Background(), []string{"help"}, IO{Out: &output, Err: &output}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "herdr-mesh dashboard") {
		t.Fatal("dashboard missing from help")
	}
}
