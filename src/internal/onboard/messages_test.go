package onboard

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

func TestJoinCommandUsesOnlyValidatedReportedAddress(t *testing.T) {
	const host = "herdr-mesh-desktop-1.tail123.ts.net"
	for _, tc := range []struct {
		dns, server, want string
	}{
		{host, host + ":50052", host},
		{host + ".", host + ".:5443", host + ":5443"},
		{host, "", host},
		{"", "", ""},
		{"desktop", "", ""},
		{host + ";shutdown", "", ""},
		{host, "different.tail123.ts.net:50052", ""},
		{host, host + ":0", ""},
		{host, host + ":65536", ""},
		{host, host + ":garbage", ""},
	} {
		got, err := JoinCommand(meshlocal.Status{DNSName: tc.dns, Server: tc.server})
		if tc.want == "" {
			if err == nil || got != "" {
				t.Fatalf("invalid address advertised: %q %q => %q, %v", tc.dns, tc.server, got, err)
			}
		} else if err != nil || got != "herdr-mesh join --server "+tc.want {
			t.Fatalf("wrong join command: %q, %v", got, err)
		}
	}
}

func TestOnboardingOutputOffersPersistentHelpWithoutStartupChatter(t *testing.T) {
	f := newFixture(t)
	var output bytes.Buffer
	if err := Run(context.Background(), initOptions(), &output, f.d); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Ready:", "herdr-mesh join --server", "herdr-mesh help"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("missing %q: %s", want, &output)
		}
	}
	for _, unwanted := range []string{"Background runtime only", "Managed daemon:", "login_required"} {
		if strings.Contains(output.String(), unwanted) {
			t.Fatalf("unhelpful output %q", unwanted)
		}
	}
	err := readinessWaitError(context.DeadlineExceeded)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "herdr-mesh help") ||
		!strings.Contains(err.Error(), "herdr-mesh status") {
		t.Fatalf("missing timeout recovery: %v", err)
	}
}
