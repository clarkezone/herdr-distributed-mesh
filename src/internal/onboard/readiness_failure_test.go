package onboard

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

func TestStoppedReadinessReportsFinalFailureWithoutTrustingStaleReadyState(t *testing.T) {
	for _, state := range []string{"failed", "error", "ready", "login_required", "missing", "unreadable"} {
		t.Run(state, func(t *testing.T) {
			f := newFixture(t)
			f.d.Running = func(string) (bool, error) { return false, nil }
			f.d.Status = func(string) (meshlocal.Status, error) {
				switch state {
				case "missing":
					return meshlocal.Status{}, os.ErrNotExist
				case "unreadable":
					return meshlocal.Status{}, errors.New("cannot read private status")
				}
				return meshlocal.Status{State: state, Error: "coordinator connection timed out",
					DNSName: "herdr-mesh-desktop.actual-tail.ts.net",
					AuthURL: "https://login.tailscale.com/a/obsolete"}, nil
			}
			var output bytes.Buffer
			err := waitReady(context.Background(), f.dir, false, &output, f.d)
			if err == nil || !strings.Contains(err.Error(), "stopped before readiness") {
				t.Fatalf("stopped daemon accepted: %v", err)
			}
			wantFailure := state == "failed" || state == "error"
			if strings.Contains(output.String(), "coordinator connection timed out") != wantFailure {
				t.Fatal("final failure omitted or stale non-failure displayed")
			}
			if f.browsers != 0 || f.verified != 0 || strings.Contains(output.String(), "Ready:") ||
				strings.Contains(output.String(), "obsolete") {
				t.Fatal("stopped daemon advertised stale readiness or enrollment")
			}
		})
	}
}
