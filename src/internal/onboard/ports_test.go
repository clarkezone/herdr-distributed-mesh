package onboard

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

func TestCustomCoordinatorPortPreservedWithoutInventingDNS(t *testing.T) {
	f := newFixture(t)
	o := joinOptions()
	o.Server += ":54321"
	if err := Run(context.Background(), o, io.Discard, f.d); err != nil {
		t.Fatal(err)
	}
	if f.cfg.Server != o.Server {
		t.Fatalf("custom port lost: %s", f.cfg.Server)
	}
	f.d.Status = func(string) (meshlocal.Status, error) {
		return meshlocal.Status{State: "ready", DNSName: "herdr-mesh-desktop.actual-tail.ts.net.", Server: "herdr-mesh-desktop.actual-tail.ts.net:54321"}, nil
	}
	var out bytes.Buffer
	if err := waitReady(context.Background(), f.dir, true, &out, f.d); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "--server herdr-mesh-desktop.actual-tail.ts.net:54321 --name") {
		t.Fatal(out.String())
	}
	f.d.Status = func(string) (meshlocal.Status, error) {
		return meshlocal.Status{State: "ready", DNSName: "herdr-mesh-desktop.actual-tail.ts.net", Server: "invented.other.ts.net:50052"}, nil
	}
	out.Reset()
	if err := waitReady(context.Background(), f.dir, true, &out, f.d); err == nil {
		t.Fatal("mismatching endpoint accepted")
	}
	if strings.Contains(out.String(), "Ready:") {
		t.Fatal("false readiness")
	}
}
