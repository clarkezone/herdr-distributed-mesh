package server

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
)

func TestHostedCoordinatorOptionsFailBeforeStartup(t *testing.T) {
	if err := validateDashboardOptions(Options{}); err != nil {
		t.Fatal(err)
	}
	base := Options{ListenAddress: ":50052", DashboardListenAddress: ":8787",
		DashboardOrigin: "http://mesh.example:8787", RequiredClientTag: "tag:client", Output: io.Discard}
	if err := validateDashboardOptions(base); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Options){
		func(o *Options) { o.DashboardListenAddress = "" },
		func(o *Options) { o.DashboardOrigin = "" },
		func(o *Options) { o.RequiredClientTag = "" },
		func(o *Options) { o.Output = nil },
		func(o *Options) { o.DashboardListenAddress = "0.0.0.0:8787" },
		func(o *Options) { o.DashboardListenAddress = ":0" },
		func(o *Options) { o.DashboardListenAddress = ":65536" },
		func(o *Options) { o.DashboardListenAddress = ":50052" },
		func(o *Options) { o.DashboardOrigin = "https://mesh.example" },
		func(o *Options) { o.DashboardOrigin = "http://mesh.example:8787/path" },
	} {
		value := base
		change(&value)
		if err := validateDashboardOptions(value); err == nil {
			t.Fatal("invalid hosted options accepted")
		}
		if err := Run(context.Background(), value); err == nil {
			t.Fatal("invalid hosted options reached runtime startup")
		}
	}
	base.DashboardListenAddress, base.DashboardOrigin = ":443", "https://mesh.example"
	if err := validateDashboardOptions(base); err != nil {
		t.Fatal(err)
	}
}

func TestHostedCoordinatorAuthenticatesFreshActualPeer(t *testing.T) {
	calls := 0
	authorize := dashboardPeerAuthorizer(func(_ context.Context, address string) (transport.PeerIdentity, error) {
		calls++
		if address != "100.64.0.8:1234" {
			t.Fatal("lookup did not use actual connection address")
		}
		tags := []string{"tag:client"}
		if calls == 2 {
			tags = []string{"tag:node"}
		}
		return transport.PeerIdentity{StableID: "peer", Tags: tags}, nil
	}, "tag:client")
	if err := authorize(context.Background(), "100.64.0.8:1234"); err != nil {
		t.Fatal(err)
	}
	if err := authorize(context.Background(), "100.64.0.8:1234"); err == nil {
		t.Fatal("removed role was still trusted")
	}
	for _, identity := range []transport.PeerIdentity{
		{Tags: []string{"tag:client"}}, {StableID: "peer"}, {StableID: "peer", Tags: []string{"tag:node"}},
	} {
		check := dashboardPeerAuthorizer(func(context.Context, string) (transport.PeerIdentity, error) { return identity, nil }, "tag:client")
		if err := check(context.Background(), "actual:1234"); err == nil {
			t.Fatal("untrusted peer accepted")
		}
	}
}

func TestPairedServicesStopTogether(t *testing.T) {
	sentinel := errors.New("web failed")
	stopped := make(chan struct{})
	err := runPairedServices(context.Background(), func(ctx context.Context) error {
		<-ctx.Done()
		close(stopped)
		return nil
	}, func(context.Context) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("service failure lost: %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("other service still running")
	}
	err = runPairedServices(context.Background(), func(context.Context) error { return nil }, func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "unexpectedly") {
		t.Fatalf("unexpected service exit reported success: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runPairedServices(ctx, func(context.Context) error { return nil }, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("normal shutdown failed: %v", err)
	}
}
