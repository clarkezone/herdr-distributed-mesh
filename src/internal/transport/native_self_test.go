package transport

import (
	"context"
	"net"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"tailscale.com/ipn/store/mem"
	"tailscale.com/net/netns"
	"tailscale.com/tailcfg"
	"tailscale.com/tsnet"
	"tailscale.com/tstest/integration/testcontrol"
)

func nativeSelfIdentity(t *testing.T) (*Network, *testcontrol.Server) {
	t.Helper()
	t.Setenv("TS_DEBUG_LOGTAIL", "false")
	netns.SetEnabled(false)
	t.Cleanup(func() { netns.SetEnabled(true) })
	control := &testcontrol.Server{
		DERPMap:        &tailcfg.DERPMap{Regions: map[int]*tailcfg.DERPRegion{}},
		DNSConfig:      &tailcfg.DNSConfig{Proxied: true},
		MagicDNSDomain: "isolated.ts.net",
		TagOwners: map[string][]string{
			"tag:server": nil, "tag:node": nil, "tag:client": nil,
		},
		Logf: t.Logf,
	}
	control.HTTPTestServer = httptest.NewUnstartedServer(control)
	control.HTTPTestServer.Start()
	t.Cleanup(control.HTTPTestServer.Close)
	embedded := &tsnet.Server{
		Dir: t.TempDir(), Store: new(mem.Store), Ephemeral: true,
		ControlURL: control.HTTPTestServer.URL, Hostname: "native-self",
		AuthKey: "isolated-test-only", AdvertiseTags: []string{"tag:server", "tag:node", "tag:client"},
		UserLogf: t.Logf, Logf: t.Logf,
	}
	t.Cleanup(func() {
		if err := embedded.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := embedded.Start()
	if err != nil {
		t.Fatal(err)
	}
	client, err := embedded.LocalClient()
	if err != nil {
		t.Fatal(err)
	}
	// The isolated control fixture supplies a real self netmap. No DERP is
	// needed for WhoIs; native self Dial is separately covered by pinned tsnet's
	// TestSelfDial, which includes its own isolated relay fixture.
	for {
		current, err := client.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		self := makeSelfStatus(current, embedded.Dir)
		if len(self.IPs) != 0 && self.StableID != "" && self.Validate([]string{"tag:server", "tag:node", "tag:client"}, time.Now()) == nil {
			return &Network{server: embedded, self: self, magicDNSSuffix: self.DNSSuffix}, control
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestNativeSelfWhoIsUsesAssignedRolesAndFreshRevocation(t *testing.T) {
	network, control := nativeSelfIdentity(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	address := net.JoinHostPort(network.self.IPs[0], "50052")
	identity, err := network.IdentifyPeer(ctx, address)
	if err != nil || identity.StableID != network.self.StableID {
		t.Fatalf("native self WhoIs identity failed: %+v %v", identity, err)
	}
	for _, tag := range []string{"tag:server", "tag:node", "tag:client"} {
		if !slices.Contains(identity.Tags, tag) {
			t.Fatalf("native self WhoIs lost assigned role %s: %+v", tag, identity)
		}
	}
	connection := &peerConn{address: &net.TCPAddr{IP: net.ParseIP(network.self.IPs[0]), Port: 50052}}
	if _, err := authorizePeerConnection(ctx, connection, "tag:server", network.IdentifyPeer); err != nil {
		t.Fatalf("ordinary server-role check rejected native self WhoIs: %v", err)
	}
	if _, err := authorizePeerConnection(ctx, connection, "tag:unassigned", network.IdentifyPeer); err == nil {
		t.Fatal("native self identity bypassed ordinary required-role check")
	}
	node := control.AllNodes()[0]
	node.Tags = []string{"tag:node", "tag:client"}
	control.UpdateNode(node)
	for {
		identity, err = network.IdentifyPeer(ctx, address)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(identity.Tags, "tag:server") {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("native self WhoIs did not observe assigned-role revocation")
		case <-time.After(10 * time.Millisecond):
		}
	}
	connection = &peerConn{address: &net.TCPAddr{IP: net.ParseIP(network.self.IPs[0]), Port: 50052}}
	if _, err := authorizePeerConnection(ctx, connection, "tag:server", network.IdentifyPeer); err == nil {
		t.Fatal("native self server-role revocation bypassed ordinary authorization")
	}
}
