package transport

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
)

type peerConn struct {
	net.Conn
	address net.Addr
	closed  bool
}

func (c *peerConn) RemoteAddr() net.Addr { return c.address }
func (c *peerConn) Close() error         { c.closed = true; return nil }

func TestAuthorizeConnectedPeer(t *testing.T) {
	for _, test := range []struct {
		name  string
		tag   string
		tags  []string
		err   error
		allow bool
	}{
		{name: "authorized", tag: "tag:server", tags: []string{"tag:server"}, allow: true},
		{name: "wrong role", tag: "tag:server", tags: []string{"tag:controller"}},
		{name: "WhoIs failure", tag: "tag:server", err: errors.New("WhoIs unavailable")},
		{name: "empty required tag"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for range 2 {
				connection := &peerConn{address: &net.TCPAddr{IP: net.ParseIP("100.100.20.30"), Port: 50052}}
				called := false
				got, err := authorizePeerConnection(context.Background(), connection, test.tag,
					func(_ context.Context, address string) (PeerIdentity, error) {
						called = true
						if address != connection.RemoteAddr().String() {
							t.Fatalf("WhoIs used %q instead of connected remote address", address)
						}
						return PeerIdentity{Tags: test.tags}, test.err
					})
				if test.allow {
					if err != nil || got != connection || connection.closed || !called {
						t.Fatalf("authorized connection rejected: %v", err)
					}
				} else if err == nil || got != nil || !connection.closed || !strings.Contains(err.Error(), "server-tag check") {
					t.Fatalf("unauthorized connection escaped: connection=%v closed=%v err=%v", got, connection.closed, err)
				}
			}
		})
	}
}

func TestDialGRPCWithPeerTagRejectsEmptyTagBeforeDial(t *testing.T) {
	network := &Network{}
	if connection, err := network.DialGRPCWithPeerTag("server:50052", " "); err == nil || connection != nil {
		t.Fatal("empty required tag accepted")
	}
}
