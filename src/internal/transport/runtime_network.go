package transport

import (
	"context"
	"net"

	"google.golang.org/grpc"
)

// RuntimeNetwork is borrowed by roles; only its managed owner calls Close.
type RuntimeNetwork interface {
	SelfStatus() SelfStatus
	Listen(string) (net.Listener, error)
	DialGRPC(string) (*grpc.ClientConn, error)
	DialGRPCWithPeerTag(string, string) (*grpc.ClientConn, error)
	IdentifyPeer(context.Context, string) (PeerIdentity, error)
	Close() error
}

// ValidNodeName validates the optional logical-name field when it is present.
func ValidNodeName(name string) bool {
	if len(name) == 0 || len(name) > 40 || name[0] < 'a' || name[0] > 'z' || name[len(name)-1] == '-' {
		return false
	}
	for _, ch := range name {
		if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '-' {
			return false
		}
	}
	return true
}
