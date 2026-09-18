package node

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc/peer"
)

const workspacePeerVerificationTimeout = 3 * time.Second

func workspacePeerTagVerifier(identify func(context.Context, string) (transport.PeerIdentity, error), tag string) func(context.Context, string) error {
	return func(ctx context.Context, address string) error {
		if identify == nil || address == "" || strings.TrimSpace(tag) == "" {
			return errors.New("workspace coordinator peer verification is unavailable")
		}
		identity, err := identify(ctx, address)
		if err != nil || !slices.Contains(identity.Tags, tag) {
			return errors.New("workspace coordinator peer is not authorized")
		}
		return nil
	}
}

func streamPeerVerifier(streamContext context.Context, verify func(context.Context, string) error) func(context.Context) error {
	actual, ok := peer.FromContext(streamContext)
	var address string
	if ok && actual != nil && actual.Addr != nil {
		address = actual.Addr.String()
	}
	return func(ctx context.Context) error {
		if address == "" || verify == nil {
			return errors.New("workspace coordinator stream peer verification is unavailable")
		}
		return verify(ctx, address)
	}
}
