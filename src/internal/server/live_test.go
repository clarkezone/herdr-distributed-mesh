package server

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestLiveHerdrFleetProjection(t *testing.T) {
	socket := os.Getenv("HERDR_MESH_TEST_SOCKET")
	if socket == "" {
		t.Skip("set HERDR_MESH_TEST_SOCKET for read-only live integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, _, err := openCoordinatorState(ctx, Options{
		InstanceID: "server-1", DatabasePath: filepath.Join(t.TempDir(), "coordinator", "mesh.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	api := &service{
		requiredNodeTag: "tag:node", requiredClientTag: "tag:client",
		bindNode: durableBinder(store),
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "stable-1", Tags: []string{"tag:node", "tag:client"}}, nil
		},
	}
	api.fleet.storage = store
	connection := newTestConnection(t, api)
	stream := startHerdrStream(t, ctx, connection)
	collected := errors.New("sample collected")
	var sent *agentflowv1.HerdrState
	err = herdr.Observe(ctx, herdr.Config{SocketPath: socket}, func(state *agentflowv1.HerdrState) error {
		if state.Status != "ready" {
			return errors.New("live Herdr unavailable: " + state.ErrorCode)
		}
		sent = proto.Clone(state).(*agentflowv1.HerdrState)
		if err := stream.Send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_HerdrState{HerdrState: state}}); err != nil {
			return err
		}
		return collected
	})
	if !errors.Is(err, collected) {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("real projection rejected by server: %v", err)
	}
	list, err := agentflowv1.NewFleetClient(connection).ListNodes(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Nodes) != 1 || !proto.Equal(list.Nodes[0].Herdr, sent) || !list.Nodes[0].Stale {
		t.Fatal("real projected snapshot did not round-trip through fleet RPC")
	}
	persisted, err := store.LoadFleet(ctx)
	if err != nil || len(persisted) != 1 || !proto.Equal(persisted[0].Herdr, sent) || persisted[0].Connected {
		t.Fatal("real projection and disconnect were not persisted")
	}
}
