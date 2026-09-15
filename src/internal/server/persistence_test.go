package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakePersistence struct {
	saved              map[string]*agentflowv1.NodeView
	saveErr, deleteErr error
	beforeSave         func(*agentflowv1.NodeView)
}

func (p *fakePersistence) SaveNode(_ context.Context, view *agentflowv1.NodeView) error {
	if p.beforeSave != nil {
		p.beforeSave(view)
	}
	if p.saveErr != nil {
		return p.saveErr
	}
	if p.saved == nil {
		p.saved = make(map[string]*agentflowv1.NodeView)
	}
	p.saved[view.InstanceId] = proto.Clone(view).(*agentflowv1.NodeView)
	return nil
}

func (p *fakePersistence) DeleteNodes(_ context.Context, ids []string) error {
	if p.deleteErr != nil {
		return p.deleteErr
	}
	for _, id := range ids {
		delete(p.saved, id)
	}
	return nil
}

func TestFleetCommitsBeforePublishingAndFencesPersistentWrites(t *testing.T) {
	p := &fakePersistence{}
	fleet := &fleetStore{storage: p}
	now := time.Now()
	entry, err := fleet.begin("node-1", "stable-1", true, now)
	if err != nil {
		t.Fatal(err)
	}
	p.beforeSave = func(view *agentflowv1.NodeView) {
		if view.Herdr.Sequence == 1 && fleet.nodes["node-1"].view.Herdr.Sequence != 0 {
			t.Fatal("cache updated before persistent write")
		}
	}
	if err := fleet.update(entry, readyState(1), now); err != nil {
		t.Fatal(err)
	}
	p.beforeSave = nil
	replacement, err := fleet.begin("node-1", "stable-1", true, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.update(replacement, readyState(1), now); err != nil {
		t.Fatal(err)
	}
	if err := fleet.end(entry); err != nil {
		t.Fatal(err)
	}
	if !p.saved["node-1"].Connected {
		t.Fatal("superseded disconnect overwrote persistent current stream")
	}
	if err := fleet.update(entry, readyState(2), now); status.Code(err) != codes.Aborted {
		t.Fatal("superseded stream persisted state")
	}
	if p.saved["node-1"].Herdr.Sequence != 1 {
		t.Fatal("superseded update changed persistence")
	}
}

func TestPersistenceFailuresInvalidateReadsAndSignalShutdown(t *testing.T) {
	for _, operation := range []string{"begin", "update", "heartbeat", "end", "prune"} {
		t.Run(operation, func(t *testing.T) {
			p := &fakePersistence{}
			fleet := &fleetStore{storage: p, fatal: make(chan error, 1)}
			now := time.Now()
			entry, err := fleet.begin("node-1", "stable-1", true, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := fleet.update(entry, readyState(1), now); err != nil {
				t.Fatal(err)
			}
			if operation == "prune" {
				if err := fleet.end(entry); err != nil {
					t.Fatal(err)
				}
			}
			before := proto.Clone(entry.view).(*agentflowv1.NodeView)
			p.saveErr, p.deleteErr = errors.New("disk failed"), errors.New("disk failed")
			switch operation {
			case "begin":
				_, err = fleet.begin("node-2", "stable-2", true, now)
			case "update":
				err = fleet.update(entry, readyState(2), now)
			case "heartbeat":
				err = fleet.heartbeat(entry, now.Add(time.Second))
			case "end":
				err = fleet.end(entry)
			case "prune":
				_, err = fleet.list(now.Add(offlineRetention + time.Second))
			}
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("failure hidden: %v", err)
			}
			if !proto.Equal(entry.view, before) {
				t.Fatal("failed transaction changed cache")
			}
			if len(fleet.nodes) != 1 {
				t.Fatal("failed transaction inserted/removed cache record")
			}
			if got, err := fleet.list(now); status.Code(err) != codes.Unavailable || got != nil {
				t.Fatal("storage failure returned apparently healthy inventory")
			}
			select {
			case <-fleet.fatal:
			default:
				t.Fatal("storage failure did not signal coordinator shutdown")
			}
			p.saveErr = nil
			if err := fleet.heartbeat(entry, now); status.Code(err) != codes.Unavailable {
				t.Fatal("failed coordinator silently resumed")
			}
		})
	}
}

func storedNode(now time.Time) *agentflowv1.NodeView {
	return &agentflowv1.NodeView{
		InstanceId: "node-1", TailscaleStableId: "stable-1", Connected: true, Stale: false,
		LastSeen: timestamppb.New(now), Herdr: readyState(5), HerdrReceivedAt: timestamppb.New(now),
	}
}

func TestRecoveryNeverRestoresLiveness(t *testing.T) {
	now := time.Now()
	p := &fakePersistence{}
	fleet := &fleetStore{storage: p}
	view := storedNode(now)
	if err := fleet.restore([]*agentflowv1.NodeView{view}, now); err != nil {
		t.Fatal(err)
	}
	got := listFleet(t, fleet, now).Nodes[0]
	if got.Connected || !got.Stale || !proto.Equal(got.Herdr, view.Herdr) {
		t.Fatal("restored observation claimed liveness or lost projection")
	}
	entry, err := fleet.begin("node-1", "stable-1", true, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := listFleet(t, fleet, now).Nodes[0]; !got.Connected || !got.Stale || got.Herdr.Status != "waiting" {
		t.Fatal("registration did not require a new baseline")
	}
	if err := fleet.update(entry, readyState(1), now); err != nil {
		t.Fatal(err)
	}
	if listFleet(t, fleet, now).Nodes[0].Stale {
		t.Fatal("new baseline did not restore freshness")
	}
}

func TestRestoreRejectsCorruptDomainAndDuplicateIdentity(t *testing.T) {
	now := time.Now()
	for name, mutate := range map[string]func(*agentflowv1.NodeView){
		"missing heartbeat":   func(v *agentflowv1.NodeView) { v.LastSeen = nil },
		"missing state":       func(v *agentflowv1.NodeView) { v.Herdr = nil },
		"missing receipt":     func(v *agentflowv1.NodeView) { v.HerdrReceivedAt = nil },
		"bad status":          func(v *agentflowv1.NodeView) { v.Herdr.Panes[0].AgentStatus = "invalid" },
		"mixed waiting state": func(v *agentflowv1.NodeView) { v.Herdr.Status = "waiting" },
		"unknown field":       func(v *agentflowv1.NodeView) { v.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			v := storedNode(now)
			mutate(v)
			if err := (&fleetStore{}).restore([]*agentflowv1.NodeView{v}, now); err == nil {
				t.Fatal("corrupt state accepted")
			}
		})
	}
	a, b := storedNode(now), storedNode(now)
	b.InstanceId = "node-2"
	if err := (&fleetStore{}).restore([]*agentflowv1.NodeView{a, b}, now); err == nil {
		t.Fatal("duplicate stable identity accepted")
	}
	for _, status := range []string{"disabled", "waiting"} {
		v := storedNode(now)
		v.Herdr = &agentflowv1.HerdrState{Status: status}
		v.HerdrReceivedAt = nil
		if err := (&fleetStore{}).restore([]*agentflowv1.NodeView{v}, now); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLegacyImportAndRealFleetRestart(t *testing.T) {
	for _, legacyText := range []string{
		`[{"stable_id":"stable-1","instance_id":"node-1"}]`,
		"{\"stable_id\":\"stable-1\",\"instance_id\":\"node-1\"}\n",
	} {
		t.Run(legacyText[:1], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			root := t.TempDir()
			options := Options{InstanceID: "server-1", BindingPath: filepath.Join(root, "node-bindings.jsonl"), DatabasePath: filepath.Join(root, "coordinator", "mesh.db")}
			if err := os.WriteFile(options.BindingPath, []byte(legacyText), 0600); err != nil {
				t.Fatal(err)
			}
			store, views, err := openCoordinatorState(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			api := &service{instanceID: "server-1", bindNode: durableBinder(store), identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
				return transport.PeerIdentity{StableID: "stable-1"}, nil
			}}
			api.fleet.storage = store
			if err := api.fleet.restore(views, time.Now()); err != nil {
				t.Fatal(err)
			}
			connection := newTestConnection(t, api)
			stream := startHerdrStream(t, ctx, connection)
			sent := readyState(1)
			if err := stream.Send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_HerdrState{HerdrState: sent}}); err != nil {
				t.Fatal(err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Recv(); err != io.EOF {
				t.Fatalf("durable stream failed: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, recovered, err := openCoordinatorState(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if err := reopened.Bind(ctx, "other-stable", "node-1"); !errors.Is(err, state.ErrIdentityConflict) {
				t.Fatal("binding lost across restart")
			}
			restoredAPI := &service{instanceID: "server-1", bindNode: durableBinder(reopened), identifyPeer: api.identifyPeer}
			restoredAPI.fleet.storage = reopened
			if err := restoredAPI.fleet.restore(recovered, time.Now()); err != nil {
				t.Fatal(err)
			}
			list, err := agentflowv1.NewFleetClient(newTestConnection(t, restoredAPI)).ListNodes(ctx, &emptypb.Empty{})
			if err != nil {
				t.Fatal(err)
			}
			if len(list.Nodes) != 1 || list.Nodes[0].Connected || !list.Nodes[0].Stale || !proto.Equal(list.Nodes[0].Herdr, sent) {
				t.Fatal("durable observation did not round-trip through restarted fleet RPC")
			}
			after, err := os.ReadFile(options.BindingPath)
			if err != nil || !bytes.Equal(after, []byte(legacyText)) {
				t.Fatal("migration modified legacy binding file")
			}
			expired := listFleet(t, &restoredAPI.fleet, time.Now().Add(offlineRetention+time.Second))
			if len(expired.Nodes) != 0 {
				t.Fatal("retention changed after restart")
			}
			stored, err := reopened.LoadFleet(ctx)
			if err != nil || len(stored) != 0 {
				t.Fatal("prune was not durable")
			}
			if err := reopened.Bind(ctx, "other-stable", "node-1"); !errors.Is(err, state.ErrIdentityConflict) {
				t.Fatal("prune removed identity binding")
			}
		})
	}
}

func TestCorruptLegacyBlocksStartupWithoutOverwritingIt(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "node-bindings.jsonl")
	content := []byte(`[{"stable_id":"stable-1","instance_id":`)
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	_, _, err := openCoordinatorState(context.Background(), Options{InstanceID: "server-1", BindingPath: path, DatabasePath: filepath.Join(root, "coordinator", "mesh.db")})
	if err == nil {
		t.Fatal("corrupt migration input accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, content) {
		t.Fatal("corrupt legacy input overwritten")
	}
}

func TestBindingWriteFailureIsNotAuthorizationRejection(t *testing.T) {
	api := &service{
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "stable-1"}, nil
		},
		bindNode: func(string, string) error { return errors.New("disk failed") },
	}
	api.fleet.fatal = make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := agentflowv1.NewNodeControlClient(newTestConnection(t, api)).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(nodeHello("node-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("storage failure treated as authorization failure: %v", err)
	}
	select {
	case <-api.fleet.fatal:
	default:
		t.Fatal("binding failure did not signal shutdown")
	}
}

func TestPersistenceFailureStopsServingCoordinator(t *testing.T) {
	persistence := &fakePersistence{}
	api := &service{identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
		return transport.PeerIdentity{StableID: "stable-1"}, nil
	}}
	api.fleet.storage = persistence
	api.fleet.fatal = make(chan error, 1)
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(grpc.WaitForHandlers(true))
	agentflowv1.RegisterNodeControlServer(grpcServer, api)
	agentflowv1.RegisterFleetServer(grpcServer, api)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveCoordinator(ctx, listener, grpcServer, &api.fleet) }()
	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	stream := startHerdrStream(t, ctx, connection)
	api.fleet.mu.Lock()
	persistence.saveErr = errors.New("simulated disk failure")
	api.fleet.mu.Unlock()
	if err := stream.Send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_HerdrState{HerdrState: readyState(1)}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("coordinator reported a successful stop after storage failure")
		}
	case <-ctx.Done():
		t.Fatal("coordinator did not stop serving after storage failure")
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("coordinator left node stream active")
	}
	queryCtx, queryCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer queryCancel()
	if _, err := agentflowv1.NewFleetClient(connection).ListNodes(queryCtx, &emptypb.Empty{}); err == nil {
		t.Fatal("stopped coordinator served cached fleet data")
	}
}
