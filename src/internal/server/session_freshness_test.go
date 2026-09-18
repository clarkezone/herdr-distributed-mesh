package server

import (
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSessionFreshnessUsesIndependentCoordinatorReceipts(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	entry := namedEntry(t, h)
	entry.view.Herdr, entry.view.HerdrReceivedAt = &pb.HerdrState{Status: "disabled"}, nil
	now := time.Now()
	firstAt := now.Add(-31 * time.Second)
	inventory := sessionInventory(2)
	inventory.Sessions[0].Herdr.ObservedAt = timestamppb.New(now.Add(-24 * time.Hour))
	second := proto.Clone(inventory.Sessions[0]).(*pb.SessionView)
	second.Name, second.Incarnation = "other", strings.Repeat("b", 64)
	inventory.Sessions = append(inventory.Sessions, second)
	if err := h.api.fleet.heartbeat(entry, firstAt, false, false, false, false, true); err != nil {
		t.Fatal(err)
	}
	if err := h.api.fleet.updateSessions(entry, inventory, firstAt); err != nil {
		t.Fatal(err)
	}
	list, err := h.api.fleet.list(firstAt)
	if err != nil {
		t.Fatal(err)
	}
	view := list.Nodes[0]
	if !view.Stale || view.Sessions[0].Stale || view.Sessions[1].Stale ||
		!view.Sessions[0].HerdrReceivedAt.AsTime().Equal(firstAt) {
		t.Fatalf("default absence or remote clock affected named freshness: %v", view)
	}
	inventory.Sequence++
	inventory.Sessions[1].Herdr.Sequence++
	if err := h.api.fleet.heartbeat(entry, now, false, false, false, false, true); err != nil {
		t.Fatal(err)
	}
	if err := h.api.fleet.updateSessions(entry, inventory, now); err != nil {
		t.Fatal(err)
	}
	list, err = h.api.fleet.list(now)
	if err != nil {
		t.Fatal(err)
	}
	view = list.Nodes[0]
	if !view.Sessions[0].Stale || view.Sessions[1].Stale ||
		!view.Sessions[0].HerdrReceivedAt.AsTime().Equal(firstAt) ||
		!view.Sessions[1].HerdrReceivedAt.AsTime().Equal(now) {
		t.Fatalf("another session renewed unchanged topology: %v", view)
	}
	if selectedSessionReady(entry, "build", "", now) || !selectedSessionReady(entry, "other", "", now) {
		t.Fatal("selected readiness disagrees with independent freshness")
	}
	sessions, err := h.api.ListSessions(agentPeer("client"), &pb.ListSessionsRequest{NodeInstanceId: "node-1"})
	if err != nil || len(sessions.GetSessions()) != 2 || !sessions.Sessions[0].Stale || sessions.Sessions[1].Stale {
		t.Fatalf("ListSessions projection disagrees: %v %v", sessions, err)
	}
	data, err := protojson.Marshal(sessions)
	if err != nil || !strings.Contains(string(data), "herdrReceivedAt") {
		t.Fatalf("session receipt time missing from JSON: %s %v", data, err)
	}
	if err := h.api.fleet.end(entry); err != nil {
		t.Fatal(err)
	}
	list, err = h.api.fleet.list(time.Now())
	if err != nil || !list.Nodes[0].Sessions[0].Stale || !list.Nodes[0].Sessions[1].Stale {
		t.Fatalf("disconnected sessions were not stale: %v %v", list, err)
	}
}

func TestSessionProjectionFieldsCannotBeSuppliedByNode(t *testing.T) {
	for _, edit := range []func(*pb.SessionView){
		func(v *pb.SessionView) { v.Stale = true },
		func(v *pb.SessionView) { v.HerdrReceivedAt = timestamppb.Now() },
	} {
		value := sessionInventory(1)
		edit(value.Sessions[0])
		if err := validateSessionInventory(value); err == nil {
			t.Fatal("node forged coordinator projection fields")
		}
	}
}

func TestSessionReplacementResetsReceiptAndStoredLegacyIsStale(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	entry := namedEntry(t, h)
	now := time.Now()
	inventory := sessionInventory(2)
	inventory.Sessions[0].Incarnation = strings.Repeat("b", 64)
	if err := h.api.fleet.updateSessions(entry, inventory, now); err != nil {
		t.Fatal(err)
	}
	if !entry.view.Sessions[0].HerdrReceivedAt.AsTime().Equal(now) {
		t.Fatal("replacement inherited previous receipt time")
	}
	stored := proto.Clone(entry.view).(*pb.NodeView)
	stored.Sessions[0].HerdrReceivedAt = nil
	if err := validateStoredSessions(stored); err != nil {
		t.Fatalf("older inventory without per-session timestamp rejected: %v", err)
	}
	projectSessionFreshness(stored, now)
	if !stored.Sessions[0].Stale {
		t.Fatal("legacy receipt-less observation asserted freshness")
	}
	stored.Sessions[0].HerdrReceivedAt = &timestamppb.Timestamp{Seconds: 1 << 62}
	if err := validateStoredSessions(stored); err == nil {
		t.Fatal("malformed persisted per-session receipt accepted")
	}
	stored = proto.Clone(entry.view).(*pb.NodeView)
	restored := &fleetStore{}
	if err := restored.restore([]*pb.NodeView{stored}, now); err != nil {
		t.Fatal(err)
	}
	if !restored.nodes[stored.InstanceId].view.Sessions[0].Stale {
		t.Fatal("restored inventory asserted live session freshness")
	}
}
