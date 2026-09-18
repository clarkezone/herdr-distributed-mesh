package node

import (
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSessionSnapshotExpiryDoesNotBorrowAggregateFreshness(t *testing.T) {
	now := time.Now()
	oldAt := now.Add(-sessionSnapshotStaleAfter)
	snapshot := &pb.HerdrState{Status: "ready", Version: "test", Protocol: 18, Sequence: 4,
		ObservedAt: timestamppb.New(oldAt), Workspaces: []*pb.HerdrEntity{{Id: "w1"}},
		Panes: []*pb.HerdrEntity{{Id: "p1", WorkspaceId: "w1"}}}
	recent := proto.Clone(snapshot).(*pb.HerdrState)
	recent.ObservedAt = timestamppb.New(now.Add(-time.Second))
	views := map[string]*pb.SessionView{
		"stuck": {Name: "stuck", Incarnation: strings.Repeat("a", 64), Status: "ready", Herdr: snapshot},
		"fresh": {Name: "fresh", Incarnation: strings.Repeat("b", 64), Status: "ready", Herdr: recent},
	}
	observed := map[string]time.Time{"stuck": oldAt, "fresh": now.Add(-time.Second)}
	if !expireSessionSnapshots(views, observed, now) {
		t.Fatal("cached snapshot did not expire at the exact threshold")
	}
	expired := views["stuck"]
	if expired.Status != "ready" || expired.Herdr.Status != "unavailable" ||
		expired.Herdr.ErrorCode != "observation_stale" || expired.Herdr.Sequence != snapshot.Sequence ||
		!proto.Equal(expired.Herdr.ObservedAt, snapshot.ObservedAt) ||
		len(expired.Herdr.Workspaces)+len(expired.Herdr.Panes) != 0 {
		t.Fatalf("expiration fabricated readiness, observation time, or stale entities: %v", expired)
	}
	if expired.Stale || expired.HerdrReceivedAt != nil {
		t.Fatal("node assigned coordinator-owned freshness fields")
	}
	if views["fresh"].Herdr != recent || recent.Status != "ready" {
		t.Fatal("one stale session invalidated a fresh sibling")
	}
	if expireSessionSnapshots(views, observed, now) {
		t.Fatal("repeated aggregate projection fabricated another observation")
	}
	views["stuck"].Herdr = proto.Clone(snapshot).(*pb.HerdrState)
	views["stuck"].Herdr.Sequence++
	views["stuck"].Herdr.ObservedAt = timestamppb.New(now)
	observed["stuck"] = now
	if expireSessionSnapshots(views, observed, now) || views["stuck"].Herdr.Status != "ready" {
		t.Fatal("new authoritative observation did not recover session readiness")
	}
}

func TestSessionSnapshotWithoutValidLocalReceiptFailsClosed(t *testing.T) {
	now := time.Now()
	for _, stamp := range []time.Time{{}, now.Add(time.Second)} {
		views := map[string]*pb.SessionView{"unknown": {Name: "unknown", Status: "ready",
			Herdr: &pb.HerdrState{Status: "ready", Sequence: 1, ObservedAt: timestamppb.New(now)}}}
		if !expireSessionSnapshots(views, map[string]time.Time{"unknown": stamp}, now) ||
			views["unknown"].Herdr.Status != "unavailable" {
			t.Fatal("missing or future local receipt was treated as fresh")
		}
	}
}
