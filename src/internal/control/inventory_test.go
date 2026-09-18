package control

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type inventoryClientFixture struct {
	pb.FleetClient
	list *pb.NodeList
}

func (c inventoryClientFixture) ListNodes(context.Context, *emptypb.Empty, ...grpc.CallOption) (*pb.NodeList, error) {
	return c.list, nil
}

func inventoryFixture() *pb.NodeList {
	ready := false
	stamp := timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	state := &pb.HerdrState{Status: "ready", ObservedAt: stamp, Agents: []*pb.HerdrEntity{
		{Id: "pane", WorkspaceId: "workspace", TabId: "tab", ProjectId: "app", Provider: "copilot",
			TerminalId: "terminal", ProviderSessionId: "provider-session", AgentStatus: "idle"},
	}}
	named := proto.Clone(state).(*pb.HerdrState)
	named.Agents[0].InteractiveReady = &ready
	named.Agents[0].TerminalId = "named-terminal"
	return &pb.NodeList{Nodes: []*pb.NodeView{
		{InstanceId: "node-2", Connected: false, Herdr: proto.Clone(state).(*pb.HerdrState), HerdrReceivedAt: stamp,
			SessionsErrorCode: "session_manager_unavailable"},
		{InstanceId: "node-1", Connected: true, Herdr: state, HerdrReceivedAt: stamp, SessionsReady: true,
			Sessions: []*pb.SessionView{{
				Name: "dev", Incarnation: strings.Repeat("a", 64), Status: "ready", Herdr: named,
			}}},
	}}
}

func TestAgentInventoryRetainsScopeAndIndependentFreshness(t *testing.T) {
	list := inventoryFixture()
	before := proto.Clone(list)
	result := projectAgentInventory(list, InventoryFilter{})
	if len(result.Agents) != 3 || len(result.Sources) != 3 {
		t.Fatal("session or source inventory was lost")
	}
	first, named, offline := result.Agents[0], result.Agents[1], result.Agents[2]
	if first.Source.NodeID != "node-1" || first.Source.SessionName != "" || first.Source.Stale || first.InteractiveReady != nil {
		t.Fatal("default scope or unknown readiness changed")
	}
	if named.Target.PaneId != first.Target.PaneId || named.Target.TerminalId != "named-terminal" ||
		named.Target.SessionName != "dev" || named.Target.SessionIncarnation != strings.Repeat("a", 64) ||
		!named.Source.Stale || named.Source.ReceivedAt != nil || named.InteractiveReady == nil || *named.InteractiveReady {
		t.Fatal("named pane scope, absent receive time, or explicit false readiness changed")
	}
	if !offline.Source.Stale || offline.Source.Connected || offline.Source.SessionManagerError != "session_manager_unavailable" {
		t.Fatal("offline or unavailable manager was presented as healthy")
	}
	if !proto.Equal(before, list) {
		t.Fatal("inventory projection mutated the fleet snapshot")
	}
}

func TestAgentInventoryFiltersAreExactAndKeepSourceFailures(t *testing.T) {
	for _, test := range []struct {
		filter  InventoryFilter
		count   int
		sources int
	}{
		{InventoryFilter{NodeID: "node-1"}, 2, 2},
		{InventoryFilter{NodeID: "node-1", ProjectID: "app", WorkspaceID: "workspace", Provider: "copilot", Readiness: "false"}, 1, 2},
		{InventoryFilter{Readiness: "unknown"}, 2, 3},
		{InventoryFilter{Readiness: "true"}, 0, 3},
		{InventoryFilter{ProjectID: "App"}, 0, 3},
		{InventoryFilter{Provider: "other"}, 0, 3},
		{InventoryFilter{WorkspaceID: "other"}, 0, 3},
		{InventoryFilter{NodeID: "unknown"}, 0, 0},
	} {
		result := projectAgentInventory(inventoryFixture(), test.filter)
		if len(result.Agents) != test.count || len(result.Sources) != test.sources {
			t.Fatalf("filter %+v: agents=%d sources=%d", test.filter, len(result.Agents), len(result.Sources))
		}
	}
}

func TestAgentInventoryJSONAndValidation(t *testing.T) {
	var out bytes.Buffer
	err := AgentInventory(context.Background(), Options{FleetClient: inventoryClientFixture{list: inventoryFixture()},
		JSON: true, Output: &out}, InventoryFilter{Readiness: "false"})
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Agents  []map[string]json.RawMessage `json:"agents"`
		Sources []inventorySource            `json:"sources"`
	}
	if json.Unmarshal(out.Bytes(), &decoded) != nil || len(decoded.Agents) != 1 || len(decoded.Sources) != 3 ||
		string(decoded.Agents[0]["interactive_ready"]) != "false" {
		t.Fatal("JSON lost explicit false readiness or independent sources")
	}
	if err := AgentInventory(context.Background(), Options{Output: io.Discard}, InventoryFilter{Readiness: "invalid"}); err == nil {
		t.Fatal("invalid readiness reached transport")
	}
	if err := AgentInventory(context.Background(), Options{Output: io.Discard,
		FleetClient: inventoryClientFixture{}}, InventoryFilter{}); err == nil {
		t.Fatal("missing fleet snapshot was reported as empty success")
	}
}
