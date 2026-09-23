package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshmcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func mcpInventorySnapshot() *pb.NodeList {
	ready, notReady := true, false
	observed := timestamppb.New(time.Unix(120, 0))
	received := timestamppb.New(time.Unix(123, 0))
	return &pb.NodeList{Nodes: []*pb.NodeView{{
		InstanceId: "node-1", Connected: true, SessionsReady: true, HerdrReceivedAt: received,
		Herdr: &pb.HerdrState{Status: "ready", ObservedAt: observed, Agents: []*pb.HerdrEntity{
			{Id: "legacy-pane", WorkspaceId: "w1"},
		}},
		Sessions: []*pb.SessionView{
			{Name: "worker", Incarnation: strings.Repeat("a", 64), Status: "ready", HerdrReceivedAt: received,
				Herdr: &pb.HerdrState{Status: "ready", ObservedAt: observed, Agents: []*pb.HerdrEntity{
					{Id: "w1:p1", WorkspaceId: "w1", TabId: "w1:t1", ProjectId: "app", Provider: "copilot",
						InteractiveReady: &notReady, TerminalId: "terminal-1", ProviderSessionId: "provider-session-1", AgentStatus: "idle"},
					{Id: "w1:p2", WorkspaceId: "w1", TabId: "w1:t2", ProjectId: "app", Provider: "codex", InteractiveReady: &ready},
					{Id: "w2:p1", WorkspaceId: "w2", TabId: "w2:t1", ProjectId: "other"},
				}}},
			{Name: "offline", Incarnation: strings.Repeat("b", 64), Status: "unavailable", ErrorCode: "session_unavailable", Stale: true},
		},
	}, {InstanceId: "node-2", SessionsErrorCode: "session_manager_unavailable"}}}
}

func TestMCPAgentInventorySharedFiltersAndSourceParity(t *testing.T) {
	list := mcpInventorySnapshot()
	calls := 0
	client := &mcpFleetFixture{nodes: func(context.Context) (*pb.NodeList, error) {
		calls++
		return list, nil
	}}
	var stdout bytes.Buffer
	options := mcpTestOptions(client)
	options.Output = &stdout
	session := mcpApplicationClient(t, options)
	for _, filter := range []control.InventoryFilter{
		{},
		{ProjectID: "app"},
		{WorkspaceID: "w1"},
		{Provider: "copilot"},
		{Readiness: "any"},
		{Readiness: "true"},
		{Readiness: "false"},
		{Readiness: "unknown"},
		{ProjectID: "app", WorkspaceID: "w1", Provider: "copilot", Readiness: "false"},
		{ProjectID: "APP"},
	} {
		filter.NodeID = "node-1"
		var expected bytes.Buffer
		cliOptions := options
		cliOptions.Output, cliOptions.JSON = &expected, true
		if err := control.AgentInventory(context.Background(), cliOptions, filter); err != nil {
			t.Fatal(err)
		}
		cli := mcpObject(t, json.RawMessage(expected.Bytes()))
		for _, name := range []string{"", "worker", "offline"} {
			scope := meshmcp.SessionInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}, HerdrSessionID: name}
			if name == "worker" {
				scope.HerdrSessionIncarnation = strings.Repeat("a", 64)
			} else if name == "offline" {
				scope.HerdrSessionIncarnation = strings.Repeat("b", 64)
			}
			input := meshmcp.ListAgentsInput{
				WorkspaceInput: meshmcp.WorkspaceInput{SessionInput: scope, ProjectID: filter.ProjectID},
				WorkspaceID:    filter.WorkspaceID, Provider: filter.Provider, Readiness: filter.Readiness,
			}
			before := calls
			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.ListAgents), Arguments: input})
			if err != nil || result == nil || result.IsError || calls != before+1 {
				t.Fatalf("inventory mapping failed for %+v/%q: %v %v calls=%d", filter, name, result, err, calls-before)
			}
			got := mcpObject(t, result.StructuredContent)
			if !reflect.DeepEqual(got["sources"], cli["sources"]) || len(got["sources"].([]any)) != 3 {
				t.Fatalf("empty/unavailable scopes or source freshness were dropped: %+v", got)
			}
			want := []any{}
			for _, agent := range cli["agents"].([]any) {
				if agent.(map[string]any)["source"].(map[string]any)["session_name"] == name {
					want = append(want, agent)
				}
			}
			if !reflect.DeepEqual(got["agents"], want) {
				t.Fatalf("inventory differs from shared service for %+v/%q: got %+v want %+v", filter, name, got["agents"], want)
			}
		}
	}
	if stdout.Len() != 0 {
		t.Fatalf("inventory JSON leaked onto protocol stdout: %s", &stdout)
	}
}

func TestMCPAgentInventoryCursorAndPinnedScope(t *testing.T) {
	list := mcpInventorySnapshot()
	client := &mcpFleetFixture{nodes: func(context.Context) (*pb.NodeList, error) { return list, nil }}
	options := mcpTestOptions(client)
	input := meshmcp.ListAgentsInput{
		WorkspaceInput: meshmcp.WorkspaceInput{SessionInput: meshmcp.SessionInput{
			NodeInput:      meshmcp.NodeInput{NodeInstanceID: "node-1"},
			HerdrSessionID: "worker", HerdrSessionIncarnation: strings.Repeat("a", 64),
		}},
		PageInput: meshmcp.PageInput{Limit: 1},
	}
	first, err := invokeMCP(context.Background(), options, meshmcp.ListAgents, input)
	if err != nil {
		t.Fatal(err)
	}
	page := mcpObject(t, first)
	input.Cursor = page["next_cursor"].(string)
	if input.Cursor == "" {
		t.Fatal("missing cursor")
	}
	next, err := invokeMCP(context.Background(), options, meshmcp.ListAgents, input)
	if err != nil || !reflect.DeepEqual(page["sources"], mcpObject(t, next)["sources"]) {
		t.Fatalf("pagination lost source metadata: %v", err)
	}
	changed := input
	changed.Readiness = "any"
	if _, err := invokeMCP(context.Background(), options, meshmcp.ListAgents, changed); err == nil {
		t.Fatal("cursor crossed filter scope")
	}
	list.Nodes[0].Sessions[1].ErrorCode = "session_replaced"
	if _, err := invokeMCP(context.Background(), options, meshmcp.ListAgents, input); err == nil {
		t.Fatal("cursor ignored a changed empty/unavailable source")
	}
	input.Cursor = ""
	for _, name := range []string{"missing", "worker"} {
		input.HerdrSessionID, input.HerdrSessionIncarnation = name, strings.Repeat("c", 64)
		if _, err := invokeMCP(context.Background(), options, meshmcp.ListAgents, input); err == nil {
			t.Fatal("missing/replaced named selection fell back to configured default")
		}
	}
}

func TestMCPAgentInventoryErrorsAndCancellation(t *testing.T) {
	input := meshmcp.ListAgentsInput{WorkspaceInput: mcpTestSelection().WorkspaceInput}
	client := &mcpFleetFixture{nodes: func(context.Context) (*pb.NodeList, error) {
		return nil, errors.New("inventory unavailable")
	}}
	value, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.ListAgents, input)
	encoded, _ := json.Marshal(value)
	if err == nil || !strings.Contains(err.Error(), "inventory unavailable") || string(encoded) != "null" {
		t.Fatalf("inventory error became success: %v %v", value, err)
	}
	client.nodes = func(ctx context.Context) (*pb.NodeList, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = invokeMCP(ctx, mcpTestOptions(client), meshmcp.ListAgents, input)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("inventory lost cancellation: %v", err)
	}
}
