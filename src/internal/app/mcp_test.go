package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshmcp"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mcpFleetFixture struct {
	pb.FleetClient
	nodes    func(context.Context) (*pb.NodeList, error)
	sessions func(context.Context, *pb.ListSessionsRequest) (*pb.SessionList, error)
	projects func(*pb.ListProjectsRequest) (*pb.ProjectList, error)
	project  func(*pb.GetProjectRequest) (*pb.ProjectRecord, error)
	register func(context.Context, *pb.RegisterProjectRequest) (*pb.ProjectRecord, error)
	submit   func(context.Context, *pb.SubmitCommandRequest) (*pb.CommandRecord, error)
	command  func(*pb.GetCommandRequest) (*pb.CommandRecord, error)
	query    func(context.Context, *pb.AgentQueryRequest) (*pb.AgentQueryResult, error)
}

func (f *mcpFleetFixture) ListNodes(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*pb.NodeList, error) {
	if f.nodes == nil {
		return nil, errors.New("unexpected ListNodes")
	}
	return f.nodes(ctx)
}

func (f *mcpFleetFixture) ListSessions(ctx context.Context, request *pb.ListSessionsRequest, _ ...grpc.CallOption) (*pb.SessionList, error) {
	if f.sessions == nil {
		return nil, errors.New("unexpected ListSessions")
	}
	return f.sessions(ctx, request)
}

func (f *mcpFleetFixture) ListProjects(_ context.Context, request *pb.ListProjectsRequest, _ ...grpc.CallOption) (*pb.ProjectList, error) {
	if f.projects == nil {
		return nil, errors.New("unexpected ListProjects")
	}
	return f.projects(request)
}

func (f *mcpFleetFixture) GetProject(_ context.Context, request *pb.GetProjectRequest, _ ...grpc.CallOption) (*pb.ProjectRecord, error) {
	if f.project == nil {
		return nil, errors.New("unexpected GetProject")
	}
	return f.project(request)
}

func (f *mcpFleetFixture) RegisterProject(ctx context.Context, request *pb.RegisterProjectRequest, _ ...grpc.CallOption) (*pb.ProjectRecord, error) {
	if f.register == nil {
		return nil, errors.New("unexpected RegisterProject")
	}
	return f.register(ctx, request)
}

func (f *mcpFleetFixture) SubmitCommand(ctx context.Context, request *pb.SubmitCommandRequest, _ ...grpc.CallOption) (*pb.CommandRecord, error) {
	if f.submit == nil {
		return nil, errors.New("unexpected SubmitCommand")
	}
	return f.submit(ctx, request)
}

func (f *mcpFleetFixture) GetCommand(_ context.Context, request *pb.GetCommandRequest, _ ...grpc.CallOption) (*pb.CommandRecord, error) {
	if f.command == nil {
		return nil, errors.New("unexpected GetCommand")
	}
	return f.command(request)
}

func (f *mcpFleetFixture) QueryAgent(ctx context.Context, request *pb.AgentQueryRequest, _ ...grpc.CallOption) (*pb.AgentQueryResult, error) {
	if f.query == nil {
		return nil, errors.New("unexpected QueryAgent or target rediscovery")
	}
	return f.query(ctx, request)
}

func mcpTestOptions(client pb.FleetClient) control.Options {
	return control.Options{FleetClient: client, RequiredServerTag: "tag:herdr-mesh-server"}
}

func mcpTestSelection() meshmcp.AgentInput {
	return meshmcp.AgentInput{
		WorkspaceInput: meshmcp.WorkspaceInput{SessionInput: meshmcp.SessionInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}}},
		Target:         meshmcp.AgentTarget{PaneID: "w1:p1", TerminalID: "terminal-original", AgentSessionID: "incarnation-original"},
	}
}

func mcpTestAgent() *pb.AgentView {
	return &pb.AgentView{
		Target:      &pb.AgentTarget{PaneId: "w1:p1", TerminalId: "terminal-original", AgentSessionId: "incarnation-original"},
		WorkspaceId: "w1", TabId: "w1:t1", Provider: "copilot", Status: "idle",
	}
}

func mcpTestReceipt(request *pb.SubmitCommandRequest, commandStatus pb.CommandStatus) *pb.CommandRecord {
	return &pb.CommandRecord{
		Command: &pb.Command{
			CommandId: strings.Repeat("a", 32), TargetId: request.NodeInstanceId, IdempotencyKey: request.IdempotencyKey,
			CommandType: request.CommandType, WorkspaceEnsure: request.WorkspaceEnsure, WorktreeCreate: request.WorktreeCreate, AgentControl: request.AgentControl,
			SessionEnsure: request.SessionEnsure,
		},
		Status: commandStatus, Detail: "operation_receipt",
	}
}

func mcpObject(t *testing.T, value any) map[string]any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatal(err)
	}
	return object
}

func TestMCPNetworkFlagsAndEarlyValidation(t *testing.T) {
	isolatedManagedConfig(t)
	var out, diagnostics bytes.Buffer
	config, err := parseMCPFlags([]string{"-server", "coordinator:50052"}, IO{Out: &out, Err: &diagnostics})
	if err != nil {
		t.Fatal(err)
	}
	if config.control.Transport.Hostname != "herdr-mesh-mcp" || config.control.Transport.AuthKeyEnv != "TS_AUTHKEY_CLIENT" ||
		!strings.HasSuffix(config.control.Transport.StateDir, filepath.Join("mcp", "tsnet")) ||
		config.control.RequiredServerTag != "tag:herdr-mesh-server" || !config.control.JSON || config.timeout != 0 {
		t.Fatalf("incorrect distinct MCP network defaults: %+v", config)
	}
	override, err := parseMCPFlags([]string{"-server", "coordinator:50052", "-auth-key-env", "CUSTOM_AUTHKEY"}, IO{Out: &out, Err: &diagnostics})
	if err != nil || override.control.Transport.AuthKeyEnv != "CUSTOM_AUTHKEY" {
		t.Fatalf("explicit enrollment environment override failed: %+v %v", override, err)
	}
	if _, err := parseMCPFlags([]string{"-server", "coordinator:50052", "-max-active-calls", "1"}, IO{Out: &out, Err: &diagnostics}); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("incompatible concurrency bound did not explain the critical reservation: %v", err)
	}
	for _, args := range [][]string{
		{}, {"-server", "coordinator", "extra"}, {"-server", "coordinator", "-required-server-tag", ""},
		{"-server", "coordinator", "-timeout", "-1s"}, {"-server", "coordinator", "-call-timeout", "0"},
		{"-server", "coordinator", "-max-active-calls", "0"}, {"-server", "coordinator", "-max-output-bytes", "100"},
		{"-unknown"}, {"-h"},
	} {
		if err := runMCP(context.Background(), args, IO{Out: &out, Err: &diagnostics}); err == nil {
			t.Fatalf("invalid/help arguments accepted: %v", args)
		}
	}
	if out.Len() != 0 || diagnostics.Len() == 0 {
		t.Fatalf("flags wrote to protocol stdout or lost diagnostics: stdout=%s stderr=%s", &out, &diagnostics)
	}
}

func TestMCPNodeProjectionsPaginationAndUnknownAssociations(t *testing.T) {
	list := &pb.NodeList{Nodes: []*pb.NodeView{
		{InstanceId: "node-1", Connected: true, Stale: true, Herdr: &pb.HerdrState{Status: "ready",
			Workspaces: []*pb.HerdrEntity{{Id: "w1"}, {Id: "w2"}},
			Agents:     []*pb.HerdrEntity{{Id: "w1:p1", WorkspaceId: "w1", AgentStatus: "idle"}, {Id: "w2:p1", WorkspaceId: "w2"}}}},
		{InstanceId: "node-2", Connected: false},
	}}
	client := &mcpFleetFixture{nodes: func(context.Context) (*pb.NodeList, error) { return list, nil }}
	var stdout bytes.Buffer
	options := mcpTestOptions(client)
	options.Output = &stdout
	first, err := invokeMCP(context.Background(), options, meshmcp.ListNodes, meshmcp.PageInput{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	page := mcpObject(t, first)
	cursor := page["next_cursor"].(string)
	if len(page["nodes"].([]any)) != 1 || cursor == "" {
		t.Fatalf("incorrect first page: %+v", page)
	}
	next, err := invokeMCP(context.Background(), options, meshmcp.ListNodes, meshmcp.PageInput{Limit: 1, Cursor: cursor})
	if err != nil {
		t.Fatal(err)
	}
	nextPage := mcpObject(t, next)
	if nextPage["nodes"].([]any)[0].(map[string]any)["instance_id"] != "node-2" || nextPage["next_cursor"] != "" {
		t.Fatalf("incorrect next page: %+v", nextPage)
	}
	list.Nodes[0].Connected = false
	if _, err := invokeMCP(context.Background(), options, meshmcp.ListNodes, meshmcp.PageInput{Limit: 1, Cursor: cursor}); err == nil {
		t.Fatal("changed snapshot cursor accepted")
	}
	node, err := invokeMCP(context.Background(), options, meshmcp.GetNode, meshmcp.NodeInput{NodeInstanceID: "node-1"})
	if err != nil || mcpObject(t, node)["stale"] != true {
		t.Fatalf("node projection changed stale state: %v %v", node, err)
	}
	selection := mcpTestSelection().WorkspaceInput
	agents, err := invokeMCP(context.Background(), options, meshmcp.ListAgents, meshmcp.ListAgentsInput{WorkspaceInput: selection, WorkspaceID: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	projected := mcpObject(t, agents)
	entities := projected["agents"].([]any)
	source := projected["sources"].([]any)[0].(map[string]any)
	if len(entities) != 1 || source["stale"] != true || source["status"] != "ready" {
		t.Fatalf("wrong filtered observation: %+v", projected)
	}
	for _, missing := range []string{"provider", "project_id", "agent_session_id", "terminal_id", "provider_session_id", "herdr_session_id", "interactive_ready"} {
		if _, exists := entities[0].(map[string]any)[missing]; exists {
			t.Fatalf("invented absent %s association", missing)
		}
	}
	workspaces, err := invokeMCP(context.Background(), options, meshmcp.ListWorkspaces, meshmcp.ListWorkspacesInput{WorkspaceInput: selection})
	if err != nil || len(mcpObject(t, workspaces)["workspaces"].([]any)) != 2 {
		t.Fatalf("workspace projection: %v %v", workspaces, err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("public service JSON escaped capture: %s", &stdout)
	}
}

func TestMCPEntityMetadataPreservesReportedValuesAndReadinessPresence(t *testing.T) {
	notReady, ready := false, true
	snapshot := &pb.HerdrState{Status: "ready",
		Agents: []*pb.HerdrEntity{
			{Id: "w1:p1", WorkspaceId: "w1"},
			{Id: "w1:p2", WorkspaceId: "w1", Provider: "copilot", InteractiveReady: &notReady,
				TerminalId: "terminal-2", ProviderSessionId: "provider-session-2", ProjectId: "project-1"},
			{Id: "w1:p3", WorkspaceId: "w1", Provider: "codex", InteractiveReady: &ready},
		},
		Workspaces: []*pb.HerdrEntity{{Id: "w1", ProjectId: "project-1"}, {Id: "w2"}},
	}
	node := &pb.NodeView{InstanceId: "node-1", Connected: true, Herdr: snapshot,
		Sessions: []*pb.SessionView{{Name: "worker", Incarnation: strings.Repeat("a", 64), Status: "ready", Herdr: snapshot}},
	}
	client := &mcpFleetFixture{nodes: func(context.Context) (*pb.NodeList, error) { return &pb.NodeList{Nodes: []*pb.NodeView{node}}, nil }}
	session := mcpApplicationClient(t, mcpTestOptions(client))
	for _, named := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "named"}[named], func(t *testing.T) {
			scope := mcpTestSelection().WorkspaceInput
			if named {
				scope.HerdrSessionID, scope.HerdrSessionIncarnation = "worker", strings.Repeat("a", 64)
			}
			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.ListAgents),
				Arguments: meshmcp.ListAgentsInput{WorkspaceInput: scope, WorkspaceID: "w1"}})
			if err != nil || result == nil || result.IsError {
				t.Fatalf("agent metadata projection failed: %v %v", result, err)
			}
			agents := mcpObject(t, result.StructuredContent)["agents"].([]any)
			if len(agents) != 3 {
				t.Fatalf("metadata normalization dropped entities: %+v", agents)
			}
			unknown := agents[0].(map[string]any)
			for _, name := range []string{"provider", "terminal_id", "provider_session_id", "project_id", "interactive_ready"} {
				if _, exists := unknown[name]; exists {
					t.Fatalf("unreported %s was invented: %+v", name, unknown)
				}
			}
			reported := agents[1].(map[string]any)
			for name, want := range map[string]any{
				"provider":   "copilot",
				"project_id": "project-1", "interactive_ready": false,
			} {
				got, exists := reported[name]
				if !exists || got != want {
					t.Fatalf("reported %s was dropped or changed: %+v", name, reported)
				}
				target := reported["target"].(map[string]any)
				if target["terminal_id"] != "terminal-2" || target["agent_session_id"] != "provider-session-2" {
					t.Fatalf("reported target identity changed: %+v", target)
				}
			}
			partial := agents[2].(map[string]any)
			if partial["provider"] != "codex" || partial["interactive_ready"] != true {
				t.Fatalf("reported partial metadata changed: %+v", partial)
			}
			for _, name := range []string{"terminal_id", "provider_session_id", "project_id"} {
				if _, exists := partial[name]; exists {
					t.Fatalf("partial projection inferred %s: %+v", name, partial)
				}
			}
			result, err = session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.ListWorkspaces),
				Arguments: meshmcp.ListWorkspacesInput{WorkspaceInput: scope}})
			if err != nil || result == nil || result.IsError {
				t.Fatalf("workspace metadata projection failed: %v %v", result, err)
			}
			workspaces := mcpObject(t, result.StructuredContent)["workspaces"].([]any)
			if workspaces[0].(map[string]any)["project_id"] != "project-1" {
				t.Fatalf("reported workspace project was dropped: %+v", workspaces)
			}
			for _, workspace := range workspaces {
				for _, name := range []string{"provider", "terminal_id", "provider_session_id", "interactive_ready"} {
					if _, exists := workspace.(map[string]any)[name]; exists {
						t.Fatalf("workspace projection inferred %s: %+v", name, workspace)
					}
				}
			}
			if _, exists := workspaces[1].(map[string]any)["project_id"]; exists {
				t.Fatalf("unreported workspace project was invented: %+v", workspaces[1])
			}
		})
	}
}

func TestMCPCentralProjectReadServices(t *testing.T) {
	project := &pb.ProjectRecord{Desired: &pb.ProjectConfig{
		NodeInstanceId: "node-1", ProjectId: "project-1", Generation: 9007199254740993, CheckoutPath: `C:\checkout`,
	}, Readiness: "applied"}
	var calls int
	client := &mcpFleetFixture{
		projects: func(request *pb.ListProjectsRequest) (*pb.ProjectList, error) {
			calls++
			if request.NodeInstanceId != "node-1" {
				return nil, errors.New("wrong node")
			}
			return &pb.ProjectList{Projects: []*pb.ProjectRecord{project}}, nil
		},
		project: func(request *pb.GetProjectRequest) (*pb.ProjectRecord, error) {
			calls++
			if request.NodeInstanceId != "node-1" || request.ProjectId != "project-1" {
				return nil, errors.New("wrong project selector")
			}
			return project, nil
		},
	}
	node := meshmcp.NodeInput{NodeInstanceID: "node-1"}
	value, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.GetProject, meshmcp.ProjectInput{NodeInput: node, ProjectID: "project-1"})
	if err != nil || mcpObject(t, value)["desired"].(map[string]any)["generation"] != "9007199254740993" {
		t.Fatalf("protobuf JSON integer representation was lost: %v %v", value, err)
	}
	value, err = invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.ListProjects, meshmcp.ListProjectsInput{NodeInput: node})
	if err != nil || len(mcpObject(t, value)["projects"].([]any)) != 1 || calls != 2 {
		t.Fatalf("project list service: calls=%d result=%v err=%v", calls, value, err)
	}
}

func TestMCPSessionListUsesPublicServiceAndPreservesFinalShape(t *testing.T) {
	list := &pb.SessionList{Sessions: []*pb.SessionView{
		{Name: "worker", Incarnation: strings.Repeat("a", 64), Status: "ready", Herdr: &pb.HerdrState{Status: "ready"},
			HerdrReceivedAt: timestamppb.New(time.Unix(123, 0)), Stale: true},
		{Name: "offline", Status: "unavailable", ErrorCode: "session_unavailable"},
	}}
	var calls int
	client := &mcpFleetFixture{sessions: func(_ context.Context, request *pb.ListSessionsRequest) (*pb.SessionList, error) {
		calls++
		if request.NodeInstanceId != "node-1" {
			return nil, errors.New("session list node changed")
		}
		return list, nil
	}}
	options := mcpTestOptions(client)
	var ordinary, stdout bytes.Buffer
	options.Output, options.JSON = &ordinary, true
	if err := control.Sessions(context.Background(), options, "node-1"); err != nil {
		t.Fatal(err)
	}
	expected := mcpObject(t, json.RawMessage(ordinary.Bytes()))["sessions"].([]any)
	options.Output, options.JSON = &stdout, false
	session := mcpApplicationClient(t, options)
	input := meshmcp.ListSessionsInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}, PageInput: meshmcp.PageInput{Limit: 1}}
	for i := range 2 {
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.ListSessions), Arguments: input})
		if err != nil || result == nil || result.IsError {
			t.Fatalf("session list failed: %v %v", result, err)
		}
		page := mcpObject(t, result.StructuredContent)
		values := page["sessions"].([]any)
		if len(values) != 1 || !reflect.DeepEqual(values[0], expected[i]) || page["error_code"] != "" {
			t.Fatalf("final SessionView shape changed: %+v", page)
		}
		view := values[0].(map[string]any)
		if i == 0 && (view["stale"] != true || view["herdr_received_at"] != "1970-01-01T00:02:03Z") {
			t.Fatalf("session projection lost its coordinator observation metadata: %+v", view)
		}
		input.Cursor = page["next_cursor"].(string)
		if (input.Cursor != "") != (i == 0) {
			t.Fatalf("wrong session pagination continuation: %+v", page)
		}
	}
	if calls != 3 || stdout.Len() != 0 {
		t.Fatalf("ordinary service was bypassed or wrote protocol stdout: calls=%d stdout=%s", calls, &stdout)
	}
}

func TestMCPSessionListErrorsRemainStructured(t *testing.T) {
	client := &mcpFleetFixture{sessions: func(context.Context, *pb.ListSessionsRequest) (*pb.SessionList, error) {
		return &pb.SessionList{ErrorCode: "session_manager_unavailable"}, nil
	}}
	session := mcpApplicationClient(t, mcpTestOptions(client))
	input := meshmcp.ListSessionsInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}}
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.ListSessions), Arguments: input})
	if err != nil || result == nil || !result.IsError || result.StructuredContent == nil {
		t.Fatalf("session inventory error lost its JSON: %v %v", result, err)
	}
	object := mcpObject(t, result.StructuredContent)
	if object["error_code"] != "session_manager_unavailable" || len(object["sessions"].([]any)) != 0 {
		t.Fatalf("session error was changed into successful empty inventory: %+v", object)
	}
	for _, test := range []struct {
		name string
		list *pb.SessionList
		err  error
	}{
		{"invalid ready view", &pb.SessionList{Sessions: []*pb.SessionView{{Name: "worker", Status: "ready"}}}, nil},
		{"RPC denied", nil, status.Error(codes.PermissionDenied, "denied")},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := &mcpFleetFixture{sessions: func(context.Context, *pb.ListSessionsRequest) (*pb.SessionList, error) { return test.list, test.err }}
			value, err := invokeMCP(context.Background(), mcpTestOptions(fixture), meshmcp.ListSessions, input)
			encoded, marshalErr := json.Marshal(value)
			if err == nil || marshalErr != nil || string(encoded) != "null" {
				t.Fatalf("invalid/RPC session result accepted: %s %v", encoded, err)
			}
		})
	}
}

func TestMCPSessionEnsureUsesPublicServiceAndOriginalIdentity(t *testing.T) {
	input := meshmcp.EnsureHerdrSessionInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}, HerdrSessionID: "worker", IdempotencyKey: "original"}
	var calls int
	var original *pb.SubmitCommandRequest
	client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		calls++
		if request.CommandType != protocol.SessionEnsureCommandType || request.NodeInstanceId != input.NodeInstanceID ||
			request.IdempotencyKey != input.IdempotencyKey || request.SessionEnsure.GetName() != input.HerdrSessionID ||
			request.Ttl.AsDuration() != protocol.DefaultCommandTTL {
			return nil, errors.New("original session command changed")
		}
		if original != nil && !proto.Equal(original, request) {
			return nil, errors.New("MCP diverged from ordinary service or changed retry identity")
		}
		original = proto.Clone(request).(*pb.SubmitCommandRequest)
		record := mcpTestReceipt(request, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
		record.Detail = "session_ready"
		record.SessionEnsure = &pb.SessionView{Name: "worker", Incarnation: strings.Repeat("a", 64), Status: "ready"}
		return record, nil
	}}
	var ordinary bytes.Buffer
	options := mcpTestOptions(client)
	options.JSON, options.Output = true, &ordinary
	if err := control.EnsureSession(context.Background(), options, input.NodeInstanceID, input.IdempotencyKey, input.HerdrSessionID, protocol.DefaultCommandTTL); err != nil {
		t.Fatal(err)
	}
	expected := mcpObject(t, json.RawMessage(ordinary.Bytes()))
	session := mcpApplicationClient(t, mcpTestOptions(client))
	for range 2 {
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.EnsureHerdrSession), Arguments: input})
		if err != nil || result == nil || result.IsError || !reflect.DeepEqual(mcpObject(t, result.StructuredContent), expected) {
			t.Fatalf("session receipt lost incarnation or ordinary-service parity: %v %v", result, err)
		}
	}
	if calls != 3 {
		t.Fatalf("session ensure did not delegate every exact retry to the service: calls=%d", calls)
	}
}

func TestMCPSessionEnsureUncertaintyPreservesReceipt(t *testing.T) {
	var calls int
	client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		calls++
		record := mcpTestReceipt(request, pb.CommandStatus_COMMAND_STATUS_INDETERMINATE)
		record.Detail = "startup_uncertain"
		return record, nil
	}}
	session := mcpApplicationClient(t, mcpTestOptions(client))
	input := meshmcp.EnsureHerdrSessionInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}, HerdrSessionID: "worker", IdempotencyKey: "original"}
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.EnsureHerdrSession), Arguments: input})
	if err != nil || result == nil || !result.IsError || result.StructuredContent == nil {
		t.Fatalf("uncertain session startup lost its receipt: %v %v", result, err)
	}
	receipt := mcpObject(t, result.StructuredContent)
	if receipt["status"] != "COMMAND_STATUS_INDETERMINATE" || receipt["detail"] != "startup_uncertain" ||
		receipt["command"].(map[string]any)["idempotency_key"] != "original" || calls != 1 {
		t.Fatalf("uncertain session startup was hidden or resubmitted: %+v calls=%d", receipt, calls)
	}
}

func TestMCPSessionEnsureValidationAndReceiptPins(t *testing.T) {
	input := meshmcp.EnsureHerdrSessionInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}, HerdrSessionID: "worker", IdempotencyKey: "original"}
	var calls int
	client := &mcpFleetFixture{submit: func(context.Context, *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		calls++
		return nil, errors.New("invalid input reached submission")
	}}
	for _, field := range []string{"key", "name", "reserved name"} {
		invalid := input
		switch field {
		case "key":
			invalid.IdempotencyKey = ""
		case "name":
			invalid.HerdrSessionID = ""
		case "reserved name":
			invalid.HerdrSessionID = "con"
		}
		if _, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.EnsureHerdrSession, invalid); err == nil {
			t.Fatalf("%s generated an implicit selector/key", field)
		}
	}
	if calls != 0 {
		t.Fatalf("invalid ensure performed a submission: calls=%d", calls)
	}
	for _, test := range []struct {
		name string
		edit func(*pb.CommandRecord)
	}{
		{"missing result", func(record *pb.CommandRecord) { record.SessionEnsure = nil }},
		{"missing incarnation", func(record *pb.CommandRecord) { record.SessionEnsure.Incarnation = "" }},
		{"different result session", func(record *pb.CommandRecord) { record.SessionEnsure.Name = "other" }},
		{"different command session", func(record *pb.CommandRecord) { record.Command.SessionEnsure.Name = "other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
				record := mcpTestReceipt(proto.Clone(request).(*pb.SubmitCommandRequest), pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
				record.Detail = "session_ready"
				record.SessionEnsure = &pb.SessionView{Name: "worker", Incarnation: strings.Repeat("a", 64), Status: "ready"}
				test.edit(record)
				return record, nil
			}}
			value, err := invokeMCP(context.Background(), mcpTestOptions(fixture), meshmcp.EnsureHerdrSession, input)
			if err == nil || value != nil {
				t.Fatalf("invalid session success accepted: %v %v", value, err)
			}
		})
	}
}

func TestMCPSessionCancellationReachesPublicServices(t *testing.T) {
	for _, operation := range []meshmcp.Operation{meshmcp.ListSessions, meshmcp.EnsureHerdrSession} {
		t.Run(string(operation), func(t *testing.T) {
			started := make(chan struct{})
			client := &mcpFleetFixture{
				sessions: func(ctx context.Context, _ *pb.ListSessionsRequest) (*pb.SessionList, error) {
					close(started)
					<-ctx.Done()
					return nil, ctx.Err()
				},
				submit: func(ctx context.Context, _ *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
					close(started)
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var input any = meshmcp.ListSessionsInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}}
			if operation == meshmcp.EnsureHerdrSession {
				input = meshmcp.EnsureHerdrSessionInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}, HerdrSessionID: "worker", IdempotencyKey: "original"}
			}
			done := make(chan error, 1)
			go func() {
				_, err := invokeMCP(ctx, mcpTestOptions(client), operation, input)
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("session service did not receive the request")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("session service cancellation was lost: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("session service ignored cancellation")
			}
		})
	}
}

func TestMCPNamedAgentWorkspaceAndWorktreeCallsStayPinned(t *testing.T) {
	selection := mcpTestSelection()
	selection.HerdrSessionID, selection.HerdrSessionIncarnation = "worker", strings.Repeat("b", 64)
	queryCalls := map[pb.AgentQueryKind]int{}
	requests := map[string]*pb.SubmitCommandRequest{}
	client := &mcpFleetFixture{
		query: func(_ context.Context, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
			queryCalls[request.Kind]++
			want := mcpTestAgent()
			want.Target.SessionName, want.Target.SessionIncarnation = selection.HerdrSessionID, selection.HerdrSessionIncarnation
			if request.NodeInstanceId != selection.NodeInstanceID || !proto.Equal(request.Target, want.Target) {
				return nil, errors.New("agent query lost original named session or terminal selectors")
			}
			result := &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: want}
			if request.Kind == pb.AgentQueryKind_AGENT_QUERY_KIND_READ {
				result.Text = "named output"
			}
			return result, nil
		},
		submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
			record := mcpTestReceipt(request, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
			name, incarnation := protocol.CommandSession(record.Command)
			if name != selection.HerdrSessionID || incarnation != selection.HerdrSessionIncarnation || request.NodeInstanceId != "node-1" {
				return nil, errors.New("command lost original named session selectors")
			}
			if previous := requests[request.IdempotencyKey]; previous != nil && !proto.Equal(previous, request) {
				return nil, errors.New("exact retry changed its original session or input")
			}
			requests[request.IdempotencyKey] = proto.Clone(request).(*pb.SubmitCommandRequest)
			return record, nil
		},
	}
	session := mcpApplicationClient(t, mcpTestOptions(client))
	for op, input := range map[meshmcp.Operation]any{
		meshmcp.GetAgent:  selection,
		meshmcp.ReadAgent: meshmcp.ReadAgentInput{AgentInput: selection, Lines: 2, MaxBytes: 100},
		meshmcp.WaitAgent: meshmcp.WaitAgentInput{AgentInput: selection, Until: []string{"idle"}, TimeoutMS: 500},
	} {
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(op), Arguments: input})
		if err != nil || result == nil || result.IsError {
			t.Fatalf("%s named query failed: %v %v", op, result, err)
		}
		target := mcpObject(t, result.StructuredContent)["agent"].(map[string]any)["target"].(map[string]any)
		if target["session_name"] != "worker" || target["session_incarnation"] != selection.HerdrSessionIncarnation ||
			target["agent_session_id"] != selection.Target.AgentSessionID {
			t.Fatalf("named and agent incarnations were conflated: %+v", target)
		}
	}
	binding := meshmcp.EnsureWorkspaceInput{WorkspaceInput: selection.WorkspaceInput, BindingRevision: "original-revision", IdempotencyKey: "workspace-key"}
	binding.ProjectID = "project-1"
	worktree := meshmcp.CreateWorktreeInput{EnsureWorkspaceInput: binding, Name: "task", Branch: "task", BaseCommit: strings.Repeat("a", 40)}
	worktree.IdempotencyKey = "worktree-key"
	mutations := map[meshmcp.Operation]any{
		meshmcp.EnsureWorkspace: binding,
		meshmcp.CreateWorktree:  worktree,
		meshmcp.PromptAgent: meshmcp.PromptAgentInput{MutateAgentInput: meshmcp.MutateAgentInput{
			AgentInput: selection, IdempotencyKey: "prompt-key"}, Text: "original prompt"},
		meshmcp.SendAgentInputOp: meshmcp.SendAgentInput{MutateAgentInput: meshmcp.MutateAgentInput{
			AgentInput: selection, IdempotencyKey: "input-key"}, Keys: []string{"enter"}},
		meshmcp.InterruptAgent: meshmcp.MutateAgentInput{AgentInput: selection, IdempotencyKey: "interrupt-key"},
	}
	for op, input := range mutations {
		for range 2 {
			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(op), Arguments: input})
			if err != nil || result == nil || result.IsError {
				t.Fatalf("%s named mutation failed: %v %v", op, result, err)
			}
			command := mcpObject(t, result.StructuredContent)["command"].(map[string]any)
			if command["idempotency_key"] != mcpObject(t, input)["idempotency_key"] {
				t.Fatalf("%s changed the original execution key", op)
			}
		}
	}
	if len(requests) != len(mutations) || queryCalls[pb.AgentQueryKind_AGENT_QUERY_KIND_GET] != 1 ||
		queryCalls[pb.AgentQueryKind_AGENT_QUERY_KIND_READ] != 1 || queryCalls[pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT] != 1 {
		t.Fatalf("named requests performed unexpected discovery or bypassed public service: queries=%v requests=%d", queryCalls, len(requests))
	}
}

func TestMCPNamedObservationSelectsExactSessionAndMetadata(t *testing.T) {
	namedState := &pb.HerdrState{Status: "ready",
		Workspaces: []*pb.HerdrEntity{{Id: "w1"}, {Id: "w2"}},
		Agents:     []*pb.HerdrEntity{{Id: "w1:p1", WorkspaceId: "w1"}, {Id: "w2:p1", WorkspaceId: "w2"}},
	}
	named := &pb.SessionView{Name: "worker", Incarnation: strings.Repeat("a", 64), Status: "ready",
		Herdr: namedState, HerdrReceivedAt: timestamppb.New(time.Unix(123, 0))}
	node := &pb.NodeView{InstanceId: "node-1", Connected: true, Stale: true,
		Herdr:    &pb.HerdrState{Status: "ready", Workspaces: []*pb.HerdrEntity{{Id: "wrong-default"}}},
		Sessions: []*pb.SessionView{named},
	}
	client := &mcpFleetFixture{nodes: func(context.Context) (*pb.NodeList, error) { return &pb.NodeList{Nodes: []*pb.NodeView{node}}, nil }}
	session := mcpApplicationClient(t, mcpTestOptions(client))
	scope := meshmcp.WorkspaceInput{SessionInput: meshmcp.SessionInput{
		NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}, HerdrSessionID: "worker", HerdrSessionIncarnation: named.Incarnation,
	}}
	input := meshmcp.ListWorkspacesInput{WorkspaceInput: scope, PageInput: meshmcp.PageInput{Limit: 1}}
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.ListWorkspaces), Arguments: input})
	if err != nil || result == nil || result.IsError {
		t.Fatalf("named snapshot failed: %v %v", result, err)
	}
	page := mcpObject(t, result.StructuredContent)
	if page["workspaces"].([]any)[0].(map[string]any)["id"] != "w1" || page["stale"] != false ||
		page["herdr_received_at"] != "1970-01-01T00:02:03Z" || page["herdr_session_incarnation"] != named.Incarnation {
		t.Fatalf("named observation fell back to default metadata or entities: %+v", page)
	}
	cursor := page["next_cursor"].(string)
	agents, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.ListAgents),
		Arguments: meshmcp.ListAgentsInput{WorkspaceInput: scope, WorkspaceID: "w1"}})
	if err != nil || agents == nil || agents.IsError {
		t.Fatalf("named agent snapshot failed: %v %v", agents, err)
	}
	projected := mcpObject(t, agents.StructuredContent)["agents"].([]any)
	if len(projected) != 1 || projected[0].(map[string]any)["target"].(map[string]any)["pane_id"] != "w1:p1" {
		t.Fatalf("named workspace filtering changed: %+v", projected)
	}
	for _, selectors := range []meshmcp.SessionInput{
		{NodeInput: scope.NodeInput, HerdrSessionID: "missing", HerdrSessionIncarnation: named.Incarnation},
		{NodeInput: scope.NodeInput, HerdrSessionID: "worker", HerdrSessionIncarnation: strings.Repeat("b", 64)},
	} {
		bad := input
		bad.SessionInput = selectors
		value, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.ListWorkspaces, bad)
		encoded, _ := json.Marshal(value)
		if err == nil || string(encoded) != "null" {
			t.Fatalf("unmatched session selector fell back to default: %s %v", encoded, err)
		}
	}
	named.Incarnation = strings.Repeat("b", 64)
	input.HerdrSessionIncarnation, input.Cursor = named.Incarnation, cursor
	if _, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.ListWorkspaces, input); err == nil || !strings.Contains(err.Error(), "cursor") {
		t.Fatalf("cursor crossed a session incarnation despite identical entities: %v", err)
	}
	named.Herdr = nil
	input.Cursor = ""
	if _, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.ListWorkspaces, input); err == nil || !strings.Contains(err.Error(), "no Herdr snapshot") {
		t.Fatalf("missing named snapshot fell back to default: %v", err)
	}
}

func TestMCPNamedMismatchedAgentAndCommandReceiptsFail(t *testing.T) {
	selection := mcpTestSelection()
	selection.HerdrSessionID, selection.HerdrSessionIncarnation = "worker", strings.Repeat("a", 64)
	client := &mcpFleetFixture{
		query: func(context.Context, *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
			agent := mcpTestAgent()
			agent.Target.SessionName, agent.Target.SessionIncarnation = "worker", strings.Repeat("b", 64)
			return &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: agent}, nil
		},
		submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
			record := mcpTestReceipt(proto.Clone(request).(*pb.SubmitCommandRequest), pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
			record.Command.AgentControl.Target.SessionIncarnation = strings.Repeat("b", 64)
			return record, nil
		},
	}
	for op, input := range map[meshmcp.Operation]any{
		meshmcp.GetAgent:       selection,
		meshmcp.InterruptAgent: meshmcp.MutateAgentInput{AgentInput: selection, IdempotencyKey: "original"},
	} {
		value, err := invokeMCP(context.Background(), mcpTestOptions(client), op, input)
		if err == nil || value != nil {
			t.Fatalf("%s accepted a different session incarnation: %v %v", op, value, err)
		}
	}
	client.submit = func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		record := mcpTestReceipt(request, pb.CommandStatus_COMMAND_STATUS_INDETERMINATE)
		record.Detail = "node_restarted"
		return record, nil
	}
	session := mcpApplicationClient(t, mcpTestOptions(client))
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.InterruptAgent),
		Arguments: meshmcp.MutateAgentInput{AgentInput: selection, IdempotencyKey: "original"}})
	if err != nil || result == nil || !result.IsError || result.StructuredContent == nil {
		t.Fatalf("valid named error receipt was discarded: %v %v", result, err)
	}
	receipt := mcpObject(t, result.StructuredContent)
	target := receipt["command"].(map[string]any)["agent_control"].(map[string]any)["target"].(map[string]any)
	if receipt["status"] != "COMMAND_STATUS_INDETERMINATE" || target["session_incarnation"] != selection.HerdrSessionIncarnation {
		t.Fatalf("named uncertainty lost its original scope: %+v", receipt)
	}
}

func TestMCPRegisterProjectDesiredStateParityAndStrictKeyRejection(t *testing.T) {
	input := meshmcp.RegisterProjectInput{
		ProjectInput: meshmcp.ProjectInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}, ProjectID: "project-1"},
		CheckoutPath: `C:\managed\checkout`, WorktreeRoot: `C:\managed\worktrees`,
	}
	want := &pb.RegisterProjectRequest{
		NodeInstanceId: input.NodeInstanceID, ProjectId: input.ProjectID,
		CheckoutPath: input.CheckoutPath, WorktreeRoot: input.WorktreeRoot,
	}
	var calls int
	client := &mcpFleetFixture{register: func(_ context.Context, request *pb.RegisterProjectRequest) (*pb.ProjectRecord, error) {
		calls++
		if !proto.Equal(request, want) {
			return nil, errors.New("desired registration request changed")
		}
		return &pb.ProjectRecord{Desired: &pb.ProjectConfig{
			NodeInstanceId: request.NodeInstanceId, ProjectId: request.ProjectId,
			CheckoutPath: request.CheckoutPath, WorktreeRoot: request.WorktreeRoot, Generation: 7,
		}, Readiness: "pending"}, nil
	}}
	var cliJSON, stdout bytes.Buffer
	options := mcpTestOptions(client)
	options.Output, options.JSON = &cliJSON, true
	if err := control.RegisterProject(context.Background(), options, want); err != nil {
		t.Fatal(err)
	}
	expected := mcpObject(t, json.RawMessage(cliJSON.Bytes()))
	options.Output, options.JSON = &stdout, false
	session := mcpApplicationClient(t, options)
	for i := 0; i < 2; i++ {
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.RegisterProject), Arguments: input})
		if err != nil || result == nil || result.IsError {
			t.Fatalf("desired upsert failed: %v %v", result, err)
		}
		if !reflect.DeepEqual(mcpObject(t, result.StructuredContent), expected) {
			t.Fatalf("registration diverged from ordinary CLI JSON: %+v", result.StructuredContent)
		}
	}
	args := mcpObject(t, input)
	args["idempotency_key"] = "not-a-configuration-field"
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.RegisterProject), Arguments: args})
	if err == nil && (result == nil || !result.IsError) {
		t.Fatalf("unsupported execution key was accepted: %v", result)
	}
	if calls != 3 || stdout.Len() != 0 {
		t.Fatalf("key reached service, repeat was hidden, or JSON escaped capture: calls=%d stdout=%s", calls, &stdout)
	}
}

func TestMCPRegisterProjectConfigurationErrorsPropagate(t *testing.T) {
	input := meshmcp.RegisterProjectInput{
		ProjectInput: meshmcp.ProjectInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}, ProjectID: "project-1"},
		CheckoutPath: `C:\managed\checkout`, WorktreeRoot: `C:\managed\worktrees`,
	}
	var calls int
	client := &mcpFleetFixture{register: func(context.Context, *pb.RegisterProjectRequest) (*pb.ProjectRecord, error) {
		calls++
		return nil, status.Error(codes.InvalidArgument, "invalid desired checkout mapping")
	}}
	invalid := input
	invalid.CheckoutPath = ""
	value, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.RegisterProject, invalid)
	if err == nil || value != nil || calls != 0 {
		t.Fatalf("ordinary registration validation was bypassed: value=%v err=%v calls=%d", value, err, calls)
	}
	value, err = invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.RegisterProject, input)
	if value != nil || status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "register project") {
		t.Fatalf("configuration RPC error changed: value=%v err=%v", value, err)
	}
	session := mcpApplicationClient(t, mcpTestOptions(client))
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.RegisterProject), Arguments: input})
	if err != nil || result == nil || !result.IsError || result.StructuredContent != nil ||
		!strings.Contains(result.Content[0].(*mcp.TextContent).Text, "invalid desired checkout mapping") {
		t.Fatalf("MCP lost configuration error or fabricated success: %v %v", result, err)
	}
	if calls != 2 {
		t.Fatalf("configuration error incorrectly retried: calls=%d", calls)
	}
}

func TestMCPMutationsUsePublicServicesAndOriginalIdempotency(t *testing.T) {
	selection := mcpTestSelection()
	binding := meshmcp.EnsureWorkspaceInput{WorkspaceInput: selection.WorkspaceInput, BindingRevision: "revision-original", IdempotencyKey: "key-original"}
	binding.ProjectID = "project-1"
	mutation := meshmcp.MutateAgentInput{AgentInput: selection, IdempotencyKey: "key-original"}
	tests := []struct {
		op    meshmcp.Operation
		input any
		kind  string
	}{
		{meshmcp.EnsureWorkspace, binding, protocol.WorkspaceEnsureCommandType},
		{meshmcp.CreateWorktree, meshmcp.CreateWorktreeInput{EnsureWorkspaceInput: binding, Name: "feature", Branch: "feature-test", BaseCommit: strings.Repeat("a", 40)}, protocol.WorktreeCreateCommandType},
		{meshmcp.PromptAgent, meshmcp.PromptAgentInput{MutateAgentInput: mutation, Text: "hello\nworld"}, protocol.AgentControlCommandType},
		{meshmcp.SendAgentInputOp, meshmcp.SendAgentInput{MutateAgentInput: mutation, Keys: []string{"down", "enter"}}, protocol.AgentControlCommandType},
		{meshmcp.InterruptAgent, mutation, protocol.AgentControlCommandType},
	}
	for _, test := range tests {
		t.Run(string(test.op), func(t *testing.T) {
			var calls int
			client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
				calls++
				if request.CommandType != test.kind || request.NodeInstanceId != "node-1" || request.IdempotencyKey != "key-original" ||
					request.Ttl.AsDuration() != protocol.DefaultCommandTTL {
					return nil, errors.New("ordinary CLI normalization or original request identity changed")
				}
				if test.kind == protocol.AgentControlCommandType && !proto.Equal(request.AgentControl.Target, mcpTestAgent().Target) {
					return nil, errors.New("original terminal/incarnation changed")
				}
				if test.op == meshmcp.PromptAgent && request.AgentControl.Text != "hello\nworld" {
					return nil, errors.New("prompt changed")
				}
				if test.op == meshmcp.SendAgentInputOp && strings.Join(request.AgentControl.Keys, ",") != "down,enter" {
					return nil, errors.New("input keys changed")
				}
				if test.op == meshmcp.EnsureWorkspace && request.WorkspaceEnsure.BindingRevision != "revision-original" {
					return nil, errors.New("binding revision changed")
				}
				if test.op == meshmcp.CreateWorktree && request.WorktreeCreate.BaseCommit != strings.Repeat("a", 40) {
					return nil, errors.New("worktree base changed")
				}
				return mcpTestReceipt(request, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED), nil
			}}
			value, err := invokeMCP(context.Background(), mcpTestOptions(client), test.op, test.input)
			if err != nil || calls != 1 || mcpObject(t, value)["status"] != "COMMAND_STATUS_SUCCEEDED" {
				t.Fatalf("service call-through failed: calls=%d value=%v err=%v", calls, value, err)
			}
		})
	}
}

func TestMCPPublicServiceRetriesKeepExactTargetAndKey(t *testing.T) {
	var calls int
	var first *pb.SubmitCommandRequest
	client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		calls++
		if first == nil {
			first = proto.Clone(request).(*pb.SubmitCommandRequest)
			return nil, status.Error(codes.Unavailable, "retryable")
		}
		if !proto.Equal(first, request) {
			return nil, errors.New("retry request changed")
		}
		return mcpTestReceipt(request, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED), nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	input := meshmcp.MutateAgentInput{AgentInput: mcpTestSelection(), IdempotencyKey: "original"}
	value, err := invokeMCP(ctx, mcpTestOptions(client), meshmcp.InterruptAgent, input)
	if err != nil || calls != 2 || mcpObject(t, value)["command"].(map[string]any)["idempotency_key"] != "original" {
		t.Fatalf("safe retry path: calls=%d value=%v err=%v", calls, value, err)
	}
}

func TestMCPPollsOrdinaryCommandReceiptAndRejectsWrongNode(t *testing.T) {
	var polls int
	record := mcpTestReceipt(&pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "original"}, pb.CommandStatus_COMMAND_STATUS_ACCEPTED)
	client := &mcpFleetFixture{
		submit: func(context.Context, *pb.SubmitCommandRequest) (*pb.CommandRecord, error) { return record, nil },
		command: func(request *pb.GetCommandRequest) (*pb.CommandRecord, error) {
			polls++
			if request.CommandId != record.Command.CommandId {
				return nil, errors.New("command poll identity changed")
			}
			finished := proto.Clone(record).(*pb.CommandRecord)
			finished.Status = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE
			return finished, nil
		},
	}
	value, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.InterruptAgent,
		meshmcp.MutateAgentInput{AgentInput: mcpTestSelection(), IdempotencyKey: "original"})
	if err == nil || polls != 1 || mcpObject(t, value)["status"] != "COMMAND_STATUS_INDETERMINATE" {
		t.Fatalf("ordinary receipt polling lost status/data: polls=%d value=%v err=%v", polls, value, err)
	}
	value, err = invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.GetCommand,
		meshmcp.GetCommandInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "other-node"}, CommandID: record.Command.CommandId})
	if err == nil || value != nil || !strings.Contains(err.Error(), "original node") {
		t.Fatalf("wrong node receipt disclosed or accepted: value=%v err=%v", value, err)
	}
}

func TestMCPPublicInputValidationCannotBeBypassed(t *testing.T) {
	var calls int
	client := &mcpFleetFixture{submit: func(context.Context, *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		calls++
		return nil, errors.New("invalid request reached submit")
	}}
	mutation := meshmcp.MutateAgentInput{AgentInput: mcpTestSelection(), IdempotencyKey: "original"}
	for _, text := range []string{"", " \t", strings.Repeat("x", protocol.MaxAgentPromptBytes+1), "\x1b[2J"} {
		if _, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.PromptAgent,
			meshmcp.PromptAgentInput{MutateAgentInput: mutation, Text: text}); err == nil {
			t.Fatal("ordinary prompt validation was bypassed")
		}
	}
	mutation.IdempotencyKey = ""
	if _, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.InterruptAgent, mutation); err == nil {
		t.Fatal("MCP generated a replacement idempotency key")
	}
	mutation.IdempotencyKey, mutation.Target.TerminalID = "original", ""
	if _, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.InterruptAgent, mutation); err == nil {
		t.Fatal("MCP rediscovered an unpinned mutation target")
	}
	if calls != 0 {
		t.Fatalf("%d invalid requests reached SubmitCommand", calls)
	}
}

func TestMCPAgentQueriesKeepValidationAndJSON(t *testing.T) {
	selection := mcpTestSelection()
	client := &mcpFleetFixture{query: func(_ context.Context, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		if !proto.Equal(request.Target, mcpTestAgent().Target) || request.NodeInstanceId != "node-1" {
			return nil, errors.New("query selector changed")
		}
		result := &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: mcpTestAgent()}
		switch request.Kind {
		case pb.AgentQueryKind_AGENT_QUERY_KIND_READ:
			if request.Lines != 3 {
				return nil, errors.New("read lines changed")
			}
			result.Text = "untrusted output\n"
		case pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT:
			if request.TimeoutMs != 500 || strings.Join(request.Until, ",") != "idle,done" {
				return nil, errors.New("wait normalization changed")
			}
		}
		return result, nil
	}}
	for op, input := range map[meshmcp.Operation]any{
		meshmcp.GetAgent:  selection,
		meshmcp.ReadAgent: meshmcp.ReadAgentInput{AgentInput: selection, Lines: 3, MaxBytes: 100},
		meshmcp.WaitAgent: meshmcp.WaitAgentInput{AgentInput: selection, Until: []string{"idle", "done"}, TimeoutMS: 500},
	} {
		value, err := invokeMCP(context.Background(), mcpTestOptions(client), op, input)
		if err != nil || mcpObject(t, value)["truncated"] != false {
			t.Fatalf("%s lost explicit protobuf fields: %v %v", op, value, err)
		}
	}
	read := meshmcp.ReadAgentInput{AgentInput: selection, Lines: 3, MaxBytes: 1}
	if _, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.ReadAgent, read); err == nil {
		t.Fatal("oversized read silently truncated or accepted")
	}
	client.query = func(context.Context, *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		return &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: mcpTestAgent(), Text: "\x1b[2J"}, nil
	}
	if _, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.ReadAgent, read); err == nil || !strings.Contains(err.Error(), "invalid server agent response") {
		t.Fatalf("ordinary query-result validation was bypassed: %v", err)
	}
}

func mcpApplicationClient(t *testing.T, options control.Options) *mcp.ClientSession {
	t.Helper()
	return mcpApplicationClientWithLimits(t, options, meshmcp.Options{})
}

func mcpApplicationClientWithLimits(t *testing.T, options control.Options, limits meshmcp.Options) *mcp.ClientSession {
	t.Helper()
	server, err := meshmcp.New(func(ctx context.Context, operation meshmcp.Operation, input any) (any, error) {
		return invokeMCP(ctx, options, operation, input)
	}, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	st, ct := mcp.NewInMemoryTransports()
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	client := mcp.NewClient(&mcp.Implementation{Name: "app-mcp-test", Version: "1"}, nil)
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func TestMCPFailedAndIndeterminateReceiptsRemainStructuredErrors(t *testing.T) {
	for _, commandStatus := range []pb.CommandStatus{pb.CommandStatus_COMMAND_STATUS_FAILED, pb.CommandStatus_COMMAND_STATUS_INDETERMINATE} {
		t.Run(commandStatus.String(), func(t *testing.T) {
			client := &mcpFleetFixture{
				submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
					return mcpTestReceipt(request, commandStatus), nil
				},
				command: func(request *pb.GetCommandRequest) (*pb.CommandRecord, error) {
					return &pb.CommandRecord{Command: &pb.Command{CommandId: request.CommandId, TargetId: "node-1", IdempotencyKey: "original"}, Status: commandStatus}, nil
				},
			}
			session := mcpApplicationClient(t, mcpTestOptions(client))
			for op, input := range map[meshmcp.Operation]any{
				meshmcp.InterruptAgent: meshmcp.MutateAgentInput{AgentInput: mcpTestSelection(), IdempotencyKey: "original"},
				meshmcp.GetCommand:     meshmcp.GetCommandInput{NodeInput: meshmcp.NodeInput{NodeInstanceID: "node-1"}, CommandID: strings.Repeat("a", 32)},
			} {
				result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(op), Arguments: input})
				if err != nil || result == nil || !result.IsError || result.StructuredContent == nil {
					t.Fatalf("lost error receipt: %v %v", result, err)
				}
				if mcpObject(t, result.StructuredContent)["status"] != commandStatus.String() || result.Meta["meshmcp/untrusted"] != true {
					t.Fatalf("receipt status or trust label changed: %+v", result)
				}
			}
		})
	}
}

func TestMCPApplicationSchemaAndUnsupportedSelectors(t *testing.T) {
	client := &mcpFleetFixture{}
	session := mcpApplicationClient(t, mcpTestOptions(client))
	for _, args := range []map[string]any{
		{"node_instance_id": "node-1", "command": "raw"},
		{"node_instance_id": "node-1", "project_id": "invented"},
		{"node_instance_id": "node-1", "herdr_session_id": "invented"},
	} {
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: string(meshmcp.ListWorkspaces), Arguments: args})
		if err == nil && (result == nil || !result.IsError) {
			t.Fatalf("unbacked/raw selector accepted: %v", result)
		}
	}
	for _, op := range []meshmcp.Operation{meshmcp.StartAgent, meshmcp.StopAgent} {
		if value, err := invokeMCP(context.Background(), mcpTestOptions(client), op, nil); err == nil || value != nil || !strings.Contains(err.Error(), "invalid input type") {
			t.Fatalf("untyped lifecycle input accepted: %s %v %v", op, value, err)
		}
	}
	selection := mcpTestSelection()
	selection.ProjectID = "unverifiable"
	if _, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.GetAgent, selection); err == nil || !strings.Contains(err.Error(), "association") {
		t.Fatalf("unsupported project selector was dropped: %v", err)
	}
	selection.ProjectID, selection.HerdrSessionID = "", "unverifiable"
	if _, err := invokeMCP(context.Background(), mcpTestOptions(client), meshmcp.GetAgent, selection); err == nil || !strings.Contains(err.Error(), "session selectors") {
		t.Fatalf("unsupported session selector was dropped: %v", err)
	}
	if _, err := invokeMCP(context.Background(), control.Options{}, meshmcp.ListNodes, meshmcp.PageInput{}); err == nil {
		t.Fatal("callback opened its own transport instead of requiring the owned client")
	}
}

func TestMCPMissingNodeKeepsOriginalError(t *testing.T) {
	client := &mcpFleetFixture{nodes: func(context.Context) (*pb.NodeList, error) { return &pb.NodeList{}, nil }}
	session := mcpApplicationClient(t, mcpTestOptions(client))
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: string(meshmcp.GetNode), Arguments: meshmcp.NodeInput{NodeInstanceID: "missing"},
	})
	if err != nil || result == nil || !result.IsError || result.StructuredContent != nil {
		t.Fatalf("invalid missing-node error: %v %v", result, err)
	}
	if !strings.Contains(result.Content[0].(*mcp.TextContent).Text, "selected node was not found") {
		t.Fatalf("typed-nil JSON replaced the original error: %+v", result.Content)
	}
}

func TestMCPCaptureBoundsAndReceipts(t *testing.T) {
	cause := errors.New("failed operation")
	data, err := captureMCP(control.Options{}, func(options control.Options) error {
		if !options.JSON || options.Diagnose {
			t.Fatal("ordinary service was not forced to bounded JSON")
		}
		_, writeErr := io.WriteString(options.Output, `{"status":"failed"}`)
		return errors.Join(cause, writeErr)
	})
	if !errors.Is(err, cause) || !json.Valid(data) {
		t.Fatalf("valid failure receipt discarded: %s %v", data, err)
	}
	for _, text := range []string{"", "null", "{}", "{}\n{}", strings.Repeat("x", mcpCaptureLimit+1)} {
		data, err := captureMCP(control.Options{}, func(options control.Options) error {
			_, err := io.WriteString(options.Output, text)
			return err
		})
		if text == "{}" {
			if err != nil || string(data) != "{}" {
				t.Fatalf("valid JSON rejected: %s %v", data, err)
			}
		} else if err == nil || len(data) != 0 {
			t.Fatalf("invalid/unbounded output accepted: len=%d err=%v", len(data), err)
		}
	}
}

func TestMCPContextCancellationReachesSharedClient(t *testing.T) {
	started := make(chan struct{})
	client := &mcpFleetFixture{query: func(ctx context.Context, _ *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := invokeMCP(ctx, mcpTestOptions(client), meshmcp.GetAgent, mcpTestSelection())
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("application call did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not reach the public service")
	}
}

func TestMCPStreamServerReusesFleetAndKeepsStdoutProtocolOnly(t *testing.T) {
	var calls atomic.Int32
	client := &mcpFleetFixture{
		nodes: func(context.Context) (*pb.NodeList, error) {
			calls.Add(1)
			return &pb.NodeList{Nodes: []*pb.NodeView{{InstanceId: "node-1"}}}, nil
		},
		submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
			calls.Add(1)
			return mcpTestReceipt(request, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED), nil
		},
	}
	clientReader, serverWriter := io.Pipe()
	serverReader, clientWriter := io.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer clientReader.Close()
	defer clientWriter.Close()
	defer serverReader.Close()
	defer serverWriter.Close()
	var diagnostics bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- serveMCP(ctx, mcpConfig{control: mcpTestOptions(client)}, IO{In: serverReader, Out: serverWriter, Err: &diagnostics})
	}()
	sdk := mcp.NewClient(&mcp.Implementation{Name: "stream-test", Version: "1"}, nil)
	session, err := sdk.Connect(ctx, &mcp.IOTransport{Reader: clientReader, Writer: clientWriter}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for i := 0; i < 2; i++ {
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: string(meshmcp.ListNodes), Arguments: meshmcp.PageInput{}})
		if err != nil || result.IsError {
			t.Fatalf("stdio framing/shared client failure: %v %v", result, err)
		}
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: string(meshmcp.InterruptAgent),
		Arguments: meshmcp.MutateAgentInput{AgentInput: mcpTestSelection(), IdempotencyKey: "original"}})
	if err != nil || result.IsError || calls.Load() != 3 {
		t.Fatalf("service logs polluted stdout or shared client was not reused: %v %v calls=%d", result, err, calls.Load())
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("MCP stream shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MCP did not release its owned connection on disconnect")
	}
}
