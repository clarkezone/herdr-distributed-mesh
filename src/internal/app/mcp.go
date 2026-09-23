package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshmcp"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/encoding/protojson"
)

const mcpCaptureLimit = 1024 * 1024

type mcpConfig struct {
	control control.Options
	limits  meshmcp.Options
	timeout time.Duration
}

func parseMCPFlags(args []string, streams IO) (mcpConfig, error) {
	var config mcpConfig
	flags := flag.NewFlagSet("mcp", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network, err := addNetworkFlags(flags, "herdr-mesh-mcp", "mcp", "TS_AUTHKEY_CLIENT", "tag:herdr-mesh-client")
	if err != nil {
		return config, err
	}
	flags.StringVar(&config.control.ServerAddress, "server", "", "server MagicDNS name or tailnet IP with port")
	flags.StringVar(&config.control.RequiredServerTag, "required-server-tag", "tag:herdr-mesh-server", "required tag on the actual coordinator connection")
	flags.DurationVar(&config.timeout, "timeout", 0, "whole MCP session timeout; zero runs until disconnect or cancellation")
	flags.DurationVar(&config.limits.CallTimeout, "call-timeout", 35*time.Second, "per-tool timeout, positive and at most 2m")
	flags.IntVar(&config.limits.MaxActiveCalls, "max-active-calls", 8, "total concurrent application calls, 2..64; one slot reserved for interrupt/stop")
	flags.IntVar(&config.limits.MaxOutputBytes, "max-output-bytes", 256*1024, "maximum encoded tool result bytes, 1024..1048576")
	if err := flags.Parse(args); err != nil {
		return config, err
	}
	if flags.NArg() != 0 {
		return config, errors.New("unexpected positional arguments")
	}
	if managedExplicitServer(args) && strings.TrimSpace(config.control.ServerAddress) == "" {
		return config, errors.New("explicit -server must not be empty")
	}
	if strings.TrimSpace(config.control.RequiredServerTag) == "" {
		return config, errors.New("a nonempty -required-server-tag is required")
	}
	if config.limits.MaxActiveCalls < meshmcp.MinActiveCalls || config.limits.MaxActiveCalls > 64 {
		return config, errors.New("-max-active-calls must be 2..64; one total slot is reserved for interrupt/stop")
	}
	if config.timeout < 0 || config.limits.CallTimeout <= 0 || config.limits.CallTimeout > 2*time.Minute ||
		config.limits.MaxOutputBytes < 1024 || config.limits.MaxOutputBytes > 1024*1024 {
		return config, errors.New("invalid MCP session, call, concurrency, or output bounds")
	}
	config.control.Transport = network.config()
	config.control.JSON = true
	return config, nil
}

// runMCP owns one authenticated fleet connection for the entire MCP session.
func runMCP(ctx context.Context, args []string, streams IO) error {
	config, err := parseMCPFlags(args, streams)
	if err != nil {
		return err
	}
	if config.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.timeout)
		defer cancel()
	}
	if config.control.ServerAddress == "" {
		client, closeClient, _, err := managedFleet(ctx)
		if err != nil {
			return err
		}
		defer closeClient()
		config.control.FleetClient = client
	}
	return serveMCP(ctx, config, streams)
}

func serveMCP(ctx context.Context, config mcpConfig, streams IO) error {
	if strings.TrimSpace(config.control.RequiredServerTag) == "" {
		return errors.New("MCP requires an expected server tag")
	}
	options := config.control
	server, err := meshmcp.New(func(ctx context.Context, operation meshmcp.Operation, input any) (any, error) {
		return invokeMCP(ctx, options, operation, input)
	}, config.limits)
	if err != nil {
		return err
	}
	if streams.In == nil {
		streams.In = os.Stdin
	}
	if streams.Out == nil {
		streams.Out = os.Stdout
	}
	reader, ok := streams.In.(io.ReadCloser)
	if !ok {
		reader = io.NopCloser(streams.In)
	}
	writer, ok := streams.Out.(io.WriteCloser)
	if !ok {
		writer = mcpWriter{streams.Out}
	}
	return control.WithFleet(ctx, options, func(client pb.FleetClient) error {
		options.FleetClient = client
		return server.Run(ctx, &mcp.IOTransport{Reader: reader, Writer: writer, MaxLineLength: 2 * meshmcp.MaxInputBytes})
	})
}

type mcpWriter struct{ io.Writer }

func (mcpWriter) Close() error { return nil }

type mcpCapture struct {
	buffer bytes.Buffer
	err    error
}

func (out *mcpCapture) Write(data []byte) (int, error) {
	if out.err != nil {
		return 0, out.err
	}
	if len(data) > mcpCaptureLimit-out.buffer.Len() {
		out.err = errors.New("application JSON exceeds MCP capture byte limit")
		return 0, out.err
	}
	return out.buffer.Write(data)
}

func captureMCP(options control.Options, call func(control.Options) error) (json.RawMessage, error) {
	var out mcpCapture
	options.Output, options.JSON, options.Diagnose = &out, true, false
	callErr := call(options)
	if out.err != nil {
		return nil, errors.Join(callErr, out.err)
	}
	if out.buffer.Len() == 0 && callErr != nil {
		return nil, callErr
	}
	data := bytes.TrimSpace(out.buffer.Bytes())
	if !json.Valid(data) || bytes.Equal(data, []byte("null")) {
		return nil, errors.Join(callErr, errors.New("application did not return one non-null JSON result"))
	}
	return json.RawMessage(bytes.Clone(data)), callErr
}

func invokeMCP(ctx context.Context, options control.Options, operation meshmcp.Operation, input any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.FleetClient == nil || strings.TrimSpace(options.RequiredServerTag) == "" {
		return nil, errors.New("MCP operation requires the session's authenticated fleet client and expected server tag")
	}
	switch operation {
	case meshmcp.ListNodes:
		in, ok := input.(meshmcp.PageInput)
		if !ok {
			break
		}
		data, err := mcpNodes(ctx, options)
		if err != nil {
			return nil, err
		}
		return mcpPage(data, "nodes", "nodes", in)
	case meshmcp.GetNode:
		in, ok := input.(meshmcp.NodeInput)
		if !ok {
			break
		}
		return mcpNode(ctx, options, in.NodeInstanceID)
	case meshmcp.ListSessions:
		in, ok := input.(meshmcp.ListSessionsInput)
		if !ok {
			break
		}
		if !protocol.ValidIdempotencyKey(in.NodeInstanceID) {
			return nil, errors.New("invalid node selector")
		}
		data, err := captureMCP(options, func(options control.Options) error {
			return control.Sessions(ctx, options, in.NodeInstanceID)
		})
		if err != nil {
			return data, err
		}
		return mcpPage(data, "sessions", "sessions:"+in.NodeInstanceID, in.PageInput)
	case meshmcp.EnsureHerdrSession:
		in, ok := input.(meshmcp.EnsureHerdrSessionInput)
		if !ok {
			break
		}
		if !protocol.ValidIdempotencyKey(in.IdempotencyKey) {
			return nil, errors.New("MCP session ensure requires the original idempotency key")
		}
		// The local herdr_session_id names the desired session, not its incarnation.
		data, err := captureMCP(options, func(options control.Options) error {
			return control.EnsureSession(ctx, options, in.NodeInstanceID, in.IdempotencyKey, in.HerdrSessionID, protocol.DefaultCommandTTL)
		})
		return mcpSessionReceipt(data, err, in)
	case meshmcp.ListProjects:
		in, ok := input.(meshmcp.ListProjectsInput)
		if !ok {
			break
		}
		if !protocol.ValidIdempotencyKey(in.NodeInstanceID) {
			return nil, errors.New("invalid node selector")
		}
		data, err := captureMCP(options, func(options control.Options) error { return control.Projects(ctx, options, in.NodeInstanceID) })
		if err != nil {
			return nil, err
		}
		var list pb.ProjectList
		if err := protojson.Unmarshal(data, &list); err != nil {
			return nil, fmt.Errorf("invalid project JSON: %w", err)
		}
		for _, project := range list.Projects {
			if project.GetDesired().GetNodeInstanceId() != in.NodeInstanceID {
				return nil, errors.New("project list does not match the selected node")
			}
		}
		return mcpPage(data, "projects", "projects:"+in.NodeInstanceID, in.PageInput)
	case meshmcp.GetProject:
		in, ok := input.(meshmcp.ProjectInput)
		if !ok {
			break
		}
		data, err := captureMCP(options, func(options control.Options) error {
			return control.GetProject(ctx, options, in.NodeInstanceID, in.ProjectID)
		})
		return mcpProjectResult(data, err, in)
	case meshmcp.RegisterProject:
		in, ok := input.(meshmcp.RegisterProjectInput)
		if !ok {
			break
		}
		data, err := captureMCP(options, func(options control.Options) error {
			return control.RegisterProject(ctx, options, &pb.RegisterProjectRequest{
				NodeInstanceId: in.NodeInstanceID, ProjectId: in.ProjectID,
				CheckoutPath: in.CheckoutPath, WorktreeRoot: in.WorktreeRoot,
			})
		})
		return mcpProjectResult(data, err, in.ProjectInput)
	case meshmcp.ListWorkspaces:
		in, ok := input.(meshmcp.ListWorkspacesInput)
		if !ok {
			break
		}
		if err := mcpScope(in.WorkspaceInput, false); err != nil {
			return nil, err
		}
		return mcpEntities(ctx, options, in.SessionInput, "", "workspaces", in.PageInput)
	case meshmcp.ListAgents:
		in, ok := input.(meshmcp.ListAgentsInput)
		if !ok {
			break
		}
		return mcpAgentInventory(ctx, options, in)
	case meshmcp.GetCommand:
		in, ok := input.(meshmcp.GetCommandInput)
		if !ok {
			break
		}
		if !protocol.ValidIdempotencyKey(in.NodeInstanceID) || !protocol.ValidCommandID(in.CommandID) {
			return nil, errors.New("invalid node or command selector")
		}
		data, err := captureMCP(options, func(options control.Options) error { return control.CommandStatus(ctx, options, in.CommandID) })
		return mcpReceipt(data, err, in.NodeInstanceID, "", in.CommandID, nil)
	case meshmcp.EnsureWorkspace:
		in, ok := input.(meshmcp.EnsureWorkspaceInput)
		if !ok {
			break
		}
		if err := mcpScope(in.WorkspaceInput, true); err != nil {
			return nil, err
		}
		data, err := captureMCP(options, func(options control.Options) error {
			return control.EnsureWorkspaceInSession(ctx, options, in.NodeInstanceID, in.IdempotencyKey, &pb.WorkspaceEnsure{
				ProjectId: in.ProjectID, BindingRevision: in.BindingRevision,
				SessionName: in.HerdrSessionID, SessionIncarnation: in.HerdrSessionIncarnation,
			}, protocol.DefaultCommandTTL)
		})
		return mcpReceipt(data, err, in.NodeInstanceID, in.IdempotencyKey, "", &in.SessionInput)
	case meshmcp.CreateWorktree:
		in, ok := input.(meshmcp.CreateWorktreeInput)
		if !ok {
			break
		}
		if err := mcpScope(in.WorkspaceInput, true); err != nil {
			return nil, err
		}
		data, err := captureMCP(options, func(options control.Options) error {
			return control.CreateWorktree(ctx, options, in.NodeInstanceID, in.IdempotencyKey, &pb.WorktreeCreate{
				ProjectId: in.ProjectID, BindingRevision: in.BindingRevision, Name: in.Name, Branch: in.Branch, BaseCommit: in.BaseCommit,
				SessionName: in.HerdrSessionID, SessionIncarnation: in.HerdrSessionIncarnation,
			}, protocol.DefaultCommandTTL)
		})
		return mcpReceipt(data, err, in.NodeInstanceID, in.IdempotencyKey, "", &in.SessionInput)
	case meshmcp.GetAgent, meshmcp.ReadAgent, meshmcp.WaitAgent, meshmcp.PromptAgent, meshmcp.SendAgentInputOp, meshmcp.InterruptAgent:
		return invokeMCPAgent(ctx, options, operation, input)
	case meshmcp.StartAgent:
		in, ok := input.(meshmcp.StartAgentInput)
		if !ok {
			break
		}
		if err := mcpScope(in.WorkspaceInput, true); err != nil {
			return nil, err
		}
		if !protocol.ValidIdempotencyKey(in.IdempotencyKey) || in.StartupTimeoutMS < 0 ||
			in.StartupTimeoutMS > protocol.MaxAgentStartupTimeoutMs {
			return nil, errors.New("start requires the original execution key and a bounded startup timeout")
		}
		data, err := captureMCP(options, func(options control.Options) error {
			return control.StartAgent(ctx, options, in.NodeInstanceID, in.IdempotencyKey, &pb.AgentStart{
				ProjectId: in.ProjectID, BindingRevision: in.BindingRevision, WorkspaceId: in.WorkspaceID,
				Name: in.Name, Provider: in.Provider, SessionName: in.HerdrSessionID, SessionIncarnation: in.HerdrSessionIncarnation,
				StartupTimeoutMs: uint32(in.StartupTimeoutMS), InitialPrompt: in.InitialPrompt,
			}, protocol.DefaultCommandTTL)
		})
		return mcpLifecycleReceipt(data, err, in.NodeInstanceID, in.IdempotencyKey, protocol.AgentStartCommandType, &in.SessionInput)
	case meshmcp.StopAgent:
		in, ok := input.(meshmcp.StopAgentInput)
		if !ok {
			break
		}
		if !protocol.ValidIdempotencyKey(in.IdempotencyKey) || !protocol.ValidSessionIncarnation(in.HerdrSessionIncarnation) {
			return nil, errors.New("stop requires the original execution key and observed Herdr incarnation, including for the configured default")
		}
		data, err := captureMCP(options, func(options control.Options) error {
			return control.StopAgent(ctx, options, in.NodeInstanceID, in.IdempotencyKey, &pb.AgentStop{
				Target: &pb.AgentTarget{
					PaneId: in.Target.PaneID, TerminalId: in.Target.TerminalID, AgentSessionId: in.Target.AgentSessionID,
					SessionName: in.HerdrSessionID, SessionIncarnation: in.HerdrSessionIncarnation,
				},
				WorkspaceId: in.WorkspaceID, TabId: in.TabID, Provider: in.Provider,
			}, protocol.DefaultCommandTTL)
		})
		return mcpLifecycleReceipt(data, err, in.NodeInstanceID, in.IdempotencyKey, protocol.AgentStopCommandType, &in.SessionInput)
	default:
		return nil, fmt.Errorf("unsupported MCP operation %q", operation)
	}
	return nil, fmt.Errorf("invalid input type %T for MCP operation %s", input, operation)
}

func mcpLifecycleReceipt(data json.RawMessage, callErr error, nodeID, key, kind string, session *meshmcp.SessionInput) (any, error) {
	value, receiptErr := mcpReceipt(data, callErr, nodeID, key, "", session)
	if value == nil {
		return nil, receiptErr
	}
	var record pb.CommandRecord
	if err := protojson.Unmarshal(data, &record); err != nil {
		return nil, errors.Join(receiptErr, fmt.Errorf("invalid lifecycle receipt JSON: %w", err))
	}
	if record.Command.CommandType != kind {
		return nil, errors.Join(receiptErr, errors.New("lifecycle receipt does not match requested operation"))
	}
	if record.AgentLifecycle != nil {
		if err := protocol.ValidateLifecycleReceipt(record.AgentLifecycle, record.Command); err != nil {
			return nil, errors.Join(receiptErr, fmt.Errorf("invalid lifecycle stage receipt: %w", err))
		}
	}
	if protocol.IsTerminalCommand(record.Status) {
		if err := protocol.ValidateLifecycleResult(&pb.CommandResult{
			CommandId: record.Command.CommandId, Status: record.Status, Detail: record.Detail, AgentLifecycle: record.AgentLifecycle,
		}, record.Command); err != nil {
			return nil, errors.Join(receiptErr, fmt.Errorf("invalid lifecycle result: %w", err))
		}
	}
	return value, receiptErr
}

func mcpSessionReceipt(data json.RawMessage, callErr error, input meshmcp.EnsureHerdrSessionInput) (any, error) {
	value, receiptErr := mcpReceipt(data, callErr, input.NodeInstanceID, input.IdempotencyKey, "", nil)
	if value == nil {
		return nil, receiptErr
	}
	var record pb.CommandRecord
	if err := protojson.Unmarshal(data, &record); err != nil {
		return nil, errors.Join(receiptErr, fmt.Errorf("invalid session receipt JSON: %w", err))
	}
	if record.Command.CommandType != protocol.SessionEnsureCommandType || record.Command.SessionEnsure.GetName() != input.HerdrSessionID {
		return nil, errors.Join(receiptErr, errors.New("session receipt does not match the original session name or operation"))
	}
	if record.Status == pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		if err := protocol.ValidateSessionEnsureResult(&pb.CommandResult{
			CommandId: record.Command.CommandId, Status: record.Status, Detail: record.Detail, SessionEnsure: record.SessionEnsure,
		}); err != nil {
			return nil, errors.Join(receiptErr, fmt.Errorf("invalid session ensure result: %w", err))
		}
		if record.SessionEnsure.Name != input.HerdrSessionID {
			return nil, errors.Join(receiptErr, errors.New("session ensure result names a different session"))
		}
	}
	return value, receiptErr
}

func mcpProjectResult(data json.RawMessage, callErr error, input meshmcp.ProjectInput) (any, error) {
	if callErr != nil {
		return nil, callErr
	}
	var project pb.ProjectRecord
	if err := protojson.Unmarshal(data, &project); err != nil {
		return nil, fmt.Errorf("invalid project JSON: %w", err)
	}
	if project.GetDesired().GetNodeInstanceId() != input.NodeInstanceID || project.GetDesired().GetProjectId() != input.ProjectID {
		return nil, errors.New("project result does not match the selected node/project")
	}
	return data, nil
}

func mcpScope(input meshmcp.WorkspaceInput, project bool) error {
	if !protocol.ValidIdempotencyKey(input.NodeInstanceID) {
		return errors.New("invalid node selector")
	}
	if (input.HerdrSessionID == "") != (input.HerdrSessionIncarnation == "") {
		return errors.New("named session selectors require both the session name and its original incarnation")
	}
	if err := protocol.ValidateSessionSelector(input.HerdrSessionID, input.HerdrSessionIncarnation, true); err != nil {
		return err
	}
	if !project && input.ProjectID != "" {
		return errors.New("this service cannot verify a project association")
	}
	if project && !protocol.ValidIdempotencyKey(input.ProjectID) {
		return errors.New("invalid project selector")
	}
	return nil
}

func invokeMCPAgent(ctx context.Context, options control.Options, operation meshmcp.Operation, input any) (any, error) {
	var selection meshmcp.AgentInput
	query := &pb.AgentQueryRequest{Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_GET}
	var action *pb.AgentControl
	var key string
	switch in := input.(type) {
	case meshmcp.AgentInput:
		if operation != meshmcp.GetAgent {
			return nil, errors.New("agent input does not match operation")
		}
		selection = in
	case meshmcp.ReadAgentInput:
		if operation != meshmcp.ReadAgent || in.Lines < 1 || in.Lines > meshmcp.MaxReadLines || in.MaxBytes < 1 || in.MaxBytes > meshmcp.MaxReadBytes {
			return nil, errors.New("invalid read operation or bounds")
		}
		selection, query.Kind, query.Lines = in.AgentInput, pb.AgentQueryKind_AGENT_QUERY_KIND_READ, uint32(in.Lines)
	case meshmcp.WaitAgentInput:
		if operation != meshmcp.WaitAgent || in.TimeoutMS < 1 || in.TimeoutMS > meshmcp.MaxWaitMilliseconds {
			return nil, errors.New("invalid wait operation or timeout")
		}
		selection, query.Kind = in.AgentInput, pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT
		query.Until, query.TimeoutMs = in.Until, uint32(in.TimeoutMS)
	case meshmcp.PromptAgentInput:
		if operation != meshmcp.PromptAgent {
			return nil, errors.New("prompt input does not match operation")
		}
		selection, key = in.AgentInput, in.IdempotencyKey
		action = &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, Text: in.Text}
	case meshmcp.SendAgentInput:
		if operation != meshmcp.SendAgentInputOp || in.Text != "" {
			return nil, errors.New("send_agent_input supports explicit keys only; use prompt_agent for prompts")
		}
		selection, key = in.AgentInput, in.IdempotencyKey
		action = &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT, Keys: in.Keys}
	case meshmcp.MutateAgentInput:
		if operation != meshmcp.InterruptAgent {
			return nil, errors.New("agent mutation input does not match operation")
		}
		selection, key = in.AgentInput, in.IdempotencyKey
		action = &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT}
	default:
		return nil, fmt.Errorf("invalid agent input type %T", input)
	}
	if err := mcpScope(selection.WorkspaceInput, false); err != nil {
		return nil, err
	}
	if selection.Target.TerminalID == "" || selection.Target.AgentSessionID == "" {
		return nil, errors.New("MCP requires the original terminal and agent incarnation; it never rediscovers mutation targets")
	}
	query.NodeInstanceId = selection.NodeInstanceID
	query.Target = &pb.AgentTarget{
		PaneId: selection.Target.PaneID, TerminalId: selection.Target.TerminalID, AgentSessionId: selection.Target.AgentSessionID,
		SessionName: selection.HerdrSessionID, SessionIncarnation: selection.HerdrSessionIncarnation,
	}
	if action != nil {
		if !protocol.ValidIdempotencyKey(key) {
			return nil, errors.New("MCP agent mutations require the original idempotency key")
		}
		action.Target = query.Target
	}
	data, err := captureMCP(options, func(options control.Options) error {
		return control.Agent(ctx, options, query, action, key, protocol.DefaultCommandTTL)
	})
	if action != nil {
		return mcpReceipt(data, err, selection.NodeInstanceID, key, "", &selection.SessionInput)
	}
	if err != nil {
		return nil, err
	}
	if read, ok := input.(meshmcp.ReadAgentInput); ok {
		var result pb.AgentQueryResult
		if err := protojson.Unmarshal(data, &result); err != nil {
			return nil, fmt.Errorf("invalid agent JSON: %w", err)
		}
		lines := strings.Count(result.Text, "\n")
		if result.Text != "" && !strings.HasSuffix(result.Text, "\n") {
			lines++
		}
		if len(result.Text) > read.MaxBytes || lines > read.Lines {
			return nil, errors.New("agent snapshot exceeds the requested read bounds; request a smaller snapshot")
		}
	}
	return data, nil
}

func mcpReceipt(data json.RawMessage, callErr error, nodeID, key, commandID string, session *meshmcp.SessionInput) (any, error) {
	if len(data) == 0 {
		if callErr != nil {
			return nil, callErr
		}
		return nil, errors.New("application returned no command receipt")
	}
	var record pb.CommandRecord
	if err := protojson.Unmarshal(data, &record); err != nil {
		return nil, errors.Join(callErr, fmt.Errorf("invalid command receipt JSON: %w", err))
	}
	if record.Command == nil || !protocol.ValidCommandID(record.Command.CommandId) || record.Command.TargetId != nodeID ||
		(key != "" && record.Command.IdempotencyKey != key) || (commandID != "" && record.Command.CommandId != commandID) {
		return nil, errors.Join(callErr, errors.New("command receipt does not match the original node, key, or command selector"))
	}
	if session != nil && (session.HerdrSessionID != "" || session.HerdrSessionIncarnation != "") {
		name, incarnation := protocol.CommandSession(record.Command)
		if name != session.HerdrSessionID || incarnation != session.HerdrSessionIncarnation {
			return nil, errors.Join(callErr, errors.New("command receipt does not match the original named session and incarnation"))
		}
	}
	if protocol.IsTerminalCommand(record.Status) && record.Status != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		return data, errors.Join(callErr, fmt.Errorf("command operation ended with %s; this receipt is not agent task completion", record.Status))
	}
	if record.Status != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED && record.Status != pb.CommandStatus_COMMAND_STATUS_ACCEPTED && record.Status != pb.CommandStatus_COMMAND_STATUS_RUNNING {
		return nil, errors.Join(callErr, errors.New("invalid command receipt status"))
	}
	return data, callErr
}

func mcpNodes(ctx context.Context, options control.Options) (json.RawMessage, error) {
	data, err := captureMCP(options, func(options control.Options) error { return control.Nodes(ctx, options) })
	if err != nil {
		return nil, err
	}
	var list struct {
		Nodes []json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(data, &list); err != nil || list.Nodes == nil {
		return nil, errors.New("invalid node list JSON")
	}
	seen := map[string]bool{}
	for _, raw := range list.Nodes {
		var node struct {
			ID string `json:"instance_id"`
		}
		if err := json.Unmarshal(raw, &node); err != nil || node.ID == "" || seen[node.ID] {
			return nil, errors.New("node list contains an invalid or duplicate node")
		}
		seen[node.ID] = true
	}
	return data, nil
}

func mcpNode(ctx context.Context, options control.Options, nodeID string) (json.RawMessage, error) {
	if !protocol.ValidIdempotencyKey(nodeID) {
		return nil, errors.New("invalid node selector")
	}
	data, err := mcpNodes(ctx, options)
	if err != nil {
		return nil, err
	}
	var list struct {
		Nodes []json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, err
	}
	for _, raw := range list.Nodes {
		var node struct {
			ID string `json:"instance_id"`
		}
		if err := json.Unmarshal(raw, &node); err != nil {
			return nil, err
		}
		if node.ID == nodeID {
			return raw, nil
		}
	}
	return nil, errors.New("selected node was not found")
}

func mcpAgentInventory(ctx context.Context, options control.Options, input meshmcp.ListAgentsInput) (json.RawMessage, error) {
	if err := mcpScope(input.WorkspaceInput, input.ProjectID != ""); err != nil {
		return nil, err
	}
	for _, selector := range []string{input.WorkspaceID, input.Provider} {
		if selector != "" && !protocol.ValidIdempotencyKey(selector) {
			return nil, errors.New("invalid agent inventory filter")
		}
	}
	filter := control.InventoryFilter{
		NodeID: input.NodeInstanceID, ProjectID: input.ProjectID, WorkspaceID: input.WorkspaceID,
		Provider: input.Provider, Readiness: input.Readiness,
	}
	data, err := captureMCP(options, func(options control.Options) error {
		return control.AgentInventory(ctx, options, filter)
	})
	if err != nil {
		return nil, err
	}
	var inventory struct {
		Agents  []json.RawMessage `json:"agents"`
		Sources []json.RawMessage `json:"sources"`
	}
	if err := json.Unmarshal(data, &inventory); err != nil || inventory.Agents == nil || inventory.Sources == nil {
		return nil, errors.New("invalid agent inventory JSON")
	}
	type sourceSelector struct {
		NodeID      string `json:"node_instance_id"`
		Name        string `json:"session_name"`
		Incarnation string `json:"session_incarnation"`
	}
	selected := sourceSelector{input.NodeInstanceID, input.HerdrSessionID, input.HerdrSessionIncarnation}
	found := false
	for _, raw := range inventory.Sources {
		var source sourceSelector
		if err := json.Unmarshal(raw, &source); err != nil || source.NodeID != input.NodeInstanceID {
			return nil, errors.New("invalid agent inventory source")
		}
		if source.Name == selected.Name {
			if found || source.Incarnation != selected.Incarnation {
				return nil, errors.New("selected session incarnation changed or source is duplicated")
			}
			found = true
		}
	}
	if !found {
		return nil, errors.New("selected node/session inventory source was not found")
	}
	agents := make([]json.RawMessage, 0, len(inventory.Agents))
	for _, raw := range inventory.Agents {
		var agent struct {
			Source sourceSelector `json:"source"`
		}
		if err := json.Unmarshal(raw, &agent); err != nil || agent.Source.NodeID != input.NodeInstanceID {
			return nil, errors.New("invalid agent inventory entry")
		}
		if agent.Source == selected {
			agents = append(agents, raw)
		}
	}
	inventory.Agents = agents
	data, err = json.Marshal(inventory)
	if err != nil {
		return nil, err
	}
	// Retain every node source, even when no agent matches. Source freshness and
	// filters also bind the cursor, so empty scopes cannot change between pages.
	scope, err := json.Marshal(struct {
		Filter   control.InventoryFilter
		Selected sourceSelector
		Sources  []json.RawMessage
	}{filter, selected, inventory.Sources})
	if err != nil {
		return nil, err
	}
	return mcpPage(data, "agents", string(scope), input.PageInput)
}

func mcpEntities(ctx context.Context, options control.Options, selection meshmcp.SessionInput, workspaceID, field string, page meshmcp.PageInput) (json.RawMessage, error) {
	raw, err := mcpNode(ctx, options, selection.NodeInstanceID)
	if err != nil {
		return nil, err
	}
	var node pb.NodeView
	if err := protojson.Unmarshal(raw, &node); err != nil {
		return nil, fmt.Errorf("invalid node snapshot: %w", err)
	}
	snapshot, stale, receivedAt := node.Herdr, node.Stale, node.HerdrReceivedAt
	result := map[string]any{"node_instance_id": selection.NodeInstanceID, "connected": node.Connected}
	if selection.HerdrSessionID != "" {
		var selected *pb.SessionView
		for _, session := range node.Sessions {
			if session == nil {
				return nil, errors.New("node returned an invalid session snapshot")
			}
			if session.Name == selection.HerdrSessionID {
				if selected != nil {
					return nil, errors.New("node returned duplicate named sessions")
				}
				selected = session
			}
		}
		if selected == nil {
			return nil, errors.New("selected named session was not found")
		}
		if selected.Incarnation != selection.HerdrSessionIncarnation {
			return nil, errors.New("selected named session incarnation changed")
		}
		snapshot, stale, receivedAt = selected.Herdr, selected.Stale, selected.HerdrReceivedAt
		result["herdr_session_id"], result["herdr_session_incarnation"] = selected.Name, selected.Incarnation
		result["session_status"], result["session_error_code"] = selected.Status, selected.ErrorCode
	}
	if snapshot == nil {
		return nil, errors.New("selected session has no Herdr snapshot")
	}
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("invalid Herdr snapshot: %w", err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		return nil, err
	}
	var entities []json.RawMessage
	if err := json.Unmarshal(object[field], &entities); err != nil || entities == nil {
		return nil, errors.New("selected node has no valid entity snapshot")
	}
	filtered := make([]json.RawMessage, 0, len(entities))
	for _, raw := range entities {
		var entity struct {
			ID          string `json:"id"`
			WorkspaceID string `json:"workspace_id"`
		}
		if err := json.Unmarshal(raw, &entity); err != nil || entity.ID == "" {
			return nil, errors.New("invalid entity snapshot")
		}
		if workspaceID == "" || entity.WorkspaceID == workspaceID {
			projection, err := mcpEntityProjection(raw)
			if err != nil {
				return nil, err
			}
			filtered = append(filtered, projection)
		}
	}
	result["stale"], result["herdr_status"], result[field] = stale, snapshot.Status, filtered
	result["herdr_received_at"] = nil
	if receivedAt != nil {
		receiptJSON, err := protojson.Marshal(receivedAt)
		if err != nil {
			return nil, fmt.Errorf("invalid observation receipt timestamp: %w", err)
		}
		result["herdr_received_at"] = json.RawMessage(receiptJSON)
	}
	data, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	scope := strings.Join([]string{field, selection.NodeInstanceID, selection.HerdrSessionID, selection.HerdrSessionIncarnation, workspaceID}, ":")
	return mcpPage(data, field, scope, page)
}

func mcpEntityProjection(raw json.RawMessage) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("invalid entity metadata: %w", err)
	}
	// ProtoJSON emits empty strings for unreported metadata. Remove only those
	// placeholders; optional readiness must retain its actual presence and value.
	for _, name := range []string{"provider", "terminal_id", "provider_session_id", "project_id"} {
		if bytes.Equal(fields[name], []byte(`""`)) {
			delete(fields, name)
		}
	}
	return json.Marshal(fields)
}

// Cursors are bound to one exact bounded snapshot and scope, without retaining
// snapshots between calls. A changed snapshot is an explicit restart, not a gap.
func mcpPage(data json.RawMessage, field, scope string, page meshmcp.PageInput) (json.RawMessage, error) {
	if page.Limit == 0 {
		page.Limit = meshmcp.DefaultPageSize
	}
	if page.Limit < 1 || page.Limit > meshmcp.MaxPageSize || len(page.Cursor) > 1024 {
		return nil, errors.New("invalid page bounds")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	var values []json.RawMessage
	if err := json.Unmarshal(object[field], &values); err != nil || values == nil {
		return nil, errors.New("application returned an invalid list")
	}
	sum := sha256.Sum256(append([]byte(scope+":"), object[field]...))
	fingerprint := hex.EncodeToString(sum[:])
	offset := 0
	if page.Cursor != "" {
		position, expected, ok := strings.Cut(page.Cursor, ":")
		var err error
		offset, err = strconv.Atoi(position)
		if !ok || err != nil || offset < 1 || offset >= len(values) || expected != fingerprint {
			return nil, errors.New("invalid cursor or changed snapshot; restart without a cursor")
		}
	}
	end := min(len(values), offset+page.Limit)
	next := ""
	if end < len(values) {
		next = strconv.Itoa(end) + ":" + fingerprint
	}
	var err error
	object[field], err = json.Marshal(values[offset:end])
	if err != nil {
		return nil, err
	}
	object["next_cursor"], err = json.Marshal(next)
	if err != nil {
		return nil, err
	}
	return json.Marshal(object)
}
