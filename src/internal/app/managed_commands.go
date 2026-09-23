package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func managedFleet(ctx context.Context) (pb.FleetClient, func(), string, error) {
	dir, err := meshlocal.StateDir(ctx)
	if err != nil {
		return nil, nil, "", err
	}
	if _, err := meshlocal.Load(dir); err != nil {
		return nil, nil, "", fmt.Errorf("load managed mesh (run init or join first): %w", err)
	}
	op, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := meshlocal.Dial(op, dir)
	if err != nil {
		return nil, nil, "", fmt.Errorf("connect to managed daemon; no independent identity will be enrolled: %w", err)
	}
	return pb.NewFleetClient(conn), func() { _ = conn.Close() }, dir, nil
}

func managedExplicitServer(args []string) bool {
	for _, arg := range args {
		if arg == "-server" || arg == "--server" || strings.HasPrefix(arg, "-server=") || strings.HasPrefix(arg, "--server=") {
			return true
		}
	}
	return false
}

func runManagedCommands(ctx context.Context, args []string, streams IO) (bool, error) {
	if len(args) == 0 || managedExplicitServer(args[1:]) {
		return false, nil
	}
	switch args[0] {
	case "dashboard":
		return true, runDashboard(ctx, args[1:], streams)
	case "mcp":
		return true, runMCP(ctx, args[1:], streams)
	case "nodes", "status", "doctor", "project", "agent":
	default:
		return false, nil
	}
	parsed, err := parseManaged(args, streams)
	if err != nil {
		return true, err
	}
	op, cancel := context.WithTimeout(ctx, parsed.timeout)
	defer cancel()
	client, closeClient, dir, err := managedFleet(op)
	if err != nil {
		if parsed.root == "doctor" || parsed.root == "status" {
			localDir, dirErr := meshlocal.StateDir(ctx)
			if dirErr != nil {
				return true, errors.Join(err, dirErr)
			}
			local, readErr := meshlocal.ReadStatus(localDir)
			if readErr != nil {
				return true, errors.Join(err, fmt.Errorf("read local daemon status: %w", readErr))
			}
			if parsed.json {
				writeErr := json.NewEncoder(streams.Out).Encode(map[string]any{"daemon": local, "server_error": err.Error()})
				return true, errors.Join(err, writeErr)
			}
			_, writeErr := fmt.Fprintf(streams.Out, "daemon=%s dns=%s server=%s error=%s\n", local.State, local.DNSName, local.Server, local.Error)
			return true, errors.Join(err, writeErr)
		}
		return true, err
	}
	defer closeClient()
	return true, executeManaged(op, client, dir, parsed, streams)
}

type managedArgs struct {
	root, verb, name, node, project, path, session, provider, prompt string
	json                                                             bool
	timeout                                                          time.Duration
}

func parseManaged(args []string, streams IO) (managedArgs, error) {
	a := managedArgs{root: args[0]}
	rest := args[1:]
	if a.root == "project" || a.root == "agent" {
		if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
			a.verb, rest = rest[0], rest[1:]
		}
		if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
			a.name, rest = rest[0], rest[1:]
		}
	}
	flags := flag.NewFlagSet(strings.TrimSpace(a.root+" "+a.verb), flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	flags.BoolVar(&a.json, "json", false, "write JSON")
	flags.DurationVar(&a.timeout, "timeout", 2*time.Minute, "bounded operation/follow budget; cancellation never relaunches")
	flags.StringVar(&a.node, "node", "", "mesh node label shown by nodes (hostname by default, --name overrides); exact node ID also accepted")
	flags.StringVar(&a.project, "project", "", "registered project name")
	flags.StringVar(&a.path, "path", "", "existing checkout path on the selected node; never creates a worktree")
	flags.StringVar(&a.session, "session", "main", "named Herdr session")
	flags.StringVar(&a.provider, "provider", "copilot", "agent provider")
	flags.StringVar(&a.prompt, "prompt", "", "initial agent prompt")
	flags.Usage = func() { printManagedUsage(flags, a) }
	if err := flags.Parse(rest); err != nil {
		return a, err
	}
	if flags.NArg() != 0 || a.timeout <= 0 || a.timeout > 24*time.Hour {
		return a, errors.New("unexpected arguments or timeout outside (0,24h]")
	}
	if a.root == "project" && (a.verb != "add" || a.name == "" || a.node == "" || a.path == "") {
		return a, errors.New("usage: project add <project-name> --node <node-name> --path <existing-checkout>")
	}
	if a.root == "agent" {
		if a.name == "" || a.node == "" || (a.verb != "start" && a.verb != "follow" && a.verb != "stop") {
			return a, errors.New("usage: agent start|follow|stop <agent-name> --node <node-name>; use --help for the selected command")
		}
		if !protocol.ValidIdempotencyKey(a.name) || !protocol.SupportedLifecycleProvider(a.provider) ||
			protocol.ValidateSessionEnsure(&pb.SessionEnsure{Name: a.session}) != nil {
			return a, errors.New("invalid agent name, provider or session")
		}
		if a.verb == "start" && !protocol.ValidIdempotencyKey(a.project) {
			return a, errors.New("agent start requires --project")
		}
	}
	return a, nil
}

func printManagedUsage(flags *flag.FlagSet, a managedArgs) {
	output := flags.Output()
	options := []string{"json", "timeout"}
	switch a.root {
	case "agent":
		if a.verb != "start" && a.verb != "follow" && a.verb != "stop" {
			fmt.Fprintln(output, "Usage: herdr-mesh agent <start|follow|stop> <agent-name> --node <node-name>")
		} else {
			fmt.Fprintf(output, "Usage: herdr-mesh agent %s <agent-name> --node <node-name>", a.verb)
			if a.verb == "start" {
				fmt.Fprint(output, " --project <project-name> --prompt <task>")
			}
			fmt.Fprintln(output, " [options]")
		}
		fmt.Fprintln(output, "\n<agent-name> is the name chosen with agent start, not a workspace, tab or Herdr session.")
		fmt.Fprintln(output, "Follow/stop use that original mesh name; renaming a TUI label does not change it.")
		fmt.Fprintln(output, "Agents started directly in the TUI do not automatically have a mesh control name.")
		fmt.Fprintln(output, "<node-name> is the mesh label assigned by init/join --name, not the Windows hostname.")
		options = append(options, "node", "session")
		switch a.verb {
		case "start":
			fmt.Fprintln(output, "\nExample: herdr-mesh agent start smoke --node laptop --project demo --prompt \"Say hello\"")
			options = append(options, "project", "prompt", "provider", "path")
		case "follow":
			fmt.Fprintln(output, "\nExample: herdr-mesh agent follow smoke --node laptop")
			fmt.Fprintln(output, "Canceling follow stops watching, not the agent.")
		case "stop":
			fmt.Fprintln(output, "\nExample: herdr-mesh agent stop smoke --node laptop")
			fmt.Fprintln(output, "Stops that agent's pane/terminal, not the mesh daemon, session, workspace or other agents.")
		}
	case "project":
		fmt.Fprintln(output, "Usage: herdr-mesh project add <project-name> --node <node-name> --path <existing-checkout> [options]")
		fmt.Fprintln(output, "\nChoose a central project name. The checkout path is on the selected node.")
		fmt.Fprintln(output, "Example: herdr-mesh project add demo --node laptop --path C:\\src\\demo")
		options = append(options, "node", "path")
	default:
		fmt.Fprintf(output, "Usage: herdr-mesh %s [options]\n", a.root)
	}
	visible := flag.NewFlagSet(flags.Name(), flag.ContinueOnError)
	visible.SetOutput(output)
	for _, name := range options {
		option := flags.Lookup(name)
		visible.Var(option.Value, option.Name, option.Usage)
		visible.Lookup(name).DefValue = option.DefValue
	}
	fmt.Fprintln(output, "\nOptions:")
	visible.PrintDefaults()
}

func managedOptions(client pb.FleetClient, a managedArgs, out io.Writer) control.Options {
	return control.Options{FleetClient: client, RequiredServerTag: "tag:herdr-mesh-server", JSON: a.json, Output: out}
}

func selectManagedNode(list *pb.NodeList, selector string) (*pb.NodeView, error) {
	var matches []*pb.NodeView
	for _, node := range list.GetNodes() {
		if node != nil && node.InstanceId == selector {
			matches = append(matches, node)
		}
	}
	if len(matches) == 0 {
		for _, node := range list.GetNodes() {
			if node != nil && node.Hostname == selector {
				matches = append(matches, node)
			}
		}
	}
	if len(matches) != 1 {
		return nil, fmt.Errorf("node %q has %d matches; use an unambiguous logical name or exact ID", selector, len(matches))
	}
	node := matches[0]
	if !node.Connected || node.LastSeen == nil || node.LastSeen.CheckValid() != nil ||
		time.Since(node.LastSeen.AsTime()) > 30*time.Second || node.LastSeen.AsTime().After(time.Now().Add(5*time.Second)) {
		return nil, fmt.Errorf("node %q is offline or stale", selector)
	}
	return node, nil
}

func executeManaged(ctx context.Context, client pb.FleetClient, dir string, a managedArgs, streams IO) error {
	options := managedOptions(client, a, streams.Out)
	if a.root == "doctor" || a.root == "status" {
		local, err := meshlocal.ReadStatus(dir)
		if err != nil {
			return fmt.Errorf("read local daemon status: %w", err)
		}
		if a.json {
			data, serverErr := captureMCP(options, func(o control.Options) error { return control.ServerInfo(ctx, o) })
			writeErr := json.NewEncoder(streams.Out).Encode(map[string]any{"daemon": local, "server": data})
			if local.Error != "" {
				serverErr = errors.Join(serverErr, fmt.Errorf("managed daemon reports %s: %s", local.State, local.Error))
			}
			return errors.Join(serverErr, writeErr)
		} else {
			if _, err := fmt.Fprintf(streams.Out, "daemon=%s dns=%s server=%s error=%s\n", local.State, local.DNSName, local.Server, local.Error); err != nil {
				return err
			}
		}
		serverErr := control.ServerInfo(ctx, options)
		if local.Error != "" {
			return errors.Join(serverErr, fmt.Errorf("managed daemon reports %s: %s", local.State, local.Error))
		}
		return serverErr
	}
	list, err := client.ListNodes(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("list managed nodes: %w", err)
	}
	if list == nil || len(list.Nodes) > 128 {
		return errors.New("daemon returned an invalid or oversized node inventory")
	}
	for _, node := range list.Nodes {
		if node == nil || !protocol.ValidIdempotencyKey(node.InstanceId) {
			return errors.New("daemon returned an invalid node inventory entry")
		}
	}
	if a.root == "nodes" {
		if a.json {
			return managedWriteJSON(streams.Out, list)
		}
		for _, node := range list.GetNodes() {
			if _, err := fmt.Fprintf(streams.Out, "%s  %s  connected=%t stale=%t\n", node.Hostname, node.InstanceId, node.Connected, node.Stale); err != nil {
				return err
			}
		}
		return nil
	}
	node, err := selectManagedNode(list, a.node)
	if err != nil {
		return err
	}
	if a.root == "project" {
		request := &pb.RegisterProjectRequest{NodeInstanceId: node.InstanceId, ProjectId: a.name, CheckoutPath: a.path}
		intent, err := protojson.Marshal(request)
		if err != nil {
			return err
		}
		saved, err := managedPersist(ctx, dir, managedScope(a)+"-project", intent)
		if err != nil {
			return err
		}
		var original pb.RegisterProjectRequest
		if err := protojson.Unmarshal(saved, &original); err != nil || !proto.Equal(&original, request) {
			return errors.Join(err, errors.New("project add differs from its preserved original node/checkout; refusing a changed retry"))
		}
		data, err := captureMCP(options, func(o control.Options) error { return control.RegisterProject(ctx, o, request) })
		if err != nil {
			return err
		}
		record := new(pb.ProjectRecord)
		if err := protojson.Unmarshal(data, record); err != nil {
			return err
		}
		if record.GetDesired().GetCheckoutPath() != a.path {
			return errors.New("registered checkout differs from the requested existing checkout")
		}
		record, err = managedWaitProject(ctx, client, node.InstanceId, a.name, record)
		if err != nil {
			return err
		}
		if a.json {
			return managedWriteJSON(streams.Out, record)
		}
		_, err = fmt.Fprintf(streams.Out, "project %s applied on %s (existing checkout; generation %d)\n", a.name, a.node, record.Desired.Generation)
		return err
	}
	if a.verb == "start" {
		return managedStart(ctx, options, dir, node.InstanceId, a)
	}
	if a.verb == "stop" {
		saved, err := managedReadRequest(dir, managedScope(a)+"-stop")
		if err != nil {
			return err
		}
		if saved != nil {
			if saved.NodeInstanceId != node.InstanceId || saved.AgentStop.GetTarget().GetSessionName() != a.session {
				return errors.New("selected node/session differs from the preserved stop target")
			}
			return control.StopAgent(ctx, options, saved.NodeInstanceId, saved.IdempotencyKey, saved.AgentStop, saved.Ttl.AsDuration())
		}
	}
	record, err := client.ResolveNamedAgent(ctx, &pb.ResolveNamedAgentRequest{NodeInstanceId: node.InstanceId, Name: a.name, SessionName: a.session})
	if err != nil {
		return fmt.Errorf("resolve named agent: %w", err)
	}
	handle, err := managedHandle(record, node.InstanceId, a)
	if err != nil {
		return err
	}
	if err := managedCurrentSession(node, handle.Target); err != nil {
		return err
	}
	if a.verb == "follow" {
		return control.FollowAgent(ctx, options, &pb.AgentQueryRequest{NodeInstanceId: node.InstanceId,
			Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ, Target: handle.Target, Lines: 100}, time.Second)
	}
	request := &pb.SubmitCommandRequest{NodeInstanceId: node.InstanceId, CommandType: protocol.AgentStopCommandType,
		AgentStop: &pb.AgentStop{Target: handle.Target, WorkspaceId: handle.WorkspaceId, TabId: handle.TabId, Provider: handle.Provider}}
	request, err = managedSaveRequest(ctx, dir, managedScope(a)+"-stop", request)
	if err != nil {
		return err
	}
	return control.StopAgent(ctx, options, request.NodeInstanceId, request.IdempotencyKey, request.AgentStop, request.Ttl.AsDuration())
}

func managedWriteJSON(out io.Writer, value proto.Message) error {
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(data))
	return err
}

func managedPause(ctx context.Context) error {
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func managedWaitProject(ctx context.Context, client pb.FleetClient, node, project string, initial *pb.ProjectRecord) (*pb.ProjectRecord, error) {
	record := initial
	var desired *pb.ProjectConfig
	for {
		if record == nil {
			var err error
			record, err = client.GetProject(ctx, &pb.GetProjectRequest{NodeInstanceId: node, ProjectId: project})
			if err != nil {
				return nil, fmt.Errorf("resolve project: %w", err)
			}
		}
		if record.GetDesired().GetNodeInstanceId() != node || record.GetDesired().GetProjectId() != project ||
			record.GetDesired().GetGeneration() == 0 {
			return nil, errors.New("project response does not match selected node/project")
		}
		if desired == nil {
			desired = proto.Clone(record.Desired).(*pb.ProjectConfig)
		} else if !proto.Equal(desired, record.Desired) {
			return nil, errors.New("project configuration changed while waiting for application")
		}
		if record.Readiness == "applied" && record.GetApplied().GetGeneration() == desired.Generation && record.GetApplied().GetStatus() == "applied" {
			return record, nil
		}
		if record.Readiness != "pending" {
			return nil, fmt.Errorf("project is not ready: %s (%s)", record.Readiness, record.GetApplied().GetErrorCode())
		}
		if err := managedPause(ctx); err != nil {
			return nil, fmt.Errorf("wait for project application: %w", err)
		}
		record = nil
	}
}

func managedHandle(value *pb.NamedAgentRecord, node string, a managedArgs) (*pb.AgentLifecycleHandle, error) {
	if value == nil || value.Record == nil || len(value.ProtoReflect().GetUnknown()) != 0 ||
		value.Record.GetCommand().GetSubmittedRequest().GetAgentStart().GetInitialPrompt() != "" {
		return nil, errors.New("named agent returned an invalid redacted projection")
	}
	record := proto.Clone(value.Record).(*pb.CommandRecord)
	if value.InitialPromptPresent {
		if record.GetCommand().GetSubmittedRequest().GetAgentStart() == nil {
			return nil, errors.New("named agent prompt presence lacks an original start")
		}
		// Receipt validation needs presence, never the prompt contents. This
		// local validation-only copy is never persisted or submitted.
		record.Command.SubmittedRequest.AgentStart.InitialPrompt = "[redacted]"
	}
	if record == nil || record.Command == nil || record.Command.TargetId != node ||
		record.Command.AgentStart.GetName() != a.name || record.Command.AgentStart.GetSessionName() != a.session ||
		protocol.ValidateCommand(record.Command, node) != nil {
		return nil, errors.New("named agent returned a mismatched original start")
	}
	r := record.AgentLifecycle
	if r == nil || protocol.ValidateLifecycleReceipt(r, record.Command) != nil {
		return nil, fmt.Errorf("named agent has no confirmed handle (status %s); retry the original start to inspect its receipt", record.Status)
	}
	if r.PaneOutcome != "confirmed" || (a.verb != "stop" && (r.LaunchOutcome != "confirmed" || r.PromptOutcome == "unknown")) {
		return nil, fmt.Errorf("named agent has unresolved launch effects: pane=%s launch=%s prompt=%s; original receipt must be reconciled", r.PaneOutcome, r.LaunchOutcome, r.PromptOutcome)
	}
	h := r.Handle
	if h == nil || protocol.ValidateAgentStop(&pb.AgentStop{Target: h.Target, WorkspaceId: h.WorkspaceId, TabId: h.TabId, Provider: h.Provider}) != nil {
		return nil, errors.New("named agent receipt lacks the original full generation-qualified handle")
	}
	return proto.Clone(h).(*pb.AgentLifecycleHandle), nil
}

func managedScope(a managedArgs) string {
	sum := sha256.Sum256([]byte(a.node + "\x00" + a.session + "\x00" + a.name))
	return hex.EncodeToString(sum[:])
}

func managedCurrentSession(node *pb.NodeView, target *pb.AgentTarget) error {
	var found *pb.SessionView
	for _, session := range node.Sessions {
		if session.GetName() == target.SessionName {
			if found != nil {
				return errors.New("named session is ambiguous")
			}
			found = session
		}
	}
	if found == nil || found.Incarnation != target.SessionIncarnation || found.Status != "ready" || found.Stale ||
		found.Herdr.GetStatus() != "ready" || found.HerdrReceivedAt == nil || found.HerdrReceivedAt.CheckValid() != nil ||
		time.Since(found.HerdrReceivedAt.AsTime()) > 30*time.Second {
		return errors.New("original agent session is stale, replaced or unavailable; refusing to rediscover a newer target")
	}
	return nil
}

func managedReadRequest(dir, name string) (*pb.SubmitCommandRequest, error) {
	file, err := os.Open(filepath.Join(dir, "managed-requests", name+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 64*1024+1))
	if err != nil || len(data) > 64*1024 {
		return nil, errors.Join(err, errors.New("cannot read bounded original request"))
	}
	request := new(pb.SubmitCommandRequest)
	if err := protojson.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("invalid original request; refusing a fresh key: %w", err)
	}
	normalized, err := protocol.NormalizeCommandRequest(request)
	if err != nil || !proto.Equal(normalized, request) {
		return nil, errors.Join(err, errors.New("invalid original request fingerprint"))
	}
	return request, nil
}

// Immutable files fence interrupted writes: a truncated file is an explicit
// error, never permission to generate a new key. Close+Sync precede dispatch.
func managedPersist(ctx context.Context, dir, name string, data []byte) (result []byte, err error) {
	root := filepath.Join(dir, "managed-requests")
	guard, err := state.PrepareRoleState(ctx, root, "client")
	for errors.Is(err, state.ErrLocked) {
		if err := managedPause(ctx); err != nil {
			return nil, err
		}
		guard, err = state.PrepareRoleState(ctx, root, "client")
	}
	if err != nil {
		return nil, fmt.Errorf("lock managed request ledger: %w", err)
	}
	defer func() { err = errors.Join(err, guard.Close()) }()
	path := filepath.Join(root, name+".json")
	info, err := os.Lstat(path)
	if err == nil {
		if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 64*1024 {
			return nil, errors.New("managed request ledger is not a bounded regular file")
		}
		existing, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if len(existing) == 0 || len(existing) > 64*1024 || !json.Valid(existing) {
			return nil, errors.New("managed request ledger is incomplete or corrupt; refusing a fresh mutation key")
		}
		return existing, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	files, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	if len(files) >= 16384 {
		return nil, errors.New("managed request ledger capacity reached; original requests were not discarded")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return nil, fmt.Errorf("persist original mutation before dispatch: %w", err)
	}
	if runtime.GOOS != "windows" {
		directory, err := os.Open(root)
		if err != nil {
			return nil, err
		}
		err = errors.Join(directory.Sync(), directory.Close())
		if err != nil {
			return nil, fmt.Errorf("persist request directory before dispatch: %w", err)
		}
	}
	return data, nil
}

func managedSaveRequest(ctx context.Context, dir, name string, request *pb.SubmitCommandRequest) (*pb.SubmitCommandRequest, error) {
	request = proto.Clone(request).(*pb.SubmitCommandRequest)
	request.IdempotencyKey, request.Ttl = protocol.NewCommandID(), durationpb.New(protocol.DefaultCommandTTL)
	request, err := protocol.NormalizeCommandRequest(request)
	if err != nil {
		return nil, err
	}
	data, err := protojson.Marshal(request)
	if err != nil {
		return nil, err
	}
	data, err = managedPersist(ctx, dir, name, data)
	if err != nil {
		return nil, err
	}
	original := new(pb.SubmitCommandRequest)
	if err := protojson.Unmarshal(data, original); err != nil {
		return nil, fmt.Errorf("read original managed mutation: %w", err)
	}
	request.IdempotencyKey = original.IdempotencyKey
	if !proto.Equal(request, original) {
		return nil, errors.New("managed operation differs from its preserved original request; refusing changed targets or a fresh retry key")
	}
	return original, nil
}

func managedCapture(options control.Options, call func(control.Options) error) (*pb.CommandRecord, error) {
	data, callErr := captureMCP(options, call)
	if len(data) == 0 {
		return nil, callErr
	}
	record := new(pb.CommandRecord)
	if err := protojson.Unmarshal(data, record); err != nil {
		return nil, errors.Join(callErr, err)
	}
	return record, callErr
}

func managedStart(ctx context.Context, options control.Options, dir, node string, a managedArgs) error {
	// Persist the complete operator intent and node binding before setup. Retrying
	// a logical name after it moves to a new node cannot retarget the workflow.
	intent := struct {
		Node, Project, Name, Session, Provider, Prompt string
	}{node, a.project, a.name, a.session, a.provider, a.prompt}
	data, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	scope := managedScope(a)
	saved, err := managedPersist(ctx, dir, scope+"-intent", data)
	if err != nil {
		return err
	}
	if string(saved) != string(data) {
		return errors.New("agent start differs from its preserved original intent; refusing a duplicate launch")
	}
	original, err := managedReadRequest(dir, scope+"-start")
	if err != nil {
		return err
	}
	if original != nil {
		if original.NodeInstanceId != node || original.AgentStart.GetName() != a.name ||
			original.AgentStart.GetSessionName() != a.session || original.AgentStart.GetProjectId() != a.project ||
			original.AgentStart.GetProvider() != a.provider || original.AgentStart.GetInitialPrompt() != a.prompt {
			return errors.New("original start request disagrees with preserved operator intent")
		}
		return control.StartAgent(ctx, options, node, original.IdempotencyKey, original.AgentStart, original.Ttl.AsDuration())
	}
	if existing, err := options.FleetClient.ResolveNamedAgent(ctx, &pb.ResolveNamedAgentRequest{NodeInstanceId: node, Name: a.name, SessionName: a.session}); err == nil {
		return fmt.Errorf("agent name already has a durable start (%s); use agent follow or stop; no new launch was submitted", existing.GetRecord().GetStatus())
	} else if status.Code(err) != codes.NotFound {
		return fmt.Errorf("resolve original named start before setup: %w", err)
	}
	request, err := managedSaveRequest(ctx, dir, scope+"-session", &pb.SubmitCommandRequest{
		NodeInstanceId: node, CommandType: protocol.SessionEnsureCommandType, SessionEnsure: &pb.SessionEnsure{Name: a.session}})
	if err != nil {
		return err
	}
	session, err := managedCapture(options, func(o control.Options) error {
		return control.EnsureSession(ctx, o, node, request.IdempotencyKey, a.session, request.Ttl.AsDuration())
	})
	if err != nil {
		return fmt.Errorf("ensure session using preserved request: %w", err)
	}
	if session.GetCommand().GetIdempotencyKey() != request.IdempotencyKey || session.GetCommand().GetTargetId() != node ||
		session.GetSessionEnsure().GetName() != a.session || protocol.ValidateSessionEnsureResult(&pb.CommandResult{
		CommandId: session.GetCommand().GetCommandId(), Status: session.GetStatus(), Detail: session.GetDetail(), SessionEnsure: session.GetSessionEnsure()}) != nil {
		return errors.New("session ensure returned a mismatched or invalid receipt")
	}
	project, err := managedWaitProject(ctx, options.FleetClient, node, a.project, nil)
	if err != nil {
		return err
	}
	request, err = managedSaveRequest(ctx, dir, scope+"-workspace", &pb.SubmitCommandRequest{NodeInstanceId: node,
		CommandType: protocol.WorkspaceEnsureCommandType, WorkspaceEnsure: &pb.WorkspaceEnsure{ProjectId: a.project,
			BindingRevision: protocol.ProjectRevision(project.Desired.Generation), SessionName: a.session, SessionIncarnation: session.SessionEnsure.Incarnation}})
	if err != nil {
		return err
	}
	workspace, err := managedCapture(options, func(o control.Options) error {
		return control.EnsureWorkspaceInSession(ctx, o, node, request.IdempotencyKey, request.WorkspaceEnsure, request.Ttl.AsDuration())
	})
	if err != nil {
		return fmt.Errorf("ensure existing checkout workspace using preserved request: %w", err)
	}
	w := workspace.GetWorkspaceEnsure()
	if workspace.GetCommand().GetIdempotencyKey() != request.IdempotencyKey || workspace.GetCommand().GetTargetId() != node ||
		w == nil || w.ProjectId != a.project || w.BindingRevision != request.WorkspaceEnsure.BindingRevision ||
		w.SessionName != a.session || w.SessionIncarnation != session.SessionEnsure.Incarnation || !protocol.ValidIdempotencyKey(w.WorkspaceId) {
		return errors.New("workspace ensure returned a mismatched or invalid pinned workspace")
	}
	if err := protocol.ValidateResultForCommand(&pb.CommandResult{CommandId: workspace.Command.CommandId, Status: workspace.Status,
		Detail: workspace.Detail, WorkspaceEnsure: w}, workspace.Command); err != nil {
		return fmt.Errorf("invalid existing checkout workspace receipt: %w", err)
	}
	request, err = managedSaveRequest(ctx, dir, scope+"-start", &pb.SubmitCommandRequest{NodeInstanceId: node,
		CommandType: protocol.AgentStartCommandType, AgentStart: &pb.AgentStart{ProjectId: a.project, BindingRevision: w.BindingRevision,
			WorkspaceId: w.WorkspaceId, Name: a.name, Provider: a.provider, SessionName: a.session, SessionIncarnation: w.SessionIncarnation,
			StartupTimeoutMs: protocol.DefaultAgentStartupTimeoutMs, InitialPrompt: a.prompt}})
	if err != nil {
		return err
	}
	return control.StartAgent(ctx, options, node, request.IdempotencyKey, request.AgentStart, request.Ttl.AsDuration())
}
