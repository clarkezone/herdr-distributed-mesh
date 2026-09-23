package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/dashboard"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type managedFleetFixture struct {
	*mcpFleetFixture
	resolve func(context.Context, *pb.ResolveNamedAgentRequest) (*pb.NamedAgentRecord, error)
}

func (f *managedFleetFixture) ResolveNamedAgent(ctx context.Context, r *pb.ResolveNamedAgentRequest, _ ...grpc.CallOption) (*pb.NamedAgentRecord, error) {
	if f.resolve == nil {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return f.resolve(ctx, r)
}

func managedNamedProjection(record *pb.CommandRecord) *pb.NamedAgentRecord {
	record = proto.Clone(record).(*pb.CommandRecord)
	value := &pb.NamedAgentRecord{Record: record}
	if start := record.GetCommand().GetSubmittedRequest().GetAgentStart(); start != nil {
		value.InitialPromptPresent = start.InitialPrompt != ""
		start.InitialPrompt = ""
	}
	return value
}

func TestManagedDefaultsAndHelpDoNotDial(t *testing.T) {
	a, err := parseManaged([]string{"agent", "start", "smoke", "--node", "laptop", "--project", "demo", "--prompt", "Say hello"}, IO{Err: io.Discard})
	if err != nil || a.provider != "copilot" || a.session != "main" || a.name != "smoke" || a.prompt != "Say hello" {
		t.Fatalf("ergonomic defaults: %+v %v", a, err)
	}
	for _, args := range [][]string{{"nodes", "-h"}, {"doctor", "-h"}, {"agent", "start", "-h"}, {"project", "-h"}, {"dashboard", "-h"}, {"mcp", "-h"}} {
		handled, err := runManagedCommands(context.Background(), args, IO{Out: io.Discard, Err: io.Discard})
		if !handled || err == nil || !strings.Contains(err.Error(), "help requested") {
			t.Fatalf("offline help %v: handled=%t err=%v", args, handled, err)
		}
	}
	for _, args := range [][]string{{"doctor", "--server=x"}, {"dashboard", "-server", "x"}, {"agent", "start", "--server", "x"}} {
		if handled, err := runManagedCommands(context.Background(), args, IO{}); handled || err != nil {
			t.Fatalf("advanced route intercepted: %v", args)
		}
	}
}

func TestManagedNodeSelectionUniqueOfflineStaleAmbiguous(t *testing.T) {
	node := &pb.NodeView{InstanceId: "node-1", Hostname: "laptop", Connected: true, LastSeen: timestamppb.Now()}
	list := &pb.NodeList{Nodes: []*pb.NodeView{node}}
	for _, selector := range []string{"laptop", "node-1"} {
		if got, err := selectManagedNode(list, selector); err != nil || got != node {
			t.Fatalf("unique selector: %v", err)
		}
	}
	if _, err := selectManagedNode(list, "unknown"); err == nil {
		t.Fatal("unknown selector accepted")
	}
	node.Connected = false
	if _, err := selectManagedNode(list, "laptop"); err == nil {
		t.Fatal("offline selector accepted")
	}
	node.Connected, node.LastSeen = true, timestamppb.New(time.Now().Add(-time.Hour))
	if _, err := selectManagedNode(list, "laptop"); err == nil {
		t.Fatal("stale selector accepted")
	}
	node.LastSeen = timestamppb.Now()
	list.Nodes = append(list.Nodes, &pb.NodeView{InstanceId: "other", Hostname: "laptop"})
	if _, err := selectManagedNode(list, "laptop"); err == nil {
		t.Fatal("ambiguous logical name accepted")
	}
}

func TestManagedStartExistingCheckoutDefaultsAndCancellationExactRetry(t *testing.T) {
	a, err := parseManaged([]string{"agent", "start", "smoke", "--node", "laptop", "--project", "demo", "--prompt", "Say hello"}, IO{Err: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	dir := maintenanceAppTempDir(t)
	var requests []*pb.SubmitCommandRequest
	cancelStart := true
	client := &managedFleetFixture{mcpFleetFixture: &mcpFleetFixture{
		project: func(*pb.GetProjectRequest) (*pb.ProjectRecord, error) {
			return &pb.ProjectRecord{Desired: &pb.ProjectConfig{NodeInstanceId: "node-1", ProjectId: "demo", Generation: 1},
				Applied: &pb.ProjectAck{ProjectId: "demo", Generation: 1, Status: "applied"}, Readiness: "applied"}, nil
		},
		submit: func(_ context.Context, r *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
			requests = append(requests, proto.Clone(r).(*pb.SubmitCommandRequest))
			record := mcpTestReceipt(r, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
			switch r.CommandType {
			case protocol.SessionEnsureCommandType:
				record.Detail = "session_ready"
				record.SessionEnsure = &pb.SessionView{Name: "main", Incarnation: strings.Repeat("b", 64), Status: "ready"}
			case protocol.WorkspaceEnsureCommandType:
				record.Detail = "workspace_present"
				record.WorkspaceEnsure = &pb.WorkspaceEnsureResult{ProjectId: "demo", BindingRevision: "managed:1", WorkspaceId: "ws:1",
					SessionName: "main", SessionIncarnation: strings.Repeat("b", 64)}
			case protocol.AgentStartCommandType:
				if cancelStart {
					return nil, context.Canceled
				}
				return mcpLifecycleRecord(t, r, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED), nil
			default:
				t.Fatalf("implicit unsupported mutation, including worktree: %s", r.CommandType)
			}
			return record, nil
		},
	}}
	var out bytes.Buffer
	options := managedOptions(client, a, &out)
	if err := managedStart(context.Background(), options, dir, "node-1", a); !errors.Is(err, context.Canceled) {
		t.Fatalf("first cancelled start: %v", err)
	}
	if len(requests) != 3 {
		t.Fatalf("unexpected setup sequence: %v", requests)
	}
	original := requests[2]
	if original.AgentStart.Provider != "copilot" || original.AgentStart.SessionName != "main" ||
		original.AgentStart.Name != "smoke" || original.AgentStart.WorkspaceId != "ws:1" || original.AgentStart.InitialPrompt != "Say hello" {
		t.Fatalf("wrong original start: %v", original)
	}
	cancelStart = false
	// A fresh controller object and no project service proves restart retries
	// the saved start directly rather than selecting a new workspace generation.
	client.project = nil
	if err := managedStart(context.Background(), options, dir, "node-1", a); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 4 || !proto.Equal(original, requests[3]) {
		t.Fatal("retry changed the key, target or original request")
	}
	a.prompt = "different"
	if err := managedStart(context.Background(), options, dir, "node-1", a); err == nil || len(requests) != 4 {
		t.Fatal("changed intent silently duplicated launch")
	}
}

func TestManagedNameHandleRefusesUnknownAndPreservesOptionalProviderSession(t *testing.T) {
	a := managedArgs{name: "smoke", session: "main"}
	r, err := protocol.NormalizeCommandRequest(&pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "start-key",
		CommandType: protocol.AgentStartCommandType, AgentStart: &pb.AgentStart{Name: "smoke", ProjectId: "demo",
			BindingRevision: "managed:1", WorkspaceId: "ws:1", Provider: "copilot", SessionName: "main",
			SessionIncarnation: strings.Repeat("b", 64), InitialPrompt: "private initial task"}})
	if err != nil {
		t.Fatal(err)
	}
	unknown := mcpLifecycleRecord(t, r, pb.CommandStatus_COMMAND_STATUS_INDETERMINATE)
	if _, err := managedHandle(managedNamedProjection(unknown), "node-1", a); err == nil || !strings.Contains(err.Error(), "not fully confirmed") {
		t.Fatalf("unknown launch not fenced: %v", err)
	}
	for _, providerSession := range []string{"", "provider-session-original"} {
		record := mcpLifecycleRecord(t, r, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
		record.AgentLifecycle.Handle.Target.AgentSessionId = providerSession
		handle, err := managedHandle(managedNamedProjection(record), "node-1", a)
		if err != nil || handle.Target.AgentSessionId != providerSession || handle.Target.SessionIncarnation != strings.Repeat("b", 64) {
			t.Fatalf("full original handle lost: %v %v", handle, err)
		}
		node := &pb.NodeView{Sessions: []*pb.SessionView{{Name: "main", Incarnation: strings.Repeat("c", 64),
			Status: "ready", Herdr: &pb.HerdrState{Status: "ready"}, HerdrReceivedAt: timestamppb.Now()}}}
		if err := managedCurrentSession(node, handle.Target); err == nil {
			t.Fatal("replacement incarnation accepted")
		}
	}
}

func TestManagedLedgerCancellationAndChangedTargets(t *testing.T) {
	dir := maintenanceAppTempDir(t)
	request := &pb.SubmitCommandRequest{NodeInstanceId: "node-1", CommandType: protocol.SessionEnsureCommandType, SessionEnsure: &pb.SessionEnsure{Name: "main"}}
	first, err := managedSaveRequest(context.Background(), dir, "session", request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := managedSaveRequest(context.Background(), dir, "session", request)
	if err != nil || !proto.Equal(first, second) {
		t.Fatalf("durable exact retry failed: %v", err)
	}
	request.NodeInstanceId = "replacement"
	if _, err := managedSaveRequest(context.Background(), dir, "session", request); err == nil {
		t.Fatal("replacement target silently accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := managedSaveRequest(ctx, dir, "cancelled", request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ledger write: %v", err)
	}
}

func TestManagedLedgerIncompleteWriteNeverGeneratesAnotherKey(t *testing.T) {
	dir := maintenanceAppTempDir(t)
	_, err := managedPersist(context.Background(), dir, "request", []byte(`{"partial":`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := managedReadRequest(dir, "request"); err == nil {
		t.Fatal("partial durable write silently became a new request")
	}
	if _, err := managedPersist(context.Background(), dir, "request", []byte(`{"fresh":"key"}`)); err == nil {
		t.Fatal("partial durable write was overwritten")
	}
	data, err := os.ReadFile(filepath.Join(dir, "managed-requests", "request.json"))
	if err != nil || string(data) != `{"partial":` {
		t.Fatalf("original uncertainty fence changed: %q %v", data, err)
	}
}

func TestManagedStartCrossClientNameDoesNotSetUpAnotherPane(t *testing.T) {
	a := managedArgs{node: "laptop", project: "demo", name: "smoke", session: "main", provider: "copilot"}
	client := &managedFleetFixture{mcpFleetFixture: &mcpFleetFixture{}, resolve: func(context.Context, *pb.ResolveNamedAgentRequest) (*pb.NamedAgentRecord, error) {
		return &pb.NamedAgentRecord{Record: &pb.CommandRecord{Status: pb.CommandStatus_COMMAND_STATUS_RUNNING}}, nil
	}}
	err := managedStart(context.Background(), managedOptions(client, a, io.Discard), maintenanceAppTempDir(t), "node-1", a)
	if err == nil || !strings.Contains(err.Error(), "already has a saved start request") {
		t.Fatalf("another controller's pending start bypassed: %v", err)
	}
}

func TestManagedStopCrossClientUsesOriginalHandleAndDurableRetry(t *testing.T) {
	a := managedArgs{root: "agent", verb: "stop", node: "laptop", name: "smoke", session: "main", provider: "copilot"}
	start, err := protocol.NormalizeCommandRequest(&pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "original-start",
		CommandType: protocol.AgentStartCommandType, AgentStart: &pb.AgentStart{Name: "smoke", ProjectId: "demo", BindingRevision: "managed:1",
			WorkspaceId: "ws:1", Provider: "copilot", SessionName: "main", SessionIncarnation: strings.Repeat("b", 64)}})
	if err != nil {
		t.Fatal(err)
	}
	original := mcpLifecycleRecord(t, start, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	original.AgentLifecycle.Handle.Target.AgentSessionId = "provider-original"
	var requests []*pb.SubmitCommandRequest
	client := &managedFleetFixture{mcpFleetFixture: &mcpFleetFixture{
		nodes: func(context.Context) (*pb.NodeList, error) {
			return &pb.NodeList{Nodes: []*pb.NodeView{{InstanceId: "node-1", Hostname: "laptop", Connected: true, LastSeen: timestamppb.Now(),
				Sessions: []*pb.SessionView{{Name: "main", Incarnation: strings.Repeat("b", 64), Status: "ready",
					Herdr: &pb.HerdrState{Status: "ready"}, HerdrReceivedAt: timestamppb.Now()}}}}}, nil
		},
		submit: func(_ context.Context, r *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
			requests = append(requests, proto.Clone(r).(*pb.SubmitCommandRequest))
			return mcpLifecycleRecord(t, r, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED), nil
		},
	}, resolve: func(context.Context, *pb.ResolveNamedAgentRequest) (*pb.NamedAgentRecord, error) {
		return managedNamedProjection(original), nil
	}}
	dir := maintenanceAppTempDir(t)
	for range 2 {
		if err := executeManaged(context.Background(), client, dir, a, IO{Out: io.Discard, Err: io.Discard}); err != nil {
			t.Fatal(err)
		}
		client.resolve = func(context.Context, *pb.ResolveNamedAgentRequest) (*pb.NamedAgentRecord, error) {
			t.Fatal("retry rediscovered a different target")
			return nil, nil
		}
	}
	if len(requests) != 2 || !proto.Equal(requests[0], requests[1]) ||
		!proto.Equal(requests[0].AgentStop.Target, original.AgentLifecycle.Handle.Target) ||
		requests[0].AgentStop.WorkspaceId != original.AgentLifecycle.Handle.WorkspaceId ||
		requests[0].AgentStop.TabId != original.AgentLifecycle.Handle.TabId {
		t.Fatalf("stop did not preserve full original target: %v", requests)
	}
}

func TestManagedProjectWaitCancellationAndGenerationFence(t *testing.T) {
	client := &managedFleetFixture{mcpFleetFixture: &mcpFleetFixture{project: func(*pb.GetProjectRequest) (*pb.ProjectRecord, error) {
		return &pb.ProjectRecord{Desired: &pb.ProjectConfig{NodeInstanceId: "node", ProjectId: "demo", Generation: 1}, Readiness: "pending"}, nil
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := managedWaitProject(ctx, client, "node", "demo", nil, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("project wait ignored cancellation: %v", err)
	}
	client.project = func(*pb.GetProjectRequest) (*pb.ProjectRecord, error) {
		return &pb.ProjectRecord{Desired: &pb.ProjectConfig{NodeInstanceId: "node", ProjectId: "demo", Generation: 2}, Readiness: "applied"}, nil
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := managedWaitProject(ctx, client, "node", "demo", &pb.ProjectRecord{
		Desired: &pb.ProjectConfig{NodeInstanceId: "node", ProjectId: "demo", Generation: 1}, Readiness: "pending"}, false)
	if err == nil || !strings.Contains(err.Error(), "configuration changed") {
		t.Fatalf("generation changed while waiting: %v", err)
	}
}

func TestManagedDashboardBorrowsFleetWithoutTransport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	err := dashboard.Run(ctx, dashboard.Options{FleetClient: &managedFleetFixture{mcpFleetFixture: &mcpFleetFixture{}},
		ListenAddress: "127.0.0.1:0", Output: &out})
	if err != nil || !strings.Contains(out.String(), "dashboard ready:") {
		t.Fatalf("borrowed dashboard attempted independent transport: %v %s", err, out.String())
	}
}

func TestManagedLongStartReleasesLedgerForIndependentNamedStop(t *testing.T) {
	for _, phase := range []string{"submit", "wait"} {
		t.Run(phase, func(t *testing.T) {
			dir := maintenanceAppTempDir(t)
			startArgs := managedArgs{root: "agent", verb: "start", node: "laptop", name: "new-agent",
				project: "demo", session: "main", provider: "copilot"}
			stopArgs := managedArgs{root: "agent", verb: "stop", node: "laptop", name: "existing-agent",
				session: "main", provider: "copilot"}
			original, err := protocol.NormalizeCommandRequest(&pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "existing-start",
				CommandType: protocol.AgentStartCommandType, AgentStart: &pb.AgentStart{Name: stopArgs.name, ProjectId: "demo",
					BindingRevision: "managed:1", WorkspaceId: "ws:1", Provider: "copilot", SessionName: "main",
					SessionIncarnation: strings.Repeat("b", 64)}})
			if err != nil {
				t.Fatal(err)
			}
			existing := managedNamedProjection(mcpLifecycleRecord(t, original, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED))
			blocked, release := make(chan struct{}), make(chan struct{})
			var completedStart *pb.CommandRecord
			blockStart := func(ctx context.Context) (*pb.CommandRecord, error) {
				close(blocked)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-release:
					return completedStart, nil
				}
			}
			client := &managedFleetFixture{mcpFleetFixture: &mcpFleetFixture{
				nodes: func(context.Context) (*pb.NodeList, error) {
					return &pb.NodeList{Nodes: []*pb.NodeView{{InstanceId: "node-1", Hostname: "laptop", Connected: true, LastSeen: timestamppb.Now(),
						Sessions: []*pb.SessionView{{Name: "main", Incarnation: strings.Repeat("b", 64), Status: "ready",
							Herdr: &pb.HerdrState{Status: "ready"}, HerdrReceivedAt: timestamppb.Now()}}}}}, nil
				},
				project: func(*pb.GetProjectRequest) (*pb.ProjectRecord, error) {
					return &pb.ProjectRecord{Desired: &pb.ProjectConfig{NodeInstanceId: "node-1", ProjectId: "demo", Generation: 1},
						Applied: &pb.ProjectAck{ProjectId: "demo", Generation: 1, Status: "applied"}, Readiness: "applied"}, nil
				},
			}, resolve: func(_ context.Context, request *pb.ResolveNamedAgentRequest) (*pb.NamedAgentRecord, error) {
				if request.Name == stopArgs.name {
					return existing, nil
				}
				return nil, status.Error(codes.NotFound, "not found")
			}}
			client.submit = func(ctx context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
				record := mcpTestReceipt(request, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
				switch request.CommandType {
				case protocol.SessionEnsureCommandType:
					record.Detail = "session_ready"
					record.SessionEnsure = &pb.SessionView{Name: "main", Incarnation: strings.Repeat("b", 64), Status: "ready"}
				case protocol.WorkspaceEnsureCommandType:
					record.Detail = "workspace_present"
					record.WorkspaceEnsure = &pb.WorkspaceEnsureResult{ProjectId: "demo", BindingRevision: "managed:1",
						WorkspaceId: "ws:1", SessionName: "main", SessionIncarnation: strings.Repeat("b", 64)}
				case protocol.AgentStartCommandType:
					completedStart = mcpLifecycleRecord(t, request, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
					if phase == "submit" {
						return blockStart(ctx)
					}
					record = proto.Clone(completedStart).(*pb.CommandRecord)
					record.Status, record.Detail, record.AgentLifecycle = pb.CommandStatus_COMMAND_STATUS_ACCEPTED, "accepted", nil
				case protocol.AgentStopCommandType:
					return mcpLifecycleRecord(t, request, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED), nil
				default:
					return nil, errors.New("unexpected managed mutation")
				}
				return record, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			client.command = func(*pb.GetCommandRequest) (*pb.CommandRecord, error) { return blockStart(ctx) }
			startDone, finished := make(chan error, 1), make(chan struct{})
			go func() {
				defer close(finished)
				startDone <- executeManaged(ctx, client, dir, startArgs, IO{Out: io.Discard, Err: io.Discard})
			}()
			defer func() {
				cancel()
				<-finished
			}()
			select {
			case <-blocked:
			case err := <-startDone:
				t.Fatalf("start ended before reaching long %s: %v", phase, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			stopCtx, stopCancel := context.WithTimeout(ctx, 3*time.Second)
			stopErr := executeManaged(stopCtx, client, dir, stopArgs, IO{Out: io.Discard, Err: io.Discard})
			stopCancel()
			if stopErr != nil {
				t.Fatalf("independent stop waited behind long start %s or failed ledger ownership: %v", phase, stopErr)
			}
			if saved, err := managedReadRequest(dir, managedScope(stopArgs)+"-stop"); err != nil || saved == nil || saved.AgentStop == nil {
				t.Fatalf("stop completed without its durable request: %v %v", saved, err)
			}
			select {
			case err := <-startDone:
				t.Fatalf("start was not still blocked while stop completed: %v", err)
			default:
			}
			close(release)
			if err := <-startDone; err != nil {
				t.Fatalf("start did not complete after independent stop: %v", err)
			}
		})
	}
}
