package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc"
)

type commandClient struct {
	agentflowv1.FleetClient
	get func(context.Context, *agentflowv1.GetCommandRequest) (*agentflowv1.CommandRecord, error)
}

func (c commandClient) GetCommand(ctx context.Context, r *agentflowv1.GetCommandRequest, _ ...grpc.CallOption) (*agentflowv1.CommandRecord, error) {
	return c.get(ctx, r)
}

func TestWaitForCommandAndJSON(t *testing.T) {
	id := protocol.NewCommandID()
	initial := &agentflowv1.CommandRecord{Command: &agentflowv1.Command{CommandId: id}, Status: agentflowv1.CommandStatus_COMMAND_STATUS_ACCEPTED}
	client := commandClient{get: func(ctx context.Context, r *agentflowv1.GetCommandRequest) (*agentflowv1.CommandRecord, error) {
		if r.CommandId != id {
			t.Error("poll changed command ID")
		}
		return &agentflowv1.CommandRecord{Command: initial.Command, Status: agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "pong"}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := waitCommand(ctx, client, initial)
	if err != nil || result.Status != agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		t.Fatal("command did not finish", err)
	}
	var output bytes.Buffer
	if err := writeCommand(Options{JSON: true, Output: &output}, result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"command_id"`) || !strings.Contains(output.String(), `"pong"`) {
		t.Fatal("incorrect JSON output")
	}
}

func TestCommandTimeoutPreservesIdentity(t *testing.T) {
	id := protocol.NewCommandID()
	initial := &agentflowv1.CommandRecord{Command: &agentflowv1.Command{CommandId: id}, Status: agentflowv1.CommandStatus_COMMAND_STATUS_RUNNING}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := waitCommand(ctx, commandClient{}, initial)
	if !errors.Is(err, context.Canceled) || result.Command.CommandId != id {
		t.Fatal("timeout lost recovery command ID")
	}
	if _, err := waitCommand(context.Background(), commandClient{}, nil); err == nil {
		t.Fatal("nil record accepted")
	}
	if err := writeCommand(Options{JSON: true, Output: failedWriter{}}, initial); err == nil {
		t.Fatal("output error suppressed")
	}
}

func TestWorkspaceCommandOutputIsOneTypedObject(t *testing.T) {
	record := &agentflowv1.CommandRecord{
		Command: &agentflowv1.Command{CommandId: protocol.NewCommandID(), TargetId: "node-1", IdempotencyKey: "ensure-1", CommandType: protocol.WorkspaceEnsureCommandType},
		Status:  agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "workspace_present",
		WorkspaceEnsure: &agentflowv1.WorkspaceEnsureResult{ProjectId: "AgentFlow", BindingRevision: "r1", WorkspaceId: "w1"},
	}
	var output bytes.Buffer
	if err := writeCommand(Options{JSON: true, Output: &output}, record); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&output)
	var value struct {
		Workspace struct {
			ProjectID   string `json:"project_id"`
			WorkspaceID string `json:"workspace_id"`
			Created     bool   `json:"created"`
		} `json:"workspace_ensure"`
	}
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	if value.Workspace.ProjectID != "AgentFlow" || value.Workspace.WorkspaceID != "w1" || value.Workspace.Created {
		t.Fatal("incorrect typed workspace JSON")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatal("stdout was not a single JSON object")
	}
	if err := writeCommand(Options{Output: &output}, record); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Project: AgentFlow\nBinding revision: r1\nWorkspace: w1\nCreated: false\n") {
		t.Fatal("workspace human output missing identifiers")
	}
}

func TestRetryIdentityPrecedesAmbiguousAgentSubmission(t *testing.T) {
	for _, suppliedKey := range []string{"", "original-key"} {
		for _, failure := range []error{context.Canceled, context.DeadlineExceeded} {
			t.Run(suppliedKey+"/"+failure.Error(), func(t *testing.T) {
				var stdout, stderr bytes.Buffer
				view := agentClientView()
				view.Target.SessionName, view.Target.SessionIncarnation = "Worker", strings.Repeat("a", 64)
				target := &agentflowv1.AgentTarget{PaneId: view.Target.PaneId, SessionName: "Worker"}
				var submitted *agentflowv1.SubmitCommandRequest
				client := agentClientFixture{
					query: func(context.Context, *agentflowv1.AgentQueryRequest) (*agentflowv1.AgentQueryResult, error) {
						return &agentflowv1.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: view}, nil
					},
					submit: func(_ context.Context, request *agentflowv1.SubmitCommandRequest) (*agentflowv1.CommandRecord, error) {
						submitted = request
						for _, value := range []string{request.IdempotencyKey, request.NodeInstanceId,
							view.Target.TerminalId, view.Target.AgentSessionId, view.Target.SessionName, view.Target.SessionIncarnation} {
							if !strings.Contains(stderr.String(), value) {
								t.Errorf("recovery value %q missing before submission: %s", value, stderr.String())
							}
						}
						return nil, failure
					},
				}
				err := Agent(context.Background(), Options{FleetClient: client, RequiredServerTag: "tag:server",
					JSON: true, Output: &stdout, RetryOutput: &stderr},
					&agentflowv1.AgentQueryRequest{NodeInstanceId: "node-1", Kind: agentflowv1.AgentQueryKind_AGENT_QUERY_KIND_GET, Target: target},
					&agentflowv1.AgentControl{Action: agentflowv1.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, Target: target, Text: "private task"},
					suppliedKey, time.Second)
				if !errors.Is(err, failure) || submitted == nil || !strings.Contains(err.Error(), submitted.IdempotencyKey) {
					t.Fatalf("ambiguous failure lost key/cause: %v", err)
				}
				if stdout.Len() != 0 || strings.Contains(stderr.String(), "private task") {
					t.Fatalf("receipt polluted JSON stdout or disclosed prompt: stdout=%s stderr=%s", &stdout, &stderr)
				}
				if suppliedKey != "" && submitted.IdempotencyKey != suppliedKey {
					t.Fatal("retry changed supplied key")
				}
			})
		}
	}
}

func TestRetryIdentityWriteFailurePreventsSubmission(t *testing.T) {
	called := false
	client := agentClientFixture{submit: func(context.Context, *agentflowv1.SubmitCommandRequest) (*agentflowv1.CommandRecord, error) {
		called = true
		return nil, errors.New("unexpected submission")
	}}
	err := Ping(context.Background(), Options{FleetClient: client, RequiredServerTag: "tag:server",
		Output: io.Discard, RetryOutput: failedFollowWriter{}}, "node", "original-key", time.Second)
	if !errors.Is(err, io.ErrClosedPipe) || called {
		t.Fatalf("mutation proceeded without recovery output: called=%t err=%v", called, err)
	}
}

func TestGeneratedLifecycleKeyPrintedBeforeAmbiguousSubmission(t *testing.T) {
	for _, stop := range []bool{false, true} {
		var stdout, stderr bytes.Buffer
		var submitted *agentflowv1.SubmitCommandRequest
		client := agentClientFixture{submit: func(_ context.Context, request *agentflowv1.SubmitCommandRequest) (*agentflowv1.CommandRecord, error) {
			submitted = request
			if request.IdempotencyKey == "" || !strings.Contains(stderr.String(), request.IdempotencyKey) {
				t.Errorf("generated lifecycle key unavailable before dispatch: %s", &stderr)
			}
			return nil, context.DeadlineExceeded
		}}
		options := Options{FleetClient: client, RequiredServerTag: "tag:server", JSON: true, Output: &stdout, RetryOutput: &stderr}
		var err error
		if stop {
			target := agentClientView().Target
			target.SessionIncarnation = strings.Repeat("a", 64)
			err = StopAgent(context.Background(), options, "node", "", &agentflowv1.AgentStop{
				Target: target, WorkspaceId: "w1", TabId: "w1:t1", Provider: "copilot",
			}, time.Second)
		} else {
			err = StartAgent(context.Background(), options, "node", "", &agentflowv1.AgentStart{
				ProjectId: "project", WorkspaceId: "w1", Name: "worker", Provider: "copilot",
			}, time.Second)
		}
		if !errors.Is(err, context.DeadlineExceeded) || submitted == nil || stdout.Len() != 0 {
			t.Fatalf("lifecycle ambiguous failure changed: submitted=%v stdout=%s err=%v", submitted != nil, &stdout, err)
		}
	}
}
