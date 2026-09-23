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
