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
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

func TestWorktreeClientRejectsMutableBaseBeforeDial(t *testing.T) {
	err := CreateWorktree(context.Background(), Options{RequiredServerTag: "tag:server"}, "node-1", "key",
		&pb.WorktreeCreate{ProjectId: "p", BindingRevision: "r", Name: "task", Branch: "task", BaseCommit: "HEAD"}, time.Second)
	if err == nil || !strings.Contains(err.Error(), "commit ID") {
		t.Fatalf("invalid base was not rejected locally: %v", err)
	}
}

func TestWorktreeOutputIsOneTypedObject(t *testing.T) {
	result := &pb.WorktreeCreateResult{ProjectId: "p", BindingRevision: "r2", WorkspaceId: "w2", Name: "task", Branch: "task", BaseCommit: strings.Repeat("a", 40)}
	record := &pb.CommandRecord{Command: &pb.Command{CommandId: protocol.NewCommandID()}, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "worktree_created", WorktreeCreate: result}
	var output bytes.Buffer
	if err := writeCommand(Options{Output: &output, JSON: true}, record); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&output)
	var value struct {
		Result struct {
			Project   string `json:"project_id"`
			Base      string `json:"base_commit"`
			Workspace string `json:"workspace_id"`
		} `json:"worktree_create"`
	}
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	if value.Result.Project != "p" || value.Result.Base != result.BaseCommit || value.Result.Workspace != "w2" {
		t.Fatal("typed result missing from JSON")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatal("stdout contains more than one object")
	}
	if err := writeCommand(Options{Output: &output}, record); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "workspace=w2 name=task branch=task base_commit=") {
		t.Fatal("human output missing worktree identity")
	}
}
