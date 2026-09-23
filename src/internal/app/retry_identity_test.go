package app

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshmcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestCLIRetryOutputUsesStderr(t *testing.T) {
	var stdout, stderr bytes.Buffer
	streams := IO{Out: &stdout, Err: &stderr}
	query, err := parseCommandQuery("ping", []string{"-server", "server:50052", "-node", "node", "-json"}, streams)
	if err != nil || query.options.RetryOutput != &stderr || query.options.Output != &stdout || query.key == "" {
		t.Fatalf("ping retry identity/output routing: %+v %v", query, err)
	}
	session, err := parseSessionQuery("session", []string{"ensure", "-server", "server:50052", "-node", "node", "-name", "Worker", "-json"}, streams)
	if err != nil || session.options.RetryOutput != &stderr || session.options.Output != &stdout || session.request.IdempotencyKey == "" {
		t.Fatalf("session retry identity/output routing: %+v %v", session, err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatal("parsing emitted a dispatch receipt before validation/connection")
	}
}

func TestMCPDoesNotEmitCLIRetryReceipt(t *testing.T) {
	var diagnostics bytes.Buffer
	client := &mcpFleetFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		if request.IdempotencyKey != "original-key" {
			t.Fatal("MCP changed caller retry key")
		}
		return mcpTestReceipt(request, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED), nil
	}}
	options := mcpTestOptions(client)
	options.Output, options.RetryOutput = io.Discard, &diagnostics
	session := mcpApplicationClient(t, options)
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      string(meshmcp.InterruptAgent),
		Arguments: meshmcp.MutateAgentInput{AgentInput: mcpTestSelection(), IdempotencyKey: "original-key"},
	})
	if err != nil || result == nil || result.IsError || result.StructuredContent == nil {
		t.Fatalf("MCP mutation result changed: %v %v", result, err)
	}
	if diagnostics.Len() != 0 {
		t.Fatalf("MCP emitted CLI recovery output: %s", &diagnostics)
	}
	if strings.Contains(mcpObject(t, result.StructuredContent)["detail"].(string), "Retry identity") {
		t.Fatal("CLI recovery text polluted structured MCP result")
	}
}
