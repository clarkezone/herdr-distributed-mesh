package meshmcp

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestLifecycleSchemasAndBudgetDefaults(t *testing.T) {
	var calls atomic.Int32
	client := connect(t, func(_ context.Context, operation Operation, input any) (any, error) {
		calls.Add(1)
		if operation == StartAgent {
			start := input.(StartAgentInput)
			if start.StartupTimeoutMS != 30000 && start.StartupTimeoutMS != 3001 && start.StartupTimeoutMS != 300000 {
				return nil, errors.New("startup budget default or exact bounds changed")
			}
		}
		return map[string]bool{"receipt": true}, nil
	}, Options{})
	start := inputMap(t, fixtures()[StartAgent])
	delete(start, "herdr_session_id")
	delete(start, "herdr_session_incarnation")
	delete(start, "startup_timeout_ms")
	start["initial_prompt"] = ""
	if result := callTool(t, client, StartAgent, start); result.IsError {
		t.Fatal(result.Content)
	}
	for _, timeout := range []int{3001, 300000} {
		start["startup_timeout_ms"] = timeout
		if result := callTool(t, client, StartAgent, start); result.IsError {
			t.Fatal(result.Content)
		}
	}
	stop := inputMap(t, fixtures()[StopAgent])
	delete(stop, "herdr_session_id")
	delete(stop["target"].(map[string]any), "agent_session_id")
	if result := callTool(t, client, StopAgent, stop); result.IsError {
		t.Fatalf("configured default stop with pinned native incarnation rejected: %+v", result.Content)
	}
	for _, test := range []struct {
		op   Operation
		args map[string]any
		edit func(map[string]any)
	}{
		{StartAgent, start, func(m map[string]any) { delete(m, "name") }},
		{StartAgent, start, func(m map[string]any) { m["startup_timeout_ms"] = 0 }},
		{StartAgent, start, func(m map[string]any) { m["startup_timeout_ms"] = 3000 }},
		{StartAgent, start, func(m map[string]any) { m["startup_timeout_ms"] = 300001 }},
		{StartAgent, start, func(m map[string]any) { m["startup_timeout_ms"] = 3001.5 }},
		{StartAgent, start, func(m map[string]any) { m["prompt"] = "obsolete alias" }},
		{StartAgent, start, func(m map[string]any) { m["initial_prompt"] = strings.Repeat("x", MaxPromptBytes+1) }},
		{StartAgent, start, func(m map[string]any) { m["initial_prompt"] = strings.Repeat("\u00e9", MaxPromptBytes/2+1) }},
		{StopAgent, stop, func(m map[string]any) { delete(m, "herdr_session_incarnation") }},
		{StopAgent, stop, func(m map[string]any) { delete(m, "workspace_id") }},
		{StopAgent, stop, func(m map[string]any) { delete(m, "tab_id") }},
		{StopAgent, stop, func(m map[string]any) { delete(m, "provider") }},
		{StopAgent, stop, func(m map[string]any) { delete(m, "idempotency_key") }},
		{StopAgent, stop, func(m map[string]any) { m["project_id"] = "unverifiable" }},
		{StopAgent, stop, func(m map[string]any) { delete(m["target"].(map[string]any), "terminal_id") }},
	} {
		args := inputMap(t, test.args)
		test.edit(args)
		result, err := client.CallTool(context.Background(), &mcp.CallToolParams{Name: string(test.op), Arguments: args})
		if err == nil && (result == nil || !result.IsError) {
			t.Fatalf("invalid lifecycle input accepted: op=%s args=%v", test.op, args)
		}
	}
	if calls.Load() != 4 {
		t.Fatalf("invalid lifecycle schema reached callback: calls=%d", calls.Load())
	}
}

func TestLifecycleTimeoutPreservesCooperativePartialReceipt(t *testing.T) {
	for _, operation := range []Operation{StartAgent, StopAgent} {
		t.Run(string(operation), func(t *testing.T) {
			client := connect(t, func(ctx context.Context, _ Operation, _ any) (any, error) {
				<-ctx.Done()
				return map[string]any{"status": "COMMAND_STATUS_RUNNING", "agent_lifecycle": map[string]string{"launch_outcome": "unknown"}}, ctx.Err()
			}, Options{CallTimeout: 20 * time.Millisecond})
			result := callTool(t, client, operation, fixtures()[operation])
			if !result.IsError || result.StructuredContent == nil {
				t.Fatalf("cooperative timeout lost known partial receipt: %+v", result)
			}
			value := result.StructuredContent.(map[string]any)
			if value["status"] != "COMMAND_STATUS_RUNNING" || value["agent_lifecycle"].(map[string]any)["launch_outcome"] != "unknown" {
				t.Fatalf("partial evidence was replaced with fabricated terminal outcome: %+v", value)
			}
			for _, content := range result.Content {
				if !strings.HasPrefix(content.(*mcp.TextContent).Text, untrustedLabel) {
					t.Fatal("partial receipt content was not labeled untrusted")
				}
			}
		})
	}
}

func TestLifecycleLateSuccessRemainsTimeout(t *testing.T) {
	client := connect(t, func(ctx context.Context, _ Operation, _ any) (any, error) {
		<-ctx.Done()
		return map[string]bool{"success": true}, nil
	}, Options{CallTimeout: 20 * time.Millisecond})
	errorText(t, callTool(t, client, StartAgent, fixtures()[StartAgent]), "deadline exceeded")
}
