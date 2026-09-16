//go:build windows

package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This opt-in test prompts and interrupts an existing isolated Copilot probe.
// Set HERDR_MESH_AGENT_TEST_SOCKET to that disposable instance's named pipe.
// HERDR_MESH_AGENT_TEST_PANE is optional only when inventory has one agent.
// It never starts a Herdr process or logs output. A fresh output marker, not
// prompt echo, idle status, or a delivery receipt, proves generated output.
func TestAgentLiveHeadlessQueryAndDurableInterrupt(t *testing.T) {
	socket := os.Getenv("HERDR_MESH_AGENT_TEST_SOCKET")
	if socket == "" {
		t.Skip("set HERDR_MESH_AGENT_TEST_SOCKET to an isolated disposable Copilot probe; this test sends a prompt and interrupt")
	}
	root := t.TempDir()
	h := newCommandHarness(t, root)
	running := startRealAdapterNode(t, h, socket, filepath.Join(root, "node", "commands.db"), time.Second)
	fleet := pb.NewFleetClient(h.connection)
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 90*time.Second)
	defer cancel()
	view := waitAgentNodeReady(t, ctx, fleet)
	pane := os.Getenv("HERDR_MESH_AGENT_TEST_PANE")
	if pane == "" {
		if len(view.Herdr.Agents) != 1 {
			t.Fatal("isolated probe must have exactly one agent, or set HERDR_MESH_AGENT_TEST_PANE explicitly")
		}
		pane = view.Herdr.Agents[0].Id
	}
	getRequest := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_GET,
		Target: &pb.AgentTarget{PaneId: pane}, TimeoutMs: 5000}
	get, err := fleet.QueryAgent(ctx, getRequest)
	if err != nil || get.GetErrorCode() != "" || protocol.ValidateAgentQueryResult(get, getRequest) != nil {
		t.Fatal("real GET did not return a valid agent projection")
	}
	if get.Agent.Provider != "copilot" || !get.Agent.InteractiveReady || get.Agent.LaunchPending ||
		(get.Agent.Status != "idle" && get.Agent.Status != "done") {
		t.Fatal("the explicitly selected disposable Copilot probe must be ready and idle")
	}
	target := proto.Clone(get.Agent.Target).(*pb.AgentTarget)
	readRequest := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ,
		Target: target, Lines: 20, TimeoutMs: 5000}
	read, err := fleet.QueryAgent(ctx, readRequest)
	if err != nil || read.GetErrorCode() != "" || protocol.ValidateAgentQueryResult(read, readRequest) != nil {
		t.Fatal("real READ did not return a bounded sanitized projection")
	}
	promptText, expected := headlessMarkerPrompt(protocol.NewCommandID())
	promptRequest := controlRequest(target, pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)
	promptRequest.AgentControl.Text = promptText
	prompt, err := fleet.SubmitCommand(ctx, promptRequest)
	if err != nil {
		t.Fatal("headless marker prompt was not admitted")
	}
	prompt, err = waitAgentReceipt(ctx, fleet, prompt.Command.CommandId)
	if err != nil || prompt.GetStatus() != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED ||
		prompt.GetDetail() != "agent_prompt_sent" || !proto.Equal(prompt.GetAgentControl().GetTarget(), target) {
		t.Fatal("headless marker prompt did not produce an exact-target delivery receipt")
	}
	outputCtx, stopOutput := context.WithTimeout(ctx, 60*time.Second)
	err = waitForAgentOutput(outputCtx, readRequest, expected, func(ctx context.Context) (*pb.AgentQueryResult, error) {
		return fleet.QueryAgent(ctx, readRequest)
	})
	stopOutput()
	if err != nil {
		t.Fatal("headless probe did not generate the fresh contiguous marker in READ output")
	}
	// An observation (including idle) is not evidence of output or task success.
	waitRequest := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT,
		Target: target, Until: []string{"idle", "working", "blocked", "done", "unknown"}, TimeoutMs: 5000}
	waited, err := fleet.QueryAgent(ctx, waitRequest)
	if err != nil || waited.GetErrorCode() != "" || protocol.ValidateAgentQueryResult(waited, waitRequest) != nil {
		t.Fatal("real WAIT did not return a valid observation")
	}
	waitRequest = proto.Clone(waitRequest).(*pb.AgentQueryRequest)
	waitRequest.Until, waitRequest.TimeoutMs = []string{"unknown"}, 10000
	waitCtx, stopWait := context.WithCancel(ctx)
	defer stopWait()
	waitDone := make(chan error, 1)
	go func() { _, err := fleet.QueryAgent(waitCtx, waitRequest); waitDone <- err }()
	for {
		h.api.fleet.mu.Lock()
		entry := h.api.fleet.nodes["node-1"]
		pending := entry != nil && len(entry.queries) != 0
		h.api.fleet.mu.Unlock()
		if pending {
			break
		}
		select {
		case <-waitDone:
			t.Fatal("probe WAIT ended before cancellation could be exercised")
		case <-ctx.Done():
			t.Fatal("real WAIT was not forwarded")
		case <-time.After(10 * time.Millisecond):
		}
	}
	type outcome struct {
		record *pb.CommandRecord
		err    error
	}
	interruptDone := make(chan outcome, 1)
	interruptRequest := controlRequest(target, pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT)
	go func() {
		record, err := fleet.SubmitCommand(ctx, interruptRequest)
		if err == nil {
			record, err = waitAgentReceipt(ctx, fleet, record.Command.CommandId)
			if err == nil && (record.GetStatus() != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED ||
				record.GetDetail() != "agent_interrupt_sent" || !proto.Equal(record.GetAgentControl().GetTarget(), target)) {
				err = fmt.Errorf("real interrupt did not produce an exact-target delivery receipt")
			}
		}
		interruptDone <- outcome{record, err}
	}()
	stopWait()
	if err := <-waitDone; status.Code(err) != codes.Canceled {
		t.Fatal("real WAIT cancellation did not propagate")
	}
	interrupted := <-interruptDone
	if interrupted.err != nil {
		t.Fatal("real durable interrupt delivery failed")
	}
	// The receipt proves acknowledged delivery, not that generation stopped.
	retry, err := fleet.SubmitCommand(ctx, interruptRequest)
	if err != nil || !proto.Equal(retry, interrupted.record) {
		t.Fatal("real interrupt idempotent receipt changed")
	}
	running.stop()
	h.stop()
	reopened := newCommandHarness(t, root)
	recovered, err := pb.NewFleetClient(reopened.connection).GetCommand(ctx, &pb.GetCommandRequest{
		CommandId: interrupted.record.Command.CommandId,
	})
	if err != nil || !proto.Equal(recovered, interrupted.record) {
		t.Fatal("real interrupt receipt did not survive coordinator journal reopen")
	}
	reopened.stop()
	assertQueryOutputAbsent(t, root, expected)
}

func headlessMarkerPrompt(nonce string) (string, string) {
	first, second := "M_", nonce
	return fmt.Sprintf("Reply with exactly the concatenation of the two quoted fragments %q and %q. Do not include quotes, spaces, or explanatory text.", first, second), first + second
}

func waitForAgentOutput(ctx context.Context, request *pb.AgentQueryRequest, expected string, read func(context.Context) (*pb.AgentQueryResult, error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := read(ctx)
		if err != nil || result.GetErrorCode() != "" || protocol.ValidateAgentQueryResult(result, request) != nil {
			return errors.New("agent READ failed before output was proven")
		}
		if strings.Contains(result.Text, expected) {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func TestAgentHeadlessOutputRejectsIdlePromptEcho(t *testing.T) {
	nonce := protocol.NewCommandID()
	prompt, expected := headlessMarkerPrompt(nonce)
	if len(expected) != 34 || expected != "M_"+nonce {
		t.Fatal("headless marker must retain the full nonce within 34 characters")
	}
	if strings.Contains(prompt, expected) {
		t.Fatal("prompt contains its expected output marker")
	}
	_, other := headlessMarkerPrompt(protocol.NewCommandID())
	if expected == other {
		t.Fatal("headless marker must distinguish previous runs")
	}
	request := agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_READ)
	for _, mode := range []string{"echo", "wrapped", "contiguous"} {
		t.Run(mode, func(t *testing.T) {
			timeout := 30 * time.Millisecond
			if mode == "contiguous" {
				timeout = time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			calls := 0
			err := waitForAgentOutput(ctx, request, expected, func(context.Context) (*pb.AgentQueryResult, error) {
				calls++
				result := agentResponse(&pb.AgentQuery{QueryId: protocol.NewCommandID(), Request: request})
				result.Agent.Status, result.Text = "idle", prompt
				if mode == "wrapped" {
					result.Text += "\n" + expected[:18] + "\n" + expected[18:]
				}
				if mode == "contiguous" && calls > 1 {
					result.Text += "\n" + expected
				}
				return result, nil
			})
			if mode == "contiguous" {
				if err != nil || calls < 2 {
					t.Fatal("fresh generated output was not distinguished from initial echo")
				}
			} else if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("idle status, prompt echo, or wrapped text falsely proved contiguous output")
			}
		})
	}
}
