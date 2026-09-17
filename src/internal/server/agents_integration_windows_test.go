//go:build windows

package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestAgentSessionWaitDoesNotBlockMutationHeartbeatOrCancel(t *testing.T) {
	root := t.TempDir()
	h := newCommandHarness(t, root)
	fake := newFakeWorkspaceHerdr(t, "", false, false)
	fleet := pb.NewFleetClient(h.connection)
	clientCtx, stopClient := context.WithTimeout(commandPeer(context.Background(), "client"), 30*time.Second)
	defer stopClient()
	nodeCtx, stopNode := context.WithCancel(commandPeer(context.Background(), "node"))
	defer stopNode()
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var controls atomic.Int32
	path := filepath.Join(root, "node", "commands.db")
	done := make(chan error, 1)
	go func() {
		_, err := node.RunSession(nodeCtx, pb.NewNodeControlClient(h.connection), node.Options{
			InstanceID: "node-1", CommandJournalPath: path, RequiredServerTag: node.DefaultRequiredServerTag,
			HerdrSocket: fake.path, EnableAgentControl: true, HeartbeatInterval: journaledTestHeartbeatInterval,
			VerifyServerPeer: func(_ context.Context, address string) error {
				if address != "bufconn" {
					return fmt.Errorf("wrong actual peer: %s", address)
				}
				return nil
			},
			QueryAgent: func(ctx context.Context, _ herdr.Config, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
				if request.Kind == pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT {
					close(started)
					<-ctx.Done()
					close(canceled)
					<-release
					return nil, ctx.Err()
				}
				return agentResponse(&pb.AgentQuery{Request: request}), nil
			},
			ControlAgent: func(_ context.Context, _ herdr.Config, control *pb.AgentControl) (*pb.AgentControlResult, error) {
				controls.Add(1)
				return &pb.AgentControlResult{Target: proto.Clone(control.Target).(*pb.AgentTarget), ObservedStatus: "working", StateChangeSeq: 2}, nil
			},
		})
		done <- err
	}()
	waitAgentNodeReady(t, clientCtx, fleet)
	waitCtx, stopWait := context.WithCancel(clientCtx)
	defer stopWait()
	waitDone := make(chan error, 1)
	go func() {
		_, err := fleet.QueryAgent(waitCtx, integrationAgentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT))
		waitDone <- err
	}()
	select {
	case <-started:
	case <-clientCtx.Done():
		t.Fatal("wait not forwarded")
	}
	before := waitAgentNodeReady(t, clientCtx, fleet).LastSeen.AsTime()
	readDone := make(chan error, 2)
	for i, kind := range []pb.AgentQueryKind{pb.AgentQueryKind_AGENT_QUERY_KIND_GET, pb.AgentQueryKind_AGENT_QUERY_KIND_READ} {
		actor := []string{"client", "client2"}[i]
		go func() {
			ctx, cancel := context.WithTimeout(commandPeer(context.Background(), actor), 10*time.Second)
			defer cancel()
			result, err := fleet.QueryAgent(ctx, integrationAgentRequest(kind))
			if err == nil && (result.Agent == nil || (kind == pb.AgentQueryKind_AGENT_QUERY_KIND_READ && result.Text != "PRIVATE QUERY OUTPUT")) {
				err = errors.New("wrong forwarded projection")
			}
			if err != nil {
				err = fmt.Errorf("%s while WAIT active: %w", kind, err)
			}
			readDone <- err
		}()
	}
	for range 2 {
		if err := <-readDone; err != nil {
			t.Fatal(err)
		}
	}
	record, err := fleet.SubmitCommand(clientCtx, &pb.SubmitCommandRequest{
		NodeInstanceId: "node-1", IdempotencyKey: protocol.NewCommandID(), CommandType: protocol.AgentControlCommandType, Ttl: durationpb.New(10 * time.Second),
		AgentControl: &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, Target: agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_GET).Target, Text: "test prompt"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for {
		record, err = fleet.GetCommand(clientCtx, &pb.GetCommandRequest{CommandId: record.Command.CommandId})
		if err != nil {
			t.Fatal(err)
		}
		if record.Status == pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
			break
		}
		select {
		case <-clientCtx.Done():
			t.Fatal("wait blocked mutation")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if controls.Load() != 1 || record.AgentControl == nil {
		t.Fatal("control receipt missing")
	}
	waitAgentHeartbeatAfter(t, clientCtx, fleet, before)
	select {
	case err := <-waitDone:
		t.Fatalf("WAIT ended before explicit cancellation: %v", err)
	case <-canceled:
		t.Fatal("executor canceled before explicit cancellation")
	default:
	}
	stopWait()
	if err := <-waitDone; status.Code(err) != codes.Canceled {
		t.Fatalf("wait cancellation: %v", err)
	}
	select {
	case <-canceled:
	case <-clientCtx.Done():
		t.Fatal("node wait not canceled")
	}
	stopNode()
	select {
	case err := <-done:
		t.Fatalf("session returned before executor joined: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("session shutdown did not join")
	}
	h.stop()
	// Only synthetic output is inspected, never an actual local agent session.
	files, err := filepath.Glob(filepath.Join(root, "node", "commands.db*"))
	if err != nil {
		t.Fatal(err)
	}
	serverFiles, err := filepath.Glob(filepath.Join(root, "coordinator", "mesh.db*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range append(files, serverFiles...) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "PRIVATE QUERY OUTPUT") {
			t.Fatalf("ephemeral output persisted in %s", filepath.Base(path))
		}
	}
}
