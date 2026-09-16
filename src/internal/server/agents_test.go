package server

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func agentPeer(name string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("test-peer", name))
}

func readyAgentEntry(t *testing.T, h *commandHarness, id, stable string) *fleetEntry {
	t.Helper()
	if err := h.api.bindNode(stable, id); err != nil {
		t.Fatal(err)
	}
	entry, err := h.api.fleet.beginSession(agentPeer("node"), id, stable, true, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	entry.probes, entry.agents = true, true
	if err := h.api.fleet.update(entry, &pb.HerdrState{Status: "ready", Version: "1", Protocol: 18,
		Sequence: 1, ObservedAt: timestamppb.Now()}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := h.api.fleet.heartbeat(entry, time.Now(), true, false, false, true); err != nil {
		t.Fatal(err)
	}
	return entry
}

func agentRequest(kind pb.AgentQueryKind) *pb.AgentQueryRequest {
	return &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: kind, Target: &pb.AgentTarget{PaneId: "p1", TerminalId: "t1", AgentSessionId: "a1"}, TimeoutMs: 5000}
}

func agentResponse(query *pb.AgentQuery) *pb.AgentQueryResult {
	result := &pb.AgentQueryResult{QueryId: query.QueryId, Agent: &pb.AgentView{
		Target: proto.Clone(query.Request.Target).(*pb.AgentTarget), WorkspaceId: "w1", TabId: "tab1", Provider: "test", Status: "idle"}}
	if query.Request.Kind == pb.AgentQueryKind_AGENT_QUERY_KIND_READ {
		result.Text = "PRIVATE QUERY OUTPUT"
	}
	return result
}

func TestAgentQueriesMultiClientMatchingAndNoInventoryPersistence(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	entry := readyAgentEntry(t, h, "node-1", "stable-1")
	type outcome struct {
		result *pb.AgentQueryResult
		err    error
	}
	results := make(chan outcome, 2)
	for i, kind := range []pb.AgentQueryKind{pb.AgentQueryKind_AGENT_QUERY_KIND_GET, pb.AgentQueryKind_AGENT_QUERY_KIND_READ} {
		actor := []string{"client", "client2"}[i]
		go func() {
			result, err := h.api.QueryAgent(agentPeer(actor), agentRequest(kind))
			results <- outcome{result, err}
		}()
	}
	first := (<-entry.queryOutbound).GetAgentQuery()
	second := (<-entry.queryOutbound).GetAgentQuery()
	if first.QueryId == second.QueryId {
		t.Fatal("queries share ID")
	}
	foreign := readyAgentEntry(t, h, "node-2", "stable-2")
	if err := h.api.finishAgentQuery(foreign, agentResponse(first)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("foreign result: %v", err)
	}
	mismatch := agentResponse(first)
	mismatch.Agent.Target.TerminalId = "other"
	if err := h.api.finishAgentQuery(entry, mismatch); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("target mismatch: %v", err)
	}
	for _, query := range []*pb.AgentQuery{second, first} {
		if err := h.api.finishAgentQuery(entry, agentResponse(query)); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case got := <-results:
			if got.err != nil || got.result == nil {
				t.Fatalf("query failed: %v", got.err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("query did not finish")
		}
	}
	if len(entry.queries) != 0 || h.api.fleet.storageErr != nil {
		t.Fatal("queries retained or storage poisoned")
	}
	list, err := h.api.fleet.list(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := protojson.Marshal(list)
	if err != nil || strings.Contains(string(raw), "PRIVATE") {
		t.Fatal("query output leaked into inventory")
	}
	if err := h.api.finishAgentQuery(entry, agentResponse(first)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unsolicited duplicate: %v", err)
	}
}

func TestAgentQueryCancellationRetirementAndCapacity(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	entry := readyAgentEntry(t, h, "node-1", "stable-1")
	ctx, cancel := context.WithCancel(agentPeer("client"))
	done := make(chan error, 1)
	go func() {
		_, err := h.api.QueryAgent(ctx, agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT))
		done <- err
	}()
	query := (<-entry.queryOutbound).GetAgentQuery()
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("cancel: %v", err)
	}
	envelope := <-entry.queryOutbound
	if envelope.GetAgentQueryCancel().GetQueryId() != query.QueryId || len(entry.queries) != 0 {
		t.Fatal("cancellation did not release lookup and forward cancellation")
	}
	if err := h.api.finishAgentQuery(entry, agentResponse(query)); err != nil {
		t.Fatalf("authenticated canceled reply: %v", err)
	}
	go func() {
		_, err := h.api.QueryAgent(agentPeer("client"), agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT))
		done <- err
	}()
	query = (<-entry.queryOutbound).GetAgentQuery()
	entry.supersede()
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatalf("retired query: %v", err)
	}
	if err := h.api.finishAgentQuery(entry, agentResponse(query)); status.Code(err) != codes.Aborted {
		t.Fatalf("retired response: %v", err)
	}
	entry = readyAgentEntry(t, h, "node-1", "stable-1")
	for range maxPendingAgentQueries {
		entry.queries[protocol.NewCommandID()] = &pendingAgentQuery{}
	}
	if _, err := h.api.QueryAgent(agentPeer("client"), agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_GET)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("capacity: %v", err)
	}
}

func TestAgentReadinessAndAuthorization(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	entry := readyAgentEntry(t, h, "node-1", "stable-1")
	if !slices.Contains(h.api.capabilities(), protocol.AgentControlCapability) {
		t.Fatal("capability missing")
	}
	if _, err := h.api.QueryAgent(agentPeer("denied"), agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_GET)); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("query authorization: %v", err)
	}
	for _, disable := range []func(){
		func() { entry.agents = false },
		func() { entry.view.AgentReady = false },
		func() { entry.view.HerdrReceivedAt = timestamppb.New(time.Now().Add(-time.Minute)) },
	} {
		entry.agents, entry.view.AgentReady = true, true
		entry.view.HerdrReceivedAt = timestamppb.Now()
		disable()
		if _, err := h.api.QueryAgent(agentPeer("client"), agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_GET)); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("unready query: %v", err)
		}
	}
	h.api.requiredNodeTag = ""
	if slices.Contains(h.api.capabilities(), protocol.AgentControlCapability) {
		t.Fatal("capability without node authorization")
	}
}

func TestAgentControlNoProjectPolicyRefreshAndImmutableRequest(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	entry := readyAgentEntry(t, h, "node-1", "stable-1")
	request := &pb.SubmitCommandRequest{NodeInstanceId: "node-1", CommandType: protocol.AgentControlCommandType,
		IdempotencyKey: protocol.NewCommandID(), Ttl: durationpb.New(10 * time.Second),
		AgentControl: &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, Target: agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_GET).Target, Text: "test prompt"}}
	if _, err := h.api.SubmitCommand(agentPeer("denied"), request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("mutation authorization: %v", err)
	}
	record, err := h.api.SubmitCommand(agentPeer("client"), request)
	if err != nil {
		t.Fatal(err)
	}
	changed := proto.Clone(request).(*pb.SubmitCommandRequest)
	changed.AgentControl.Text = "different"
	if _, err := h.api.SubmitCommand(agentPeer("client"), changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("changed payload: %v", err)
	}
	queued := <-entry.outbound
	command, err := h.api.prepareCommand(entry, queued)
	if err != nil || command == nil || command.CommandId != record.Command.CommandId {
		t.Fatalf("no-policy dispatch: %v", err)
	}
	bad := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "agent_prompt_sent",
		AgentControl: &pb.AgentControlResult{Target: &pb.AgentTarget{PaneId: "other", TerminalId: "t1"}, ObservedStatus: "working"}}
	if _, err := h.api.finishCommand(entry, bad); err == nil || h.api.fleet.storageErr != nil {
		t.Fatalf("invalid result poisoned storage: %v", err)
	}
	for _, who := range []string{"client", "node"} {
		request.IdempotencyKey = protocol.NewCommandID()
		if _, err := h.api.SubmitCommand(agentPeer("client"), request); err != nil {
			t.Fatal(err)
		}
		queued = <-entry.outbound
		if who == "client" {
			queued.actorContext = agentPeer("denied")
		} else {
			entry.peerContext = agentPeer("denied")
		}
		if command, err := h.api.prepareCommand(entry, queued); err != nil || command != nil {
			t.Fatalf("revoked %s dispatched: %v", who, err)
		}
	}
}
