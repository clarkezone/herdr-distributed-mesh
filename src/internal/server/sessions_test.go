package server

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func sessionInventory(sequence uint64) *pb.SessionInventory {
	return &pb.SessionInventory{Sequence: sequence, ObservedAt: timestamppb.Now(), Sessions: []*pb.SessionView{{
		Name: "build", Incarnation: strings.Repeat("a", 64), Status: "ready", Herdr: readyState(1),
	}}}
}

func namedEntry(t *testing.T, h *commandHarness) *fleetEntry {
	t.Helper()
	entry := readyWorkspaceEntry(t, h)
	entry.sessions, entry.agents, entry.worktrees = true, true, true
	if err := h.api.fleet.heartbeat(entry, time.Now(), false, false, false, false, true); err != nil {
		t.Fatal(err)
	}
	if err := h.api.fleet.updateSessions(entry, sessionInventory(1), time.Now()); err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestSessionInventoryValidationAndPersistence(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*pb.SessionInventory)
	}{
		{"zero-sequence", func(v *pb.SessionInventory) { v.Sequence = 0 }},
		{"no-timestamp", func(v *pb.SessionInventory) { v.ObservedAt = nil }},
		{"unknown-field", func(v *pb.SessionInventory) { v.ProtoReflect().SetUnknown([]byte{0x78, 1}) }},
		{"unsafe-name", func(v *pb.SessionInventory) { v.Sessions[0].Name = `C:\private\socket` }},
		{"unsafe-error", func(v *pb.SessionInventory) { v.ErrorCode = `C:\private\socket` }},
		{"duplicate", func(v *pb.SessionInventory) {
			v.Sessions = append(v.Sessions, proto.Clone(v.Sessions[0]).(*pb.SessionView))
		}},
		{"missing-identity", func(v *pb.SessionInventory) { v.Sessions[0].Incarnation = "" }},
		{"invalid-status", func(v *pb.SessionInventory) { v.Sessions[0].Status = "running" }},
		{"unsafe-session-error", func(v *pb.SessionInventory) { v.Sessions[0].ErrorCode = "private" }},
		{"discovery-failure-with-tree", func(v *pb.SessionInventory) { v.ErrorCode = "session_unavailable" }},
		{"unavailable-tree", func(v *pb.SessionInventory) { v.Sessions[0].Status = "unavailable" }},
		{"unknown-tree", func(v *pb.SessionInventory) { v.Sessions[0].Herdr.ProtoReflect().SetUnknown([]byte{0x78, 1}) }},
		{"count", func(v *pb.SessionInventory) {
			for i := range 64 {
				value := proto.Clone(v.Sessions[0]).(*pb.SessionView)
				value.Name = fmt.Sprintf("session-%d", i)
				v.Sessions = append(v.Sessions, value)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			inventory := sessionInventory(1)
			test.edit(inventory)
			if validateSessionInventory(inventory) == nil {
				t.Fatal("invalid inventory accepted")
			}
		})
	}
	inventory := sessionInventory(1)
	inventory.ErrorCode, inventory.Sessions[0].Herdr = "session_capacity", nil
	if err := validateSessionInventory(inventory); err != nil {
		t.Fatal("bounded capacity report rejected", err)
	}
	for _, code := range []string{"session_manager_unavailable", "session_capacity", "unsupported_protocol"} {
		if err := validateSessionInventory(&pb.SessionInventory{Sequence: 1, ObservedAt: timestamppb.Now(), ErrorCode: code}); err != nil {
			t.Fatal(err)
		}
	}
	h := newCommandHarness(t, t.TempDir())
	entry := namedEntry(t, h)
	if err := h.api.fleet.updateSessions(entry, sessionInventory(1), time.Now()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("sequence replay accepted: %v", err)
	}
	views, err := h.api.commands.LoadFleet(context.Background())
	if err != nil || len(views) != 1 || len(views[0].Sessions) != 1 {
		t.Fatalf("inventory not persisted: %v %v", views, err)
	}
	restored := &fleetStore{}
	if err := restored.restore(views, time.Now()); err != nil {
		t.Fatal(err)
	}
	if restored.nodes["node-1"].view.SessionsReady || restored.nodes["node-1"].view.Connected {
		t.Fatal("restored inventory asserted live readiness")
	}
	invalid := proto.Clone(views[0]).(*pb.NodeView)
	invalid.SessionsReceivedAt = nil
	if validateStoredNode(invalid) == nil {
		t.Fatal("persisted session without receipt accepted")
	}
	legacy := proto.Clone(views[0]).(*pb.NodeView)
	legacy.Sessions, legacy.SessionsReceivedAt, legacy.SessionsReady = nil, nil, false
	if err := validateStoredNode(legacy); err != nil {
		t.Fatal("legacy record rejected", err)
	}
}

func TestNamedSessionReadinessRetryAndReplacement(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	h.api.workspacePolicy = coordinatorWorkspacePolicy(t)
	entry := namedEntry(t, h)
	for _, kind := range []string{protocol.WorkspaceEnsureCommandType, protocol.WorktreeCreateCommandType, protocol.AgentControlCommandType} {
		if !mutationReady(entry, kind, time.Now(), "build", strings.Repeat("a", 64)) {
			t.Fatalf("%s depended on configured-default readiness", kind)
		}
		if mutationReady(entry, kind, time.Now()) || mutationReady(entry, kind, time.Now(), "missing", "") ||
			mutationReady(entry, kind, time.Now(), "build", strings.Repeat("b", 64)) {
			t.Fatalf("%s ignored selected identity/default readiness", kind)
		}
	}
	request := workspaceRequest("named-retry")
	request.WorkspaceEnsure.SessionName, request.WorkspaceEnsure.SessionIncarnation = "build", strings.Repeat("a", 64)
	first, err := h.api.SubmitCommand(agentPeer("client"), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Command.SubmittedRequest == nil || first.Command.SubmittedRequest.WorkspaceEnsure.SessionName != "build" {
		t.Fatal("original selector was not journaled")
	}
	dispatched, err := h.api.prepareCommand(entry, <-entry.outbound)
	if err != nil || dispatched == nil {
		t.Fatalf("named workspace dispatch: %v", err)
	}
	queryRequest := agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_GET)
	queryRequest.Target.SessionName = "build"
	type outcome struct {
		result *pb.AgentQueryResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := h.api.QueryAgent(agentPeer("client"), queryRequest)
		done <- outcome{result, err}
	}()
	var query *pb.AgentQuery
	select {
	case envelope := <-entry.queryOutbound:
		query = envelope.GetAgentQuery()
	case got := <-done:
		t.Fatalf("named query rejected: %v", got.err)
	case <-time.After(3 * time.Second):
		t.Fatal("named query was not routed")
	}
	if query.Request.Target.SessionIncarnation != strings.Repeat("a", 64) {
		t.Fatal("fresh query did not pin observed incarnation")
	}
	replacement := sessionInventory(2)
	replacement.Sessions[0].Incarnation = strings.Repeat("b", 64)
	if err := h.api.fleet.updateSessions(entry, replacement, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.result.GetErrorCode() != "session_replaced" {
			t.Fatalf("replacement did not cancel query: %v %v", got.result, got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("replacement left query running")
	}
	if cancel := (<-entry.queryOutbound).GetAgentQueryCancel(); cancel.GetQueryId() != query.QueryId {
		t.Fatal("node query cancellation not forwarded")
	}
	if err := h.api.finishAgentQuery(entry, agentResponse(query)); err != nil {
		t.Fatal("late canceled response ended stream", err)
	}
	retry, err := h.api.SubmitCommand(agentPeer("client"), request)
	if err != nil || retry.Command.CommandId != first.Command.CommandId {
		t.Fatal("replacement blocked exact retry", err)
	}
	request.IdempotencyKey = "replacement"
	if _, err := h.api.SubmitCommand(agentPeer("client"), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("replacement was not preflight fenced", err)
	}
	entry.view.SessionsReceivedAt = timestamppb.New(time.Now().Add(-time.Minute))
	if mutationReady(entry, protocol.AgentControlCommandType, time.Now(), "build", "") {
		t.Fatal("stale inventory admitted operation")
	}
}

func TestSessionFleetAggregateBoundsAndNegotiation(t *testing.T) {
	now := time.Now()
	large := readyState(1)
	for i := range 600 {
		large.Workspaces = append(large.Workspaces, &pb.HerdrEntity{
			Id: fmt.Sprintf("%0128d", i), WorkspaceId: strings.Repeat("w", 128),
			TabId: strings.Repeat("t", 128), AgentStatus: "idle",
		})
	}
	inventory := sessionInventory(1)
	inventory.Sessions[0].Herdr = large
	if err := validateSessionInventory(inventory); err != nil {
		t.Fatal("test payload must fit a node inventory", err)
	}
	oversized := proto.Clone(inventory).(*pb.SessionInventory)
	second := proto.Clone(oversized.Sessions[0]).(*pb.SessionView)
	second.Name = "other"
	oversized.Sessions = append(oversized.Sessions, second)
	if status.Code(validateSessionInventory(oversized)) != codes.ResourceExhausted {
		t.Fatal("aggregate session inventory exceeded its byte bound")
	}
	fleet := &fleetStore{}
	entry, err := fleet.begin("node-0", "stable-0", true, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.updateSessions(entry, inventory, now); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("unnegotiated inventory accepted", err)
	}
	entry.sessions = true
	if err := fleet.updateSessions(entry, inventory, now); err != nil {
		t.Fatal(err)
	}
	if err := fleet.update(entry, large, now); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("combined default and session state exceeded the persisted node bound", err)
	}
	if err := fleet.update(entry, readyState(1), now); err != nil {
		t.Fatal(err)
	}
	total := nodeStateSize(entry.view)
	var last *fleetEntry
	for i := 1; total+proto.Size(inventory.Sessions[0])+10 < maxFleetBytes; i++ {
		last, err = fleet.begin(fmt.Sprintf("node-%d", i), fmt.Sprintf("stable-%d", i), false, now)
		if err != nil {
			t.Fatal(err)
		}
		last.sessions = true
		if err := fleet.updateSessions(last, inventory, now); err != nil {
			t.Fatal(err)
		}
		total += nodeStateSize(last.view)
	}
	extra, err := fleet.begin("node-extra", "stable-extra", true, now)
	if err != nil {
		t.Fatal(err)
	}
	extra.sessions = true
	if err := fleet.updateSessions(extra, inventory, now); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("fleet session aggregate bound ignored configured default", err)
	}
	if err := fleet.update(extra, large, now); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("default update ignored fleet session bytes", err)
	}
	views := []*pb.NodeView{}
	for _, item := range fleet.nodes {
		views = append(views, proto.Clone(item.view).(*pb.NodeView))
	}
	for _, value := range views {
		if value.InstanceId == "node-extra" {
			value.Sessions = proto.Clone(inventory).(*pb.SessionInventory).Sessions
			value.SessionsReceivedAt = timestamppb.New(now)
		}
	}
	if err := (&fleetStore{}).restore(views, now); err == nil {
		t.Fatal("persisted aggregate bound omitted sessions")
	}
}

func TestCombinedSessionStateLimitDoesNotFailCoordinatorStorage(t *testing.T) {
	for _, sessionsFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("sessions-first-%t", sessionsFirst), func(t *testing.T) {
			h := newCommandHarness(t, t.TempDir())
			entry := namedEntry(t, h)
			large := readyState(2)
			for i := range 600 {
				large.Workspaces = append(large.Workspaces, &pb.HerdrEntity{
					Id: fmt.Sprintf("%0128d", i), WorkspaceId: strings.Repeat("w", 128),
					TabId: strings.Repeat("t", 128), AgentStatus: "idle",
				})
			}
			inventory := sessionInventory(2)
			inventory.Sessions[0].Herdr = large
			if err := validateHerdrState(large); err != nil {
				t.Fatal(err)
			}
			if err := validateSessionInventory(inventory); err != nil {
				t.Fatal(err)
			}
			first := func() error { return h.api.fleet.update(entry, large, time.Now()) }
			second := func() error { return h.api.fleet.updateSessions(entry, inventory, time.Now()) }
			if sessionsFirst {
				first, second = second, first
			}
			if err := first(); err != nil {
				t.Fatal(err)
			}
			before := proto.Clone(entry.view).(*pb.NodeView)
			if err := second(); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("combined oversized inventory must be a capacity rejection, not a fatal storage error: %v", err)
			}
			if h.api.fleet.storageErr != nil || !proto.Equal(entry.view, before) {
				t.Fatal("capacity rejection poisoned storage or changed the live snapshot")
			}
			stored, err := h.api.commands.LoadFleet(context.Background())
			if err != nil || len(stored) != 1 || !proto.Equal(stored[0], before) {
				t.Fatalf("capacity rejection changed persisted inventory: %v", err)
			}
			if err := h.api.fleet.heartbeat(entry, time.Now(), true); err != nil {
				t.Fatal("coordinator failed after rejected inventory:", err)
			}
		})
	}
}

func TestCombinedNodeStateExactPersistenceBoundary(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	entry := namedEntry(t, h)
	view := proto.Clone(entry.view).(*pb.NodeView)
	for _, topology := range []*pb.HerdrState{view.Herdr, view.Sessions[0].Herdr} {
		for i := range 32 {
			topology.Workspaces = append(topology.Workspaces, &pb.HerdrEntity{
				Id: fmt.Sprintf("workspace-%d", i), AgentStatus: "idle",
				Directory: strings.Repeat("d", protocol.MaxDirectoryBytes),
			})
		}
	}
	padding := &pb.HerdrEntity{Id: "padding", AgentStatus: "idle"}
	view.Herdr.Workspaces = append(view.Herdr.Workspaces, padding)
	for range 4 {
		difference := state.MaxNodeBytes - proto.Size(view)
		if difference == 0 {
			break
		}
		length := len(padding.Directory) + difference
		if length < 0 || length > protocol.MaxDirectoryBytes {
			t.Fatal("fixture cannot reach the exact boundary with valid metadata")
		}
		padding.Directory = strings.Repeat("d", length)
	}
	if proto.Size(view) != state.MaxNodeBytes {
		t.Fatal("fixture did not reach the exact persisted node byte limit")
	}
	if err := validateStoredNode(view); err != nil {
		t.Fatal("boundary fixture must have valid independently bounded inventories:", err)
	}
	if err := h.api.fleet.save(view); err != nil {
		t.Fatal("exactly 260 KiB did not persist:", err)
	}
	stored, err := h.api.commands.LoadFleet(context.Background())
	if err != nil || len(stored) != 1 || !proto.Equal(stored[0], view) {
		t.Fatalf("boundary inventory did not round trip: %v", err)
	}
	padding.Directory += "d"
	if proto.Size(view) != state.MaxNodeBytes+1 {
		t.Fatal("fixture is not exactly one byte over capacity")
	}
	if err := h.api.fleet.save(view); status.Code(err) != codes.ResourceExhausted || h.api.fleet.storageErr != nil {
		t.Fatalf("one byte over capacity was not safely rejected: %v", err)
	}
}

func TestSessionEnsureRevalidatesEnrolledPeerTags(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	entry := namedEntry(t, h)
	request := &pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "ensure-auth",
		CommandType: protocol.SessionEnsureCommandType, SessionEnsure: &pb.SessionEnsure{Name: "build"}}
	record, err := h.api.SubmitCommand(agentPeer("client"), request)
	if err != nil {
		t.Fatal(err)
	}
	h.api.requiredNodeTag = "tag:revoked"
	wire, err := h.api.prepareCommand(entry, <-entry.outbound)
	if err != nil || wire != nil {
		t.Fatal("ensure ignored changed node role", err)
	}
	got, err := h.api.commands.GetOperatorCommand(context.Background(), record.Command.CommandId)
	if err != nil || got.Status != pb.CommandStatus_COMMAND_STATUS_REJECTED || got.Detail != "authorization_changed" {
		t.Fatal("authorization rejection was not durable", got, err)
	}
	retry, err := h.api.SubmitCommand(agentPeer("client"), request)
	if err != nil || retry.Command.CommandId != record.Command.CommandId {
		t.Fatal("authorization preflight ran before historical retry", err)
	}
}
func TestSessionEnsureStreamWithoutDefaultAndListSessions(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "node"), 8*time.Second)
	defer cancel()
	stream, err := pb.NewNodeControlClient(h.connection).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	hello := nodeHello("node-1")
	hello.GetHello().Capabilities = []string{protocol.SessionManageCapability}
	if err := stream.Send(hello); err != nil {
		t.Fatal(err)
	}
	ack, err := stream.Recv()
	if err != nil || !slices.Contains(ack.GetHelloAck().GetCapabilities(), protocol.SessionManageCapability) {
		t.Fatal("session negotiation missing", err)
	}
	client := pb.NewFleetClient(h.connection)
	clientCtx, stop := context.WithTimeout(commandPeer(context.Background(), "client"), 8*time.Second)
	defer stop()
	list, err := client.ListSessions(clientCtx, &pb.ListSessionsRequest{NodeInstanceId: "node-1"})
	if err != nil || list.ErrorCode != "session_manager_unavailable" {
		t.Fatalf("unobserved inventory looked ready: %v %v", list, err)
	}
	if err := stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_Heartbeat{Heartbeat: &pb.Heartbeat{
		Sequence: 1, SentAt: timestamppb.Now(), SessionsReady: true,
	}}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		h.api.fleet.mu.Lock()
		ready := h.api.fleet.nodes["node-1"].view.SessionsReady
		h.api.fleet.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("independent session manager readiness not observed")
		}
		time.Sleep(time.Millisecond)
	}
	request := &pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "ensure-build",
		CommandType: protocol.SessionEnsureCommandType, SessionEnsure: &pb.SessionEnsure{Name: "build"}}
	record, err := client.SubmitCommand(clientCtx, request)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := stream.Recv()
	if err != nil || wire.GetCommand().GetSessionEnsure().GetName() != "build" {
		t.Fatal("typed ensure not dispatched", err)
	}
	result := &pb.CommandResult{CommandId: record.Command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED,
		Detail: "session_ready", SessionEnsure: &pb.SessionView{Name: "build", Incarnation: strings.Repeat("a", 64), Status: "ready"}}
	if err := stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_CommandResult{CommandResult: result}}); err != nil {
		t.Fatal(err)
	}
	receipt, err := stream.Recv()
	if err != nil || receipt.GetCommandAck().GetCommandId() != record.Command.CommandId {
		t.Fatal("session outcome not acknowledged", err)
	}
	if err := stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_SessionInventory{SessionInventory: sessionInventory(1)}}); err != nil {
		t.Fatal(err)
	}
	for {
		list, err = client.ListSessions(clientCtx, &pb.ListSessionsRequest{NodeInstanceId: "node-1"})
		if err == nil && len(list.Sessions) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("inventory not exposed: %v %v", list, err)
		}
		time.Sleep(time.Millisecond)
	}
	raw, err := protojson.Marshal(list)
	if err != nil || strings.Contains(string(raw), "socket") || strings.Contains(string(raw), "path") {
		t.Fatal("native endpoint leaked", string(raw), err)
	}
	retry, err := client.SubmitCommand(clientCtx, request)
	if err != nil || retry.SessionEnsure.GetIncarnation() != strings.Repeat("a", 64) || retry.Command.CommandId != record.Command.CommandId {
		t.Fatal("durable typed retry missing", retry, err)
	}
	if err := stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_SessionInventory{SessionInventory: sessionInventory(1)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatal("stream did not fence inventory replay", err)
	}
	if _, err := client.ListSessions(clientCtx, &pb.ListSessionsRequest{NodeInstanceId: "node-1"}); status.Code(err) != codes.Unavailable {
		t.Fatal("disconnected cached inventory exposed as live", err)
	}
	retry, err = client.SubmitCommand(clientCtx, request)
	if err != nil || retry.Command.CommandId != record.Command.CommandId {
		t.Fatal("disconnection prevented historical retry", err)
	}
}

func TestNamedSessionMutationAdmissionAndDispatch(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	h.api.workspacePolicy = coordinatorWorktreePolicy(t)
	entry := namedEntry(t, h)
	worktree := worktreeRequest("named-worktree")
	worktree.WorktreeCreate.SessionName = "build"
	worktree.WorktreeCreate.SessionIncarnation = strings.Repeat("a", 64)
	target := agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_GET).Target
	target.SessionName, target.SessionIncarnation = "build", strings.Repeat("a", 64)
	agent := &pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "named-agent", CommandType: protocol.AgentControlCommandType,
		AgentControl: &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, Target: target, Text: "hello"}}
	for _, request := range []*pb.SubmitCommandRequest{worktree, agent} {
		record, err := h.api.SubmitCommand(agentPeer("client"), request)
		if err != nil {
			t.Fatalf("named %s admission: %v", request.CommandType, err)
		}
		dispatched, err := h.api.prepareCommand(entry, <-entry.outbound)
		if err != nil || dispatched == nil || dispatched.CommandId != record.Command.CommandId {
			t.Fatalf("named %s dispatch: %v", request.CommandType, err)
		}
		if name, incarnation := protocol.CommandSession(dispatched); name != "build" || incarnation != strings.Repeat("a", 64) {
			t.Fatal("dispatch lost session selector")
		}
		entry.view.SessionsReady = false
		retry, err := h.api.SubmitCommand(agentPeer("client"), request)
		if err != nil || retry.Command.CommandId != record.Command.CommandId {
			t.Fatal("retry depended on current manager readiness", err)
		}
		changed := proto.Clone(request).(*pb.SubmitCommandRequest)
		if changed.WorktreeCreate != nil {
			changed.WorktreeCreate.SessionName = "other"
		} else {
			changed.AgentControl.Target.SessionName = "other"
		}
		if _, err := h.api.SubmitCommand(agentPeer("client"), changed); status.Code(err) != codes.AlreadyExists {
			t.Fatal("changed selector reused original idempotency key", err)
		}
		entry.view.SessionsReady = true
	}
}
