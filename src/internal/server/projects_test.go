package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func managedTestRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(".managed-test-" + protocol.NewCommandID())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	return root
}

func managedAPIContext(peer string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("test-peer", peer))
}

func managedRegistration() *pb.RegisterProjectRequest {
	return &pb.RegisterProjectRequest{NodeInstanceId: "node-1", ProjectId: "AgentFlow",
		CheckoutPath: `C:\PRIVATE-checkout`, WorktreeRoot: `C:\PRIVATE-worktrees`}
}

func managedWorkspaceRequest(key string) *pb.SubmitCommandRequest {
	request := workspaceRequest(key)
	request.WorkspaceEnsure.BindingRevision = ""
	return request
}

func readyManagedEntry(t *testing.T, h *commandHarness) *fleetEntry {
	t.Helper()
	entry := readyWorkspaceEntry(t, h)
	h.api.fleet.mu.Lock()
	entry.projects = true
	entry.projectWake = make(chan struct{}, 1)
	entry.projectSent = make(map[string]*pb.ProjectConfig)
	entry.projectApplied = make(map[string]*pb.ProjectAck)
	h.api.fleet.mu.Unlock()
	return entry
}

func managedRegister(t *testing.T, h *commandHarness, request *pb.RegisterProjectRequest) *pb.ProjectRecord {
	t.Helper()
	record, err := h.api.RegisterProject(managedAPIContext("client"), request)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func managedGet(t *testing.T, h *commandHarness, project, readiness string, generation uint64) *pb.ProjectRecord {
	t.Helper()
	record, err := h.api.GetProject(managedAPIContext("client"), &pb.GetProjectRequest{NodeInstanceId: "node-1", ProjectId: project})
	if err != nil {
		t.Fatal(err)
	}
	if record.Readiness != readiness || record.Desired.Generation != generation {
		t.Fatalf("project %s: readiness=%s generation=%d, want %s/%d", project, record.Readiness, record.Desired.Generation, readiness, generation)
	}
	return record
}

func managedSendConfig(t *testing.T, h *commandHarness, entry *fleetEntry, generation uint64) *pb.ProjectAck {
	t.Helper()
	configs, err := h.api.projectUpdates(entry)
	if err != nil || len(configs) != 1 {
		t.Fatalf("project updates=%v err=%v, want one", configs, err)
	}
	config := configs[0]
	if config.Generation != generation || config.NodeInstanceId != "node-1" || config.ProjectId != "AgentFlow" {
		t.Fatalf("unexpected config: %v", config)
	}
	return &pb.ProjectAck{ProjectId: config.ProjectId, Generation: config.Generation, Status: "applied",
		CheckoutPath: config.CheckoutPath, WorktreeRoot: config.WorktreeRoot}
}

func managedAcknowledge(t *testing.T, h *commandHarness, entry *fleetEntry, ack *pb.ProjectAck) {
	t.Helper()
	if err := h.api.acknowledgeProject(entry, ack); err != nil {
		t.Fatal(err)
	}
}

func TestCentralProjectPrivateRegistrationAndStreamDelivery(t *testing.T) {
	h := newCommandHarness(t, managedTestRoot(t))
	h.api.requiredCommandTag = "tag:operator"
	identify := h.api.identifyPeer
	h.api.identifyPeer = func(ctx context.Context) (transport.PeerIdentity, error) {
		peer, err := identify(ctx)
		if peer.StableID == "client-1" {
			peer.Tags = append(peer.Tags, "tag:operator")
		}
		return peer, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := pb.NewFleetClient(h.connection)
	request := managedRegistration()
	for _, peer := range []string{"client2", "node", "denied"} {
		if _, err := client.RegisterProject(commandPeer(ctx, peer), request); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("%s registered project: %v", peer, err)
		}
	}
	if list, err := client.ListProjects(commandPeer(ctx, "client"), &pb.ListProjectsRequest{}); err != nil || len(list.GetProjects()) != 0 {
		t.Fatalf("denied writes changed storage: %v %v", list, err)
	}
	record, err := client.RegisterProject(commandPeer(ctx, "client"), request)
	if err != nil || record.GetReadiness() != "offline" || record.GetDesired().GetGeneration() != 1 {
		t.Fatalf("offline registration: %v %v", record, err)
	}
	for _, peer := range []string{"node", "denied"} {
		if _, err := client.GetProject(commandPeer(ctx, peer), &pb.GetProjectRequest{NodeInstanceId: "node-1", ProjectId: "AgentFlow"}); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("%s read private project: %v", peer, err)
		}
		if _, err := client.ListProjects(commandPeer(ctx, peer), &pb.ListProjectsRequest{}); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("%s listed private projects: %v", peer, err)
		}
	}
	list, err := client.ListProjects(commandPeer(ctx, "client2"), &pb.ListProjectsRequest{NodeInstanceId: "node-1"})
	if err != nil || len(list.GetProjects()) != 1 || !proto.Equal(list.Projects[0].Desired, record.Desired) {
		t.Fatalf("private reader lost desired paths: %v %v", list, err)
	}
	empty, err := client.ListProjects(commandPeer(ctx, "client2"), &pb.ListProjectsRequest{NodeInstanceId: "other-node"})
	if err != nil || len(empty.GetProjects()) != 0 {
		t.Fatalf("node filter leaked another node's projects: %v %v", empty, err)
	}
	stream, err := pb.NewNodeControlClient(h.connection).Connect(commandPeer(ctx, "node"))
	if err != nil {
		t.Fatal(err)
	}
	hello := nodeHello("node-1")
	hello.GetHello().Capabilities = []string{protocol.ProjectConfigCapability}
	if err := stream.Send(hello); err != nil {
		t.Fatal(err)
	}
	envelope, err := stream.Recv()
	if err != nil || envelope.GetHelloAck() == nil {
		t.Fatalf("hello: %v %v", envelope, err)
	}
	envelope, err = stream.Recv()
	if err != nil || !proto.Equal(envelope.GetProjectConfig(), record.Desired) {
		t.Fatalf("offline desired state not delivered: %v %v", envelope, err)
	}
	managedGet(t, h, "AgentFlow", "pending", 1)
	ack := &pb.ProjectAck{ProjectId: "AgentFlow", Generation: 1, Status: "applied",
		CheckoutPath: request.CheckoutPath, WorktreeRoot: request.WorktreeRoot}
	if err := stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_ProjectAck{ProjectAck: ack}}); err != nil {
		t.Fatal(err)
	}
	for {
		record, err = client.GetProject(commandPeer(ctx, "client2"), &pb.GetProjectRequest{NodeInstanceId: "node-1", ProjectId: "AgentFlow"})
		if err != nil {
			t.Fatal(err)
		}
		if record.Readiness == "applied" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("stream ACK never established readiness")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if !proto.Equal(record.Applied, ack) {
		t.Fatalf("private ACK paths lost: %v", record)
	}
	fleet, err := client.ListNodes(commandPeer(ctx, "client2"), &emptypb.Empty{})
	if err != nil || len(fleet.GetNodes()) != 1 {
		t.Fatalf("fleet: %v %v", fleet, err)
	}
	raw, err := protojson.Marshal(fleet)
	if err != nil || strings.Contains(string(raw), "PRIVATE") || strings.Contains(string(raw), "checkoutPath") || strings.Contains(string(raw), "worktreeRoot") {
		t.Fatalf("private project paths leaked to fleet: %s (%v)", raw, err)
	}
}

func TestManagedProjectGenerationACKAndAdmissionFencing(t *testing.T) {
	h := newCommandHarness(t, managedTestRoot(t))
	request := managedRegistration()
	managedRegister(t, h, request)
	entry := readyManagedEntry(t, h)
	ctx := managedAPIContext("client")
	admit := func(key string, want codes.Code) *pb.CommandRecord {
		t.Helper()
		record, err := h.api.SubmitCommand(ctx, managedWorkspaceRequest(key))
		if status.Code(err) != want {
			t.Fatalf("submit %s: %v, want %s", key, err, want)
		}
		return record
	}
	admit("before-ack", codes.FailedPrecondition)
	unsent := &pb.ProjectAck{ProjectId: "AgentFlow", Generation: 1, Status: "applied", CheckoutPath: request.CheckoutPath, WorktreeRoot: request.WorktreeRoot}
	if err := h.api.acknowledgeProject(entry, unsent); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unsent generation acknowledged: %v", err)
	}
	ack1 := managedSendConfig(t, h, entry, 1)
	managedAcknowledge(t, h, entry, ack1)
	managedAcknowledge(t, h, entry, proto.Clone(ack1).(*pb.ProjectAck))
	managedGet(t, h, "AgentFlow", "applied", 1)
	if updates, err := h.api.projectUpdates(entry); err != nil || len(updates) != 0 {
		t.Fatalf("unchanged configuration sent again: %v %v", updates, err)
	}
	first := admit("historical", codes.OK)
	if first.Command.WorkspaceEnsure.BindingRevision != "managed:1" || first.Command.SubmittedRequest == nil ||
		first.Command.SubmittedRequest.WorkspaceEnsure.BindingRevision != "" {
		t.Fatalf("resolved binding overwrote submitted retry identity: %v", first.Command)
	}
	queued := <-entry.outbound
	request.CheckoutPath = `C:\PRIVATE-checkout-v2`
	updated := managedRegister(t, h, request)
	if updated.Readiness != "pending" || updated.Desired.Generation != 2 || updated.Applied.Generation != 1 {
		t.Fatalf("update failed to invalidate readiness: %v", updated)
	}
	// The old ACK was sent legitimately, but desired state changed before its arrival.
	managedAcknowledge(t, h, entry, ack1)
	managedGet(t, h, "AgentFlow", "pending", 2)
	admit("stale-ack", codes.FailedPrecondition)
	if command, err := h.api.prepareCommand(entry, queued); err != nil || command != nil {
		t.Fatalf("queued stale generation dispatched: %v %v", command, err)
	}
	rejected := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_REJECTED)
	if rejected.Detail != "authorization_changed" {
		t.Fatalf("missing dispatch fence audit: %v", rejected)
	}
	retry := admit("historical", codes.OK)
	if retry.Command.CommandId != first.Command.CommandId || retry.Status != rejected.Status {
		t.Fatal("desired-state change invalidated exact historical retry")
	}
	ack2 := managedSendConfig(t, h, entry, 2)
	future := proto.Clone(ack2).(*pb.ProjectAck)
	future.Generation++
	if err := h.api.acknowledgeProject(entry, future); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("future generation acknowledged: %v", err)
	}
	managedAcknowledge(t, h, entry, ack2)
	managedAcknowledge(t, h, entry, ack1)
	record := managedGet(t, h, "AgentFlow", "applied", 2)
	if !proto.Equal(record.Applied, ack2) {
		t.Fatal("out-of-order ACK replaced newest applied state")
	}
	conflict := proto.Clone(ack2).(*pb.ProjectAck)
	conflict.CheckoutPath = `C:\PRIVATE-unrelated`
	if err := h.api.acknowledgeProject(entry, conflict); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("conflicting repeated ACK accepted: %v", err)
	}
	managedGet(t, h, "AgentFlow", "applied", 2)
	if same := managedRegister(t, h, request); same.Desired.Generation != 2 || same.Readiness != "applied" {
		t.Fatalf("identical registration changed generation/readiness: %v", same)
	}
	next := admit("current", codes.OK)
	if next.Command.WorkspaceEnsure.BindingRevision != "managed:2" {
		t.Fatal("new command did not bind current generation")
	}
	explicit := managedWorkspaceRequest("explicit-old-revision")
	explicit.WorkspaceEnsure.BindingRevision = "managed:1"
	if _, err := h.api.SubmitCommand(ctx, explicit); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("explicit stale revision admitted: %v", err)
	}
}

func TestManagedProjectRestartAndSupersededStreamRequireFreshACK(t *testing.T) {
	root := managedTestRoot(t)
	h := newCommandHarness(t, root)
	managedRegister(t, h, managedRegistration())
	old := readyManagedEntry(t, h)
	ack := managedSendConfig(t, h, old, 1)
	managedAcknowledge(t, h, old, ack)
	managedGet(t, h, "AgentFlow", "applied", 1)
	current := readyManagedEntry(t, h)
	managedGet(t, h, "AgentFlow", "pending", 1)
	if err := h.api.acknowledgeProject(old, ack); status.Code(err) != codes.Aborted {
		t.Fatalf("superseded stream ACK accepted: %v", err)
	}
	if _, err := h.api.projectUpdates(old); status.Code(err) != codes.Aborted {
		t.Fatalf("superseded stream received private config: %v", err)
	}
	if err := h.api.acknowledgeProject(current, ack); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("fresh stream reused unsent historical ACK: %v", err)
	}
	managedAcknowledge(t, h, current, managedSendConfig(t, h, current, 1))
	h.stop()
	h = newCommandHarness(t, root)
	history := managedGet(t, h, "AgentFlow", "offline", 1)
	if !proto.Equal(history.Applied, ack) {
		t.Fatal("restart lost private historical ACK")
	}
	current = readyManagedEntry(t, h)
	managedGet(t, h, "AgentFlow", "pending", 1)
	if _, err := h.api.SubmitCommand(managedAPIContext("client"), managedWorkspaceRequest("restart-no-ack")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("restart restored live project readiness: %v", err)
	}
	managedSendConfig(t, h, current, 1)
	invalid := &pb.ProjectAck{ProjectId: "AgentFlow", Generation: 1, Status: "invalid", ErrorCode: "invalid_path"}
	managedAcknowledge(t, h, current, invalid)
	managedGet(t, h, "AgentFlow", "invalid", 1)
	if _, err := h.api.SubmitCommand(managedAPIContext("client"), managedWorkspaceRequest("invalid-path")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("invalid config admitted mutation: %v", err)
	}
	current = readyManagedEntry(t, h)
	managedAcknowledge(t, h, current, managedSendConfig(t, h, current, 1))
	managedGet(t, h, "AgentFlow", "applied", 1)
	if _, err := h.api.SubmitCommand(managedAPIContext("client"), managedWorkspaceRequest("fresh-ack")); err != nil {
		t.Fatalf("freshly revalidated project not admitted: %v", err)
	}
	unsupported := readyWorkspaceEntry(t, h)
	managedGet(t, h, "AgentFlow", "unsupported", 1)
	if err := h.api.acknowledgeProject(unsupported, ack); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unsupported node ACK accepted: %v", err)
	}
	if _, err := h.api.SubmitCommand(managedAPIContext("client"), managedWorkspaceRequest("unsupported")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unsupported node admitted managed mutation: %v", err)
	}
}

func TestCentralLegacyProjectAdoptionIsOneTimeAndConflictIsDurable(t *testing.T) {
	root := managedTestRoot(t)
	h := newCommandHarness(t, root)
	central := managedRegistration()
	managedRegister(t, h, central)
	entry := readyManagedEntry(t, h)
	conflict := proto.Clone(central).(*pb.RegisterProjectRequest)
	conflict.CheckoutPath = `C:\PRIVATE-legacy-conflict`
	adopted := proto.Clone(central).(*pb.RegisterProjectRequest)
	adopted.ProjectId, adopted.CheckoutPath = "Legacy", `C:\PRIVATE-legacy`
	offer := &pb.LegacyProjects{Projects: []*pb.RegisterProjectRequest{conflict, adopted}}
	if err := h.api.adoptProjects(entry, offer); err != nil {
		t.Fatal(err)
	}
	record := managedGet(t, h, "AgentFlow", "pending", 1)
	if record.AdoptionStatus != "conflict" || record.Desired.CheckoutPath != central.CheckoutPath {
		t.Fatalf("legacy offer overwrote central registration: %v", record)
	}
	record = managedGet(t, h, "Legacy", "pending", 1)
	if record.AdoptionStatus != "adopted" || record.Desired.CheckoutPath != adopted.CheckoutPath {
		t.Fatalf("initial legacy adoption missing: %v", record)
	}
	central.CheckoutPath = `C:\PRIVATE-operator-update`
	managedRegister(t, h, central)
	h.stop()
	h = newCommandHarness(t, root)
	entry = readyManagedEntry(t, h)
	adopted.CheckoutPath = `C:\PRIVATE-reoffered`
	if err := h.api.adoptProjects(entry, offer); err != nil {
		t.Fatal(err)
	}
	record = managedGet(t, h, "AgentFlow", "pending", 2)
	if record.AdoptionStatus != "conflict" || record.Desired.CheckoutPath != central.CheckoutPath {
		t.Fatal("restart/reoffer lost operator update or conflict marker")
	}
	record = managedGet(t, h, "Legacy", "pending", 1)
	if record.AdoptionStatus != "adopted" || record.Desired.CheckoutPath != `C:\PRIVATE-legacy` {
		t.Fatal("repeat adoption changed first imported mapping")
	}
	foreign := proto.Clone(central).(*pb.RegisterProjectRequest)
	foreign.NodeInstanceId = "node-2"
	if err := h.api.adoptProjects(entry, &pb.LegacyProjects{Projects: []*pb.RegisterProjectRequest{foreign}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("node offered another node's project: %v", err)
	}
	newEntry := readyManagedEntry(t, h)
	if err := h.api.adoptProjects(entry, offer); status.Code(err) != codes.Aborted {
		t.Fatalf("superseded stream adopted projects: %v", err)
	}
	configs, err := h.api.projectUpdates(newEntry)
	if err != nil || len(configs) != 2 {
		t.Fatalf("adopted and central configs not deliverable: %v %v", configs, err)
	}
}
