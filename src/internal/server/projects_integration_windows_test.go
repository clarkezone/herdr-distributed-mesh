//go:build windows

package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type managedIntegrationNode struct {
	client *countedNodeClient
	done   chan struct{}
	err    error
	stop   func()
}

func startManagedIntegrationNode(t *testing.T, h *commandHarness, journal, socket string, dropResult bool) *managedIntegrationNode {
	t.Helper()
	session := &managedIntegrationNode{
		client: &countedNodeClient{NodeControlClient: pb.NewNodeControlClient(h.connection)},
		done:   make(chan struct{}),
	}
	session.client.dropResult.Store(dropResult)
	ctx, cancel := context.WithCancel(commandPeer(context.Background(), "node"))
	go func() {
		defer close(session.done)
		_, session.err = node.RunSession(ctx, session.client, node.Options{
			InstanceID: "node-1", RequiredServerTag: node.DefaultRequiredServerTag, CommandJournalPath: journal,
			ManagedProjects: projects.NewManaged("node-1"), HerdrSocket: socket, HeartbeatInterval: 50 * time.Millisecond,
			VerifyServerPeer: func(ctx context.Context, address string) error {
				if address != "bufconn" {
					return errors.New("unexpected managed integration coordinator")
				}
				return ctx.Err()
			},
		})
	}()
	var once sync.Once
	session.stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-session.done:
			case <-time.After(5 * time.Second):
				t.Error("managed integration node did not stop")
			}
		})
	}
	t.Cleanup(session.stop)
	return session
}

func waitManagedIntegration(t *testing.T, h *commandHarness, session *managedIntegrationNode, generation uint64, readiness string) *pb.ProjectRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 10*time.Second)
	defer cancel()
	client := pb.NewFleetClient(h.connection)
	var last *pb.ProjectRecord
	for {
		var err error
		last, err = client.GetProject(ctx, &pb.GetProjectRequest{NodeInstanceId: "node-1", ProjectId: "AgentFlow"})
		if err != nil {
			t.Fatal(err)
		}
		fleet, err := client.ListNodes(ctx, &emptypb.Empty{})
		if err != nil {
			t.Fatal(err)
		}
		if last.Desired.Generation == generation && last.Readiness == readiness && len(fleet.Nodes) == 1 &&
			fleet.Nodes[0].CommandReady && (readiness != "applied" || (fleet.Nodes[0].WorkspaceReady && fleet.Nodes[0].WorktreeReady)) {
			return last
		}
		select {
		case <-session.done:
			t.Fatalf("managed node ended before readiness: %v", session.err)
		case <-ctx.Done():
			t.Fatalf("managed node did not become %s/%d: %v", readiness, generation, last)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func managedCheckout(t *testing.T, root string) string {
	t.Helper()
	checkout := filepath.Join(root, "PRIVATE-checkout")
	if err := os.Mkdir(checkout, 0700); err != nil {
		t.Fatal(err)
	}
	worktreeGit(t, checkout, "init", "--quiet", "-b", "main")
	worktreeGit(t, checkout, "-c", "user.name=Mesh Test", "-c", "user.email=mesh@example.invalid",
		"commit", "--quiet", "--allow-empty", "-m", "managed integration fixture")
	canonical, err := filepath.EvalSymlinks(checkout)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

func TestCentralManagedWorkspaceThroughNodeIPCAndBothJournals(t *testing.T) {
	for _, dropResult := range []bool{false, true} {
		name := "known-result"
		if dropResult {
			name = "lost-coordinator-result"
		}
		t.Run(name, func(t *testing.T) {
			root := managedTestRoot(t)
			checkout := managedCheckout(t, root)
			fake := newFakeWorkspaceHerdr(t, checkout, false, false)
			serverRoot, journal := filepath.Join(root, "server"), filepath.Join(root, "node", "journal.db")
			h := newCommandHarness(t, serverRoot)
			registration := &pb.RegisterProjectRequest{NodeInstanceId: "node-1", ProjectId: "AgentFlow", CheckoutPath: checkout}
			if record := managedRegister(t, h, registration); record.Readiness != "offline" {
				t.Fatalf("registration required a live node: %v", record)
			}
			session := startManagedIntegrationNode(t, h, journal, fake.path, dropResult)
			record := waitManagedIntegration(t, h, session, 1, "applied")
			defaultRoot := checkout + "-worktrees"
			if record.Desired.WorktreeRoot != "" || record.Applied.CheckoutPath != checkout || record.Applied.WorktreeRoot != defaultRoot {
				t.Fatalf("node did not resolve private default root: %v", record)
			}
			if info, err := os.Stat(defaultRoot); err != nil || !info.IsDir() {
				t.Fatalf("default worktree root not created: %v", err)
			}
			ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 30*time.Second)
			defer cancel()
			request := managedWorkspaceRequest("managed-workspace")
			first, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if first.Command.WorkspaceEnsure.BindingRevision != "managed:1" ||
				first.Command.SubmittedRequest.GetWorkspaceEnsure().GetBindingRevision() != "" {
				t.Fatalf("default revision was not bound independently: %v", first.Command)
			}
			if dropResult {
				select {
				case <-session.done:
					if session.err == nil {
						t.Fatal("simulated result loss did not report a failed session")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("simulated result loss did not end node session")
				}
				awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_INDETERMINATE)
			} else {
				completed := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
				if !completed.WorkspaceEnsure.GetCreated() || completed.WorkspaceEnsure.GetWorkspaceId() != "w1" {
					t.Fatalf("missing typed workspace result: %v", completed)
				}
			}
			if fake.creates.Load() != 1 || session.client.count.Load() != 1 {
				t.Fatal("workspace not dispatched and created exactly once")
			}
			session.stop()
			h.stop()
			h = newCommandHarness(t, serverRoot)
			managedGet(t, h, "AgentFlow", "offline", 1)
			session = startManagedIntegrationNode(t, h, journal, fake.path, false)
			waitManagedIntegration(t, h, session, 1, "applied")
			completed := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
			if !completed.WorkspaceEnsure.GetCreated() || completed.WorkspaceEnsure.GetWorkspaceId() != "w1" {
				t.Fatalf("journal replay lost typed outcome: %v", completed)
			}
			client := pb.NewFleetClient(h.connection)
			retry, err := client.SubmitCommand(ctx, request)
			if err != nil || !proto.Equal(retry, completed) || session.client.count.Load() != 0 || fake.creates.Load() != 1 {
				t.Fatalf("restart/exact retry repeated effects or changed outcome: %v %v", retry, err)
			}
			second, err := client.SubmitCommand(ctx, managedWorkspaceRequest("new-workspace-key"))
			if err != nil {
				t.Fatal(err)
			}
			existing := awaitCommand(t, h, second.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
			if existing.WorkspaceEnsure.GetCreated() || existing.WorkspaceEnsure.GetWorkspaceId() != "w1" || fake.creates.Load() != 1 {
				t.Fatalf("fresh key did not reuse existing workspace: %v", existing)
			}
			raw, err := protojson.Marshal(existing)
			if err != nil || strings.Contains(string(raw), "PRIVATE") || strings.Contains(string(raw), "checkoutPath") {
				t.Fatalf("command leaked local metadata: %s %v", raw, err)
			}
			// Historical exact retries are independent of current registration validity.
			registration.CheckoutPath = filepath.Join(root, "missing-checkout")
			managedRegister(t, h, registration)
			waitManagedIntegration(t, h, session, 2, "invalid")
			retry, err = client.SubmitCommand(ctx, request)
			if err != nil || !proto.Equal(retry, completed) {
				t.Fatalf("registration update rebound historical retry: %v %v", retry, err)
			}
			if _, err := client.SubmitCommand(ctx, managedWorkspaceRequest("invalid-current-config")); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("invalid desired config admitted command: %v", err)
			}
			if fake.creates.Load() != 1 {
				t.Fatal("registration changes repeated workspace effects")
			}
		})
	}
}

func TestCentralManagedWorktreeDefaultsThroughNodeGitAndRestart(t *testing.T) {
	root := managedTestRoot(t)
	checkout := managedCheckout(t, root)
	base := worktreeGit(t, checkout, "rev-parse", "HEAD")
	defaultRoot := checkout + "-worktrees"
	fake := &fakeWorktreeHerdr{checkout: checkout, destination: filepath.Join(defaultRoot, "task-one"), base: base}
	fake.path = listenFakeHerdr(t, fake.serve)
	serverRoot, journal := filepath.Join(root, "server"), filepath.Join(root, "node", "journal.db")
	h := newCommandHarness(t, serverRoot)
	managedRegister(t, h, &pb.RegisterProjectRequest{NodeInstanceId: "node-1", ProjectId: "AgentFlow", CheckoutPath: checkout})
	session := startManagedIntegrationNode(t, h, journal, fake.path, false)
	applied := waitManagedIntegration(t, h, session, 1, "applied")
	if applied.Applied.WorktreeRoot != defaultRoot {
		t.Fatalf("default worktree root: %v", applied)
	}
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 30*time.Second)
	defer cancel()
	request := worktreeRequest("managed-worktree-defaults")
	request.WorktreeCreate.BindingRevision = ""
	request.WorktreeCreate.Branch = ""
	request.WorktreeCreate.BaseCommit = ""
	first, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Command.WorktreeCreate.BindingRevision != "managed:1" || first.Command.WorktreeCreate.Branch != "task-one" ||
		first.Command.SubmittedRequest == nil || first.Command.SubmittedRequest.WorktreeCreate.Branch != "" ||
		first.Command.SubmittedRequest.WorktreeCreate.BaseCommit != "" || first.Command.SubmittedRequest.WorktreeCreate.BindingRevision != "" {
		t.Fatalf("managed defaults overwrote original request identity: %v", first.Command)
	}
	completed := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	if completed.WorktreeCreate.GetWorkspaceId() != "w2" || completed.WorktreeCreate.GetBaseCommit() != base || fake.creates.Load() != 1 {
		t.Fatalf("defaults did not reach exact Git/Herdr creation: %v", completed)
	}
	if got := worktreeGit(t, fake.destination, "rev-parse", "HEAD"); got != base {
		t.Fatalf("default base resolved to %s, want %s", got, base)
	}
	if got := worktreeGit(t, fake.destination, "symbolic-ref", "--short", "HEAD"); got != "task-one" {
		t.Fatalf("default branch=%s, want task-one", got)
	}
	if got := worktreeGit(t, checkout, "symbolic-ref", "--short", "HEAD"); got != "main" {
		t.Fatalf("source checkout branch changed: %s", got)
	}
	if got := worktreeGit(t, checkout, "rev-parse", "HEAD"); got != base {
		t.Fatal("source checkout HEAD changed")
	}
	raw, err := protojson.Marshal(completed)
	if err != nil || strings.Contains(string(raw), "PRIVATE") {
		t.Fatalf("worktree command leaked paths: %s %v", raw, err)
	}
	session.stop()
	h.stop()
	h = newCommandHarness(t, serverRoot)
	managedGet(t, h, "AgentFlow", "offline", 1)
	session = startManagedIntegrationNode(t, h, journal, fake.path, false)
	// The default root is now nonempty: successful reapplication must restore
	// journaled ownership, not treat this project's own worktree as foreign.
	waitManagedIntegration(t, h, session, 1, "applied")
	retry, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, request)
	if err != nil || !proto.Equal(retry, completed) || session.client.count.Load() != 0 || fake.creates.Load() != 1 {
		t.Fatalf("restart lost worktree identity or repeated effects: %v %v", retry, err)
	}
	changed := proto.Clone(request).(*pb.SubmitCommandRequest)
	changed.WorktreeCreate.Branch = "task-one"
	if _, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("historical request identity collapsed omitted and explicit defaults: %v", err)
	}
	changed = proto.Clone(request).(*pb.SubmitCommandRequest)
	changed.IdempotencyKey = "new-key-existing-worktree"
	second, err := pb.NewFleetClient(h.connection).SubmitCommand(ctx, changed)
	if err != nil {
		t.Fatal(err)
	}
	rejected := awaitCommand(t, h, second.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_REJECTED)
	if rejected.Detail != "precondition_failed" || fake.creates.Load() != 1 {
		t.Fatalf("fresh key repeated existing worktree creation: %v", rejected)
	}
}
