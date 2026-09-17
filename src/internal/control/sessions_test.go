package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type sessionsClientFixture struct {
	pb.FleetClient
	list func(context.Context, *pb.ListSessionsRequest) (*pb.SessionList, error)
}

func (f sessionsClientFixture) ListSessions(ctx context.Context, request *pb.ListSessionsRequest, _ ...grpc.CallOption) (*pb.SessionList, error) {
	return f.list(ctx, request)
}

func TestSessionsListsTypedProjectionAndErrors(t *testing.T) {
	for _, code := range []string{"", "session_manager_unavailable", "session_capacity"} {
		t.Run(code, func(t *testing.T) {
			var out bytes.Buffer
			list := &pb.SessionList{ErrorCode: code}
			if code == "" {
				list.Sessions = []*pb.SessionView{{Name: "worker", Incarnation: strings.Repeat("a", 64), Status: "ready"}}
			}
			options := Options{Output: &out, JSON: true, RequiredServerTag: "tag:server", FleetClient: sessionsClientFixture{
				list: func(_ context.Context, request *pb.ListSessionsRequest) (*pb.SessionList, error) {
					if request.NodeInstanceId != "node-1" {
						t.Fatal("node selector lost")
					}
					return list, nil
				},
			}}
			err := Sessions(context.Background(), options, "node-1")
			if (err != nil) != (code != "") || (code != "" && !strings.Contains(err.Error(), code)) {
				t.Fatalf("error = %v", err)
			}
			var value map[string]json.RawMessage
			if err := json.Unmarshal(out.Bytes(), &value); err != nil {
				t.Fatal(err)
			}
			if len(value) != 2 || value["sessions"] == nil || value["error_code"] == nil {
				t.Fatalf("wrong JSON shape: %s", &out)
			}
			if strings.Contains(out.String(), "path") || strings.Contains(out.String(), "socket") {
				t.Fatalf("private metadata exposed: %s", &out)
			}
		})
	}
}

func TestSessionsRejectInvalidResultsAndTransportErrors(t *testing.T) {
	for _, list := range []*pb.SessionList{
		nil, {ErrorCode: `C:\private`}, {Sessions: []*pb.SessionView{nil}},
		{Sessions: []*pb.SessionView{{Name: "worker", Status: "ready"}}},
		{Sessions: []*pb.SessionView{{Name: "con", Status: "stopped"}}},
	} {
		if err := writeSessions(Options{Output: &bytes.Buffer{}, JSON: true}, list); err == nil {
			t.Fatalf("invalid list accepted: %v", list)
		}
	}
	want := status.Error(codes.PermissionDenied, "denied")
	err := Sessions(context.Background(), Options{RequiredServerTag: "tag:server", FleetClient: sessionsClientFixture{
		list: func(context.Context, *pb.ListSessionsRequest) (*pb.SessionList, error) { return nil, want },
	}}, "node-1")
	if !errors.Is(err, want) {
		t.Fatalf("lost RPC error: %v", err)
	}
}

func TestEnsureSessionUsesDurableExactRequestWithoutDiscovery(t *testing.T) {
	calls := 0
	var original *pb.SubmitCommandRequest
	client := agentClientFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		calls++
		if request.CommandType != protocol.SessionEnsureCommandType || request.SessionEnsure.GetName() != "worker" ||
			request.IdempotencyKey != "retry-key" || request.Ttl.AsDuration() != 15*time.Second {
			t.Fatalf("request changed: %v", request)
		}
		if original != nil && !proto.Equal(original, request) {
			t.Fatal("same-key retry changed request")
		}
		original = proto.Clone(request).(*pb.SubmitCommandRequest)
		return &pb.CommandRecord{Command: &pb.Command{CommandId: protocol.NewCommandID()},
			Status:        pb.CommandStatus_COMMAND_STATUS_SUCCEEDED,
			SessionEnsure: &pb.SessionView{Name: "worker", Incarnation: strings.Repeat("a", 64), Status: "ready"}}, nil
	}}
	var out bytes.Buffer
	opts := Options{Output: &out, JSON: true, RequiredServerTag: "tag:server", FleetClient: client}
	for range 2 {
		if err := EnsureSession(context.Background(), opts, "node-1", "retry-key", "worker", 15*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 || strings.Count(out.String(), "\n") != 2 || !strings.Contains(out.String(), `"session_ensure"`) {
		t.Fatalf("calls=%d output=%s", calls, &out)
	}
}

func TestNamedWorkspaceAndWorktreePreserveExplicitRetry(t *testing.T) {
	incarnation := strings.Repeat("b", 64)
	workspace := &pb.WorkspaceEnsure{ProjectId: "Project", BindingRevision: "r1", SessionName: "worker", SessionIncarnation: incarnation}
	worktree := &pb.WorktreeCreate{ProjectId: "Project", BindingRevision: "r1", Name: "task", Branch: "task",
		BaseCommit: strings.Repeat("a", 40), SessionName: "worker", SessionIncarnation: incarnation}
	for _, kind := range []string{protocol.WorkspaceEnsureCommandType, protocol.WorktreeCreateCommandType} {
		t.Run(kind, func(t *testing.T) {
			var original *pb.SubmitCommandRequest
			client := agentClientFixture{submit: func(_ context.Context, request *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
				if request.CommandType != kind || request.IdempotencyKey != "fixed-key" {
					t.Fatal("command identity changed")
				}
				if kind == protocol.WorkspaceEnsureCommandType && !proto.Equal(request.WorkspaceEnsure, workspace) ||
					kind == protocol.WorktreeCreateCommandType && !proto.Equal(request.WorktreeCreate, worktree) {
					t.Fatal("session/project pins changed")
				}
				if original != nil && !proto.Equal(original, request) {
					t.Fatal("exact retry re-resolved")
				}
				original = proto.Clone(request).(*pb.SubmitCommandRequest)
				return &pb.CommandRecord{Command: &pb.Command{CommandId: protocol.NewCommandID()}, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED}, nil
			}}
			opts := Options{Output: &bytes.Buffer{}, RequiredServerTag: "tag:server", FleetClient: client}
			for range 2 {
				var err error
				if kind == protocol.WorkspaceEnsureCommandType {
					err = EnsureWorkspaceInSession(context.Background(), opts, "node-1", "fixed-key", workspace, time.Second)
				} else {
					err = CreateWorktree(context.Background(), opts, "node-1", "fixed-key", worktree, time.Second)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
