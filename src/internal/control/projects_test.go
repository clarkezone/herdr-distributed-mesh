package control

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type projectClient struct {
	pb.FleetClient
	register func(*pb.RegisterProjectRequest) (*pb.ProjectRecord, error)
	get      func(*pb.GetProjectRequest) (*pb.ProjectRecord, error)
	list     func(*pb.ListProjectsRequest) (*pb.ProjectList, error)
}

func (c projectClient) RegisterProject(_ context.Context, request *pb.RegisterProjectRequest, _ ...grpc.CallOption) (*pb.ProjectRecord, error) {
	return c.register(request)
}

func (c projectClient) GetProject(_ context.Context, request *pb.GetProjectRequest, _ ...grpc.CallOption) (*pb.ProjectRecord, error) {
	return c.get(request)
}

func (c projectClient) ListProjects(_ context.Context, request *pb.ListProjectsRequest, _ ...grpc.CallOption) (*pb.ProjectList, error) {
	return c.list(request)
}

func projectFixture() *pb.ProjectRecord {
	return &pb.ProjectRecord{
		Desired:   &pb.ProjectConfig{NodeInstanceId: "node-1", ProjectId: "AgentFlow", Generation: 2, CheckoutPath: `C:\desired`, WorktreeRoot: `C:\worktrees`},
		Applied:   &pb.ProjectAck{ProjectId: "AgentFlow", Generation: 1, Status: "applied", CheckoutPath: `C:\applied`, WorktreeRoot: `C:\old-worktrees`},
		Readiness: "pending", AdoptionStatus: "adopted",
	}
}

func TestProjectRPCRequestsAndOutput(t *testing.T) {
	record := projectFixture()
	request := &pb.RegisterProjectRequest{NodeInstanceId: "node-1", ProjectId: "AgentFlow", CheckoutPath: `C:\desired`, WorktreeRoot: `C:\worktrees`}
	client := projectClient{
		register: func(got *pb.RegisterProjectRequest) (*pb.ProjectRecord, error) {
			if !proto.Equal(got, request) {
				t.Fatal("registration fields changed")
			}
			return record, nil
		},
		get: func(got *pb.GetProjectRequest) (*pb.ProjectRecord, error) {
			if got.NodeInstanceId != "node-1" || got.ProjectId != "AgentFlow" {
				t.Fatal("lookup target changed")
			}
			return record, nil
		},
		list: func(got *pb.ListProjectsRequest) (*pb.ProjectList, error) {
			if got.NodeInstanceId != "node-1" {
				t.Fatal("list filter changed")
			}
			return &pb.ProjectList{Projects: []*pb.ProjectRecord{record}}, nil
		},
	}
	for _, call := range []func(context.Context, Options) error{
		func(ctx context.Context, options Options) error {
			return registerProjectWithClient(ctx, options, client, request)
		},
		func(ctx context.Context, options Options) error {
			return getProjectWithClient(ctx, options, client, &pb.GetProjectRequest{NodeInstanceId: "node-1", ProjectId: "AgentFlow"})
		},
		func(ctx context.Context, options Options) error {
			return projectsWithClient(ctx, options, client, &pb.ListProjectsRequest{NodeInstanceId: "node-1"})
		},
	} {
		for _, jsonOutput := range []bool{false, true} {
			var out bytes.Buffer
			if err := call(context.Background(), Options{JSON: jsonOutput, Output: &out}); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "pending") || !strings.Contains(out.String(), "adopted") || !strings.Contains(out.String(), "desired") || !strings.Contains(out.String(), "applied") {
				t.Fatalf("incomplete config inspection: %s", out.String())
			}
			if jsonOutput {
				decoder := json.NewDecoder(&out)
				var value map[string]any
				if err := decoder.Decode(&value); err != nil {
					t.Fatal(err)
				}
				if err := decoder.Decode(new(any)); err != io.EOF {
					t.Fatal("stdout is not one JSON object")
				}
			}
		}
	}
}

func TestProjectReadinessAndJSONFields(t *testing.T) {
	for _, readiness := range []string{"pending", "offline", "applied", "invalid", "unsupported"} {
		record := projectFixture()
		record.Readiness = readiness
		var out bytes.Buffer
		if err := writeProject(Options{JSON: true, Output: &out}, record); err != nil {
			t.Fatal(err)
		}
		var value struct {
			Desired struct {
				Generation string `json:"generation"`
				Path       string `json:"checkout_path"`
			} `json:"desired"`
			Applied struct {
				Generation string `json:"generation"`
				Path       string `json:"checkout_path"`
			} `json:"applied"`
			Readiness string `json:"readiness"`
		}
		if err := json.Unmarshal(out.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		if value.Readiness != readiness || value.Desired.Generation != "2" || value.Applied.Generation != "1" || value.Desired.Path != `C:\desired` || value.Applied.Path != `C:\applied` {
			t.Fatalf("inspection lost desired/applied distinction: %s", out.String())
		}
	}
	var out bytes.Buffer
	if err := writeProjects(Options{JSON: true, Output: &out}, &pb.ProjectList{}); err != nil {
		t.Fatal(err)
	}
	var empty struct {
		Projects []json.RawMessage `json:"projects"`
	}
	if err := json.Unmarshal(out.Bytes(), &empty); err != nil || empty.Projects == nil || len(empty.Projects) != 0 {
		t.Fatalf("empty list shape: %s %v", out.String(), err)
	}
}

func TestProjectPendingWithoutAcknowledgement(t *testing.T) {
	record := projectFixture()
	record.Applied = nil
	var out bytes.Buffer
	if err := writeProject(Options{Output: &out}, record); err != nil || !strings.Contains(out.String(), "applied=none") {
		t.Fatalf("pending record invented an acknowledgement: %s %v", out.String(), err)
	}
	client := projectClient{list: func(request *pb.ListProjectsRequest) (*pb.ProjectList, error) {
		if request.NodeInstanceId != "" {
			t.Fatal("unfiltered listing acquired a node filter")
		}
		return &pb.ProjectList{}, nil
	}}
	out.Reset()
	if err := projectsWithClient(context.Background(), Options{Output: &out}, client, &pb.ListProjectsRequest{}); err != nil || !strings.Contains(out.String(), "no registered projects") {
		t.Fatalf("empty human listing failed: %s %v", out.String(), err)
	}
}

func TestProjectErrors(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		options := Options{JSON: jsonOutput, Output: failedWriter{}}
		if err := writeProject(options, projectFixture()); err == nil {
			t.Fatal("record output error hidden")
		}
		if err := writeProjects(options, &pb.ProjectList{}); err == nil {
			t.Fatal("list output error hidden")
		}
		if err := writeProject(options, nil); err == nil {
			t.Fatal("nil record accepted")
		}
		if err := writeProjects(options, nil); err == nil {
			t.Fatal("nil list accepted")
		}
	}
	client := projectClient{
		register: func(*pb.RegisterProjectRequest) (*pb.ProjectRecord, error) {
			return nil, status.Error(codes.PermissionDenied, "denied")
		},
		get: func(*pb.GetProjectRequest) (*pb.ProjectRecord, error) {
			return nil, status.Error(codes.NotFound, "missing")
		},
		list: func(*pb.ListProjectsRequest) (*pb.ProjectList, error) {
			return nil, status.Error(codes.Unimplemented, "upgrade required")
		},
	}
	if err := registerProjectWithClient(context.Background(), Options{}, client, &pb.RegisterProjectRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("registration error lost: %v", err)
	}
	if err := getProjectWithClient(context.Background(), Options{}, client, &pb.GetProjectRequest{}); status.Code(err) != codes.NotFound {
		t.Fatalf("lookup error lost: %v", err)
	}
	if err := projectsWithClient(context.Background(), Options{}, client, &pb.ListProjectsRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unsupported server error lost: %v", err)
	}
	if err := Projects(context.Background(), Options{}, ""); err == nil {
		t.Fatal("unverified coordinator accepted")
	}
}
