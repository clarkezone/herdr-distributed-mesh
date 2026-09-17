package server

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCentralProjectListIsBoundedAndContinued(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	for i := 0; i <= protocol.MaxProjectPageSize; i++ {
		if _, err := h.api.commands.UpsertProject(context.Background(), &pb.RegisterProjectRequest{
			NodeInstanceId: "node", ProjectId: fmt.Sprintf("project-%03d", i), CheckoutPath: `C:\checkout`,
		}); err != nil {
			t.Fatal(err)
		}
	}
	client := pb.NewFleetClient(h.connection)
	ctx := commandPeer(context.Background(), "client")
	first, err := client.ListProjects(ctx, &pb.ListProjectsRequest{})
	if err != nil || len(first.GetProjects()) != protocol.MaxProjectPageSize || first.NextPageToken == "" {
		t.Fatalf("first page: %v %v", first, err)
	}
	next, err := client.ListProjects(ctx, &pb.ListProjectsRequest{PageToken: first.NextPageToken})
	if err != nil || len(next.GetProjects()) != 1 || next.NextPageToken != "" ||
		next.Projects[0].Desired.ProjectId <= first.Projects[len(first.Projects)-1].Desired.ProjectId {
		t.Fatalf("continuation: %v %v", next, err)
	}
	for _, request := range []*pb.ListProjectsRequest{
		{PageSize: protocol.MaxProjectPageSize + 1}, {PageToken: "invalid"},
		{NodeInstanceId: "other", PageToken: first.NextPageToken},
	} {
		if _, err := client.ListProjects(ctx, request); status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid pagination accepted", err)
		}
	}
}
