package dashboard

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDisplayBrowserFixture(t *testing.T) {
	address := os.Getenv("HERDR_MESH_DASHBOARD_FIXTURE")
	if address == "" {
		t.Skip("set HERDR_MESH_DASHBOARD_FIXTURE to a loopback address for a five-minute synthetic browser fixture")
	}
	if err := ValidateListenAddress(address); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client := fakeClient{list: func(context.Context) (*pb.NodeList, error) {
		stamp := timestamppb.Now()
		return &pb.NodeList{Nodes: []*pb.NodeView{{
			InstanceId: "node-1", Hostname: "desktop", TailscaleStableId: "peer-1", Connected: true,
			LastSeen: stamp, HerdrReceivedAt: stamp,
			Herdr: &pb.HerdrState{
				Status: "ready", Version: "0.9.0", Protocol: 18, Sequence: 1, ObservedAt: stamp,
				Workspaces: []*pb.HerdrEntity{
					{Id: "w1", DisplayName: "API service", AgentStatus: "working"},
					{Id: "w2", DisplayName: "Operations", AgentStatus: "idle"},
				},
				Tabs: []*pb.HerdrEntity{
					{Id: "t1", WorkspaceId: "w1", DisplayName: "Unit tests", AgentStatus: "working"},
					{Id: "t2", WorkspaceId: "w2", DisplayName: "Deploy", AgentStatus: "idle"},
				},
				Panes: []*pb.HerdrEntity{
					{Id: "p1", WorkspaceId: "w1", TabId: "t1", DisplayName: "Review shell", Directory: `C:\src\API service\tests`, AgentStatus: "working"},
					{Id: "p2", WorkspaceId: "w2", TabId: "t2", Directory: "/srv/ops", AgentStatus: "idle"},
				},
				Agents: []*pb.HerdrEntity{
					{Id: "p1", WorkspaceId: "w1", TabId: "t1", DisplayName: "Reviewer <script>window.displayInjected=1</script>",
						Directory: `C:\src\API service\tests`, Provider: "copilot", AgentStatus: "working"},
					{Id: "p2", WorkspaceId: "w2", TabId: "t2", Directory: "/srv/ops", Provider: "copilot", AgentStatus: "idle"},
				},
			},
		}}}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := serve(ctx, listener, client, os.Stdout); err != nil {
		t.Fatal(err)
	}
}
