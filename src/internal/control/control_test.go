package control

import (
	"context"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type retryFleetClient struct {
	calls int
}

func (client *retryFleetClient) GetServerInfo(
	context.Context,
	*emptypb.Empty,
	...grpc.CallOption,
) (*agentflowv1.ServerInfo, error) {
	client.calls++
	if client.calls == 1 {
		return nil, status.Error(codes.Unavailable, "starting")
	}
	return &agentflowv1.ServerInfo{InstanceId: "server-1"}, nil
}

func TestGetServerInfoRetriesUnavailable(t *testing.T) {
	client := &retryFleetClient{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	info, err := getServerInfo(ctx, client)
	if err != nil {
		t.Fatalf("getServerInfo() error = %v", err)
	}
	if info.InstanceId != "server-1" || client.calls != 2 {
		t.Fatalf("getServerInfo() info=%v calls=%d, want server-1 after 2 calls", info, client.calls)
	}
}

type deniedFleetClient struct{}

func (*deniedFleetClient) GetServerInfo(
	context.Context,
	*emptypb.Empty,
	...grpc.CallOption,
) (*agentflowv1.ServerInfo, error) {
	return nil, status.Error(codes.PermissionDenied, "wrong role")
}

func TestGetServerInfoDoesNotRetryPermissionDenied(t *testing.T) {
	_, err := getServerInfo(context.Background(), &deniedFleetClient{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("getServerInfo() code = %s, want %s", status.Code(err), codes.PermissionDenied)
	}
}
