package server

import (
	"context"
	"sync"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/fleetwatch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fleetSubscription struct {
	once    sync.Once
	watcher *fleetwatch.Watcher
	err     error
}

func (s *service) WatchNodes(_ *emptypb.Empty, stream grpc.ServerStreamingServer[pb.NodeList]) error {
	s.fleetSubscription.once.Do(func() {
		s.fleetSubscription.watcher, s.fleetSubscription.err = fleetwatch.New(func(ctx context.Context) (*pb.NodeList, error) {
			readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			return s.ListNodes(readCtx, &emptypb.Empty{})
		})
	})
	if s.fleetSubscription.err != nil {
		return status.Error(codes.Internal, "fleet subscription initialization failed")
	}
	return s.fleetSubscription.watcher.Watch(stream.Context(), stream.Send)
}
