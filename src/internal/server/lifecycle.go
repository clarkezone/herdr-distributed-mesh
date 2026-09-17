package server

import (
	"context"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *service) commandProgress(entry *fleetEntry, progress *pb.CommandProgress) error {
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if !s.fleet.current(entry) || !entry.lifecycle || s.commands == nil {
		return status.Error(codes.FailedPrecondition, "lifecycle stream is not current")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.commands.SaveCommandProgress(ctx, entry.view.InstanceId, progress); err != nil {
		return s.commandErrorLocked(err)
	}
	return nil
}
