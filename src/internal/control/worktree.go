package control

import (
	"context"
	"errors"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/types/known/durationpb"
)

func CreateWorktree(ctx context.Context, options Options, nodeID, key string, worktree *pb.WorktreeCreate, ttl time.Duration) error {
	if options.RequiredServerTag == "" {
		return errors.New("command requests require an expected server tag")
	}
	request, err := protocol.NormalizeCommandRequest(&pb.SubmitCommandRequest{
		NodeInstanceId: nodeID, IdempotencyKey: key, CommandType: protocol.WorktreeCreateCommandType, Ttl: durationpb.New(ttl),
		WorktreeCreate: worktree,
	})
	if err != nil {
		return err
	}
	return submitAndWait(ctx, options, request)
}
