package server

import (
	"context"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxPendingAgentQueries = 8

type pendingAgentQuery struct {
	request *pb.AgentQueryRequest
	expires time.Time
	result  chan *pb.AgentQueryResult
}

func (s *service) agentConfigured() bool {
	return s.commands != nil && s.requiredClientTag != "" && s.requiredCommandTag != "" && s.requiredNodeTag != ""
}

func (s *service) QueryAgent(ctx context.Context, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
	if !s.agentConfigured() {
		return nil, status.Error(codes.PermissionDenied, "agent authorization is not configured")
	}
	if _, err := s.authorizePeer(ctx, s.requiredClientTag); err != nil {
		return nil, err
	}
	request, err := protocol.NormalizeAgentQuery(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid agent query")
	}
	// The transport bound remains finite even if a caller has no deadline.
	timeout := time.Duration(request.TimeoutMs) * time.Millisecond
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	expiry, _ := ctx.Deadline()
	id := protocol.NewCommandID()
	pending := &pendingAgentQuery{request: request, expires: expiry, result: make(chan *pb.AgentQueryResult, 1)}
	s.fleet.mu.Lock()
	if s.fleet.storageErr != nil {
		s.fleet.mu.Unlock()
		return nil, storageUnavailable()
	}
	entry := s.fleet.nodes[request.NodeInstanceId]
	if entry == nil || !s.fleet.current(entry) || !mutationReady(entry, protocol.AgentControlCommandType, time.Now()) {
		s.fleet.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "target node has no ready agent session")
	}
	if len(entry.queries) >= maxPendingAgentQueries || len(entry.queryOutbound) == cap(entry.queryOutbound) {
		s.fleet.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "node query capacity reached")
	}
	entry.queries[id] = pending
	entry.queryOutbound <- &pb.NodeEnvelope{Body: &pb.NodeEnvelope_AgentQuery{AgentQuery: &pb.AgentQuery{
		QueryId: id, Request: request, ExpiresAt: timestamppb.New(expiry),
	}}}
	s.fleet.mu.Unlock()
	defer s.cancelAgentQuery(entry, id, pending)
	select {
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case <-entry.done:
		return nil, status.Error(codes.Unavailable, "node session ended")
	case result := <-pending.result:
		s.fleet.mu.Lock()
		current := s.fleet.current(entry)
		s.fleet.mu.Unlock()
		if !current {
			return nil, status.Error(codes.Unavailable, "node session ended")
		}
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return result, nil
	}
}

func (s *service) cancelAgentQuery(entry *fleetEntry, id string, pending *pendingAgentQuery) {
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if entry.queries[id] != pending {
		return
	}
	delete(entry.queries, id)
	if !s.fleet.current(entry) {
		return
	}
	for key, expires := range entry.queryCanceled {
		if time.Now().After(expires) {
			delete(entry.queryCanceled, key)
		}
	}
	// Tombstones distinguish canceled responses from foreign/unsolicited IDs.
	if len(entry.queryCanceled) >= 128 {
		for key := range entry.queryCanceled {
			delete(entry.queryCanceled, key)
			break
		}
	}
	entry.queryCanceled[id] = pending.expires.Add(10 * time.Second)
	select {
	case entry.queryOutbound <- &pb.NodeEnvelope{Body: &pb.NodeEnvelope_AgentQueryCancel{AgentQueryCancel: &pb.AgentQueryCancel{QueryId: id}}}:
	default:
		// The finite wire expiry is the fallback when the bounded queue is full.
	}
}

func (s *service) prepareAgentEnvelope(entry *fleetEntry, envelope *pb.NodeEnvelope) bool {
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if !s.fleet.current(entry) {
		return false
	}
	if query := envelope.GetAgentQuery(); query != nil {
		return entry.queries[query.QueryId] != nil && time.Now().Before(query.ExpiresAt.AsTime())
	}
	return envelope.GetAgentQueryCancel() != nil
}

func (s *service) finishAgentQuery(entry *fleetEntry, result *pb.AgentQueryResult) error {
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if !s.fleet.current(entry) {
		return status.Error(codes.Aborted, "node stream superseded")
	}
	if !entry.agents || result == nil || !protocol.ValidCommandID(result.QueryId) || len(result.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "invalid agent query response")
	}
	pending := entry.queries[result.QueryId]
	if pending == nil {
		if expiry, ok := entry.queryCanceled[result.QueryId]; ok && time.Now().Before(expiry) {
			return nil
		}
		return status.Error(codes.InvalidArgument, "unmatched agent query response")
	}
	if err := protocol.ValidateAgentQueryResult(result, pending.request); err != nil {
		return status.Error(codes.InvalidArgument, "invalid agent query response")
	}
	delete(entry.queries, result.QueryId)
	pending.result <- proto.Clone(result).(*pb.AgentQueryResult)
	return nil
}
