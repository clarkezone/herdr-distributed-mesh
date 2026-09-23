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

const maxSessionInventoryBytes = 256 * 1024

func (s *service) sessionsConfigured() bool {
	return s.commands != nil && s.requiredCommandTag != "" && s.requiredNodeTag != ""
}

func validateSessionViews(values []*pb.SessionView) error {
	bad := status.Error(codes.InvalidArgument, "invalid session inventory")
	if len(values) > 64 {
		return status.Error(codes.ResourceExhausted, "session count limit exceeded")
	}
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if value == nil || value.Name == "" || protocol.ValidateSessionSelector(value.Name, value.Incarnation, false) != nil ||
			len(value.ProtoReflect().GetUnknown()) != 0 || seen[value.Name] {
			return bad
		}
		seen[value.Name] = true
		switch value.Status {
		case "ready":
			if !protocol.ValidSessionIncarnation(value.Incarnation) || value.ErrorCode != "" {
				return bad
			}
		case "stopped", "starting", "unavailable", "unsupported":
			if value.Herdr != nil {
				return bad
			}
		default:
			return bad
		}
		switch value.ErrorCode {
		case "", "session_unavailable", "unsupported_protocol", "session_default_unmanaged":
		default:
			return bad
		}
		if value.Herdr != nil {
			if err := validateHerdrState(value.Herdr); err != nil {
				return err
			}
		}
		if stamp := value.HerdrReceivedAt; stamp != nil &&
			(value.Herdr == nil || stamp.CheckValid() != nil || len(stamp.ProtoReflect().GetUnknown()) != 0) {
			return bad
		}
	}
	return nil
}

func validateSessionInventory(inventory *pb.SessionInventory) error {
	if inventory == nil || inventory.Sequence == 0 || inventory.ObservedAt == nil ||
		inventory.ObservedAt.CheckValid() != nil || len(inventory.ProtoReflect().GetUnknown()) != 0 ||
		len(inventory.ObservedAt.ProtoReflect().GetUnknown()) != 0 ||
		(inventory.ErrorCode != "" && (!protocol.ValidSessionError(inventory.ErrorCode) ||
			(inventory.ErrorCode != "session_capacity" && len(inventory.Sessions) != 0))) {
		return status.Error(codes.InvalidArgument, "invalid session inventory")
	}
	if proto.Size(inventory) > maxSessionInventoryBytes {
		return status.Error(codes.ResourceExhausted, "session inventory size limit exceeded")
	}
	if inventory.ErrorCode == "session_capacity" {
		for _, value := range inventory.Sessions {
			if value.GetHerdr() != nil {
				return status.Error(codes.InvalidArgument, "capacity report must omit session topology")
			}
		}
	}
	for _, value := range inventory.Sessions {
		if value.GetHerdrReceivedAt() != nil || value.GetStale() {
			return status.Error(codes.InvalidArgument, "nodes cannot assign session projection freshness")
		}
	}
	return validateSessionViews(inventory.Sessions)
}

func validateStoredSessions(view *pb.NodeView) error {
	if view.SessionsReceivedAt == nil {
		if len(view.Sessions) != 0 || view.SessionsErrorCode != "" {
			return status.Error(codes.InvalidArgument, "session inventory has no receipt timestamp")
		}
		return nil // Older databases have no session fields; readiness may precede discovery.
	}
	if err := validateSessionViews(view.Sessions); err != nil {
		return err
	}
	inventory := &pb.SessionInventory{Sequence: 1, ObservedAt: view.SessionsReceivedAt, ErrorCode: view.SessionsErrorCode}
	for _, value := range view.Sessions {
		copy := proto.Clone(value).(*pb.SessionView)
		copy.HerdrReceivedAt, copy.Stale = nil, false
		inventory.Sessions = append(inventory.Sessions, copy)
	}
	return validateSessionInventory(inventory)
}

func nodeStateSize(view *pb.NodeView) int {
	size := proto.Size(view.Herdr)
	for _, session := range view.Sessions {
		size += proto.Size(session)
	}
	return size
}

func (f *fleetStore) updateSessions(entry *fleetEntry, inventory *pb.SessionInventory, now time.Time) error {
	if err := validateSessionInventory(inventory); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.current(entry) {
		return status.Error(codes.Aborted, "node stream superseded")
	}
	if !entry.sessions {
		return status.Error(codes.FailedPrecondition, "session inventory was not negotiated")
	}
	if inventory.Sequence <= entry.sessionSequence {
		return status.Error(codes.InvalidArgument, "session sequence must increase within a stream")
	}
	copy := proto.Clone(entry.view).(*pb.NodeView)
	copy.Sessions = proto.Clone(inventory).(*pb.SessionInventory).Sessions
	copy.SessionsReceivedAt = timestamppb.New(now)
	copy.SessionsErrorCode = inventory.ErrorCode
	for _, value := range copy.Sessions {
		if value.Herdr == nil {
			continue
		}
		previous := findSession(entry.view, value.Name)
		if previous != nil && previous.Incarnation == value.Incarnation &&
			proto.Equal(previous.Herdr, value.Herdr) {
			value.HerdrReceivedAt = previous.HerdrReceivedAt
		} else {
			value.HerdrReceivedAt = timestamppb.New(now)
		}
	}
	projectSessionFreshness(copy, now)
	size := nodeStateSize(copy)
	for _, other := range f.nodes {
		if other != entry {
			size += nodeStateSize(other.view)
		}
	}
	if size > maxFleetBytes {
		return status.Error(codes.ResourceExhausted, "fleet state size limit exceeded")
	}
	if err := f.save(copy); err != nil {
		return err
	}
	previous := entry.view
	entry.view, entry.sessionSequence = copy, inventory.Sequence
	for id, pending := range entry.queries {
		name := pending.request.GetTarget().GetSessionName()
		if name == "" {
			continue
		}
		before, after := findSession(previous, name), findSession(copy, name)
		if before == nil || after == nil || before.Incarnation != after.Incarnation || !selectedSessionReady(entry, name, before.Incarnation, now) {
			f.cancelSessionQueryLocked(entry, id, pending)
		}
	}
	return nil
}

func (f *fleetStore) cancelSessionQueryLocked(entry *fleetEntry, id string, pending *pendingAgentQuery) {
	delete(entry.queries, id)
	for key, expiry := range entry.queryCanceled {
		if time.Now().After(expiry) || len(entry.queryCanceled) >= 128 {
			delete(entry.queryCanceled, key)
		}
	}
	entry.queryCanceled[id] = pending.expires.Add(10 * time.Second)
	select {
	case entry.queryOutbound <- &pb.NodeEnvelope{Body: &pb.NodeEnvelope_AgentQueryCancel{AgentQueryCancel: &pb.AgentQueryCancel{QueryId: id}}}:
	default:
	}
	pending.result <- &pb.AgentQueryResult{QueryId: id, ErrorCode: "session_replaced"}
}

func findSession(view *pb.NodeView, name string) *pb.SessionView {
	for _, session := range view.Sessions {
		if session.Name == name {
			return session
		}
	}
	return nil
}

func sessionManagerReady(entry *fleetEntry, now time.Time) bool {
	return entry.sessions && entry.view.Connected && entry.view.SessionsReady &&
		entry.view.LastSeen != nil && now.Sub(entry.view.LastSeen.AsTime()) < herdrStaleAfter
}

func selectedSessionReady(entry *fleetEntry, name, incarnation string, now time.Time) bool {
	if !sessionManagerReady(entry, now) || entry.view.SessionsReceivedAt == nil ||
		now.Sub(entry.view.SessionsReceivedAt.AsTime()) >= herdrStaleAfter || entry.view.SessionsErrorCode != "" {
		return false
	}
	value := findSession(entry.view, name)
	return value != nil && (incarnation == "" || value.Incarnation == incarnation) &&
		freshSession(entry.view, value, now)
}

func freshSession(view *pb.NodeView, value *pb.SessionView, now time.Time) bool {
	return view.Connected && view.SessionsReady && view.SessionsErrorCode == "" &&
		view.LastSeen != nil && freshSessionTimestamp(view.LastSeen, now) &&
		view.SessionsReceivedAt != nil && freshSessionTimestamp(view.SessionsReceivedAt, now) &&
		value.Status == "ready" && value.ErrorCode == "" && value.Herdr.GetStatus() == "ready" &&
		value.HerdrReceivedAt != nil && freshSessionTimestamp(value.HerdrReceivedAt, now)
}

func freshSessionTimestamp(stamp *timestamppb.Timestamp, now time.Time) bool {
	age := now.Sub(stamp.AsTime())
	return age >= 0 && age < herdrStaleAfter
}

func projectSessionFreshness(view *pb.NodeView, now time.Time) {
	for _, value := range view.Sessions {
		value.Stale = !freshSession(view, value, now)
	}
}

func requestSession(request *pb.SubmitCommandRequest) (string, string) {
	return protocol.CommandSession(&pb.Command{CommandType: request.CommandType, WorkspaceEnsure: request.WorkspaceEnsure,
		WorktreeCreate: request.WorktreeCreate, AgentControl: request.AgentControl, SessionEnsure: request.SessionEnsure,
		AgentStart: request.AgentStart, AgentStop: request.AgentStop})
}

func (s *service) ListSessions(ctx context.Context, request *pb.ListSessionsRequest) (*pb.SessionList, error) {
	if _, err := s.authorizePeer(ctx, s.requiredClientTag); err != nil {
		return nil, err
	}
	if request == nil || !safeIdentifier.MatchString(request.NodeInstanceId) || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "a valid node instance ID is required")
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if s.fleet.storageErr != nil {
		return nil, storageUnavailable()
	}
	entry := s.fleet.nodes[request.NodeInstanceId]
	if entry == nil {
		return nil, status.Error(codes.NotFound, "node not found")
	}
	if !s.fleet.current(entry) || !entry.view.Connected {
		return nil, status.Error(codes.Unavailable, "node disconnected")
	}
	now := time.Now()
	if !sessionManagerReady(entry, now) || entry.view.SessionsReceivedAt == nil ||
		now.Sub(entry.view.SessionsReceivedAt.AsTime()) >= herdrStaleAfter {
		return &pb.SessionList{ErrorCode: "session_manager_unavailable"}, nil
	}
	copy := proto.Clone(entry.view).(*pb.NodeView)
	projectSessionFreshness(copy, now)
	return &pb.SessionList{Sessions: copy.Sessions, ErrorCode: copy.SessionsErrorCode}, nil
}
