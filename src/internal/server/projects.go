package server

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *service) projectAccess(ctx context.Context, write bool) error {
	tag := s.requiredClientTag
	if write {
		tag = s.requiredCommandTag
	}
	if tag == "" {
		return status.Error(codes.PermissionDenied, "project transport role is not configured")
	}
	if _, err := s.authorizePeer(ctx, tag); err != nil {
		return err
	}
	if s.commands == nil {
		return status.Error(codes.Unimplemented, "project storage is not configured")
	}
	return nil
}

func (s *service) projectErrorLocked(err error) error {
	switch {
	case errors.Is(err, state.ErrProjectNotFound):
		return status.Error(codes.NotFound, "project not found")
	case errors.Is(err, state.ErrProjectConflict):
		return status.Error(codes.FailedPrecondition, "project configuration conflict")
	case errors.Is(err, state.ErrProjectCapacity):
		return status.Error(codes.ResourceExhausted, "project configuration capacity reached")
	default:
		return s.commandErrorLocked(err)
	}
}

func (s *service) projectViewLocked(record *pb.ProjectRecord) *pb.ProjectRecord {
	record = proto.Clone(record).(*pb.ProjectRecord)
	entry := s.fleet.nodes[record.Desired.NodeInstanceId]
	switch {
	case entry == nil || !s.fleet.current(entry) || !entry.view.Connected:
		record.Readiness = "offline"
	case !entry.projects:
		record.Readiness = "unsupported"
	default:
		record.Readiness = "pending"
		if ack := entry.projectApplied[record.Desired.ProjectId]; ack != nil && ack.Generation == record.Desired.Generation {
			record.Readiness = ack.Status
		}
	}
	return record
}

func wakeProjects(entry *fleetEntry) {
	if entry == nil || !entry.projects {
		return
	}
	select {
	case entry.projectWake <- struct{}{}:
	default:
	}
}

func (s *service) RegisterProject(ctx context.Context, r *pb.RegisterProjectRequest) (*pb.ProjectRecord, error) {
	if err := s.projectAccess(ctx, true); err != nil {
		return nil, err
	}
	if err := protocol.ValidateProjectRegistration(r); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if s.fleet.storageErr != nil {
		return nil, storageUnavailable()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	record, err := s.commands.UpsertProject(ctx, r)
	if err != nil {
		return nil, s.projectErrorLocked(err)
	}
	wakeProjects(s.fleet.nodes[r.NodeInstanceId])
	return s.projectViewLocked(record), nil
}

func (s *service) GetProject(ctx context.Context, r *pb.GetProjectRequest) (*pb.ProjectRecord, error) {
	if err := s.projectAccess(ctx, false); err != nil {
		return nil, err
	}
	if r == nil || !protocol.ValidIdempotencyKey(r.NodeInstanceId) || !protocol.ValidIdempotencyKey(r.ProjectId) || len(r.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "node and project IDs are required")
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if s.fleet.storageErr != nil {
		return nil, storageUnavailable()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	record, err := s.commands.GetProject(ctx, r.NodeInstanceId, r.ProjectId)
	if err != nil {
		return nil, s.projectErrorLocked(err)
	}
	return s.projectViewLocked(record), nil
}

func (s *service) ListProjects(ctx context.Context, r *pb.ListProjectsRequest) (*pb.ProjectList, error) {
	if err := s.projectAccess(ctx, false); err != nil {
		return nil, err
	}
	if r == nil || (r.NodeInstanceId != "" && !protocol.ValidIdempotencyKey(r.NodeInstanceId)) ||
		r.PageSize > protocol.MaxProjectPageSize || len(r.PageToken) > 512 || len(r.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid project filter")
	}
	after := ""
	if r.PageToken != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(r.PageToken)
		node, project, ok := strings.Cut(string(decoded), "\x00")
		if err != nil || !ok || !protocol.ValidIdempotencyKey(node) || !protocol.ValidIdempotencyKey(project) ||
			(r.NodeInstanceId != "" && r.NodeInstanceId != node) {
			return nil, status.Error(codes.InvalidArgument, "invalid project page token")
		}
		after = string(decoded)
	}
	size := int(r.PageSize)
	if size == 0 {
		size = protocol.MaxProjectPageSize
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if s.fleet.storageErr != nil {
		return nil, storageUnavailable()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	records, err := s.commands.ListProjects(ctx, r.NodeInstanceId)
	if err != nil {
		return nil, s.projectErrorLocked(err)
	}
	result := &pb.ProjectList{}
	for _, record := range records {
		key := record.Desired.NodeInstanceId + "\x00" + record.Desired.ProjectId
		if key <= after {
			continue
		}
		if len(result.Projects) == size {
			last := result.Projects[len(result.Projects)-1].Desired
			result.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(last.NodeInstanceId + "\x00" + last.ProjectId))
			break
		}
		result.Projects = append(result.Projects, s.projectViewLocked(record))
	}
	return result, nil
}

// The stream's sole sender fetches the latest desired values, coalescing updates
// instead of keeping a second queue of configuration mutations.
func (s *service) projectUpdates(entry *fleetEntry) ([]*pb.ProjectConfig, error) {
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if !s.fleet.current(entry) {
		return nil, status.Error(codes.Aborted, "node stream superseded")
	}
	if s.fleet.storageErr != nil {
		return nil, storageUnavailable()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	records, err := s.commands.ListProjects(ctx, entry.view.InstanceId)
	if err != nil {
		return nil, s.projectErrorLocked(err)
	}
	var updates []*pb.ProjectConfig
	for _, record := range records {
		c := record.Desired
		if old := entry.projectSent[c.ProjectId]; old != nil && old.Generation == c.Generation {
			continue
		}
		entry.projectSent[c.ProjectId] = c
		updates = append(updates, c)
	}
	return updates, nil
}

func (s *service) acknowledgeProject(entry *fleetEntry, ack *pb.ProjectAck) error {
	if err := protocol.ValidateProjectAck(ack); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if !s.fleet.current(entry) {
		return status.Error(codes.Aborted, "node stream superseded")
	}
	if !entry.projects {
		return status.Error(codes.FailedPrecondition, "project capability not negotiated")
	}
	if s.fleet.storageErr != nil {
		return storageUnavailable()
	}
	sent := entry.projectSent[ack.ProjectId]
	if sent == nil || ack.Generation > sent.Generation {
		return status.Error(codes.InvalidArgument, "unsolicited project acknowledgement")
	}
	if ack.Generation < sent.Generation {
		return nil
	}
	if old := entry.projectApplied[ack.ProjectId]; old != nil && old.Generation == ack.Generation && !proto.Equal(old, ack) {
		return status.Error(codes.InvalidArgument, "conflicting project acknowledgement")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	record, err := s.commands.AckProject(ctx, entry.view.InstanceId, ack)
	if err != nil {
		return s.projectErrorLocked(err)
	}
	if record.Desired.Generation == ack.Generation {
		entry.projectApplied[ack.ProjectId] = proto.Clone(ack).(*pb.ProjectAck)
	}
	return nil
}

func (s *service) adoptProjects(entry *fleetEntry, offer *pb.LegacyProjects) error {
	if offer == nil || len(offer.Projects) > 128 || len(offer.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "invalid legacy project offer")
	}
	for _, r := range offer.Projects {
		if protocol.ValidateProjectRegistration(r) != nil || r.NodeInstanceId != entry.view.InstanceId {
			return status.Error(codes.InvalidArgument, "invalid legacy project binding")
		}
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if !s.fleet.current(entry) {
		return status.Error(codes.Aborted, "node stream superseded")
	}
	if !entry.projects {
		return status.Error(codes.FailedPrecondition, "project capability not negotiated")
	}
	if s.fleet.storageErr != nil {
		return storageUnavailable()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.commands.AdoptProjects(ctx, entry.view.InstanceId, offer.Projects); err != nil {
		return s.projectErrorLocked(err)
	}
	wakeProjects(entry)
	return nil
}

func (s *service) managedProjectReadyLocked(ctx context.Context, entry *fleetEntry, project, revision string) (*pb.ProjectRecord, error) {
	if !entry.projects {
		return nil, status.Error(codes.FailedPrecondition, "node does not support centrally managed projects")
	}
	record, err := s.commands.GetProject(ctx, entry.view.InstanceId, project)
	if err != nil {
		return nil, s.projectErrorLocked(err)
	}
	if s.projectViewLocked(record).Readiness != "applied" ||
		(revision != "" && revision != protocol.ProjectRevision(record.Desired.Generation)) {
		return nil, status.Error(codes.FailedPrecondition, "current project configuration is not applied")
	}
	return record, nil
}
