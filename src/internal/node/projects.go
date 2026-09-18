package node

import (
	"context"
	"errors"
	"log"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type projectJournal interface {
	SaveProjectConfig(context.Context, *pb.ProjectConfig) error
	ProjectConfigs(context.Context) ([]*pb.ProjectConfig, error)
	SaveProjectAck(context.Context, *pb.ProjectAck) error
	ProjectAppliedAcks(context.Context) ([]*pb.ProjectAck, error)
}

func (h *commandHandler) restoreProjects(ctx context.Context, cache projectJournal, legacy *projects.Policy) error {
	h.projectCache = cache
	h.projectSeen = make(map[string]*pb.ProjectConfig)
	h.projectCurrent = make(map[string]*pb.ProjectAck)
	h.projectLegacy = make(map[string]projects.Binding)
	h.projectOwnership = make(map[string]*pb.ProjectAck)
	op, cancel := context.WithTimeout(ctx, journalOperationTimeout)
	configs, err := cache.ProjectConfigs(op)
	cancel()
	if err != nil {
		return storageError("read project cache", err)
	}
	for _, c := range configs {
		h.projectSeen[c.ProjectId] = c
	}
	op, cancel = context.WithTimeout(ctx, journalOperationTimeout)
	acks, err := cache.ProjectAppliedAcks(op)
	cancel()
	if err != nil {
		return storageError("read project ownership", err)
	}
	revalidation, done := context.WithTimeout(ctx, 15*time.Second)
	defer done()
	for _, ack := range acks {
		h.projectOwnership[ack.ProjectId] = ack
		if err := h.managedProjects.RestoreOwnership(revalidation, ack.ProjectId, protocol.ProjectRevision(ack.Generation), ack.CheckoutPath, ack.WorktreeRoot); err != nil {
			// Cache is not authority for readiness. Current delivery will return
			// a sanitized invalid ACK if this path still cannot be validated.
			log.Printf("project cache revalidation unavailable project_id=%s", ack.ProjectId)
		}
	}
	for _, b := range legacy.LegacyBindings() {
		if _, owned := h.projectOwnership[b.ProjectID]; owned {
			continue
		}
		h.projectLegacy[b.ProjectID] = b
	}
	return nil
}

func (h *commandHandler) applyProject(ctx context.Context, config *pb.ProjectConfig) (*pb.ProjectAck, error) {
	if protocol.ValidateProjectConfig(config) != nil || config.NodeInstanceId != h.nodeID {
		return nil, status.Error(codes.InvalidArgument, "invalid project configuration target")
	}
	if h.projectCache == nil || h.managedProjects == nil {
		return nil, status.Error(codes.FailedPrecondition, "project capability not negotiated")
	}
	if h.verifyCoordinator == nil {
		return nil, status.Error(codes.PermissionDenied, "project coordinator verification unavailable")
	}
	verification, stop := context.WithTimeout(ctx, workspacePeerVerificationTimeout)
	err := h.verifyCoordinator(verification)
	verificationErr := verification.Err()
	stop()
	if err != nil || verificationErr != nil {
		return nil, status.Error(codes.PermissionDenied, "project coordinator verification failed")
	}
	h.projectMu.Lock()
	defer h.projectMu.Unlock()
	if previous := h.projectSeen[config.ProjectId]; previous != nil {
		if config.Generation < previous.Generation {
			return nil, nil
		}
		if config.Generation == previous.Generation && !proto.Equal(config, previous) {
			return nil, status.Error(codes.InvalidArgument, "conflicting project generation")
		}
		if current := h.projectCurrent[config.ProjectId]; current != nil && current.Generation == config.Generation {
			return proto.Clone(current).(*pb.ProjectAck), nil
		}
	}
	op, cancel := context.WithTimeout(ctx, journalOperationTimeout)
	err = h.projectCache.SaveProjectConfig(op, config)
	cancel()
	if err != nil {
		return nil, storageError("save project configuration", err)
	}
	h.projectSeen[config.ProjectId] = proto.Clone(config).(*pb.ProjectConfig)
	delete(h.projectCurrent, config.ProjectId)
	ack := &pb.ProjectAck{ProjectId: config.ProjectId, Generation: config.Generation, Status: "invalid", ErrorCode: "invalid_path"}
	if owner := h.projectOwnership[config.ProjectId]; owner != nil {
		if _, err := h.managedProjects.Resolve(h.nodeID, config.ProjectId, protocol.ProjectRevision(owner.Generation), ""); err != nil {
			if err := h.managedProjects.RestoreOwnership(ctx, owner.ProjectId, protocol.ProjectRevision(owner.Generation), owner.CheckoutPath, owner.WorktreeRoot); err != nil {
				log.Printf("project ownership revalidation unavailable project_id=%s", config.ProjectId)
			}
		}
	}
	if legacy, ok := h.projectLegacy[config.ProjectId]; ok &&
		legacy.Path == config.CheckoutPath && legacy.WorktreeRoot == config.WorktreeRoot {
		if _, err := h.managedProjects.AdoptLegacy(ctx, legacy); err != nil {
			log.Printf("legacy project adoption unavailable project_id=%s", config.ProjectId)
		}
		delete(h.projectLegacy, config.ProjectId)
	}
	binding, err := h.managedProjects.Apply(ctx, config.ProjectId, protocol.ProjectRevision(config.Generation), config.CheckoutPath, config.WorktreeRoot)
	if err == nil && protocol.ValidProjectPath(binding.Path, false) && protocol.ValidProjectPath(binding.WorktreeRoot, false) {
		ack.Status, ack.ErrorCode, ack.CheckoutPath, ack.WorktreeRoot = "applied", "", binding.Path, binding.WorktreeRoot
	} else if errors.Is(err, projects.ErrInvalidPolicy) {
		ack.ErrorCode = "configuration_conflict"
	}
	op, cancel = context.WithTimeout(context.WithoutCancel(ctx), journalOperationTimeout)
	err = h.projectCache.SaveProjectAck(op, ack)
	cancel()
	if err != nil {
		return nil, storageError("save project acknowledgement", err)
	}
	h.projectCurrent[config.ProjectId] = ack
	if ack.Status == "applied" {
		h.projectOwnership[config.ProjectId] = ack
	}
	return proto.Clone(ack).(*pb.ProjectAck), nil
}

func (h *commandHandler) resolveManaged(project, revision string) (projects.Binding, error) {
	h.projectMu.RLock()
	defer h.projectMu.RUnlock()
	current := h.projectCurrent[project]
	if current == nil || current.Status != "applied" || protocol.ProjectRevision(current.Generation) != revision {
		return projects.Binding{}, projects.ErrDenied
	}
	return h.managedProjects.Resolve(h.nodeID, project, revision, "")
}

var _ projectJournal = (*state.NodeJournal)(nil)
