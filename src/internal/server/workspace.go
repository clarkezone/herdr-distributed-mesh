package server

import (
	"context"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

func (s *service) refreshMutationAuthorization(entry *fleetEntry, queued queuedCommand) bool {
	s.fleet.mu.Lock()
	current := s.fleet.current(entry)
	nodeID, stableID, nodeContext := entry.view.InstanceId, entry.view.TailscaleStableId, entry.peerContext
	s.fleet.mu.Unlock()
	if !current || queued.actorContext == nil || nodeContext == nil {
		return false
	}
	actorCtx, cancel := context.WithTimeout(queued.actorContext, 3*time.Second)
	defer cancel()
	actor, err := s.authorizePeer(actorCtx, s.requiredCommandTag)
	if err != nil || actor.StableID != queued.command.Actor.ActorId {
		return false
	}
	nodeCtx, stop := context.WithTimeout(nodeContext, 3*time.Second)
	defer stop()
	node, err := s.authorizePeer(nodeCtx, s.requiredNodeTag)
	if err != nil || node.StableID != stableID {
		return false
	}
	projectID, revision := protocol.CommandProject(queued.command)
	binding, err := s.workspacePolicy.Resolve(nodeID, projectID, revision, actor.StableID)
	return err == nil && (queued.command.CommandType != protocol.WorktreeCreateCommandType || binding.AllowWorktrees)
}

func mutationReady(entry *fleetEntry, commandType string, now time.Time) bool {
	if !entry.workspaces || !entry.view.WorkspaceReady || !freshHerdr(entry.view, now) {
		return false
	}
	switch commandType {
	case protocol.WorkspaceEnsureCommandType:
		return true
	case protocol.WorktreeCreateCommandType:
		return entry.worktrees && entry.view.WorktreeReady
	default:
		return false
	}
}
