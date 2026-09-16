package server

import (
	"context"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

func (s *service) refreshMutationAuthorization(entry *fleetEntry, queued queuedCommand) bool {
	if queued.command == nil || queued.command.Actor == nil {
		return false
	}
	nodeID, authorized := s.refreshCommandPeers(entry, queued)
	if !authorized {
		return false
	}
	projectID, revision := protocol.CommandProject(queued.command)
	if projectID == "" {
		return queued.command.CommandType == protocol.AgentControlCommandType && s.agentConfigured()
	}
	binding, err := s.workspacePolicy.Resolve(nodeID, projectID, revision, queued.command.Actor.ActorId)
	return err == nil && (queued.command.CommandType != protocol.WorktreeCreateCommandType || binding.AllowWorktrees)
}

func (s *service) refreshCommandPeers(entry *fleetEntry, queued queuedCommand) (string, bool) {
	s.fleet.mu.Lock()
	current := s.fleet.current(entry)
	nodeID, stableID, nodeContext := entry.view.InstanceId, entry.view.TailscaleStableId, entry.peerContext
	s.fleet.mu.Unlock()
	if !current || queued.actorContext == nil || nodeContext == nil {
		return "", false
	}
	actorCtx, cancel := context.WithTimeout(queued.actorContext, 3*time.Second)
	defer cancel()
	actor, err := s.authorizePeer(actorCtx, s.requiredCommandTag)
	if err != nil || actor.StableID != queued.command.Actor.ActorId {
		return "", false
	}
	nodeCtx, stop := context.WithTimeout(nodeContext, 3*time.Second)
	defer stop()
	node, err := s.authorizePeer(nodeCtx, s.requiredNodeTag)
	if err != nil || node.StableID != stableID {
		return "", false
	}
	return nodeID, true
}

func mutationReady(entry *fleetEntry, commandType string, now time.Time) bool {
	if commandType == protocol.AgentControlCommandType {
		return entry.agents && entry.view.AgentReady && entry.view.CommandReady && freshHerdr(entry.view, now)
	}
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
