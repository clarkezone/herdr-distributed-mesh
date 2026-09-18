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
		if queued.command.CommandType == protocol.SessionEnsureCommandType {
			return s.sessionsConfigured()
		}
		return (queued.command.CommandType == protocol.AgentControlCommandType || queued.command.CommandType == protocol.AgentStopCommandType) && s.agentConfigured()
	}
	if queued.command.SubmittedRequest != nil && (entry.projects || s.workspacePolicy == nil) {
		return true // Current desired/applied generation is checked under the dispatch lock.
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

func mutationReady(entry *fleetEntry, commandType string, now time.Time, selector ...string) bool {
	if protocol.IsLifecycleCommand(commandType) {
		return entry.lifecycle && (commandType != protocol.AgentStartCommandType || entry.projects) &&
			mutationReady(entry, protocol.AgentControlCommandType, now, selector...)
	}
	if commandType == protocol.SessionEnsureCommandType {
		return sessionManagerReady(entry, now)
	}
	if len(selector) > 0 && selector[0] != "" {
		incarnation := ""
		if len(selector) > 1 {
			incarnation = selector[1]
		}
		if !selectedSessionReady(entry, selector[0], incarnation, now) {
			return false
		}
		switch commandType {
		case protocol.AgentControlCommandType:
			return entry.agents
		case protocol.WorkspaceEnsureCommandType:
			return entry.workspaces
		case protocol.WorktreeCreateCommandType:
			return entry.workspaces && entry.worktrees
		default:
			return false
		}
	}
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
