package server

import (
	"context"
	"time"
)

func (s *service) refreshWorkspaceAuthorization(entry *fleetEntry, queued queuedCommand) bool {
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
	request := queued.command.WorkspaceEnsure
	_, err = s.workspacePolicy.Resolve(nodeID, request.ProjectId, request.BindingRevision, actor.StableID)
	return err == nil
}
