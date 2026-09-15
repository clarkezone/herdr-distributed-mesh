package server

import (
	"context"
	"regexp"
	"sort"
	"sync"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxFleetNodes      = 128
	maxHerdrStateBytes = 256 * 1024
	maxFleetBytes      = 2 * 1024 * 1024
	herdrStaleAfter    = 30 * time.Second
	offlineRetention   = 15 * time.Minute
)

type fleetEntry struct {
	view *agentflowv1.NodeView
	done chan struct{}
}

type fleetStore struct {
	mu    sync.Mutex
	nodes map[string]*fleetEntry
}

func (f *fleetStore) prune(now time.Time) {
	for id, entry := range f.nodes {
		if !entry.view.Connected && now.Sub(entry.view.LastSeen.AsTime()) > offlineRetention {
			delete(f.nodes, id)
		}
	}
}

func (f *fleetStore) begin(instanceID, stableID string, enabled bool, now time.Time) (*fleetEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.nodes == nil {
		f.nodes = make(map[string]*fleetEntry)
	}
	f.prune(now)
	old := f.nodes[instanceID]
	if old == nil && len(f.nodes) >= maxFleetNodes {
		return nil, status.Error(codes.ResourceExhausted, "fleet node limit reached")
	}
	if old != nil && old.view.Connected {
		close(old.done)
	}
	herdrStatus := "disabled"
	if enabled {
		herdrStatus = "waiting"
	}
	entry := &fleetEntry{
		view: &agentflowv1.NodeView{
			InstanceId: instanceID, TailscaleStableId: stableID,
			Connected: true, LastSeen: timestamppb.New(now),
			Herdr: &agentflowv1.HerdrState{Status: herdrStatus},
		},
		done: make(chan struct{}),
	}
	f.nodes[instanceID] = entry
	return entry, nil
}

func (f *fleetStore) end(entry *fleetEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.nodes[entry.view.InstanceId] == entry {
		entry.view.Connected = false
	}
}

func (f *fleetStore) heartbeat(entry *fleetEntry, now time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.nodes[entry.view.InstanceId] != entry {
		return status.Error(codes.Aborted, "node stream superseded")
	}
	entry.view.LastSeen = timestamppb.New(now)
	return nil
}

var safeIdentifier = regexp.MustCompile(`^[A-Za-z0-9:_-]{1,128}$`)
var safeVersion = regexp.MustCompile(`^[A-Za-z0-9.+_-]{1,128}$`)

func validateHerdrState(state *agentflowv1.HerdrState) error {
	bad := status.Error(codes.InvalidArgument, "invalid read-only Herdr state")
	if state == nil || state.Sequence == 0 || state.ObservedAt == nil || state.ObservedAt.CheckValid() != nil ||
		len(state.ProtoReflect().GetUnknown()) != 0 || len(state.ObservedAt.ProtoReflect().GetUnknown()) != 0 {
		return bad
	}
	if proto.Size(state) > maxHerdrStateBytes {
		return status.Error(codes.ResourceExhausted, "Herdr state size limit exceeded")
	}
	count := len(state.Workspaces) + len(state.Tabs) + len(state.Panes) + len(state.Agents)
	if count > 4096 {
		return status.Error(codes.ResourceExhausted, "Herdr entity limit exceeded")
	}
	switch state.Status {
	case "ready":
		if state.Protocol == 0 || !safeVersion.MatchString(state.Version) || state.ErrorCode != "" {
			return bad
		}
	case "unavailable":
		if !safeIdentifier.MatchString(state.ErrorCode) || count != 0 || state.Version != "" || state.Protocol != 0 {
			return bad
		}
	default:
		return bad
	}
	for _, group := range [][]*agentflowv1.HerdrEntity{state.Workspaces, state.Tabs, state.Panes, state.Agents} {
		seen := make(map[string]bool, len(group))
		for _, entity := range group {
			if entity == nil || !safeIdentifier.MatchString(entity.Id) || seen[entity.Id] ||
				(entity.WorkspaceId != "" && !safeIdentifier.MatchString(entity.WorkspaceId)) ||
				(entity.TabId != "" && !safeIdentifier.MatchString(entity.TabId)) ||
				len(entity.ProtoReflect().GetUnknown()) != 0 {
				return bad
			}
			seen[entity.Id] = true
			switch entity.AgentStatus {
			case "idle", "working", "blocked", "done", "unknown":
			default:
				return bad
			}
		}
	}
	return nil
}

func (f *fleetStore) update(entry *fleetEntry, state *agentflowv1.HerdrState, now time.Time) error {
	if err := validateHerdrState(state); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.nodes[entry.view.InstanceId] != entry {
		return status.Error(codes.Aborted, "node stream superseded")
	}
	if state.Sequence <= entry.view.Herdr.Sequence {
		return status.Error(codes.InvalidArgument, "Herdr sequence must increase within a stream")
	}
	size := proto.Size(state)
	for _, other := range f.nodes {
		if other != entry {
			size += proto.Size(other.view.Herdr)
		}
	}
	if size > maxFleetBytes {
		return status.Error(codes.ResourceExhausted, "fleet state size limit exceeded")
	}
	entry.view.Herdr = proto.Clone(state).(*agentflowv1.HerdrState)
	entry.view.HerdrReceivedAt = timestamppb.New(now)
	return nil
}

func (f *fleetStore) list(now time.Time) *agentflowv1.NodeList {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prune(now)
	list := &agentflowv1.NodeList{Nodes: make([]*agentflowv1.NodeView, 0, len(f.nodes))}
	for _, entry := range f.nodes {
		view := proto.Clone(entry.view).(*agentflowv1.NodeView)
		view.Stale = !view.Connected || view.Herdr.Status != "ready" ||
			view.HerdrReceivedAt == nil || now.Sub(view.HerdrReceivedAt.AsTime()) > herdrStaleAfter
		list.Nodes = append(list.Nodes, view)
	}
	sort.Slice(list.Nodes, func(i, j int) bool { return list.Nodes[i].InstanceId < list.Nodes[j].InstanceId })
	return list
}

func (s *service) ListNodes(ctx context.Context, _ *emptypb.Empty) (*agentflowv1.NodeList, error) {
	if _, err := s.authorizePeer(ctx, s.requiredClientTag); err != nil {
		return nil, err
	}
	return s.fleet.list(time.Now()), nil
}
