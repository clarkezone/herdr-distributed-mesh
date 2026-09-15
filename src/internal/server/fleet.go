package server

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	mu         sync.Mutex
	nodes      map[string]*fleetEntry
	storage    fleetPersistence
	storageErr error
	fatal      chan error
}

type fleetPersistence interface {
	SaveNode(context.Context, *agentflowv1.NodeView) error
	DeleteNodes(context.Context, []string) error
}

func storageUnavailable() error {
	return status.Error(codes.Unavailable, "coordinator storage is unavailable")
}

func (f *fleetStore) fail(err error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failLocked(err)
}

func (f *fleetStore) failLocked(err error) error {
	if f.storageErr == nil {
		f.storageErr = err
		log.Printf("coordinator persistence failed: %v", err)
		select {
		case f.fatal <- err:
		default:
		}
	}
	return storageUnavailable()
}

func (f *fleetStore) save(view *agentflowv1.NodeView) error {
	if f.storageErr != nil {
		return storageUnavailable()
	}
	if f.storage == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.storage.SaveNode(ctx, view); err != nil {
		return f.failLocked(err)
	}
	return nil
}

func (f *fleetStore) prune(now time.Time) error {
	if f.storageErr != nil {
		return storageUnavailable()
	}
	var expired []string
	for id, entry := range f.nodes {
		if !entry.view.Connected && now.Sub(entry.view.LastSeen.AsTime()) > offlineRetention {
			expired = append(expired, id)
		}
	}
	if len(expired) == 0 {
		return nil
	}
	if f.storage != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.storage.DeleteNodes(ctx, expired); err != nil {
			return f.failLocked(err)
		}
	}
	for _, id := range expired {
		delete(f.nodes, id)
	}
	return nil
}

func (f *fleetStore) restore(views []*agentflowv1.NodeView, now time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.nodes) != 0 {
		return errors.New("fleet is already initialized")
	}
	if len(views) > maxFleetNodes {
		return errors.New("stored fleet exceeds node limit")
	}
	nodes := make(map[string]*fleetEntry, len(views))
	stableIDs := make(map[string]bool, len(views))
	total := 0
	for _, view := range views {
		if err := validateStoredNode(view); err != nil {
			return fmt.Errorf("validate stored fleet: %w", err)
		}
		if nodes[view.InstanceId] != nil {
			return errors.New("stored fleet contains duplicate instance IDs")
		}
		if stableIDs[view.TailscaleStableId] {
			return errors.New("stored fleet contains duplicate Tailscale identities")
		}
		stableIDs[view.TailscaleStableId] = true
		total += proto.Size(view.Herdr)
		if total > maxFleetBytes {
			return errors.New("stored fleet exceeds payload limit")
		}
		copy := proto.Clone(view).(*agentflowv1.NodeView)
		// A persisted observation is not evidence of a live stream in this process.
		copy.Connected, copy.Stale = false, true
		nodes[copy.InstanceId] = &fleetEntry{view: copy, done: make(chan struct{})}
	}
	f.nodes = nodes
	return f.prune(now)
}

func validateStoredNode(view *agentflowv1.NodeView) error {
	bad := errors.New("invalid persisted node record")
	if view == nil || view.InstanceId == "" || len(view.InstanceId) > 128 ||
		view.TailscaleStableId == "" || len(view.TailscaleStableId) > 128 ||
		view.LastSeen == nil || view.LastSeen.CheckValid() != nil || view.Herdr == nil ||
		len(view.ProtoReflect().GetUnknown()) != 0 || len(view.LastSeen.ProtoReflect().GetUnknown()) != 0 {
		return bad
	}
	state := view.Herdr
	switch state.Status {
	case "disabled", "waiting":
		if !proto.Equal(state, &agentflowv1.HerdrState{Status: state.Status}) || view.HerdrReceivedAt != nil {
			return bad
		}
	default:
		if err := validateHerdrState(state); err != nil {
			return bad
		}
		if view.HerdrReceivedAt == nil || view.HerdrReceivedAt.CheckValid() != nil ||
			len(view.HerdrReceivedAt.ProtoReflect().GetUnknown()) != 0 {
			return bad
		}
	}
	return nil
}

func (f *fleetStore) begin(instanceID, stableID string, enabled bool, now time.Time) (*fleetEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.nodes == nil {
		f.nodes = make(map[string]*fleetEntry)
	}
	if err := f.prune(now); err != nil {
		return nil, err
	}
	old := f.nodes[instanceID]
	if old == nil && len(f.nodes) >= maxFleetNodes {
		return nil, status.Error(codes.ResourceExhausted, "fleet node limit reached")
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
	if err := f.save(entry.view); err != nil {
		return nil, err
	}
	if old != nil && old.view.Connected {
		close(old.done)
	}
	f.nodes[instanceID] = entry
	return entry, nil
}

func (f *fleetStore) end(entry *fleetEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.nodes[entry.view.InstanceId] == entry {
		copy := proto.Clone(entry.view).(*agentflowv1.NodeView)
		copy.Connected, copy.Stale = false, true
		if err := f.save(copy); err != nil {
			return err
		}
		entry.view = copy
	}
	return nil
}

func (f *fleetStore) heartbeat(entry *fleetEntry, now time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.nodes[entry.view.InstanceId] != entry {
		return status.Error(codes.Aborted, "node stream superseded")
	}
	copy := proto.Clone(entry.view).(*agentflowv1.NodeView)
	copy.LastSeen = timestamppb.New(now)
	if err := f.save(copy); err != nil {
		return err
	}
	entry.view = copy
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
	copy := proto.Clone(entry.view).(*agentflowv1.NodeView)
	copy.Herdr = proto.Clone(state).(*agentflowv1.HerdrState)
	copy.HerdrReceivedAt = timestamppb.New(now)
	if err := f.save(copy); err != nil {
		return err
	}
	entry.view = copy
	return nil
}

func (f *fleetStore) list(now time.Time) (*agentflowv1.NodeList, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.prune(now); err != nil {
		return nil, err
	}
	list := &agentflowv1.NodeList{Nodes: make([]*agentflowv1.NodeView, 0, len(f.nodes))}
	for _, entry := range f.nodes {
		view := proto.Clone(entry.view).(*agentflowv1.NodeView)
		view.Stale = !view.Connected || view.Herdr.Status != "ready" ||
			view.HerdrReceivedAt == nil || now.Sub(view.HerdrReceivedAt.AsTime()) > herdrStaleAfter
		list.Nodes = append(list.Nodes, view)
	}
	sort.Slice(list.Nodes, func(i, j int) bool { return list.Nodes[i].InstanceId < list.Nodes[j].InstanceId })
	return list, nil
}

func (s *service) ListNodes(ctx context.Context, _ *emptypb.Empty) (*agentflowv1.NodeList, error) {
	if _, err := s.authorizePeer(ctx, s.requiredClientTag); err != nil {
		return nil, err
	}
	return s.fleet.list(time.Now())
}
