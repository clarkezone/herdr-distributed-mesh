package server

import (
	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func validateLogicalNodeName(name string) error {
	if name != "" && !transport.ValidNodeName(name) {
		return status.Error(codes.InvalidArgument, "invalid logical node name")
	}
	return nil
}

func (f *fleetStore) setLogicalName(entry *fleetEntry, name string) error {
	if err := validateLogicalNodeName(name); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.storageErr != nil {
		return storageUnavailable()
	}
	if !f.current(entry) {
		return status.Error(codes.Aborted, "node stream superseded")
	}
	if entry.view.Hostname == name {
		return nil
	}
	view := proto.Clone(entry.view).(*pb.NodeView)
	view.Hostname = name
	if err := f.save(view); err != nil {
		return err
	}
	entry.view = view
	return nil
}
