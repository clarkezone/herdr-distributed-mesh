package server

import (
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLogicalNodeNamePersistsBeforePublicationAndSurvivesRestore(t *testing.T) {
	persistence := &fakePersistence{}
	fleet := &fleetStore{storage: persistence}
	now := time.Now()
	entry, err := fleet.begin("independent-instance", "shared-tsnet", false, now)
	if err != nil {
		t.Fatal(err)
	}
	persistence.beforeSave = func(view *pb.NodeView) {
		if view.Hostname == "desktop" && entry.view.Hostname != "" {
			t.Fatal("logical name published before durable write")
		}
	}
	if err := fleet.setLogicalName(entry, "desktop"); err != nil {
		t.Fatal(err)
	}
	persistence.beforeSave = nil
	if entry.view.Hostname != "desktop" || entry.view.InstanceId != "independent-instance" || entry.view.TailscaleStableId != "shared-tsnet" {
		t.Fatalf("name projection changed authenticated identity: %+v", entry.view)
	}
	if err := fleet.end(entry); err != nil {
		t.Fatal(err)
	}
	restored := &fleetStore{}
	if err := restored.restore([]*pb.NodeView{persistence.saved["independent-instance"]}, now); err != nil {
		t.Fatal(err)
	}
	nodes, err := restored.list(now)
	if err != nil || len(nodes.GetNodes()) != 1 || nodes.Nodes[0].Hostname != "desktop" || nodes.Nodes[0].Connected {
		t.Fatalf("name did not survive offline restore: %+v %v", nodes, err)
	}
}

func TestLogicalNodeNameRejectsInvalidSupersededAndFailedWrites(t *testing.T) {
	for _, name := range []string{"UPPER", "bad name", "desktop\n", "desktop-", strings.Repeat("a", 41)} {
		if err := validateLogicalNodeName(name); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid logical name accepted: %q %v", name, err)
		}
	}
	if err := validateLogicalNodeName(""); err != nil {
		t.Fatalf("legacy unnamed node rejected: %v", err)
	}
	persistence := &fakePersistence{}
	fleet := &fleetStore{storage: persistence}
	now := time.Now()
	original, err := fleet.begin("node", "stable", false, now)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := fleet.begin("node", "stable", false, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.setLogicalName(original, "old"); status.Code(err) != codes.Aborted {
		t.Fatalf("superseded stream renamed current node: %v", err)
	}
	persistence.saveErr = errors.New("disk failed")
	if err := fleet.setLogicalName(replacement, "desktop"); status.Code(err) != codes.Unavailable {
		t.Fatalf("failed name persistence not surfaced: %v", err)
	}
	if replacement.view.Hostname != "" {
		t.Fatal("uncommitted logical name was published")
	}
}
