package server

import (
	"context"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

func TestMixedCaseSessionInventoryPersistsAndRestores(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	entry := namedEntry(t, h)
	inventory := sessionInventory(2)
	inventory.Sessions[0].Name = "default"
	inventory.Sessions = append(inventory.Sessions, &pb.SessionView{Name: "QEI", Status: "stopped"})
	if err := h.api.fleet.updateSessions(entry, inventory, time.Now()); err != nil {
		t.Fatal(err)
	}
	views, err := h.api.commands.LoadFleet(context.Background())
	if err != nil || len(views) != 1 || len(views[0].Sessions) != 2 {
		t.Fatalf("inventory not persisted: %+v, %v", views, err)
	}
	restored := &fleetStore{}
	if err := restored.restore(views, time.Now()); err != nil {
		t.Fatal(err)
	}
	sessions := restored.nodes["node-1"].view.Sessions
	if sessions[0].Name != "default" || sessions[1].Name != "QEI" || sessions[1].Status != "stopped" {
		t.Fatalf("session identity lost across persistence: %+v", sessions)
	}
}
