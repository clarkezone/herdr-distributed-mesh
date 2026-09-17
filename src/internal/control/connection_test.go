package control

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

func TestWithFleetReusesInjectedClient(t *testing.T) {
	client := &agentClientFixture{}
	calls := 0
	sentinel := errors.New("operation failed")
	err := WithFleet(context.Background(), Options{FleetClient: client}, func(actual pb.FleetClient) error {
		calls++
		if actual != client {
			t.Fatal("shared transport client replaced")
		}
		return sentinel
	})
	if calls != 1 || !errors.Is(err, sentinel) {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
}

func TestWithFleetDoesNotFabricateTransportDiagnostics(t *testing.T) {
	opts := Options{FleetClient: &agentClientFixture{}, Diagnose: true}
	if err := WithFleet(context.Background(), opts, func(pb.FleetClient) error {
		t.Fatal("diagnostic callback invoked without transport evidence")
		return nil
	}); err == nil || !strings.Contains(err.Error(), "diagnostics") {
		t.Fatalf("diagnostics = %v", err)
	}
}

func TestWithFleetCancellationAndMissingCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WithFleet(ctx, Options{FleetClient: &agentClientFixture{}}, func(pb.FleetClient) error {
		t.Fatal("canceled callback invoked")
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := WithFleet(context.Background(), Options{}, nil); err == nil {
		t.Fatal("nil callback accepted")
	}
}
