package server

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func canceledStoreWrite(t *testing.T) error {
	t.Helper()
	store, err := state.Open(context.Background(), filepath.Join(t.TempDir(), "coordinator.db"), "server")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err = store.Bind(ctx, "stable", "node")
	if !state.RetryableCancellation(err) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("store did not certify a canceled write: %v", err)
	}
	return err
}

func TestCoordinatorPersistenceRetriesOnlyCertifiedCancellation(t *testing.T) {
	canceled := canceledStoreWrite(t)
	disk := errors.New("injected disk error")
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"rolled-back", canceled, 2},
		{"uncertain-context", context.DeadlineExceeded, 1},
		{"disk", disk, 1},
		{"rollback-failed", errors.Join(canceled, disk), 1},
		{"wrapped-unknown", fmt.Errorf("unknown outcome: %w", canceled), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			var first context.Context
			err := persistCoordinator("test", func(ctx context.Context) error {
				calls++
				if ctx.Err() != nil {
					t.Fatal("retry inherited the expired context")
				}
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 5*time.Second {
					t.Fatal("persistence attempt is not bounded")
				}
				if calls == 1 {
					first = ctx
					return tc.err
				}
				if first.Err() == nil {
					t.Fatal("first attempt context was not released")
				}
				return nil
			})
			if calls != tc.want || (tc.want == 2 && err != nil) || (tc.want == 1 && err != tc.err) {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestCoordinatorPersistenceRetryIsBounded(t *testing.T) {
	canceled := canceledStoreWrite(t)
	calls := 0
	err := persistCoordinator("test", func(context.Context) error {
		calls++
		return canceled
	})
	if calls != 2 || err != canceled {
		t.Fatalf("unbounded or hidden persistent failure: calls=%d err=%v", calls, err)
	}
}

type transientPersistence struct {
	fakePersistence
	failures int
	err      error
	calls    int
}

func (p *transientPersistence) SaveNode(ctx context.Context, view *pb.NodeView) error {
	p.calls++
	if p.failures > 0 {
		p.failures--
		return p.err
	}
	return p.fakePersistence.SaveNode(ctx, view)
}

func TestRolledBackTimeoutDoesNotKillCoordinatorOrPublishBeforeCommit(t *testing.T) {
	p := &transientPersistence{err: canceledStoreWrite(t)}
	fleet := &fleetStore{storage: p, fatal: make(chan error, 1)}
	now := time.Now()
	entry, err := fleet.begin("node", "stable", true, now)
	if err != nil {
		t.Fatal(err)
	}
	p.failures = 1
	p.beforeSave = func(*pb.NodeView) {
		if entry.view.Herdr.Sequence != 0 {
			t.Fatal("uncommitted observation was published")
		}
	}
	if err := fleet.update(entry, readyState(1), now); err != nil {
		t.Fatalf("rolled-back timeout killed coordinator: %v", err)
	}
	if p.calls != 3 || fleet.storageErr != nil || entry.view.Herdr.Sequence != 1 ||
		!proto.Equal(p.saved["node"], entry.view) || len(fleet.fatal) != 0 {
		t.Fatal("recovery lost commit-before-publish or poisoned storage")
	}
	p.beforeSave = nil
	p.failures = 2
	err = fleet.update(entry, readyState(2), now)
	if status.Code(err) != codes.Unavailable || fleet.storageErr == nil || len(fleet.fatal) != 1 ||
		entry.view.Herdr.Sequence != 1 || p.saved["node"].Herdr.Sequence != 1 {
		t.Fatal("repeated failures must remain fail-closed without publishing")
	}
}
