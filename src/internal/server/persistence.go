package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

func openCoordinatorState(ctx context.Context, options Options) (*state.Store, []*agentflowv1.NodeView, error) {
	store, err := state.Open(ctx, options.DatabasePath, options.InstanceID)
	if err != nil {
		return nil, nil, fmt.Errorf("open coordinator state: %w", err)
	}
	fail := func(err error) (*state.Store, []*agentflowv1.NodeView, error) {
		return nil, nil, errors.Join(err, store.Close())
	}
	// Retain the legacy reader for upgrades; all runtime writes go to SQLite.
	legacy, err := newBindingStore(options.BindingPath)
	if err != nil {
		return fail(err)
	}
	bindings := make([]state.Binding, 0, len(legacy.byStableID))
	for stableID, instanceID := range legacy.byStableID {
		bindings = append(bindings, state.Binding{StableID: stableID, InstanceID: instanceID})
	}
	if err := store.ImportBindings(ctx, bindings); err != nil {
		return fail(fmt.Errorf("import legacy identity bindings: %w", err))
	}
	if err := store.RecoverCommands(ctx, time.Now()); err != nil {
		return fail(fmt.Errorf("recover command journal: %w", err))
	}
	views, err := store.LoadFleet(ctx)
	if err != nil {
		return fail(fmt.Errorf("load durable fleet: %w", err))
	}
	return store, views, nil
}

func durableBinder(store *state.Store) func(string, string) error {
	return func(stableID, instanceID string) error {
		return persistCoordinator("bind_node", func(ctx context.Context) error {
			return store.Bind(ctx, stableID, instanceID)
		})
	}
}
