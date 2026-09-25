package onboard

import (
	"context"
	"errors"
	"io"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/deinitnet"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

func DefaultShutdownDependencies(input io.Reader, output io.Writer) ShutdownDependencies {
	return ShutdownDependencies{
		Dir: meshlocal.DefaultDir, Identity: meshlocal.ResolveManagedIdentity,
		Stop:    func(ctx context.Context, dir string) error { return stopManaged(ctx, dir, output) },
		Confirm: func(ctx context.Context, expected string) (bool, error) { return confirmDestroy(ctx, input, expected) },
		Token:   func(ctx context.Context, name string) ([]byte, error) { return shutdownToken(ctx, name, input, output) },
		Remote:  prepareRemoteCleanup,
		Purge:   meshlocal.PurgeManaged,
	}
}

type remoteCleanup struct {
	client *deinitnet.Client
	device string
	policy *deinitnet.PolicyPlan
}

func (r *remoteCleanup) Close() { r.client.Close() }

func (r *remoteCleanup) DeleteDevice(ctx context.Context) error {
	_, err := r.client.DeleteDevice(ctx, r.device)
	return err
}

func (r *remoteCleanup) RemovePolicy(ctx context.Context) error {
	if r.policy == nil {
		return nil
	}
	_, err := r.client.ApplyPolicy(ctx, r.policy)
	return err
}

func prepareRemoteCleanup(ctx context.Context, dir string, cfg meshlocal.Config, identity meshlocal.ManagedIdentity, removePolicy bool, token []byte) (RemoteCleanup, error) {
	client, err := deinitnet.New(token, deinitnet.Options{})
	if err != nil {
		return nil, err
	}
	remote, err := prepareCleanupWithClient(ctx, dir, cfg, identity, removePolicy, client)
	if err != nil {
		client.Close()
		return nil, err
	}
	return remote, nil
}

func prepareCleanupWithClient(ctx context.Context, dir string, cfg meshlocal.Config, identity meshlocal.ManagedIdentity, removePolicy bool, client *deinitnet.Client) (*remoteCleanup, error) {
	if _, err := client.GetDevice(ctx, identity.DeviceID); err != nil && !errors.Is(err, deinitnet.ErrDeviceAbsent) {
		return nil, err
	}
	remote := &remoteCleanup{client: client, device: identity.DeviceID}
	if !removePolicy {
		return remote, nil
	}
	var err error
	remote.policy, err = client.PrepareMeshPolicyRemoval(ctx, cfg.Tailnet)
	if err != nil {
		return nil, err
	}
	if err := client.CheckPolicyDependencies(ctx, remote.policy, identity.DeviceID); err != nil {
		return nil, err
	}
	return remote, nil
}
