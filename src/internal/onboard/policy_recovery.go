package onboard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/deinitnet"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

const pendingPolicyText = "Policy application may have remote effects. Inspect before retrying.\n"

// reconcilePendingPolicy performs only a GET before deciding whether the old
// apply is provably absent or complete. Other states retain the pending marker.
func reconcilePendingPolicy(ctx context.Context, dir, tailnet, pending, done string, token []byte,
	readPolicy func(context.Context, string, []byte) ([]byte, error)) (bool, error) {
	if readPolicy == nil {
		return false, errors.New("read-only policy recovery is unavailable; pending marker retained")
	}
	marker, err := meshlocal.ReadPrivateArtifact(dir, pending, 256)
	if err != nil || !bytes.Equal(marker, []byte(pendingPolicyText)) {
		return false, errors.New("pending marker is invalid; inspect saved state before retrying")
	}
	backups, err := filepath.Glob(filepath.Join(dir, "policy-preview-*", "apply", "policy-before-*.json"))
	if err != nil || len(backups) != 1 {
		return false, errors.New("cannot identify one saved apply backup; pending marker retained")
	}
	before, err := meshlocal.ReadPrivateArtifact(dir, backups[0], 1048576)
	if err != nil {
		return false, fmt.Errorf("cannot read saved pre-apply policy; pending marker retained: %w", err)
	}
	proposed, err := meshlocal.ReadPrivateArtifact(dir, filepath.Join(filepath.Dir(backups[0]), "policy-proposed.json"), 2*1048576)
	if err != nil {
		return false, fmt.Errorf("cannot read saved proposal; pending marker retained: %w", err)
	}
	_, beforeJSON, err := deinitnet.ParsePolicy(before)
	if err != nil {
		return false, errors.New("saved pre-apply policy is invalid; pending marker retained")
	}
	_, proposedJSON, err := deinitnet.ParsePolicy(proposed)
	if err != nil {
		return false, errors.New("saved proposal is invalid; pending marker retained")
	}
	current, err := readPolicy(ctx, tailnet, token)
	if err != nil {
		return false, fmt.Errorf("cannot read live policy; pending marker retained: %w", err)
	}
	_, currentJSON, err := deinitnet.ParsePolicy(current)
	if err != nil {
		return false, errors.New("live policy is invalid; pending marker retained")
	}
	if bytes.Equal(currentJSON, beforeJSON) {
		if _, err := os.Lstat(done); err == nil {
			return false, errors.New("completion receipt conflicts with the live pre-apply policy; pending marker retained")
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		return false, removePendingMarker(pending)
	}
	if bytes.Equal(currentJSON, proposedJSON) {
		if err := writePolicyComplete(done, filepath.Base(filepath.Dir(filepath.Dir(backups[0])))); err != nil {
			return false, err
		}
		if err := removePendingMarker(pending); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, errors.New("live policy differs from both saved snapshots; inspect remote policy and saved artifacts before retrying; pending marker retained")
}

func writePolicyComplete(path, previewName string) error {
	if !validPolicyPreviewName(previewName) {
		return errors.New("invalid completed policy preview name")
	}
	receipt := []byte("Policy-only setup completed; no device keys created.\napply-preview=" + previewName + "\n")
	if err := createMarker(path, receipt); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := meshlocal.ReadPrivateArtifact(filepath.Dir(path), path, 256)
			if readErr == nil && bytes.Equal(existing, receipt) {
				return nil
			}
		}
		return err
	}
	return nil
}

func validPolicyPreviewName(name string) bool {
	const prefix = "policy-preview-"
	if !strings.HasPrefix(name, prefix) || len(name) == len(prefix) {
		return false
	}
	for _, digit := range name[len(prefix):] {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

func removePendingMarker(path string) error {
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("cannot clear verified pending marker: %w", err)
	}
	return nil
}
