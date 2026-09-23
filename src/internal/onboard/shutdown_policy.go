package onboard

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

func readOwnedPolicy(dir string) ([]byte, []byte, error) {
	if _, err := os.Lstat(filepath.Join(dir, "policy-complete")); err != nil {
		return nil, nil, errors.New("policy cleanup requires the original completed init policy receipt")
	}
	if _, err := os.Lstat(filepath.Join(dir, "policy-apply-pending")); !errors.Is(err, os.ErrNotExist) {
		return nil, nil, errors.New("policy application is pending or unknown; reconcile it before policy cleanup")
	}
	backups, err := filepath.Glob(filepath.Join(dir, "policy-preview-*", "apply", "policy-before-*.json"))
	if err != nil || len(backups) != 1 {
		return nil, nil, errors.New("cannot prove policy ownership from a unique completed apply backup; policy and local recovery data retained")
	}
	before, err := meshlocal.ReadPrivateArtifact(dir, backups[0], 1048576)
	if err != nil {
		return nil, nil, err
	}
	after, err := meshlocal.ReadPrivateArtifact(dir, filepath.Join(filepath.Dir(backups[0]), "policy-proposed.json"), 2*1048576)
	return before, after, err
}
