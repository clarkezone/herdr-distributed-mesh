package onboard

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

func readOwnedPolicy(dir string) ([]byte, []byte, error) {
	receipt, err := meshlocal.ReadPrivateArtifact(dir, filepath.Join(dir, "policy-complete"), 256)
	if err != nil {
		return nil, nil, errors.New("policy cleanup requires the original completed init policy receipt")
	}
	if _, err := os.Lstat(filepath.Join(dir, "policy-apply-pending")); !errors.Is(err, os.ErrNotExist) {
		return nil, nil, errors.New("policy application is pending or unknown; reconcile it before policy cleanup")
	}
	previewName := "policy-preview-*"
	if strings.Contains(string(receipt), "apply-preview=") {
		lines := strings.Split(string(receipt), "\n")
		if len(lines) != 3 || lines[0] != "Policy-only setup completed; no device keys created." || lines[2] != "" ||
			!strings.HasPrefix(lines[1], "apply-preview=") {
			return nil, nil, errors.New("completed policy receipt is malformed; policy and local recovery data retained")
		}
		previewName = strings.TrimPrefix(lines[1], "apply-preview=")
		if !validPolicyPreviewName(previewName) {
			return nil, nil, errors.New("completed policy receipt has an invalid preview name; policy and local recovery data retained")
		}
	}
	backups, err := filepath.Glob(filepath.Join(dir, previewName, "apply", "policy-before-*.json"))
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
