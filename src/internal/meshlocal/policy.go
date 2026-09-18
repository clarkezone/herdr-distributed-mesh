package meshlocal

import (
	"errors"
	"io"
	"path/filepath"
)

const policyCompleteRecord = "Policy-only setup completed; no device keys created.\n"

// Onboarding fsyncs this completion record only after confirmed policy success.
// A pending record is never sufficient to authorize enrollment.
func requirePolicyComplete(dir string) error {
	file, err := openPrivateRead(filepath.Join(dir, "policy-complete"))
	if err != nil {
		return err
	}
	content, readErr := io.ReadAll(io.LimitReader(file, int64(len(policyCompleteRecord)+1)))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return err
	}
	if string(content) != policyCompleteRecord {
		return errors.New("invalid or incomplete private policy completion record")
	}
	return nil
}
