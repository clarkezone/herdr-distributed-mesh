package meshlocal

import (
	"errors"
	"io"
	"path/filepath"
	"strings"
)

const policyCompleteRecord = "Policy-only setup completed; no device keys created.\n"

// Onboarding fsyncs this completion record only after confirmed policy success.
// A pending record is never sufficient to authorize enrollment.
func requirePolicyComplete(dir string) error {
	file, err := openPrivateRead(filepath.Join(dir, "policy-complete"))
	if err != nil {
		return err
	}
	content, readErr := io.ReadAll(io.LimitReader(file, 257))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return err
	}
	if !validPolicyCompleteRecord(string(content)) {
		return errors.New("invalid or incomplete private policy completion record")
	}
	return nil
}

func validPolicyCompleteRecord(record string) bool {
	if record == policyCompleteRecord {
		return true
	}
	const prefix = policyCompleteRecord + "apply-preview=policy-preview-"
	if !strings.HasPrefix(record, prefix) || !strings.HasSuffix(record, "\n") {
		return false
	}
	suffix := strings.TrimSuffix(strings.TrimPrefix(record, prefix), "\n")
	if len(suffix) == 0 || len(suffix) > 64 {
		return false
	}
	for _, digit := range suffix {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}
