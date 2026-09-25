package onboard

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCompletedReceiptSelectsVerifiedApplyAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"policy-preview-111", "policy-preview-222"} {
		apply := filepath.Join(dir, name, "apply")
		if err := os.MkdirAll(apply, 0700); err != nil {
			t.Fatal(err)
		}
		for file, data := range map[string]string{
			"policy-before-1.json": `{"preview":"` + name + `"}`,
			"policy-proposed.json": `{"proposal":"` + name + `"}`,
		} {
			if err := os.WriteFile(filepath.Join(apply, file), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writePolicyComplete(filepath.Join(dir, "policy-complete"), "policy-preview-222"); err != nil {
		t.Fatal(err)
	}
	before, after, err := readOwnedPolicy(dir)
	if err != nil || string(before) != `{"preview":"policy-preview-222"}` || string(after) != `{"proposal":"policy-preview-222"}` {
		t.Fatalf("completed receipt did not select verified apply: %s %s %v", before, after, err)
	}
}
