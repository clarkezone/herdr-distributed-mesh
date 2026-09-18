package meshlocal

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCoordinatorCannotStartNetworkBeforeCompletedPolicy(t *testing.T) {
	for _, record := range []string{"", "pending", "Policy-only setup completed", policyCompleteRecord + "extra"} {
		t.Run(record, func(t *testing.T) {
			dir := canonicalTempDir(t)
			if err := Save(dir, Config{Version: 1, Name: "desktop", Tailnet: "example.test", Coordinator: true}); err != nil {
				t.Fatal(err)
			}
			if record != "" {
				if err := os.WriteFile(filepath.Join(dir, "policy-complete"), []byte(record), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "policy-apply-pending"), []byte("uncertain"), 0600); err != nil {
				t.Fatal(err)
			}
			network := &localNetwork{}
			if err := run(context.Background(), dir, io.Discard, network.dependencies(t)); err == nil {
				t.Fatal("coordinator started with unconfirmed policy")
			}
			if network.starts.Load() != 0 {
				t.Fatal("unconfirmed policy triggered network startup/enrollment")
			}
			value, err := ReadStatus(dir)
			if err != nil || value.State != "failed" {
				t.Fatalf("policy gate failure not reported: %+v %v", value, err)
			}
		})
	}
}

func TestCompletedPolicyRecordAuthorizesStartupDespitePendingCleanup(t *testing.T) {
	dir, err := privateDir(canonicalTempDir(t), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"policy-complete", "policy-apply-pending"} {
		if err := os.WriteFile(filepath.Join(dir, marker), []byte(policyCompleteRecord), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := requirePolicyComplete(dir); err != nil {
		t.Fatal(err)
	}
}
