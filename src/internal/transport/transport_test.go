package transport

import (
	"context"
	"testing"
	"time"
)

func TestStartValidatesConfiguration(t *testing.T) {
	if _, err := Start(context.Background(), Config{}); err == nil {
		t.Fatal("Start() error = nil, want error")
	}
}

func TestSelfStatusValidateRequiresAssignedTags(t *testing.T) {
	status := SelfStatus{Tags: []string{"tag:observer"}}
	err := status.Validate([]string{"tag:herdr-mesh-client"}, time.Now())
	if err == nil {
		t.Fatal("Validate() error = nil, want error")
	}
}

func TestSelfStatusValidateRejectsExpiredKey(t *testing.T) {
	expiry := time.Now().Add(-time.Minute)
	status := SelfStatus{
		KeyExpiry: &expiry,
		Tags:      []string{"tag:herdr-mesh-client"},
	}
	err := status.Validate([]string{"tag:herdr-mesh-client"}, time.Now())
	if err == nil {
		t.Fatal("Validate() error = nil, want error")
	}
}

func TestNormalizeMagicDNSTargetUsesShortName(t *testing.T) {
	got := normalizeMagicDNSTarget(
		"herdr-mesh-server.tail06f2e1.ts.net:50052",
		"tail06f2e1.ts.net",
	)
	if got != "herdr-mesh-server:50052" {
		t.Fatalf("normalizeMagicDNSTarget() = %q, want short MagicDNS target", got)
	}
}

func TestNormalizeMagicDNSTargetPreservesOtherTargets(t *testing.T) {
	for _, target := range []string{
		"herdr-mesh-server:50052",
		"100.96.176.19:50052",
		"example.com:50052",
	} {
		if got := normalizeMagicDNSTarget(target, "tail06f2e1.ts.net"); got != target {
			t.Fatalf("normalizeMagicDNSTarget(%q) = %q, want unchanged", target, got)
		}
	}
}
