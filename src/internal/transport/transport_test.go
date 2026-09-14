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
