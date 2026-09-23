package node

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReconnectDelayBacksOffAndCaps(t *testing.T) {
	base := 2 * time.Second
	maximum := 30 * time.Second
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 2 * time.Second},
		{attempt: 1, want: 4 * time.Second},
		{attempt: 2, want: 8 * time.Second},
		{attempt: 10, want: 30 * time.Second},
	}
	for _, test := range tests {
		if got := reconnectDelay(base, maximum, test.attempt, 0.5); got != test.want {
			t.Fatalf("reconnectDelay(attempt=%d) = %s, want %s", test.attempt, got, test.want)
		}
	}
}

func TestIsPermanentSessionError(t *testing.T) {
	if !isPermanentSessionError(status.Error(codes.PermissionDenied, "denied")) {
		t.Fatal("PermissionDenied was not classified as permanent")
	}
	if isPermanentSessionError(status.Error(codes.Unavailable, "offline")) {
		t.Fatal("Unavailable was classified as permanent")
	}
	if isPermanentSessionError(errors.New("network failure")) {
		t.Fatal("ordinary error was classified as permanent")
	}
}

func TestReconnectDelayAppliesBoundedJitter(t *testing.T) {
	base := 10 * time.Second
	if got := reconnectDelay(base, time.Minute, 0, 0); got != 8*time.Second {
		t.Fatalf("minimum jitter delay = %s, want 8s", got)
	}
	if got := reconnectDelay(base, time.Minute, 0, 1); got != 12*time.Second {
		t.Fatalf("maximum jitter delay = %s, want 12s", got)
	}
}
