package protocol

import (
	"testing"
	"time"
)

func TestLifecycleStartupBoundsDoNotChangeDispatchTTL(t *testing.T) {
	for _, ms := range []uint32{0, 3001, 30000, 300000} {
		resolved, err := NormalizeAgentStartupTimeout(ms)
		if err != nil || resolved <= 3000 || resolved > MaxAgentStartupTimeoutMs {
			t.Fatalf("timeout %d: %d, %v", ms, resolved, err)
		}
		dispatch := time.Unix(1234567890, 0)
		execution, err := LifecycleExecutionDeadline(dispatch, ms)
		if err != nil || execution.Sub(dispatch) != time.Duration(resolved)*time.Millisecond+LifecycleSetupAllowance {
			t.Fatal("execution budget did not remain separate from dispatch expiry")
		}
	}
	for _, ms := range []uint32{1, 3000, 300001, ^uint32(0)} {
		if _, err := NormalizeAgentStartupTimeout(ms); err == nil {
			t.Fatalf("accepted startup timeout %d", ms)
		}
	}
	if MaxCommandTTL != 30*time.Second || DefaultCommandTTL != 10*time.Second {
		t.Fatal("lifecycle work changed ordinary dispatch limits")
	}
}
