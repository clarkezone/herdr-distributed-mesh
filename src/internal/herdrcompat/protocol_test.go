package herdrcompat

import "testing"

func TestExplicitNativeProtocolAllowlist(t *testing.T) {
	for _, version := range []int64{-1, 0, 1, 17, 18, 19, 20, 21, 22, 23, 1<<32 + 18, 1<<63 - 1} {
		if got, want := SupportsProtocol(version), version == 18 || version == 20 || version == 22; got != want {
			t.Errorf("protocol %d: got %t, want %t", version, got, want)
		}
	}
}
