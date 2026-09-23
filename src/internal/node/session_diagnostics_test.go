package node

import (
	"fmt"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrsession"
)

func TestSessionDiagnosticsReportTransitionsWithoutNativeOutputOrPollingSpam(t *testing.T) {
	var lines []string
	d := sessionDiagnostics{logf: func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	}}
	err := fmt.Errorf("%w: PRIVATE native output", herdrsession.ErrInvalidResponse)
	d.update(nil, err)
	d.update(nil, err)
	if len(lines) != 1 || !strings.Contains(lines[0], "invalid_discovery_response") || strings.Contains(lines[0], "PRIVATE") {
		t.Fatalf("discovery error must be safe and logged only on transition: %v", lines)
	}
	sessions := []herdrsession.Session{{Name: "default", Status: "unsupported", Protocol: 21,
		ErrorCode: "unsupported_protocol", SocketPath: "PRIVATE socket"}}
	d.update(sessions, nil)
	d.update(sessions, nil)
	if len(lines) != 3 || !strings.Contains(lines[2], "native_protocol=21") || strings.Contains(lines[2], "PRIVATE") {
		t.Fatalf("unsupported native protocol not diagnosed exactly once: %v", lines)
	}
	sessions[0].Status, sessions[0].ErrorCode, sessions[0].Protocol = "ready", "", 18
	d.update(sessions, nil)
	d.update(sessions, nil)
	if len(lines) != 4 || !strings.Contains(lines[3], `status="ready"`) {
		t.Fatalf("recovery was not logged exactly once: %v", lines)
	}
	d.update(nil, nil)
	if len(lines) != 5 || !strings.Contains(lines[4], "sessions=0") {
		t.Fatalf("empty inventory must not remain silent: %v", lines)
	}
}
