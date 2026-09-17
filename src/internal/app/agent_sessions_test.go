package app

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestAgentSessionFlagsReachTypedSelectionValidation(t *testing.T) {
	for _, operation := range []string{"get", "read", "wait", "prompt", "input", "interrupt", "follow"} {
		t.Run(operation, func(t *testing.T) {
			verb := operation
			if operation == "follow" {
				verb = "read"
			}
			args := []string{"ctl", "agent", verb, "-server", "server:50052", "-node", "invalid/node", "-agent", "w1:p1",
				"-terminal", "term_original", "-agent-session", "provider-session", "-session", "worker", "-session-incarnation", strings.Repeat("a", 64)}
			switch operation {
			case "prompt":
				args = append(args, "-prompt", "hello")
			case "input":
				args = append(args, "-key", "enter")
			case "follow":
				args = append(args, "-follow")
			}
			err := Run(context.Background(), args, IO{Out: io.Discard, Err: io.Discard})
			if err == nil || !strings.Contains(err.Error(), "node ID") {
				t.Fatalf("session flags did not reach typed selection validation: %v", err)
			}
		})
	}
}
