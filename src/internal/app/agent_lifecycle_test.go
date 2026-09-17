package app

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestLifecycleCLIRejectsUnsafeSelectorsBeforeEnrollment(t *testing.T) {
	for _, test := range []struct {
		verb, message string
		args          []string
		input         string
	}{
		{"start", "project/revision", nil, ""},
		{"start", "startup-timeout", []string{"-startup-timeout", "3s"}, ""},
		{"start", "startup-timeout", []string{"-startup-timeout", "3001.5ms"}, ""},
		{"start", "startup-timeout", []string{"-startup-timeout", "301s"}, ""},
		{"start", "8192", []string{"-project", "project", "-name", "task", "-prompt-file", "-"}, strings.Repeat("x", 8193)},
		{"start", "UTF-8", []string{"-project", "project", "-name", "task", "-prompt-file", "-"}, "\xff"},
		{"start", "not both", []string{"-prompt", "task", "-prompt-file", "-"}, ""},
		{"start", "flag provided but not defined", []string{"-provider-args", "--unsafe"}, ""},
		{"start", "-ttl", []string{"-ttl", "31s"}, ""},
		{"stop", "exact session incarnation", nil, ""},
		{"stop", "exact session incarnation", []string{"-agent", "p1", "-terminal", "t1", "-tab", "tab1"}, ""},
		{"stop", "flag provided but not defined", []string{"-prompt", "task"}, ""},
		{"stop", "idempotency key", []string{"-idempotency-key", "../escape"}, ""},
	} {
		t.Run(test.verb+"-"+test.message+strings.Join(test.args, ""), func(t *testing.T) {
			args := []string{"ctl", "agent", test.verb, "-server", "server:50052", "-node", "node",
				"-workspace", "ws:1", "-provider", "copilot", "-state-dir", t.TempDir()}
			args = append(args, test.args...)
			err := Run(context.Background(), args, IO{In: strings.NewReader(test.input), Out: io.Discard, Err: io.Discard})
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("wanted %q before runtime enrollment, got %v", test.message, err)
			}
		})
	}
}
