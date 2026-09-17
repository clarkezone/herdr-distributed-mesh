package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAgentFlagsRejectBeforeEnrollment(t *testing.T) {
	for _, test := range []struct {
		verb    string
		args    []string
		input   string
		message string
	}{
		{"prompt", nil, "", "prompt must contain"},
		{"prompt", []string{"-prompt-file", "-"}, strings.Repeat("x", 8193), "prompt must contain"},
		{"prompt", []string{"-prompt-file", "-"}, "\xff", "prompt must contain"},
		{"prompt", []string{"-prompt", "hello", "-prompt-file", "-"}, "", "not both"},
		{"prompt", []string{"-prompt", "hello", "-idempotency-key", "bad/key"}, "", "idempotency key"},
		{"prompt", []string{"-prompt", "hello", "-ttl", "31s"}, "", "-ttl"},
		{"input", nil, "", "explicit keys"},
		{"input", []string{"-key", "ctrl+z"}, "", "supported"},
		{"interrupt", []string{"-prompt", "hello"}, "", "flag provided but not defined"},
		{"get", []string{"-agent", "../bad"}, "", "identifiers"},
		{"get", []string{"-required-server-tag", ""}, "", "server tag"},
		{"get", []string{"-session", "con"}, "", "session name or incarnation"},
		{"read", []string{"-session", "UPPER"}, "", "session name or incarnation"},
		{"read", []string{"-follow", "-session", "../escape"}, "", "session name or incarnation"},
		{"wait", []string{"-session-incarnation", "bad"}, "", "session name or incarnation"},
		{"prompt", []string{"-session-incarnation", strings.Repeat("A", 64), "-prompt", "hello"}, "", "session name or incarnation"},
		{"input", []string{"-session", "lpt1", "-key", "enter"}, "", "session name or incarnation"},
		{"interrupt", []string{"-session", "nul"}, "", "session name or incarnation"},
		{"read", []string{"-lines", "1001"}, "", "-lines"},
		{"read", []string{"-lines", "0"}, "", "-lines"},
		{"wait", []string{"-until", "idle,idle"}, "", "duplicate"},
		{"wait", []string{"-until", "success"}, "", "wait state"},
		{"wait", []string{"-wait-timeout", "301s"}, "", "-wait-timeout"},
		{"wait", []string{"-wait-timeout", "1ns"}, "", "-wait-timeout"},
	} {
		t.Run(test.verb+" "+strings.Join(test.args, " "), func(t *testing.T) {
			args := []string{"ctl", "agent", test.verb, "-server", "server:50052", "-node", "node-1", "-agent", "w1:p1", "-state-dir", t.TempDir()}
			args = append(args, test.args...)
			err := Run(context.Background(), args, IO{In: strings.NewReader(test.input), Out: io.Discard, Err: io.Discard})
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("expected %q, got %v", test.message, err)
			}
		})
	}
}

type failingAgentInput struct{}

func (failingAgentInput) Read([]byte) (int, error) { return 0, errors.New("test input failed") }

func TestAgentPromptFileAndStdinFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prompt.txt")
	if err := os.WriteFile(path, bytes.Repeat([]byte{'x'}, 8193), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"ctl", "agent", "prompt", "-server", "server:50052", "-node", "node-1", "-agent", "w1:p1", "-prompt-file", path}
	err := Run(context.Background(), args, IO{Out: io.Discard, Err: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "8192") {
		t.Fatalf("unbounded file read: %v", err)
	}
	args[len(args)-1] = "-"
	err = Run(context.Background(), args, IO{In: failingAgentInput{}, Out: io.Discard, Err: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "test input failed") {
		t.Fatalf("lost input failure: %v", err)
	}
}
