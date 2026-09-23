package app

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func TestSessionFlagsValidateBeforeEnrollment(t *testing.T) {
	for _, args := range [][]string{
		{"session"}, {"session", "start"}, {"sessions"},
		{"sessions", "-server", "server:50052"},
		{"sessions", "-server", "server:50052", "-node", "node-1", "-timeout", "0s"},
		{"sessions", "-server", "server:50052", "-node", "node-1", "-required-server-tag", ""},
	} {
		if err := Run(context.Background(), append([]string{"ctl"}, args...), IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}); err == nil {
			t.Fatalf("invalid flags accepted: %v", args)
		}
	}
	base := []string{"ensure", "-server", "server:50052", "-node", "node-1"}
	for _, suffix := range [][]string{
		{}, {"-name", "../escape"}, {"-name", "AUX"}, {"-name", "con"}, {"-name", "lpt1"}, {"-name", "nul"},
		{"-name", "worker", "-ttl", "31s"}, {"-name", "worker", "-ttl", "0s"},
		{"-name", "worker", "-key", "bad/key"}, {"-name", "worker", "extra"},
	} {
		args := append(append([]string{}, base...), suffix...)
		if _, err := parseSessionQuery("session", args, IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}); err == nil {
			t.Fatalf("invalid session request accepted: %v", args)
		}
	}
}

func TestSessionFlagsKeepExactRequest(t *testing.T) {
	streams := IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}
	args := []string{"ensure", "-server", "server:50052", "-node", "node-1", "-name", "Build-QEI", "-key", "retry-key", "-ttl", "15s", "-json"}
	first, err := parseSessionQuery("session", args, streams)
	if err != nil {
		t.Fatal(err)
	}
	second, err := parseSessionQuery("session", args, streams)
	if err != nil || !proto.Equal(first.request, second.request) {
		t.Fatalf("exact retry changed: %v", err)
	}
	if first.request.CommandType != protocol.SessionEnsureCommandType || first.request.SessionEnsure.Name != "Build-QEI" ||
		first.request.IdempotencyKey != "retry-key" || first.request.Ttl.AsDuration() != 15*time.Second || !first.options.JSON {
		t.Fatalf("flags lost: %+v", first)
	}
	list, err := parseSessionQuery("sessions", []string{"-server", "server:50052", "-node", "node-1"}, streams)
	if err != nil || list.request != nil || list.nodeID != "node-1" {
		t.Fatalf("list parsing: %+v %v", list, err)
	}
}

func TestWorkspaceSessionSelectorsPreservedAndValidated(t *testing.T) {
	for _, command := range []string{"ensure-workspace", "create-worktree"} {
		args := []string{"-server", "server:50052", "-node", "node-1", "-project", "Project", "-idempotency-key", "fixed-key"}
		if command == "create-worktree" {
			args = append(args, "-name", "task")
		}
		for _, name := range []string{"", "worker", "QEI"} {
			incarnation := strings.Repeat("a", 64)
			selected := append(append([]string{}, args...), "-session", name, "-session-incarnation", incarnation)
			query, err := parseCommandQuery(command, selected, IO{Err: &bytes.Buffer{}})
			if err != nil || query.worktree.SessionName != name || query.worktree.SessionIncarnation != incarnation || query.key != "fixed-key" {
				t.Fatalf("selectors changed: %+v %v", query, err)
			}
		}
		for _, suffix := range [][]string{{"-session", "con"}, {"-session", "AUX"}, {"-session-incarnation", "bad"}, {"-session-incarnation", strings.Repeat("A", 64)}} {
			if _, err := parseCommandQuery(command, append(append([]string{}, args...), suffix...), IO{Err: &bytes.Buffer{}}); err == nil {
				t.Fatalf("invalid selector accepted: %v", suffix)
			}
		}
	}
}

func TestNodeExecutableEnablesCommandsWithoutDefaultSocket(t *testing.T) {
	for _, policy := range []bool{false, true} {
		args := []string{"node", "-server", "server:50052", "-herdr-executable", `C:\Herdr\herdr.exe`, "-required-server-tag", ""}
		if policy {
			args = append(args, "-workspace-policy", "unused-policy")
		}
		err := Run(context.Background(), args, IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}})
		if err == nil || !strings.Contains(err.Error(), "required-server-tag") {
			t.Fatalf("executable-only configuration not treated as enabled: %v", err)
		}
	}
}
