package app

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestProjectFlags(t *testing.T) {
	streams := IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}
	for _, test := range []struct {
		command string
		args    []string
		want    string
	}{
		{"project", []string{"register", "-server", "server:50052", "-node", "node-1", "-project", "AgentFlow", "-path", `C:\remote checkout`, "-worktree-root", `C:\remote worktrees`, "-json"}, "register"},
		{"project", []string{"register", "-server", "server:50052", "-node", "node-1", "-project", "AgentFlow", "-path", `C:\remote checkout`}, "register"},
		{"project", []string{"get", "-server", "server:50052", "-node", "node-1", "-project", "AgentFlow"}, "get"},
		{"projects", []string{"-server", "server:50052"}, "projects"},
		{"projects", []string{"-server", "server:50052", "-node", "node-1"}, "projects"},
	} {
		query, err := parseProjectQuery(test.command, test.args, streams)
		if err != nil {
			t.Fatal(err)
		}
		if query.command != test.want || query.options.RequiredServerTag != "tag:herdr-mesh-server" {
			t.Fatalf("incorrect query: %+v", query)
		}
		if test.want == "register" && query.request.CheckoutPath != `C:\remote checkout` {
			t.Fatal("remote path altered or interpreted locally")
		}
		if len(test.args) > 10 && (query.request.WorktreeRoot != `C:\remote worktrees` || !query.options.JSON) {
			t.Fatal("worktree root or JSON flag lost")
		}
	}
}

func TestProjectFlagsRejectBeforeEnrollment(t *testing.T) {
	for _, args := range [][]string{
		{"ctl", "project"},
		{"ctl", "project", "remove"},
		{"ctl", "projects"},
		{"ctl", "project", "get", "-server", "server:50052"},
		{"ctl", "project", "get", "-server", "server:50052", "-node", "node-1"},
		{"ctl", "project", "register", "-server", "server:50052", "-node", "node-1", "-project", "AgentFlow"},
		{"ctl", "projects", "-server", "server:50052", "-required-server-tag", ""},
		{"ctl", "projects", "-server", "server:50052", "-timeout", "0s"},
		{"ctl", "projects", "-server", "server:50052", "unexpected"},
		{"ctl", "projects", "-server", "server:50052", "-path", `C:\not-a-list-option`},
	} {
		if err := Run(context.Background(), args, IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}); err == nil {
			t.Fatalf("invalid flags accepted: %v", args)
		}
	}
}

func TestServerRejectsLegacyPolicyWithoutLoadingIt(t *testing.T) {
	for _, path := range []string{"does-not-exist", ""} {
		err := Run(context.Background(), []string{"server", "-workspace-policy", path}, IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}})
		if err == nil || !strings.Contains(err.Error(), "remove it and use ctl project register") {
			t.Fatalf("missing actionable migration error: %v", err)
		}
	}
}
