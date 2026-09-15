package app

import (
	"bytes"
	"context"
	"testing"
)

func TestCommandFlagsValidateBeforeEnrollment(t *testing.T) {
	for _, args := range [][]string{
		{"ctl", "ping"},
		{"ctl", "ping", "-server", "server:50052"},
		{"ctl", "ping", "-server", "server:50052", "-node", "node-1", "-ttl", "31s"},
		{"ctl", "ping", "-server", "server:50052", "-node", "node-1", "-timeout", "0s"},
		{"ctl", "command", "-server", "server:50052", "-id", "wrong"},
		{"node", "-server", "server:50052", "-enable-probes", "-required-server-tag", ""},
		{"server", "-required-command-tag", ""},
	} {
		if err := Run(context.Background(), args, IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}); err == nil {
			t.Fatalf("invalid args accepted: %v", args)
		}
	}
}
