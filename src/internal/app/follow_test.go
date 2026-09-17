package app

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"strings"
	"testing"
)

func TestAgentFollowHelpAndValidation(t *testing.T) {
	var out bytes.Buffer
	if err := runAgent(context.Background(), []string{"read", "-help"}, IO{Out: &out, Err: &out}); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "-follow") || !strings.Contains(out.String(), "JSONL") {
		t.Fatal("follow contract missing from help")
	}
	for _, interval := range []string{"0", "-1s", "249ms", "31s"} {
		err := runAgent(context.Background(), []string{"read", "-server", "server:50052", "-node", "node-1",
			"-agent", "w1:p1", "-follow", "-poll-interval", interval}, IO{Out: &out, Err: &out})
		if err == nil || !strings.Contains(err.Error(), "poll-interval") {
			t.Fatalf("interval %s: %v", interval, err)
		}
	}
	for _, verb := range []string{"get", "wait", "prompt", "input", "interrupt"} {
		err := runAgent(context.Background(), []string{verb, "-follow"}, IO{Out: &out, Err: &out})
		if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Fatalf("%s accepted following: %v", verb, err)
		}
	}
}
