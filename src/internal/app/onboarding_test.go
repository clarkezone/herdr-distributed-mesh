package app

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/onboard"
)

func TestGuidedOnboardingDefaultsAndHelpNoEffects(t *testing.T) {
	for _, command := range []string{"init", "join"} {
		var output bytes.Buffer
		streams := IO{In: strings.NewReader(""), Out: &output, Err: &output}
		calls := 0
		run := func(_ context.Context, o onboard.Options, _ io.Writer, _ onboard.Dependencies) error {
			calls++
			if o.HerdrExecutable != "herdr" || o.Name != "desktop" || o.Coordinator != (command == "init") {
				t.Fatalf("%+v", o)
			}
			return nil
		}
		if err := runOnboardingWith(context.Background(), command, []string{"--help"}, streams, run); !errors.Is(err, flag.ErrHelp) || calls != 0 {
			t.Fatalf("help effects: %v calls=%d", err, calls)
		}
		args := []string{"--name", "desktop", "--tailnet", "example.com"}
		if command == "join" {
			args = []string{"--name", "desktop", "--server", "herdr-mesh-server.actual.ts.net"}
		}
		if err := runOnboardingWith(context.Background(), command, args, streams, run); err != nil || calls != 1 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
		if err := runOnboardingWith(context.Background(), command, append(args, "extra"), streams, run); err == nil || calls != 1 {
			t.Fatal("positional arg accepted")
		}
	}
}

func TestManagedRunInternalContract(t *testing.T) {
	calls := 0
	dir := t.TempDir()
	streams := IO{Out: io.Discard, Err: io.Discard}
	run := func(_ context.Context, actual string, _ io.Writer) error {
		calls++
		if actual != dir {
			t.Fatal(actual)
		}
		return nil
	}
	for _, args := range [][]string{{"--help"}, {"--state-dir", "relative"}, {"--state-dir", filepath.Join(dir, ".."), "extra"}} {
		_ = runManagedDaemonWith(context.Background(), args, streams, run)
		if calls != 0 {
			t.Fatal("invalid/help had effects")
		}
	}
	if err := runManagedDaemonWith(context.Background(), []string{"--state-dir", dir}, streams, run); err != nil || calls != 1 {
		t.Fatalf("%v calls=%d", err, calls)
	}
}
