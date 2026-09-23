package app

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"
)

func TestManagedHelpExplainsNamesAndRelevantOptions(t *testing.T) {
	for _, verb := range []string{"start", "follow", "stop"} {
		var output bytes.Buffer
		_, err := parseManaged([]string{"agent", verb, "--help"}, IO{Err: &output})
		if !errors.Is(err, flag.ErrHelp) {
			t.Fatal(err)
		}
		text := output.String()
		for _, want := range []string{
			"agent " + verb + " <agent-name> --node <node-name>",
			"name chosen with agent start", "defaults to the hostname unless --name overrides it",
			"Agents started directly in the TUI", "Example: herdr-mesh agent " + verb,
		} {
			if !strings.Contains(text, want) {
				t.Fatalf("%s help omits %q", verb, want)
			}
		}
		if verb != "start" && (strings.Contains(text, "  -prompt ") || strings.Contains(text, "  -project ") || strings.Contains(text, "  -provider ")) {
			t.Fatal("follow/stop help advertised start-only options")
		}
		if verb == "stop" && !strings.Contains(text, "not the mesh daemon") {
			t.Fatal("agent stop was confused with daemon shutdown")
		}
	}
	var root bytes.Buffer
	printUsage(&root)
	if strings.Contains(root.String(), "<name>") || strings.Contains(root.String(), "<computer-name>") {
		t.Fatal("root help still has ambiguous name placeholders")
	}
}
