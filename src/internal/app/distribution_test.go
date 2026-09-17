package app

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestProductionHasOneCLIEntrypoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, "go", "list", "-f", `{{if eq .Name "main"}}{{.ImportPath}}{{end}}`, "./src/...")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list production entrypoints: %v\n%s", err, output)
	}
	want := "github.com/clarkezone/herdr-distributed-mesh/src/cmd/herdr-mesh"
	if strings.TrimSpace(string(output)) != want {
		t.Fatalf("production must expose only %s; got %q", want, output)
	}
}

func TestOperatorGuideUsesOneProductCommand(t *testing.T) {
	for _, guide := range []string{
		"operator-guide.md",
		"windows-live-validation.md",
		"tailnet-setup.md",
		"node-bootstrap.md",
		"coordinator-dashboard.md",
		"journal-maintenance.md",
	} {
		t.Run(guide, func(t *testing.T) {
			checkOperatorGuideCommands(t, guide)
		})
	}
}

func checkOperatorGuideCommands(t *testing.T, guide string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", guide))
	if err != nil {
		t.Fatal(err)
	}
	inCommands, continuation, commands := false, false, 0
	var commandLine string
	tokens := regexp.MustCompile(`'(?:''|[^'])*'|"[^"]*"|[^\s]+`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for index, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "```powershell" {
			inCommands, continuation = true, false
			continue
		}
		if line == "```" {
			if continuation {
				t.Fatalf("unfinished command at operator guide line %d", index+1)
			}
			inCommands = false
			continue
		}
		if !inCommands || line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if continuation {
			if !strings.HasPrefix(line, "-") {
				t.Fatalf("unexpected continuation at operator guide line %d", index+1)
			}
		} else {
			if !strings.HasPrefix(line, "herdr-mesh ") {
				t.Fatalf("alternate product command at operator guide line %d: %s", index+1, line)
			}
			commands++
			commandLine = ""
		}
		continuation = strings.HasSuffix(line, "`")
		commandLine += " " + strings.TrimSuffix(line, "`")
		if !continuation {
			args := tokens.FindAllString(strings.TrimSpace(commandLine), -1)[1:]
			for i, arg := range args {
				if strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'") {
					args[i] = strings.ReplaceAll(arg[1:len(arg)-1], "''", "'")
				} else if strings.HasPrefix(arg, `"`) && strings.HasSuffix(arg, `"`) {
					args[i] = arg[1 : len(arg)-1]
				}
			}
			err := Run(ctx, append(args, "-h"), IO{Out: io.Discard, Err: io.Discard})
			if !errors.Is(err, flag.ErrHelp) && !(err == nil && (args[0] == "help" || args[0] == "version" || args[0] == "setup" || args[0] == "bootstrap")) {
				t.Fatalf("operator example is not accepted by CLI help at line %d: %v", index+1, err)
			}
		}
	}
	if inCommands || commands == 0 {
		t.Fatal("operator guide has no complete command examples")
	}
}
