package herdr

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// UI-only mode never enters the nonce or input path:
// HERDR_MESH_LIFECYCLE_LIVE=1
// HERDR_MESH_LIFECYCLE_UI_DIAGNOSTIC=1
// HERDR_MESH_LIFECYCLE_EVIDENCE_DIR=<existing private local directory>
// Run TestLiveAgentLifecycle once with -count=1 -timeout=5m.
// Only redacted question/choice excerpts are retained, before native cleanup.
func lifecycleCaptureStartupUI(ctx context.Context, o *observer, handle LifecycleHandle, directory, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	type observation struct {
		excerpt  string
		status   string
		bytes    int
		question bool
		options  bool
	}
	sources := []string{"visible", "recent_unwrapped"}
	latest := map[string]observation{}
	var observationErr error
	complete := false
	for !complete {
		for _, source := range sources {
			text, status, err := lifecycleStartupScreen(ctx, o, handle, source)
			if err != nil {
				observationErr = err
				break
			}
			excerpt, question, options := lifecycleStartupExcerpt(text)
			latest[source] = observation{excerpt, status, len(text), question, options}
			if status == "blocked" && question && options {
				complete = true
			}
		}
		if observationErr != nil || complete || lifecycleLivePause(ctx, 750*time.Millisecond) != nil {
			break
		}
	}
	var evidence strings.Builder
	evidence.WriteString("Startup UI diagnostic: no task prompt or approval input sent.\n")
	for _, source := range sources {
		snapshot, ok := latest[source]
		if !ok {
			fmt.Fprintf(&evidence, "\nSource: %s; no readable snapshot\n", source)
			continue
		}
		fmt.Fprintf(&evidence, "\nSource: %s; status: %s; observed bytes: %d; question: %t; choices: %t\n",
			source, snapshot.status, snapshot.bytes, snapshot.question, snapshot.options)
		evidence.WriteString(snapshot.excerpt)
		evidence.WriteByte('\n')
	}
	if !complete {
		evidence.WriteString("\nComplete question/options were not found within the bounded observation window.\n")
	}
	artifact := filepath.Join(directory, name+"-startup-ui-redacted.txt")
	file, err := os.OpenFile(artifact, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return artifact, errors.New("private evidence file creation failed")
	}
	defer file.Close()
	if _, err := file.WriteString(evidence.String()); err != nil {
		return artifact, errors.New("private evidence write failed")
	}
	if err := file.Sync(); err != nil {
		return artifact, errors.New("private evidence persistence failed")
	}
	if observationErr != nil {
		return artifact, observationErr
	}
	if !complete {
		return artifact, errors.New("bounded visible/recent reads did not contain a complete startup question and choices")
	}
	return artifact, nil
}

func lifecycleStartupScreen(ctx context.Context, o *observer, handle LifecycleHandle, source string) (string, string, error) {
	before, err := o.lifecyclePane(ctx, handle)
	if err != nil {
		return "", "", err
	}
	result, err := o.agentRequest(ctx, "pane.read", "pane_read", struct {
		PaneID    string `json:"pane_id"`
		Source    string `json:"source"`
		Lines     uint32 `json:"lines"`
		StripANSI bool   `json:"strip_ansi"`
		Format    string `json:"format"`
	}{handle.PaneID, source, 100, true, "text"})
	if err != nil {
		return "", "", errors.New("bounded startup screen read failed")
	}
	var read object
	var pane, workspace, tab, actualSource, format, text string
	var revision uint64
	var truncated bool
	if required(result, "read", &read) != nil {
		return "", "", ErrAgentUnavailable
	}
	for _, field := range []struct {
		key string
		out any
	}{
		{"pane_id", &pane}, {"workspace_id", &workspace}, {"tab_id", &tab},
		{"source", &actualSource}, {"format", &format}, {"text", &text},
		{"revision", &revision}, {"truncated", &truncated},
	} {
		if required(read, field.key, field.out) != nil {
			return "", "", ErrAgentUnavailable
		}
	}
	if pane != handle.PaneID || workspace != handle.WorkspaceID || tab != handle.TabID ||
		actualSource != source || format != "text" {
		return "", "", ErrAgentChanged
	}
	after, err := o.lifecyclePane(ctx, handle)
	if err != nil || !lifecycleMatches(lifecycleHandle(before), after, false) {
		return "", "", ErrAgentChanged
	}
	text, _ = boundedAgentText(text, 100)
	return text, after.Status, nil
}

var (
	lifecycleUIChoice = regexp.MustCompile(`(?i)^[\s>\x{276f}\x{25cf}\x{25cb}\x{2502}]*(?:[1-9][.)]|\[[1-9]\]|yes\b|no\b|allow\b|deny\b|cancel\b|continue\b|exit\b)`)
	lifecycleUIStart  = regexp.MustCompile(`(?i)^(?:do|would|can|should|may|will|are|is|which|what|how|confirm|select|choose|allow|enable|access|trust|sign in)\b`)
	lifecycleUIURL    = regexp.MustCompile(`(?i)(?:[a-z][a-z0-9+.-]*://|www\.)\S+`)
	lifecycleUIPath   = regexp.MustCompile(`(?i)(?:[a-z]:[\\/]|\\\\|/(?:home|users|tmp|var|mnt)/).*$`)
	lifecycleUISecret = regexp.MustCompile(`(?i)\b(?:[a-z0-9_-]{20,}|[a-z0-9]{4}-[a-z0-9]{4}|[0-9]{4,})\b`)
	lifecycleUIEmail  = regexp.MustCompile(`[^\s@]+@[^\s@]+`)
)

func lifecycleStartupExcerpt(text string) (string, bool, bool) {
	lines := strings.Split(text, "\n")
	selected := map[int]bool{}
	question, choices := false, false
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "?") {
			question = true
			selected[i] = true
			for j := i - 1; j >= 0 && j >= i-3; j-- {
				if strings.TrimSpace(lines[j]) == "" {
					break
				}
				if lifecycleUIStart.MatchString(strings.TrimSpace(lines[j])) {
					for k := j; k < i; k++ {
						selected[k] = true
					}
					break
				}
			}
		}
		if lifecycleUIChoice.MatchString(line) {
			choices = true
			selected[i] = true
			if i > 0 && lifecycleUIStart.MatchString(strings.TrimSpace(lines[i-1])) {
				selected[i-1] = true
			}
		}
	}
	var out strings.Builder
	count := 0
	for i, line := range lines {
		if !selected[i] {
			continue
		}
		redacted := lifecycleRedactStartupLine(strings.TrimSpace(line))
		if len(redacted) > 300 {
			redacted = "[REDACTED: overlong UI line]"
		}
		if count == 20 || out.Len()+len(redacted) > 4096 {
			out.WriteString("[bounded excerpt truncated]\n")
			break
		}
		out.WriteString(redacted)
		out.WriteByte('\n')
		count++
	}
	return out.String(), question, choices
}

func lifecycleRedactStartupLine(line string) string {
	lower := strings.ToLower(line)
	for _, field := range []string{"account", "username", "signed in", "logged in", "email",
		"login code", "verification code", "device code", "one-time code", "code=", "token", "secret", "password"} {
		if strings.Contains(lower, field) {
			return "[REDACTED: account or sensitive line]"
		}
	}
	for _, key := range []string{"USERNAME", "USER"} {
		if value := os.Getenv(key); len(value) >= 3 && strings.Contains(lower, strings.ToLower(value)) {
			return "[REDACTED: account line]"
		}
	}
	line = lifecycleUIURL.ReplaceAllString(line, "[REDACTED URL]")
	line = lifecycleUIPath.ReplaceAllString(line, "[REDACTED PATH]")
	line = lifecycleUIEmail.ReplaceAllString(line, "[REDACTED ACCOUNT]")
	line = lifecycleUISecret.ReplaceAllString(line, "[REDACTED CODE]")
	return line
}

func TestLifecycleStartupExcerptRedactsBeforeRetention(t *testing.T) {
	text := "Welcome private-person\nSigned in as private-person@example.invalid\n" +
		"Do you want to continue?\n1. Yes, for this session\n2. No, exit\n" +
		"Which account private-person@example.invalid?\n" +
		"Open https://example.invalid/path?code=ABCD-1234\n" +
		"Allow access to C:\\Users\\private-person\\checkout?\n" +
		"Enter device code ABCD-1234?\n"
	excerpt, question, choices := lifecycleStartupExcerpt(text)
	if !question || !choices || !strings.Contains(excerpt, "Do you want to continue?") ||
		!strings.Contains(excerpt, "1. Yes, for this session") || !strings.Contains(excerpt, "2. No, exit") {
		t.Fatal("nonsecret question and labels were not preserved")
	}
	for _, forbidden := range []string{"private-person", "example.invalid", "ABCD-1234", "https://", `C:\Users`} {
		if strings.Contains(excerpt, forbidden) {
			t.Fatal("retained UI excerpt contains private data")
		}
	}
}
