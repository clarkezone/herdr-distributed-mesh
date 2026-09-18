// Package bootstrap stages a node and optionally starts an existing runner over
// an operator-prepared SSH alias. It never connects through the mesh.
package bootstrap

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/scripthost"
)

//go:embed assets/*.ps1
var assets embed.FS

const (
	DefaultTimeout = 5 * time.Minute
	MaxTimeout     = 30 * time.Minute
	maxResponse    = 16 * 1024
)

type Options struct {
	SSHHost          string        `json:"SSHHost"`
	ArchivePath      string        `json:"ArchivePath"`
	ExpectedSHA256   string        `json:"ExpectedSHA256"`
	Architecture     string        `json:"Architecture"`
	InstallDirectory string        `json:"InstallDirectory"`
	StateDirectory   string        `json:"StateDirectory"`
	Server           string        `json:"Server"`
	HerdrExecutable  string        `json:"HerdrExecutable"`
	Start            bool          `json:"Start"`
	ExistingTaskName string        `json:"ExistingTaskName"`
	Timeout          time.Duration `json:"-"`
}

type Result struct {
	SchemaVersion       int      `json:"schema_version"`
	Status              string   `json:"status"`
	OperationID         string   `json:"operation_id"`
	InstallDirectory    string   `json:"install_directory"`
	StateDirectory      string   `json:"state_directory"`
	Executable          string   `json:"executable"`
	NodeArguments       []string `json:"node_arguments"`
	ArchiveSHA256       string   `json:"archive_sha256"`
	BinarySHA256        string   `json:"binary_sha256"`
	Architecture        string   `json:"architecture"`
	Startup             string   `json:"startup"`
	Enrollment          string   `json:"enrollment"`
	Connectivity        string   `json:"connectivity"`
	RunnerRequired      bool     `json:"runner_required"`
	InstallOperationID  string   `json:"install_operation_id,omitempty"`
	ExistingTaskName    string   `json:"existing_task_name,omitempty"`
	RunnerState         string   `json:"runner_state,omitempty"`
	RunnerObservedAtUTC string   `json:"runner_observed_at_utc,omitempty"`
}

// Error never contains process output, task arguments, SSH diagnostics or secrets.
type Error struct {
	Code        string `json:"code"`
	Outcome     string `json:"status"`
	Phase       string `json:"phase,omitempty"`
	OperationID string `json:"operation_id,omitempty"`
}

func (e *Error) Error() string {
	message, ok := messages[e.Code]
	if !ok {
		message = messages["process_failed"]
	}
	return fmt.Sprintf("bootstrap: %s (outcome=%s)", message, e.Outcome)
}

func (e *Error) Is(target error) bool {
	return (e.Code == "canceled" && target == context.Canceled) ||
		(e.Code == "timeout" && target == context.DeadlineExceeded)
}

var messages = map[string]string{
	"invalid_options":              "invalid typed options; use bootstrap -help",
	"platform":                     "Windows is required for this bootstrap implementation",
	"pwsh_missing":                 "install or locate PowerShell 7.4+ (pwsh) separately before bootstrap",
	"ssh_missing":                  "prepare OpenSSH ssh and an authenticated host alias separately",
	"ssh_config":                   "an existing user .ssh\\config with an explicit literal Host alias is required; Include, Match and environment-forwarding directives are not supported",
	"ssh_connection":               "the trusted SSH alias must be reachable with noninteractive public-key authentication and an existing powershell subsystem; check known_hosts, account access and endpoint configuration separately",
	"process_failed":               "the bounded embedded PowerShell process failed; inspect the intended install/task before retrying; raw process diagnostics are withheld",
	"output_invalid":               "the embedded process returned an invalid or oversized receipt; inspect before retrying",
	"cleanup_failed":               "private bootstrap artifact cleanup failed; inspect the private temporary directory and target before retrying",
	"temporary_io":                 "unable to create private bootstrap artifacts; check local temporary-directory permissions and space",
	"canceled":                     "bootstrap canceled; remote work may continue; do not automatically retry a possibly dispatched start",
	"timeout":                      "bootstrap deadline expired; remote work may continue; inspect before retrying",
	"operation_failed":             "bootstrap operation failed; inspect the documented target prerequisites and retained staging artifacts",
	"BOOTSTRAP_INPUT":              "a trusted SHA256, native Windows architecture and bounded timeout are required",
	"BOOTSTRAP_PATH":               "use bounded absolute local target paths without expressions, device names or traversal",
	"BOOTSTRAP_SERVER":             "use a coordinator DNS/IPv4 host and port, not a URL or shell expression",
	"BOOTSTRAP_STATE_OVERLAP":      "install, staging and state paths must be disjoint",
	"BOOTSTRAP_DIRECTORY_REQUIRED": "prepare the private install parent and separate state directory on the target first",
	"BOOTSTRAP_PRIVATE_DIRECTORY":  "target directories must be private to the connected account, SYSTEM and Administrators",
	"BOOTSTRAP_REPARSE":            "target paths must not contain symlinks, junctions or other reparse points",
	"BOOTSTRAP_LOCAL_DISK":         "target paths must be on fixed local disks",
	"BOOTSTRAP_LOCAL_ARCHIVE":      "copy the trusted ZIP to a fixed local controller drive first",
	"BOOTSTRAP_EXISTS":             "the install path already exists; no overwrite is supported; use a new path or verify/reuse a matching install with -start",
	"BOOTSTRAP_HASH":               "trusted archive or binary SHA256 mismatch",
	"BOOTSTRAP_ARCHIVE_FORMAT":     "supply the Windows ZIP produced by build-release.ps1",
	"BOOTSTRAP_ARCHIVE_LAYOUT":     "the ZIP must contain only the regular root entry herdr-mesh.exe",
	"BOOTSTRAP_ARCHIVE_SIZE":       "the complete archive must be nonempty and at most 256 MiB",
	"BOOTSTRAP_BINARY_SIZE":        "the expanded executable must have a consistent size at most 512 MiB",
	"BOOTSTRAP_BINARY_FORMAT":      "the payload must be a Windows PE32+ executable",
	"BOOTSTRAP_ARCHITECTURE":       "the release architecture must match the target's native Windows architecture",
	"BOOTSTRAP_PLATFORM":           "PowerShell 7.4+ and Windows are required on both ends",
	"BOOTSTRAP_HERDR_EXECUTABLE":   "prepare the optional target-side Herdr .exe separately; it must already exist and be accessible",
	"BOOTSTRAP_RUNNER_REQUIRED":    "supply -start and -existing-task together, or omit both to stage only",
	"BOOTSTRAP_TASK_NAME":          "use an explicit full task name without wildcard or command text",
	"BOOTSTRAP_TASK_TOOLS":         "the target must already provide Windows ScheduledTasks commands",
	"BOOTSTRAP_TASK_UNAVAILABLE":   "the named Scheduled Task must already exist and be accessible; no task is created or repaired",
	"BOOTSTRAP_TASK_ACTION":        "the task must have exactly one direct Exec action matching this binary and the complete fixed node arguments",
	"BOOTSTRAP_TASK_INSTANCES":     "the task must use the IgnoreNew instance policy",
	"BOOTSTRAP_TASK_SETTINGS":      "the task must be enabled, allow demand start, use PT0S and a supported noninteractive logon type",
	"BOOTSTRAP_TASK_STATE":         "the task must report Ready, Running or Queued",
	"BOOTSTRAP_INSTALLED_MISMATCH": "the existing receipt, archive, binary and intended arguments must match for read-only reuse",
	"BOOTSTRAP_TASK_START":         "the one-shot task start failed to acknowledge; inspect task state and normal mesh inventory before any retry",
	"BOOTSTRAP_TASK_OBSERVATION":   "a fresh Running state was not observed within ten seconds; no automatic start retry was made",
	"BOOTSTRAP_CHUNK":              "the bounded upload was rejected; inspect staging before retrying",
	"BOOTSTRAP_OFFSET":             "the upload offset changed; do not append or replay automatically",
	"BOOTSTRAP_TRANSPORT":          "the fixed remote operation returned an unexpected receipt; inspect before retrying",
	"BOOTSTRAP_IO":                 "check local release readability and target permissions, space and connection; raw diagnostics are withheld",
}

var (
	hashPattern      = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)
	idPattern        = regexp.MustCompile(`^[0-9a-f]{32}$`)
	aliasPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	componentPattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9 ._-]{0,79}$`)
	devicePattern    = regexp.MustCompile(`(?i)^(CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])(?:\.|$)`)
	serverPattern    = regexp.MustCompile(`^([A-Za-z0-9][A-Za-z0-9.-]{0,252}):([0-9]{1,5})$`)
	labelPattern     = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)
)

func validPath(path string) bool {
	if len(path) < 4 || len(path) > 200 || path[1:3] != `:\` ||
		!((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) {
		return false
	}
	for _, part := range strings.Split(path[3:], `\`) {
		if !componentPattern.MatchString(part) || strings.HasSuffix(part, ".") ||
			strings.HasSuffix(part, " ") || devicePattern.MatchString(part) {
			return false
		}
	}
	return true
}

// Validate is local and side-effect free; filesystem/task checks occur remotely.
func Validate(o Options) error {
	bad := func() error { return &Error{Code: "invalid_options", Outcome: "not-installed"} }
	if !aliasPattern.MatchString(o.SSHHost) || o.ArchivePath == "" || len(o.ArchivePath) > 4096 ||
		strings.ContainsRune(o.ArchivePath, 0) || !hashPattern.MatchString(o.ExpectedSHA256) ||
		(o.Architecture != "amd64" && o.Architecture != "arm64") ||
		!validPath(o.InstallDirectory) || !validPath(o.StateDirectory) ||
		o.Timeout < time.Second || o.Timeout > MaxTimeout || o.Timeout%time.Second != 0 {
		return bad()
	}
	install, state := strings.ToLower(o.InstallDirectory), strings.ToLower(o.StateDirectory)
	if install == state || strings.HasPrefix(install, state+`\`) || strings.HasPrefix(state, install+`\`) {
		return bad()
	}
	if o.HerdrExecutable != "" && (!validPath(o.HerdrExecutable) ||
		!strings.HasSuffix(strings.ToLower(o.HerdrExecutable), ".exe")) {
		return bad()
	}
	match := serverPattern.FindStringSubmatch(o.Server)
	if match == nil {
		return bad()
	}
	port, _ := strconv.Atoi(match[2])
	if port < 1 || port > 65535 {
		return bad()
	}
	for _, label := range strings.Split(match[1], ".") {
		if !labelPattern.MatchString(label) {
			return bad()
		}
	}
	if o.Start != (o.ExistingTaskName != "") {
		return bad()
	}
	if o.Start {
		if len(o.ExistingTaskName) > 200 || !strings.HasPrefix(o.ExistingTaskName, `\`) {
			return bad()
		}
		for _, part := range strings.Split(o.ExistingTaskName[1:], `\`) {
			if !componentPattern.MatchString(part) || strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
				return bad()
			}
		}
	}
	return nil
}

type hostFunc func(context.Context, scripthost.Request) (scripthost.Result, error)

// Run uses only embedded assets and a private bounded script host. The shared
// host owns the local process tree; killing it is not remote transactional undo.
func Run(ctx context.Context, o Options) (*Result, error) {
	if runtime.GOOS != "windows" {
		return nil, &Error{Code: "platform", Outcome: "not-installed"}
	}
	return run(ctx, o, exec.LookPath, scripthost.Run)
}

func run(ctx context.Context, o Options, find func(string) (string, error), host hostFunc) (*Result, error) {
	if err := Validate(o); err != nil {
		return nil, err
	}
	op, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	if op.Err() != nil {
		return nil, contextError(op, false, o.Start)
	}
	for _, tool := range []string{"pwsh", "ssh"} {
		path, err := find(tool)
		if err != nil || !filepath.IsAbs(path) {
			return nil, &Error{Code: tool + "_missing", Outcome: "not-installed"}
		}
	}
	archive, err := filepath.Abs(o.ArchivePath)
	if err != nil {
		return nil, &Error{Code: "invalid_options", Outcome: "not-installed"}
	}
	o.ArchivePath = archive
	o.ExpectedSHA256 = strings.ToLower(o.ExpectedSHA256)
	entry, err := assets.ReadFile("assets/entry.ps1")
	if err != nil {
		return nil, &Error{Code: "process_failed", Outcome: "not-installed"}
	}
	core, err := assets.ReadFile("assets/bootstrap-node.ps1")
	if err != nil {
		return nil, &Error{Code: "process_failed", Outcome: "not-installed"}
	}
	request := scripthost.Request{
		Script: entry, Assets: map[string][]byte{"bootstrap-node.ps1": core},
		Options: struct {
			Options
			TimeoutSeconds int `json:"TimeoutSeconds"`
		}{o, int(o.Timeout / time.Second)},
		Timeout: o.Timeout,
	}
	reply, err := host(op, request)
	if op.Err() != nil {
		return nil, contextError(op, reply.Started, o.Start)
	}
	if err != nil {
		code := "process_failed"
		var hostError *scripthost.Error
		if errors.As(err, &hostError) {
			switch hostError.Code {
			case "timeout":
				code = "timeout"
			case "canceled":
				code = "canceled"
			case "cleanup_failed", "temporary_io":
				code = hostError.Code
			case "output_limit":
				code = "output_invalid"
			case "prerequisite_missing":
				code = "pwsh_missing"
			}
		}
		return nil, &Error{Code: code, Outcome: uncertain(reply.Started, o.Start)}
	}
	return decode(reply.Output, o, reply.Started)
}

func uncertain(dispatched, start bool) string {
	if !dispatched {
		return "not-installed"
	}
	if start {
		return "start-unknown"
	}
	return "indeterminate"
}

func contextError(ctx context.Context, dispatched, start bool) error {
	code := "canceled"
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		code = "timeout"
	}
	return &Error{Code: code, Outcome: uncertain(dispatched, start)}
}

type envelope struct {
	Version int     `json:"version"`
	OK      bool    `json:"ok"`
	Result  *Result `json:"result,omitempty"`
	Error   *Error  `json:"error,omitempty"`
}

func decode(data []byte, o Options, dispatched bool) (*Result, error) {
	bad := func() (*Result, error) {
		return nil, &Error{Code: "output_invalid", Outcome: uncertain(dispatched, o.Start)}
	}
	if len(data) == 0 || len(data) > maxResponse {
		return bad()
	}
	if !uniqueJSON(json.NewDecoder(bytes.NewReader(data)), 0) {
		return bad()
	}
	var reply envelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reply); err != nil {
		return bad()
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF || reply.Version != 1 {
		return bad()
	}
	if !reply.OK {
		e := reply.Error
		if e == nil || reply.Result != nil {
			return bad()
		}
		if _, known := messages[e.Code]; !known {
			return bad()
		}
		if e.Outcome != "not-installed" && e.Outcome != "indeterminate" && !(o.Start && e.Outcome == "start-unknown") {
			return bad()
		}
		switch e.Phase {
		case "validation", "connection", "prepare", "upload", "commit", "start", "cleanup":
		default:
			return bad()
		}
		if e.OperationID != "" && !idPattern.MatchString(e.OperationID) {
			return bad()
		}
		if e.Phase == "start" && e.Outcome != "start-unknown" {
			return bad()
		}
		return nil, e
	}
	r := reply.Result
	if r == nil || reply.Error != nil || !validResult(r, o) {
		return bad()
	}
	return r, nil
}

func uniqueJSON(decoder *json.Decoder, depth int) bool {
	if depth > 8 {
		return false
	}
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	switch token {
	case json.Delim('{'):
		seen := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			name, ok := key.(string)
			if err != nil || !ok || seen[name] {
				return false
			}
			seen[name] = true
			if !uniqueJSON(decoder, depth+1) {
				return false
			}
		}
		end, err := decoder.Token()
		return err == nil && end == json.Delim('}')
	case json.Delim('['):
		for decoder.More() {
			if !uniqueJSON(decoder, depth+1) {
				return false
			}
		}
		end, err := decoder.Token()
		return err == nil && end == json.Delim(']')
	default:
		return true
	}
}

func validResult(r *Result, o Options) bool {
	if r.SchemaVersion != 1 || !idPattern.MatchString(r.OperationID) ||
		r.ArchiveSHA256 != strings.ToLower(o.ExpectedSHA256) || !hashPattern.MatchString(r.BinarySHA256) ||
		r.Architecture != o.Architecture || !strings.EqualFold(r.InstallDirectory, o.InstallDirectory) ||
		!strings.EqualFold(r.StateDirectory, o.StateDirectory) ||
		!strings.EqualFold(r.Executable, o.InstallDirectory+`\herdr-mesh.exe`) ||
		r.Enrollment != "not-checked" || r.Connectivity != "not-checked" {
		return false
	}
	want := []string{"node", "-server", o.Server, "-state-dir", o.StateDirectory}
	if o.HerdrExecutable != "" {
		want = append(want, "-herdr-executable", o.HerdrExecutable)
	}
	if len(r.NodeArguments) != len(want) {
		return false
	}
	for i := range want {
		if i == 4 || i == 6 {
			if !strings.EqualFold(r.NodeArguments[i], want[i]) {
				return false
			}
		} else if r.NodeArguments[i] != want[i] {
			return false
		}
	}
	if !o.Start {
		return r.Status == "staged-not-started" && r.Startup == "not-attempted" && r.RunnerRequired &&
			r.InstallOperationID == "" && r.ExistingTaskName == "" && r.RunnerState == "" && r.RunnerObservedAtUTC == ""
	}
	_, err := time.Parse(time.RFC3339Nano, r.RunnerObservedAtUTC)
	return r.Status == "runner-running-mesh-unverified" && !r.RunnerRequired &&
		idPattern.MatchString(r.InstallOperationID) && r.ExistingTaskName == o.ExistingTaskName &&
		r.RunnerState == "Running" && err == nil &&
		(r.Startup == "requested-once" || r.Startup == "reused-running" || r.Startup == "observed-pending")
}
