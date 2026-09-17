// Package scripthost runs bundled PowerShell with private JSON options.
package scripthost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MaxOutputBytes = 256 * 1024
	maxInputBytes  = 256 * 1024
	maxScriptBytes = 2 * 1024 * 1024
	MaxTimeout     = 30 * time.Minute
	DefaultTimeout = 2 * time.Minute
)

type Request struct {
	Script  []byte
	Assets  map[string][]byte
	Options any
	Timeout time.Duration
}

// Output is available only on success and still requires caller schema validation.
// Started on an error means remote effects may be unknown, not canceled.
type Result struct {
	Output  []byte
	Started bool
}

type Error struct{ Code string }

func (e *Error) Error() string { return "embedded PowerShell: " + e.Code }

var assetName = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]{0,63}$`)

const launcher = `param([Parameter(Mandatory)][string]$OptionsPath)
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object Text.UTF8Encoding($false)
$OutputEncoding = [Console]::OutputEncoding
& (Join-Path $PSScriptRoot 'entry.ps1') -OptionsPath $OptionsPath
`

func Run(ctx context.Context, request Request) (Result, error) {
	return run(ctx, request, findPowerShell, execute)
}

func findPowerShell() (string, error) {
	if path, err := exec.LookPath("pwsh"); err == nil {
		return path, nil
	}
	if runtime.GOOS == "windows" {
		if path, err := exec.LookPath("powershell.exe"); err == nil {
			return path, nil
		}
	}
	return "", &Error{Code: "prerequisite_missing"}
}

func run(ctx context.Context, request Request, find func() (string, error), start func(context.Context, *exec.Cmd) (bool, error)) (result Result, err error) {
	if len(request.Script) == 0 || len(request.Script) > maxScriptBytes || len(request.Assets) > 8 ||
		request.Timeout < 0 || request.Timeout > MaxTimeout {
		return result, &Error{Code: "invalid_request"}
	}
	timeout := request.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	op, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	total := len(request.Script)
	names := map[string]bool{"host.ps1": true, "entry.ps1": true, "options.json": true}
	for name, data := range request.Assets {
		lower := strings.ToLower(name)
		device := strings.SplitN(lower, ".", 2)[0]
		if !assetName.MatchString(name) || names[lower] || strings.HasSuffix(name, ".") ||
			device == "con" || device == "prn" || device == "aux" || device == "nul" ||
			regexp.MustCompile(`^(com|lpt)[0-9]$`).MatchString(device) || len(data) > maxScriptBytes {
			return result, &Error{Code: "invalid_request"}
		}
		names[lower] = true
		total += len(data)
	}
	if total > 8*maxScriptBytes {
		return result, &Error{Code: "invalid_request"}
	}
	options, marshalErr := json.Marshal(request.Options)
	if marshalErr != nil || len(options) > maxInputBytes {
		return result, &Error{Code: "invalid_request"}
	}
	if op.Err() != nil {
		return result, contextFailure(op)
	}
	path, findErr := find()
	if findErr != nil || !filepath.IsAbs(path) {
		return result, &Error{Code: "prerequisite_missing"}
	}
	root, tempErr := privateDirectory()
	if tempErr != nil {
		return result, &Error{Code: "temporary_io"}
	}
	var created []string
	defer func() {
		failed := false
		for i := len(created) - 1; i >= 0; i-- {
			if removeErr := os.Remove(created[i]); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				failed = true
			}
		}
		if removeErr := os.Remove(root); removeErr != nil {
			failed = true
		}
		if failed {
			result.Output = nil
			err = &Error{Code: "cleanup_failed"}
		}
	}()
	files := map[string][]byte{"host.ps1": []byte(launcher), "entry.ps1": request.Script, "options.json": options}
	for name, data := range request.Assets {
		files[name] = data
	}
	for name, data := range files {
		if op.Err() != nil {
			return result, contextFailure(op)
		}
		filePath := filepath.Join(root, name)
		file, fileErr := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if fileErr != nil {
			return result, &Error{Code: "temporary_io"}
		}
		created = append(created, filePath)
		n, writeErr := file.Write(data)
		syncErr, closeErr := file.Sync(), file.Close()
		if writeErr != nil || n != len(data) || syncErr != nil || closeErr != nil {
			return result, &Error{Code: "temporary_io"}
		}
	}
	var overflow atomic.Bool
	onLimit := func() { overflow.Store(true); cancel() }
	stdout, stderr := &limitedOutput{limit: MaxOutputBytes, exceeded: onLimit}, &limitedOutput{limit: MaxOutputBytes, exceeded: onLimit}
	cmd := exec.Command(path, "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
		"-File", filepath.Join(root, "host.ps1"), "-OptionsPath", filepath.Join(root, "options.json"))
	cmd.Dir = root
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.WaitDelay = time.Second
	result.Started, err = start(op, cmd)
	switch {
	case overflow.Load():
		return result, &Error{Code: "output_limit"}
	case op.Err() != nil:
		return result, contextFailure(op)
	case err != nil:
		return result, &Error{Code: "execution_failed"}
	default:
		result.Output = append([]byte(nil), stdout.buffer.Bytes()...)
		return result, nil
	}
}

func contextFailure(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &Error{Code: "timeout"}
	}
	return &Error{Code: "canceled"}
}

type limitedOutput struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	limit    int
	exceeded func()
}

func (b *limitedOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) > b.limit-b.buffer.Len() {
		b.exceeded()
		return 0, io.ErrShortWrite
	}
	return b.buffer.Write(p)
}

func execute(ctx context.Context, cmd *exec.Cmd) (bool, error) {
	tree, err := prepareTree(cmd)
	if err != nil {
		return false, err
	}
	defer tree.close()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if err := cmd.Start(); err != nil {
		return false, err
	}
	if err := tree.attach(cmd.Process); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return true, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return true, err
	case <-ctx.Done():
		if err := tree.stop(); err != nil {
			_ = cmd.Process.Kill()
		}
		<-done
		return true, ctx.Err()
	}
}
