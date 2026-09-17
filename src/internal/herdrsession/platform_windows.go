//go:build windows

package herdrsession

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
)

var markerPattern = regexp.MustCompile(`^[0-9]+:[0-9]+$`)

func socketIdentity(path string) (string, error) {
	if len(path) < 3 || path[1] != ':' || path[2] != '\\' || strings.ContainsAny(path, "\x00\r\n") {
		return "", ErrIdentity
	}
	file, err := os.Open(path)
	if err != nil {
		return "", ErrIdentity
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 257))
	if err != nil || len(data) > 256 || !markerPattern.MatchString(strings.TrimSpace(string(data))) {
		return "", ErrIdentity
	}
	// Protocol-18 Windows markers contain PID:start-nanoseconds; hashing
	// preserves incarnation evidence without publishing local process data.
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.TrimSpace(string(data))))), nil
}

func configureCommand(cmd *exec.Cmd) {
	cmd.Env = cleanEnvironment(os.Environ())
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
}
