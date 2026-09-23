//go:build !windows

package herdrsession

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

func socketIdentity(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", ErrIdentity
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		return "", ErrIdentity
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", ErrIdentity
	}
	identity := fmt.Sprintf("%d:%d:%d", stat.Dev, stat.Ino, info.ModTime().UnixNano())
	return fmt.Sprintf("%x", sha256.Sum256([]byte(identity))), nil
}

func configureCommand(cmd *exec.Cmd) {
	cmd.Env = cleanEnvironment(os.Environ())
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
