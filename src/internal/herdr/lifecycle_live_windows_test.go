//go:build windows

package herdr

import (
	"os/exec"
	"syscall"
)

func configureLifecycleLiveCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
}
