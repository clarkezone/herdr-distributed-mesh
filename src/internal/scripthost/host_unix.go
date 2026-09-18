//go:build linux || darwin

package scripthost

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func privateDirectory() (string, error) { return os.MkdirTemp("", "herdr-mesh-script-") }

type processTree struct{ pid int }

func prepareTree(cmd *exec.Cmd) (*processTree, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return &processTree{}, nil
}
func (t *processTree) attach(process *os.Process) error { t.pid = process.Pid; return nil }
func (t *processTree) stop() error {
	if t.pid == 0 {
		return nil
	}
	err := syscall.Kill(-t.pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
func (t *processTree) close() { _ = t.stop() }
