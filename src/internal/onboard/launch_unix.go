//go:build linux || darwin

package onboard

import (
	"os/exec"
	"runtime"
	"syscall"
)

func supportedPlatform() error { return nil }

func configureDaemonCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

func daemonLaunchError(err error) error { return err }

func openBrowser(url string) error {
	name := "xdg-open"
	if runtime.GOOS == "darwin" {
		name = "open"
	}
	cmd := exec.Command(name, url)
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}
