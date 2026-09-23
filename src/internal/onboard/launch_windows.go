//go:build windows

package onboard

import (
	"errors"
	"os/exec"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/windows"
)

func supportedPlatform() error { return nil }

func configureDaemonCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow: true, CreationFlags: windows.DETACHED_PROCESS | windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_BREAKAWAY_FROM_JOB,
	}
}

func daemonLaunchError(err error) error {
	if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		return errors.New("Windows refused an independent background process; use a normal terminal outside a restrictive process job, and check executable permissions (no terminal-bound fallback was started)")
	}
	return err
}

func openBrowser(url string) error {
	system, err := windows.GetSystemDirectory()
	if err != nil {
		return err
	}
	cmd := exec.Command(filepath.Join(system, "rundll32.exe"), "url.dll,FileProtocolHandler", url)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}
