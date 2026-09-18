//go:build windows

package onboard

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

func supportedPlatform() error { return nil }

func loginCommand(executable, dir string) (string, error) {
	if !filepath.IsAbs(executable) || !filepath.IsAbs(dir) ||
		strings.ContainsAny(executable+dir, "\"\x00\r\n") {
		return "", errors.New("login command requires absolute, unambiguous executable and state paths")
	}
	// There are no embedded quotes (rejected above). Double trailing
	// backslashes so they cannot escape the fixed closing quote.
	quote := func(value string) string {
		trailing := len(value) - len(strings.TrimRight(value, `\`))
		return `"` + value + strings.Repeat(`\`, trailing) + `"`
	}
	command := quote(executable) + " managed-run --state-dir " + quote(dir)
	if len(command) > 260 {
		return "", errors.New("the Windows sign-in command exceeds the Run-entry limit; place herdr-mesh.exe in a shorter permanent path before retrying")
	}
	return command, nil
}

func registerLogin(executable, dir string) error {
	key, _, err := registry.CreateKey(registry.CURRENT_USER, `Software\Microsoft\Windows\CurrentVersion\Run`, registry.QUERY_VALUE|registry.SET_VALUE)
	if err != nil {
		return err
	}
	defer key.Close()
	return RegisterOwnedLogin(executable, dir, func() (string, bool, error) {
		value, kind, err := key.GetStringValue("HerdrMeshManaged")
		if errors.Is(err, registry.ErrNotExist) {
			return "", false, nil
		}
		if err != nil {
			return "", false, err
		}
		if kind != registry.SZ {
			return "", true, errors.New("existing startup entry is not a plain string; refusing replacement")
		}
		return value, true, nil
	}, func(value string) error { return key.SetStringValue("HerdrMeshManaged", value) })
}

func startDaemon(executable, dir string, environment []string) error {
	if _, err := loginCommand(executable, dir); err != nil {
		return err
	}
	log, err := os.OpenFile(filepath.Join(dir, "daemon.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer log.Close()
	cmd := exec.Command(executable, "managed-run", "--state-dir", dir)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = dir, environment, log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow: true, CreationFlags: windows.DETACHED_PROCESS | windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_BREAKAWAY_FROM_JOB,
	}
	if err := cmd.Start(); err != nil {
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
			return errors.New("Windows refused an independent background process; use a normal terminal outside a restrictive process job, and check executable permissions (no terminal-bound fallback was started)")
		}
		return err
	}
	return cmd.Process.Release()
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
