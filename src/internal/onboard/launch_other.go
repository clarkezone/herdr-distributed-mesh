//go:build !windows && !linux && !darwin

package onboard

import (
	"errors"
	"os/exec"
)

func supportedPlatform() error {
	return errors.New("managed background launch supports Windows, Linux and macOS; use the advanced foreground commands on this OS")
}
func configureDaemonCommand(*exec.Cmd)  {}
func daemonLaunchError(err error) error { return err }
func openBrowser(string) error          { return supportedPlatform() }
