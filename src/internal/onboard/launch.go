package onboard

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func startDaemon(executable, dir string, environment []string) error {
	if err := supportedPlatform(); err != nil {
		return err
	}
	if !filepath.IsAbs(executable) || !filepath.IsAbs(dir) ||
		strings.ContainsAny(executable+dir, "\"\x00\r\n") {
		return errors.New("background launch requires absolute, unambiguous executable and state paths")
	}
	log, err := os.OpenFile(filepath.Join(dir, "daemon.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer log.Close()
	cmd := exec.Command(executable, "managed-run", "--state-dir", dir)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = dir, environment, log, log
	configureDaemonCommand(cmd)
	if err := cmd.Start(); err != nil {
		return daemonLaunchError(err)
	}
	return cmd.Process.Release()
}
