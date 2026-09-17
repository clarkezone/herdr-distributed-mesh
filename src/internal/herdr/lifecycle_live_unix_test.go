//go:build !windows

package herdr

import "os/exec"

func configureLifecycleLiveCommand(*exec.Cmd) {}
