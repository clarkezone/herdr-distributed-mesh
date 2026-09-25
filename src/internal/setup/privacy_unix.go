//go:build !windows

package setup

import (
	"errors"
	"os"
	"syscall"
)

func verifyPrivateDirectory(_ string, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0077 != 0 {
		return errors.New("output directory is not private")
	}
	return nil
}
