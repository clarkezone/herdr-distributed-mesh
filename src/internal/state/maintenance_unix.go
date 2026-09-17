//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package state

import (
	"errors"
	"os"
	"syscall"
)

func maintenanceOrdinary(_ string, info os.FileInfo) bool { return info.Mode()&os.ModeSymlink == 0 }

func maintenanceSingleLink(_ *os.File, info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}

func maintenanceSyncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
