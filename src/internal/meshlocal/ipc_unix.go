//go:build !windows

package meshlocal

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func samePath(a, b string) bool { return a == b }

func replaceStatus(from, to string) error { return os.Rename(from, to) }

func openPrivateRead(path string) (*os.File, error) {
	handle, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(handle), path)
	var info unix.Stat_t
	if err := unix.Fstat(handle, &info); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if info.Nlink != 1 || info.Mode&0077 != 0 || info.Uid != uint32(os.Geteuid()) || info.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, errors.Join(errors.New("managed state must be a private owner-held regular file"), file.Close())
	}
	return file, nil
}

func listenIPC(dir string) (net.Listener, error) {
	path := filepath.Join(dir, "fleet.sock")
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("managed IPC path is not a socket")
		}
		// The caller already holds the managed runtime ownership lock.
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		return nil, errors.Join(err, listener.Close())
	}
	return listener, nil
}

func dialIPC(ctx context.Context, dir string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(dir, "fleet.sock"))
}
