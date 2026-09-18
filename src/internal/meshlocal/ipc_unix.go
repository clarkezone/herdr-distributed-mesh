//go:build !windows

package meshlocal

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"

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

func ipcSocketPath(dir string, create bool) (string, error) {
	path := filepath.Join(dir, "fleet.sock")
	limit := len(unix.RawSockaddrUnix{}.Path)
	if len(path) < limit {
		return path, nil
	}
	// macOS's canonical temporary/configuration roots can exceed SUN_PATH.
	// Keep the fallback short and private; never fall back to a TCP listener.
	base, err := filepath.EvalSymlinks("/tmp")
	if err != nil {
		return "", err
	}
	var parent unix.Stat_t
	if err := unix.Lstat(base, &parent); err != nil {
		return "", err
	}
	if parent.Mode&unix.S_IFMT != unix.S_IFDIR ||
		(parent.Uid != 0 && parent.Uid != uint32(os.Geteuid())) ||
		(parent.Mode&0022 != 0 && parent.Mode&unix.S_ISVTX == 0) {
		return "", errors.New("short managed IPC requires a trusted sticky or private temporary directory")
	}
	private := filepath.Join(base, "herdr-mesh-ipc-"+strconv.Itoa(os.Geteuid()))
	if create {
		if err := os.Mkdir(private, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}
	var info unix.Stat_t
	if err := unix.Lstat(private, &info); err != nil {
		return "", err
	}
	if info.Mode&unix.S_IFMT != unix.S_IFDIR || info.Mode&0777 != 0700 || info.Uid != uint32(os.Geteuid()) {
		return "", errors.New("short managed IPC directory must be an owner-held private directory, not a link")
	}
	digest := sha256.Sum256([]byte(dir))
	path = filepath.Join(private, fmt.Sprintf("%x.sock", digest[:16]))
	if len(path) >= limit {
		return "", errors.New("private managed IPC path exceeds the platform socket path limit")
	}
	return path, nil
}

func listenIPC(dir string) (net.Listener, error) {
	path, err := ipcSocketPath(dir, true)
	if err != nil {
		return nil, err
	}
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
	path, err := ipcSocketPath(dir, false)
	if err != nil {
		return nil, err
	}
	return (&net.Dialer{}).DialContext(ctx, "unix", path)
}
