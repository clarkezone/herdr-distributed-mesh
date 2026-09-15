//go:build windows

package herdr

import (
	"context"
	"errors"
	"net"
	"strings"

	winio "github.com/tailscale/go-winio"
)

func localAddress(path string) (string, error) {
	const prefix = `\\.\pipe\`
	if path == "" || strings.ContainsAny(path, "/\x00\r\n") {
		return "", errors.New("herdr: an explicit local socket path is required")
	}
	if strings.HasPrefix(strings.ToLower(path), prefix) {
		if len(path) == len(prefix) {
			return "", errors.New("herdr: a local pipe name is required")
		}
		return prefix + path[len(prefix):], nil
	}
	if len(path) < 3 || !((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) ||
		path[1] != ':' || path[2] != '\\' {
		return "", errors.New("herdr: socket path must be a local absolute marker or local pipe")
	}
	// Herdr uses the entire marker path as the pipe name, including the drive,
	// colon, and directory separators. Do not clean, hash, or replace them.
	return prefix + path, nil
}

func dialLocal(ctx context.Context, address string) (net.Conn, error) {
	return winio.DialPipeContext(ctx, address)
}
