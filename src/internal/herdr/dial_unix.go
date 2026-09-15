//go:build !windows

package herdr

import (
	"context"
	"errors"
	"net"
	"strings"
)

func localAddress(path string) (string, error) {
	if path == "" || strings.ContainsAny(path, "\x00\r\n") || strings.HasPrefix(path, `\\`) || strings.HasPrefix(path, "//") {
		return "", errors.New("herdr: an explicit local Unix socket path is required")
	}
	return path, nil
}

func dialLocal(ctx context.Context, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "unix", address)
}
