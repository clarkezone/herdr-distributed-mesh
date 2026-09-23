//go:build !windows

package onboard

import (
	"context"
	"errors"
)

// StopLegacyDaemon stops a managed daemon that predates the stop RPC.
func StopLegacyDaemon(context.Context, string) error {
	return errors.New("legacy managed daemon stopping is supported only on Windows")
}
