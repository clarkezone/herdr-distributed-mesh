//go:build !windows

package onboard

import (
	"context"
	"errors"
)

// UnregisterManagedLogin removes the selected managed daemon's sign-in entry.
func UnregisterManagedLogin(string) error {
	return errors.New("managed sign-in startup removal is supported only on Windows")
}

// StopLegacyDaemon stops a managed daemon that predates the stop RPC.
func StopLegacyDaemon(context.Context, string) error {
	return errors.New("legacy managed daemon stopping is supported only on Windows")
}
