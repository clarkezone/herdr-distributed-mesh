//go:build !windows

package onboard

import "errors"

func supportedPlatform() error {
	return errors.New("guided persistent onboarding currently supports Windows per-user sign-in startup only; on this OS use the documented advanced foreground commands (no autostart was installed)")
}
func loginCommand(string, string) (string, error) { return "", supportedPlatform() }
func registerLogin(string, string) error          { return supportedPlatform() }
func startDaemon(string, string, []string) error  { return supportedPlatform() }
func openBrowser(string) error                    { return supportedPlatform() }
