//go:build !windows

package projects

// Filesystem mounts are administered locally and are part of the trust boundary.
func localVolume(string) bool { return true }
