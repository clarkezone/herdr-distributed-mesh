//go:build linux || darwin

package scripthost

import (
	"os"
	"testing"
)

func assertPrivate(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatal("temporary directory is not private")
	}
}
