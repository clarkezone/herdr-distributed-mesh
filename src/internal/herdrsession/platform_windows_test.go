//go:build windows

package herdrsession

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsMarkerIncarnation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "herdr.sock")
	if err := os.WriteFile(path, []byte("1234:9000\n"), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := socketIdentity(path)
	if err != nil || len(first) != 64 {
		t.Fatalf("identity = %q, %v", first, err)
	}
	if err := os.WriteFile(path, []byte("1234:9001\n"), 0600); err != nil {
		t.Fatal(err)
	}
	second, err := socketIdentity(path)
	if err != nil || first == second {
		t.Fatalf("replacement not detected: %v", err)
	}
	for _, contents := range []string{"", "malformed", "1234", "1234:abc"} {
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := socketIdentity(path); err == nil {
			t.Fatalf("accepted invalid marker %q", contents)
		}
	}
}
