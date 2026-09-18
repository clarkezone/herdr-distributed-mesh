//go:build windows

package onboard

import (
	"strings"
	"testing"
)

func TestOwnedLoginCommandNeverOverwritesUnrelatedEntry(t *testing.T) {
	exe, dir := `C:\Program Files\Herdr\herdr-mesh.exe`, `C:\Users\Someone\AppData\Roaming\herdr-mesh\managed`
	command, err := loginCommand(exe, dir)
	if err != nil || !strings.HasPrefix(command, `"C:\Program Files\Herdr\herdr-mesh.exe" managed-run --state-dir `) {
		t.Fatalf("%s %v", command, err)
	}
	for _, current := range []string{"", command, `unrelated.exe`} {
		writes := 0
		err := RegisterOwnedLogin(exe, dir, func() (string, bool, error) { return current, current != "", nil }, func(value string) error {
			writes++
			if value != command {
				t.Fatal("changed command")
			}
			return nil
		})
		if current == "unrelated.exe" {
			if err == nil || writes != 0 {
				t.Fatal("unrelated entry overwritten")
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if current == command && writes != 0 {
			t.Fatal("matching entry unnecessarily overwritten")
		}
	}
}
