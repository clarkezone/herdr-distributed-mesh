package state

import (
	"errors"
	"os"
)

// ProtectPrivatePath applies the journal's owner-only privacy policy to a
// managed runtime file or directory. Links and special files are rejected.
func ProtectPrivatePath(path string, directory bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !maintenanceOrdinary(path, info) || (directory && !info.IsDir()) || (!directory && !info.Mode().IsRegular()) {
		return errors.New("private state must be an ordinary file or directory")
	}
	return protectPath(path, directory)
}
