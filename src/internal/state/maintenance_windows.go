//go:build windows

package state

import (
	"os"

	"golang.org/x/sys/windows"
)

func maintenanceOrdinary(path string, info os.FileInfo) bool {
	if info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	ptr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return false
	}
	attributes, err := windows.GetFileAttributes(ptr)
	return err == nil && attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT == 0
}

func maintenanceSingleLink(file *os.File, _ os.FileInfo) bool {
	var info windows.ByHandleFileInformation
	return windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &info) == nil && info.NumberOfLinks == 1
}

// Windows files are FlushFileBuffers-synced individually. Directory handles do
// not support Go's File.Sync; completion is a separate flushed marker.
func maintenanceSyncDirectory(string) error { return nil }
