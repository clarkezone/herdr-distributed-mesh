package setup

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestSetupEmbeddedWorkflowWithShortOutputPath(t *testing.T) {
	testEmbeddedWorkflow(t, func(path string) string {
		parent, err := windows.UTF16PtrFromString(filepath.Dir(path))
		if err != nil {
			t.Fatal(err)
		}
		buffer := make([]uint16, 32768)
		n, err := windows.GetShortPathName(parent, &buffer[0], uint32(len(buffer)))
		if err != nil || n >= uint32(len(buffer)) {
			t.Fatalf("short path: length=%d err=%v", n, err)
		}
		short := windows.UTF16ToString(buffer[:n])
		if strings.EqualFold(short, filepath.Dir(path)) {
			t.Skip("filesystem does not generate short names")
		}
		return filepath.Join(short, filepath.Base(path))
	})
}
