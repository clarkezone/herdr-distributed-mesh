//go:build windows

package setup

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestNativeSetupWithShortOutputPath(t *testing.T) {
	o := nativeOptions(t)
	parent, err := windows.UTF16PtrFromString(filepath.Dir(o.OutputDirectory))
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]uint16, 32768)
	n, err := windows.GetShortPathName(parent, &buffer[0], uint32(len(buffer)))
	if err != nil || n >= uint32(len(buffer)) {
		t.Fatalf("short path: length=%d err=%v", n, err)
	}
	short := windows.UTF16ToString(buffer[:n])
	if strings.EqualFold(short, filepath.Dir(o.OutputDirectory)) {
		t.Skip("filesystem does not generate short names")
	}
	o.OutputDirectory = filepath.Join(short, filepath.Base(o.OutputDirectory))
	report, err := runNative(context.Background(), o, testAPIToken, apiClientFor(func(*http.Request) (*http.Response, error) {
		return apiReply(http.StatusOK, testPolicy, `"etag-1"`), nil
	}))
	if err != nil || filepath.Dir(report.PolicyBackup) != o.OutputDirectory {
		t.Fatalf("short path changed: %+v %v", report, err)
	}
}
