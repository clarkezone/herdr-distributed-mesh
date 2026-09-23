package scripthost

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func assertPrivate(t *testing.T, path string) {
	t.Helper()
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	control, _, err := sd.Control()
	if err != nil || control&windows.SE_DACL_PROTECTED == 0 {
		t.Fatal("temporary directory inherits permissions")
	}
}

type childPIDWriter struct{ pid chan uint32 }

func (w childPIDWriter) Write(p []byte) (int, error) {
	pid, err := strconv.ParseUint(strings.TrimSpace(string(p)), 10, 32)
	if err == nil {
		select {
		case w.pid <- uint32(pid):
		default:
		}
	}
	return len(p), nil
}

func TestHostCancellationStopsOnlyOwnedTree(t *testing.T) {
	outside := childCommand("hold")
	if err := outside.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = outside.Process.Kill(); _ = outside.Wait() }()
	otherHandle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(outside.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(otherHandle)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := childCommand("tree")
	pids := make(chan uint32, 1)
	command.Stdout = childPIDWriter{pids}
	done := make(chan error, 1)
	go func(cmd *exec.Cmd) { _, err := execute(ctx, cmd); done <- err }(command)
	var pid uint32
	select {
	case pid = <-pids:
	case err := <-done:
		t.Fatalf("tree exited before child: %v", err)
	case <-ctx.Done():
		t.Fatal("child did not start")
	}
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, pid)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(handle)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation hung")
	}
	if state, err := windows.WaitForSingleObject(handle, 3000); err != nil || state != windows.WAIT_OBJECT_0 {
		t.Fatal("owned child survived")
	}
	if state, err := windows.WaitForSingleObject(otherHandle, 0); err != nil || state != uint32(windows.WAIT_TIMEOUT) {
		t.Fatal("unrelated child was stopped")
	}
}
