//go:build windows

package meshlocal

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func assertPrivateDACL(t *testing.T, sd *windows.SECURITY_DESCRIPTOR) {
	t.Helper()
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := sd.DACL()
	if err != nil || dacl == nil || dacl.AceCount != 2 {
		t.Fatalf("expected only owner and SYSTEM: %v %s", err, sd.String())
	}
	var ownerSeen, systemSeen bool
	for i := uint32(0); i < uint32(dacl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, i, &ace); err != nil {
			t.Fatal(err)
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE || (ace.Mask != windows.GENERIC_ALL && ace.Mask != 0x1f01ff) {
			t.Fatalf("unexpected private ACE: %s", sd.String())
		}
		switch {
		case sid.Equals(user.User.Sid):
			ownerSeen = true
		case sid.Equals(system):
			systemSeen = true
		default:
			t.Fatalf("unexpected private trustee: %s", sd.String())
		}
	}
	if !ownerSeen || !systemSeen {
		t.Fatalf("missing owner or SYSTEM: %s", sd.String())
	}
}

func TestWindowsManagedPipeAndFilesHaveOwnerSystemACL(t *testing.T) {
	dir := canonicalTempDir(t)
	if err := Save(dir, Config{Version: 1, Name: "desktop", Tailnet: "example.test", Coordinator: true}); err != nil {
		t.Fatal(err)
	}
	if err := writeStatus(dir, Status{State: "login", AuthURL: "https://login.tailscale.com/a/private"}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, filepath.Join(dir, "config.json"), filepath.Join(dir, "status.json")} {
		sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
		if err != nil {
			t.Fatal(err)
		}
		assertPrivateDACL(t, sd)
	}
	listener, err := listenIPC(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() { connection, _ := listener.Accept(); accepted <- connection }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := dialIPC(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted
	if server == nil {
		t.Fatal("pipe accept failed")
	}
	defer server.Close()
	handle, ok := client.(interface{ Fd() uintptr })
	if !ok {
		t.Fatal("pipe lacks native handle")
	}
	sd, err := windows.GetSecurityInfo(windows.Handle(handle.Fd()), windows.SE_KERNEL_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	assertPrivateDACL(t, sd)
}
