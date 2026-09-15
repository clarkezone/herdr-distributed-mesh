//go:build windows

package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
	"google.golang.org/protobuf/proto"
)

func TestWindowsLiteralDatabasePathAndRepeatedClose(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "coordinator space #", "mesh state #100%25.db")
	first := openTestStore(t, path)
	requireOK(t, first.Bind(ctx, "stable", "instance"))
	view := node("stable", "instance")
	requireOK(t, first.SaveNode(ctx, view))
	var sequence int
	var name, databasePath string
	requireOK(t, first.conn.QueryRowContext(ctx, "PRAGMA database_list").Scan(&sequence, &name, &databasePath))
	canonicalPath, err := filepath.EvalSymlinks(path)
	requireOK(t, err)
	if name != "main" || !strings.EqualFold(filepath.Clean(databasePath), canonicalPath) {
		t.Fatalf("SQLite opened %q instead of literal path %q", databasePath, canonicalPath)
	}
	requireOK(t, first.Close())
	requireOK(t, first.Close())
	second := openTestStore(t, path)
	requireOK(t, first.Close())
	fleet, err := second.LoadFleet(ctx)
	requireOK(t, err)
	if len(fleet) != 1 || !proto.Equal(fleet[0], view) {
		t.Fatal("literal-path snapshot did not survive reopen")
	}
	third, err := Open(ctx, path, "server")
	if third != nil {
		requireOK(t, third.Close())
	}
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("closing the old store disturbed the new owner's lock: %v", err)
	}
}

func securityString(t *testing.T, path string) string {
	t.Helper()
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	requireOK(t, err)
	if sd == nil {
		t.Fatal("missing security descriptor")
	}
	dacl, _, err := sd.DACL()
	requireOK(t, err)
	if dacl == nil || dacl.AceCount != 2 {
		t.Fatalf("expected exactly current-user and SYSTEM ACEs: %s", sd.String())
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	requireOK(t, err)
	sddl := sd.String()
	for _, ace := range strings.Split(sddl, "(")[1:] {
		if !strings.HasPrefix(ace, "A;") ||
			(!strings.HasSuffix(ace, ";;;"+user.User.Sid.String()+")") && !strings.HasSuffix(ace, ";;;SY)")) {
			t.Fatalf("unexpected access rule: %s", sddl)
		}
		if !strings.Contains(ace, ";FA;") {
			t.Fatalf("expected full control: %s", sddl)
		}
	}
	return sddl
}

func TestWindowsPrivateDirectoryAndFiles(t *testing.T) {
	path := testPath(t)
	ancestor := filepath.Dir(filepath.Dir(path))
	before, err := windows.GetNamedSecurityInfo(ancestor, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	requireOK(t, err)
	initial := openTestStore(t, path)
	requireOK(t, initial.Close())
	// Existing protected files do not inherit the tightened parent DACL.
	wide, err := windows.SecurityDescriptorFromString("D:P(A;;FA;;;WD)")
	requireOK(t, err)
	acl, _, err := wide.DACL()
	requireOK(t, err)
	for _, file := range []string{path, path + ".lock"} {
		requireOK(t, windows.SetNamedSecurityInfo(file, windows.SE_FILE_OBJECT,
			windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
			nil, nil, acl, nil))
	}
	s := openTestStore(t, path)
	requireOK(t, s.Bind(context.Background(), "stable", "instance"))
	requireOK(t, s.SaveNode(context.Background(), node("stable", "instance")))
	directorySDDL := securityString(t, filepath.Dir(path))
	if !strings.HasPrefix(directorySDDL, "D:P") || strings.Count(directorySDDL, "OICI") != 2 {
		t.Fatalf("directory must have protected, inheritable private DACL: %s", directorySDDL)
	}
	for _, suffix := range []string{"", ".lock", "-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); err != nil {
			t.Fatalf("expected active SQLite file %s: %v", suffix, err)
		}
		securityString(t, path+suffix)
	}
	after, err := windows.GetNamedSecurityInfo(ancestor, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	requireOK(t, err)
	if before.String() != after.String() {
		t.Fatal("ancestor permissions were changed")
	}
}
