//go:build windows

package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

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

func privateAccessRules(t *testing.T, dacl *windows.ACL, user *windows.SID) bool {
	t.Helper()
	if dacl == nil || dacl.AceCount != 2 {
		return false
	}
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	requireOK(t, err)
	const fullControl = 0x1f01ff // Windows FILE_ALL_ACCESS (SDDL FA).
	var sawUser, sawSystem bool
	for i := uint32(0); i < uint32(dacl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		requireOK(t, windows.GetAce(dacl, i, &ace))
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE || ace.Mask != fullControl {
			return false
		}
		// Compare SID identities, not SDDL aliases such as LA for Administrator.
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		switch {
		case sid.Equals(user) && !sawUser:
			sawUser = true
		case sid.Equals(system) && !sawSystem:
			sawSystem = true
		default:
			return false
		}
	}
	return sawUser && sawSystem
}

func TestWindowsPrivateAccessRulesCompareSIDIdentity(t *testing.T) {
	user, err := windows.StringToSid("LA")
	requireOK(t, err)
	administrator := user.String()
	for _, test := range []struct {
		name  string
		rules string
		valid bool
	}{
		{"administrator", "(A;;FA;;;" + administrator + ")(A;;FA;;;SY)", true},
		{"administrator-alias", "(A;;FA;;;LA)(A;;FA;;;SY)", true},
		{"numeric-system", "(A;;FA;;;S-1-5-18)(A;;FA;;;" + administrator + ")", true},
		{"administrators-group", "(A;;FA;;;BA)(A;;FA;;;SY)", false},
		{"everyone", "(A;;FA;;;WD)(A;;FA;;;SY)", false},
		{"other-user", "(A;;FA;;;S-1-5-21-1-2-3-501)(A;;FA;;;SY)", false},
		{"duplicate-user", "(A;;FA;;;" + administrator + ")(A;;FA;;;" + administrator + ")", false},
		{"deny", "(D;;FA;;;" + administrator + ")(A;;FA;;;SY)", false},
		{"read-only", "(A;;FR;;;" + administrator + ")(A;;FA;;;SY)", false},
		{"missing-system", "(A;;FA;;;" + administrator + ")", false},
		{"extra-trustee", "(A;;FA;;;" + administrator + ")(A;;FA;;;SY)(A;;FA;;;WD)", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			sd, err := windows.SecurityDescriptorFromString("D:P" + test.rules)
			requireOK(t, err)
			if test.name == "administrator" {
				t.Logf("serialized administrator descriptor: %s", sd.String())
			}
			dacl, _, err := sd.DACL()
			requireOK(t, err)
			if privateAccessRules(t, dacl, user) != test.valid {
				t.Fatalf("incorrect private DACL validation: %s", sd.String())
			}
		})
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
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	requireOK(t, err)
	if !privateAccessRules(t, dacl, user.User.Sid) {
		t.Fatalf("expected exactly current-user and SYSTEM full-control ACEs: %s", sd.String())
	}
	return sd.String()
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
