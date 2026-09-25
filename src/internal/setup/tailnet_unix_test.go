//go:build linux || darwin

package setup

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeSetupCreatesPrivateUnixArtifacts(t *testing.T) {
	o := nativeOptions(t)
	client := apiClientFor(func(*http.Request) (*http.Response, error) {
		return apiReply(200, testPolicy, `"etag-1"`), nil
	})
	if _, err := runNative(context.Background(), o, testAPIToken, client); err != nil {
		t.Fatal(err)
	}
	if err := filepath.WalkDir(o.OutputDirectory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		want := os.FileMode(0600)
		if entry.IsDir() {
			want = 0700
		}
		if info.Mode().Perm() != want {
			t.Errorf("%s: permissions %o, want %o", filepath.Base(path), info.Mode().Perm(), want)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestNativeSetupRejectsNonPrivateOutput(t *testing.T) {
	o := nativeOptions(t)
	if err := os.Mkdir(o.OutputDirectory, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(o.OutputDirectory, 0755); err != nil {
		t.Fatal(err)
	}
	_, err := runNative(context.Background(), o, testAPIToken, apiClientFor(func(*http.Request) (*http.Response, error) {
		t.Fatal("non-private output caused API access")
		return nil, nil
	}))
	setupErrorCode(t, err, "output_unavailable", false)
}

func TestNativeSetupAllowsInstallerLinkedParent(t *testing.T) {
	o := nativeOptions(t)
	root := filepath.Dir(o.OutputDirectory)
	alias := filepath.Join(t.TempDir(), "linked-parent")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	o.OutputDirectory = filepath.Join(alias, "artifacts")
	report, err := runNative(context.Background(), o, testAPIToken, apiClientFor(func(*http.Request) (*http.Response, error) {
		return apiReply(http.StatusOK, testPolicy, `"etag-1"`), nil
	}))
	if err != nil || filepath.Dir(report.PolicyBackup) != o.OutputDirectory {
		t.Fatalf("installer-linked parent rejected: %+v %v", report, err)
	}
}
