//go:build linux || darwin

package setup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSetupEmbeddedWorkflowWithLinkedTempRoot(t *testing.T) {
	requirePowerShell(t)
	o := testOptions(t)
	root := filepath.Dir(o.OutputDirectory)
	link := filepath.Join(root, "linked")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", link)
	testEmbeddedWorkflow(t, func(path string) string { return path })
}

func TestSetupCreatesPrivateUnixArtifacts(t *testing.T) {
	requirePowerShell(t)
	o := testOptions(t)
	o.Apply = true
	if _, err := run(context.Background(), o, mockedSetupHost); err != nil {
		t.Fatal(err)
	}
	err := filepath.WalkDir(o.OutputDirectory, func(path string, entry os.DirEntry, err error) error {
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
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSetupRejectsLinkedOutputDirectory(t *testing.T) {
	requirePowerShell(t)
	o := testOptions(t)
	root := filepath.Dir(o.OutputDirectory)
	link := filepath.Join(root, "linked")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	o.OutputDirectory = link
	_, err := run(context.Background(), o, mockedSetupHost)
	var setupError *Error
	if !errors.As(err, &setupError) || setupError.Code != "output_unavailable" {
		t.Fatalf("linked output was not rejected before API access: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "output")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("rejected output was created")
	}
}

func TestSetupRejectsNonPrivateOutput(t *testing.T) {
	requirePowerShell(t)
	o := testOptions(t)
	if err := os.Mkdir(o.OutputDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(o.OutputDirectory, 0755); err != nil {
		t.Fatal(err)
	}
	_, err := run(context.Background(), o, mockedSetupHost)
	var setupError *Error
	if !errors.As(err, &setupError) || setupError.Code != "output_unavailable" {
		t.Fatalf("non-private output was not rejected before API access: %v", err)
	}
}
