//go:build windows

package onboard

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

const teardownTestDir = `C:\Users\Someone\AppData\Roaming\herdr-mesh\managed`
const teardownOldExe = `C:\Old Mesh Install\herdr-mesh.exe`

func teardownTestCommand(t *testing.T, exe, dir string) string {
	t.Helper()
	command, err := loginCommand(exe, dir)
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func TestUnregisterOwnedManagedLogin(t *testing.T) {
	owned := teardownTestCommand(t, teardownOldExe, teardownTestDir)
	failure := errors.New("registry unavailable")
	for _, tt := range []struct {
		name, value string
		exists      bool
		readErr     error
		removeErr   error
		wantErr     bool
		wantDeletes int
	}{
		{name: "absent"},
		{name: "old executable need not exist", value: owned, exists: true, wantDeletes: 1},
		{name: "other state", value: teardownTestCommand(t, teardownOldExe, teardownTestDir+"-other"), exists: true, wantErr: true},
		{name: "other role", value: strings.Replace(owned, "managed-run", "server", 1), exists: true, wantErr: true},
		{name: "extra arguments", value: owned + " --other", exists: true, wantErr: true},
		{name: "empty existing entry", exists: true, wantErr: true},
		{name: "foreign entry", value: `"C:\Other\other.exe"`, exists: true, wantErr: true},
		{name: "noncanonical command formatting", value: strings.Replace(owned, " managed-run ", "  managed-run ", 1), exists: true, wantErr: true},
		{name: "read or registry type failure", readErr: failure, wantErr: true},
		{name: "delete failure", value: owned, exists: true, removeErr: failure, wantErr: true, wantDeletes: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			deletes := 0
			err := unregisterOwnedManagedLogin(teardownTestDir, func() (string, bool, error) {
				return tt.value, tt.exists, tt.readErr
			}, func() error {
				deletes++
				return tt.removeErr
			})
			if (err != nil) != tt.wantErr || deletes != tt.wantDeletes {
				t.Fatalf("err=%v deletes=%d; want error=%v deletes=%d", err, deletes, tt.wantErr, tt.wantDeletes)
			}
			if (tt.readErr != nil || tt.removeErr != nil) && !errors.Is(err, failure) {
				t.Fatalf("lost registry failure: %v", err)
			}
		})
	}
}

func TestUnregisterOwnedManagedLoginRechecksBeforeDeletion(t *testing.T) {
	owned := teardownTestCommand(t, teardownOldExe, teardownTestDir)
	for _, tt := range []struct {
		name, latest string
		exists       bool
		err          error
		wantErr      bool
	}{
		{name: "concurrently removed"},
		{name: "concurrently replaced", latest: "foreign.exe", exists: true, wantErr: true},
		{name: "recheck denied", err: windows.ERROR_ACCESS_DENIED, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reads := 0
			err := unregisterOwnedManagedLogin(teardownTestDir, func() (string, bool, error) {
				reads++
				if reads == 1 {
					return owned, true, nil
				}
				return tt.latest, tt.exists, tt.err
			}, func() error {
				t.Fatal("deleted changed or uninspectable entry")
				return nil
			})
			if (err != nil) != tt.wantErr || reads != 2 {
				t.Fatalf("err=%v reads=%d", err, reads)
			}
		})
	}
}

func TestManagedTeardownRejectsUnsafeStateDirBeforeSideEffects(t *testing.T) {
	for _, dir := range []string{"", "managed", `C:managed`, `C:\`, `\\server\share\managed`, `\\?\C:\managed`, `C:\managed\..\other`, `C:\managed.`, `C:\managed `, `C:\managed:stream`, "C:\\bad\x00dir", "C:\\bad\ndir", `C:\bad"dir`} {
		t.Run(fmt.Sprintf("%q", dir), func(t *testing.T) {
			if _, err := teardownStateDir(dir); err == nil {
				t.Fatal("accepted unsafe state directory")
			}
			err := unregisterOwnedManagedLogin(dir, func() (string, bool, error) {
				t.Fatal("read registry for unsafe path")
				return "", false, nil
			}, func() error {
				t.Fatal("removed registry for unsafe path")
				return nil
			})
			if err == nil {
				t.Fatal("unregister accepted unsafe path")
			}
			if err := stopLegacyDaemon(context.Background(), dir, legacyProcessSource{}); err == nil {
				t.Fatal("stop accepted unsafe path")
			}
		})
	}
}

func TestMatchesLegacyDaemonExactCommandAndImage(t *testing.T) {
	owned := teardownTestCommand(t, teardownOldExe, teardownTestDir)
	for _, tt := range []struct {
		name, command, image string
		want, wantErr        bool
	}{
		{name: "owned", command: owned, image: teardownOldExe, want: true},
		{name: "case insensitive image", command: owned, image: strings.ToUpper(teardownOldExe), want: true},
		{name: "case insensitive state", command: teardownTestCommand(t, teardownOldExe, strings.ToUpper(teardownTestDir)), image: teardownOldExe, want: true},
		{name: "trailing state slash", command: teardownTestCommand(t, teardownOldExe, teardownTestDir+`\`), image: teardownOldExe, want: true},
		{name: "different state", command: teardownTestCommand(t, teardownOldExe, teardownTestDir+"-other"), image: teardownOldExe},
		{name: "state is not a prefix", command: teardownTestCommand(t, teardownOldExe, teardownTestDir+`\nested`), image: teardownOldExe},
		{name: "image mismatch", command: owned, image: `C:\Other\herdr-mesh.exe`, wantErr: true},
		{name: "relative image", command: owned, image: "herdr-mesh.exe", wantErr: true},
		{name: "unrelated mesh role", command: strings.Replace(owned, "managed-run", "node", 1), image: teardownOldExe},
		{name: "Herdr child", command: `"C:\Tools\herdr.exe" run --prompt "managed-run --state-dir ` + teardownTestDir + `"`, image: `C:\Tools\herdr.exe`},
		{name: "provider child", command: `"C:\Tools\copilot.exe" --prompt "managed-run --state-dir ` + teardownTestDir + `"`, image: `C:\Tools\copilot.exe`},
		{name: "shell wrapper", command: `"C:\Windows\System32\cmd.exe" /c ` + owned, image: `C:\Windows\System32\cmd.exe`},
		{name: "additional argument", command: owned + " --verbose", image: teardownOldExe},
		{name: "duplicate flag", command: owned + ` --state-dir "C:\Other"`, image: teardownOldExe},
		{name: "relative argv zero", command: `herdr-mesh.exe managed-run --state-dir "` + teardownTestDir + `"`, image: teardownOldExe},
		{name: "embedded NUL", command: owned + "\x00", image: teardownOldExe, wantErr: true},
		{name: "empty command"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			match, err := matchesLegacyDaemon(tt.command, tt.image, teardownTestDir)
			if match != tt.want || (err != nil) != tt.wantErr {
				t.Fatalf("match=%v err=%v; want match=%v error=%v", match, err, tt.want, tt.wantErr)
			}
		})
	}
}

type fakeLegacyTeardownProcess struct {
	info          legacyProcessIdentity
	inspectErr    error
	stopErr       error
	closeErr      error
	inspect       func() (legacyProcessIdentity, error)
	stops, closes int
}

func (p *fakeLegacyTeardownProcess) identity() (legacyProcessIdentity, error) {
	if p.inspect != nil {
		return p.inspect()
	}
	return p.info, p.inspectErr
}

func (p *fakeLegacyTeardownProcess) stop(ctx context.Context) error {
	if p.closes != 0 {
		return errors.New("inspection handle was closed before stop")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Second {
		return errors.New("stop does not have a bounded deadline")
	}
	p.stops++
	return p.stopErr
}

func (p *fakeLegacyTeardownProcess) close() error {
	p.closes++
	return p.closeErr
}

func TestStopLegacyDaemonSelectionIsFailClosed(t *testing.T) {
	owned := legacyProcessIdentity{
		command: teardownTestCommand(t, teardownOldExe, teardownTestDir),
		image:   teardownOldExe, sameUser: true,
	}
	otherUser := owned
	otherUser.sameUser = false
	otherDir := owned
	otherDir.command = teardownTestCommand(t, teardownOldExe, teardownTestDir+"-other")
	for _, tt := range []struct {
		name      string
		processes []*fakeLegacyTeardownProcess
		openErr   error
		listErr   error
		wantErr   bool
		wantStops int
	}{
		{name: "no processes"},
		{name: "one exact process", processes: []*fakeLegacyTeardownProcess{{info: owned}}, wantStops: 1},
		{name: "unrelated state", processes: []*fakeLegacyTeardownProcess{{info: otherDir}}},
		{name: "other user even if elevated", processes: []*fakeLegacyTeardownProcess{{info: otherUser}}},
		{name: "ambiguous", processes: []*fakeLegacyTeardownProcess{{info: owned}, {info: owned}}, wantErr: true},
		{name: "inspection failure after match", processes: []*fakeLegacyTeardownProcess{{info: owned}, {inspectErr: windows.ERROR_ACCESS_DENIED}}, wantErr: true},
		{name: "process exited during inspect", processes: []*fakeLegacyTeardownProcess{{inspectErr: os.ErrProcessDone}}},
		{name: "process exited before open", processes: []*fakeLegacyTeardownProcess{{}}, openErr: os.ErrProcessDone},
		{name: "opening denied", processes: []*fakeLegacyTeardownProcess{{}}, openErr: windows.ERROR_ACCESS_DENIED, wantErr: true},
		{name: "enumeration failure", listErr: windows.ERROR_ACCESS_DENIED, wantErr: true},
		{name: "termination denied", processes: []*fakeLegacyTeardownProcess{{info: owned, stopErr: windows.ERROR_ACCESS_DENIED}}, wantErr: true, wantStops: 1},
		{name: "wait deadline exceeded", processes: []*fakeLegacyTeardownProcess{{info: owned, stopErr: context.DeadlineExceeded}}, wantErr: true, wantStops: 1},
		{name: "close failure", processes: []*fakeLegacyTeardownProcess{{info: otherDir, closeErr: windows.ERROR_INVALID_HANDLE}}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := stopLegacyDaemon(context.Background(), teardownTestDir, legacyProcessSource{
				list: func() ([]uint32, error) {
					var pids []uint32
					for i := range tt.processes {
						pids = append(pids, uint32(i+10))
					}
					return pids, tt.listErr
				},
				open: func(pid uint32) (legacyTeardownProcess, error) {
					if tt.openErr != nil {
						return nil, tt.openErr
					}
					return tt.processes[pid-10], nil
				},
			})
			stops := 0
			for _, p := range tt.processes {
				stops += p.stops
				if tt.openErr == nil && p.closes != 1 {
					t.Errorf("inspection handle closed %d times", p.closes)
				}
			}
			if (err != nil) != tt.wantErr || stops != tt.wantStops {
				t.Fatalf("err=%v stops=%d; want error=%v stops=%d", err, stops, tt.wantErr, tt.wantStops)
			}
		})
	}
}

func TestStopLegacyDaemonCancellationAndDuplicatePIDs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := stopLegacyDaemon(ctx, teardownTestDir, legacyProcessSource{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation not propagated: %v", err)
	}
	owned := legacyProcessIdentity{
		command: teardownTestCommand(t, teardownOldExe, teardownTestDir),
		image:   teardownOldExe, sameUser: true,
	}
	for _, duplicate := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		process := &fakeLegacyTeardownProcess{info: owned}
		err := stopLegacyDaemon(ctx, teardownTestDir, legacyProcessSource{
			list: func() ([]uint32, error) {
				if duplicate {
					return []uint32{10, 10}, nil
				}
				return []uint32{10}, nil
			},
			open: func(uint32) (legacyTeardownProcess, error) {
				if !duplicate {
					cancel()
				}
				return process, nil
			},
		})
		cancel()
		if err == nil || process.stops != 0 || process.closes != 1 {
			t.Fatalf("duplicate=%v err=%v stops=%d closes=%d", duplicate, err, process.stops, process.closes)
		}
	}
}

func TestStopLegacyDaemonRechecksHeldProcess(t *testing.T) {
	owned := legacyProcessIdentity{
		command: teardownTestCommand(t, teardownOldExe, teardownTestDir),
		image:   teardownOldExe, sameUser: true,
	}
	for _, scenario := range []string{"exited", "denied", "owner changed", "command changed", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			inspections := 0
			process := &fakeLegacyTeardownProcess{inspect: func() (legacyProcessIdentity, error) {
				inspections++
				if inspections == 1 {
					return owned, nil
				}
				changed := owned
				switch scenario {
				case "exited":
					return changed, os.ErrProcessDone
				case "denied":
					return changed, windows.ERROR_ACCESS_DENIED
				case "owner changed":
					changed.sameUser = false
				case "command changed":
					changed.command += " --different"
				case "cancelled":
					cancel()
				}
				return changed, nil
			}}
			err := stopLegacyDaemon(ctx, teardownTestDir, legacyProcessSource{
				list: func() ([]uint32, error) { return []uint32{10}, nil },
				open: func(uint32) (legacyTeardownProcess, error) { return process, nil },
			})
			if (err != nil) != (scenario != "exited") || process.stops != 0 || process.closes != 1 || inspections != 2 {
				t.Fatalf("err=%v stops=%d closes=%d inspections=%d", err, process.stops, process.closes, inspections)
			}
		})
	}
}

// Only a deliberately launched test child takes this branch, before testing's
// flag parser sees the production-shaped managed-run command line.
func init() {
	if os.Getenv("HERDR_MESH_TEARDOWN_TEST_HELPER") == "1" &&
		len(os.Args) == 4 && os.Args[1] == "managed-run" && os.Args[2] == "--state-dir" {
		fmt.Println("teardown-helper-ready")
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}
}

func TestLegacyDaemonDisposableHelperProcess(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "managed state with spaces")
	cmd := exec.Command(executable, "managed-run", "--state-dir", dir)
	cmd.Env = append(os.Environ(), "HERDR_MESH_TEARDOWN_TEST_HELPER=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		// This PID was created by this test, never discovered from the host.
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("disposable helper did not exit")
		}
	})
	ready := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(stdout).ReadString('\n')
		if err == nil && strings.TrimSpace(line) != "teardown-helper-ready" {
			err = fmt.Errorf("unexpected helper handshake %q", line)
		}
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("disposable helper did not become ready")
	}
	pid := uint32(cmd.Process.Pid)
	held, err := openLegacyProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	defer held.close()
	identity, err := held.identity()
	if err != nil {
		t.Fatal(err)
	}
	match, err := matchesLegacyDaemon(identity.command, identity.image, dir)
	if err != nil || !match || !identity.sameUser {
		t.Fatalf("helper identity mismatch: match=%v sameUser=%v err=%v", match, identity.sameUser, err)
	}
	// Intentionally inject the ONLY disposable PID. Never enumerate, inspect,
	// terminate, or modify the startup registry of the user's running mesh.
	err = stopLegacyDaemon(context.Background(), dir, legacyProcessSource{
		list: func() ([]uint32, error) { return []uint32{pid}, nil },
		open: openLegacyProcess,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := held.identity(); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("stop returned before the process handle signaled: %v", err)
	}
}
