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
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

const teardownTestDir = `C:\Users\Someone\AppData\Roaming\herdr-mesh\managed`
const teardownOldExe = `C:\Old Mesh Install\herdr-mesh.exe`

func teardownTestCommand(t *testing.T, exe, dir string) string {
	t.Helper()
	return windows.ComposeCommandLine([]string{exe, "managed-run", "--state-dir", dir})
}

func TestManagedTeardownRejectsUnsafeStateDirBeforeSideEffects(t *testing.T) {
	for _, dir := range []string{"", "managed", `C:managed`, `C:\`, `\\server\share\managed`, `\\?\C:\managed`, `C:\managed\..\other`, `C:\managed.`, `C:\managed `, `C:\managed:stream`, "C:\\bad\x00dir", "C:\\bad\ndir", `C:\bad"dir`} {
		t.Run(fmt.Sprintf("%q", dir), func(t *testing.T) {
			if _, err := teardownStateDir(dir); err == nil {
				t.Fatal("accepted unsafe state directory")
			}
			if _, err := legacyCandidatePIDs(dir, teardownOldExe, func() ([]legacyProcessEntry, error) {
				t.Fatal("enumerated processes for unsafe path")
				return nil, nil
			}); err == nil {
				t.Fatal("discovery accepted unsafe path")
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

func TestLegacyCandidateNamesOnlyShippedAndCurrentExecutable(t *testing.T) {
	current := `C:\New Install\mesh-renamed.exe`
	for _, tt := range []struct {
		name, executable string
		want             []string
		wantErr          bool
	}{
		{
			name: "renamed current executable", executable: current,
			want: []string{"herdr-mesh.exe", "mesh-renamed.exe"},
		},
		{
			name: "case insensitive duplicate names", executable: `C:\New\HERDR-MESH.EXE`,
			want: []string{"herdr-mesh.exe"},
		},
		{
			name: "relative current executable", executable: "mesh.exe", wantErr: true,
		},
		{
			name: "root is not current executable", executable: `C:\`, wantErr: true,
		},
		{
			name: "quoted current executable", executable: `"C:\New\mesh.exe"`, wantErr: true,
		},
		{
			name: "invalid current executable", executable: "C:\\New\\mesh\x00.exe", wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			names, err := legacyCandidateNames(teardownTestDir, tt.executable)
			if (err != nil) != tt.wantErr || !slices.Equal(names, tt.want) {
				t.Fatalf("names=%v err=%v; want names=%v error=%v", names, err, tt.want, tt.wantErr)
			}
			snapshots := 0
			_, err = legacyCandidatePIDs(teardownTestDir, tt.executable, func() ([]legacyProcessEntry, error) {
				snapshots++
				return nil, nil
			})
			if tt.wantErr && (err == nil || snapshots != 0) {
				t.Fatalf("enumerated despite invalid executable: snapshots=%d err=%v", snapshots, err)
			}
		})
	}
}

func TestLegacyCandidatePIDsNarrowBeforeOwnershipInspection(t *testing.T) {
	current := `C:\New Install\mesh-renamed.exe`
	entries := []legacyProcessEntry{
		{pid: 236, name: "System"},
		{pid: 1364, name: "arbitrary-protected-system-process.exe"},
		{pid: 3000, name: "herdr.exe"},
		{pid: 3001, name: "copilot.exe"},
		{pid: 3002, name: "herdr-mesh.exe.other"},
		{pid: 3003, name: ""},
		{pid: 4000, name: "HERDR-MESH.EXE"},
		{pid: 4001, name: "mesh-renamed.exe"},
		{pid: 4002, name: "MESH-RETIRED.EXE"},
	}
	pids, err := legacyCandidatePIDs(teardownTestDir, current, func() ([]legacyProcessEntry, error) {
		return entries, nil
	})
	want := []uint32{4000, 4001}
	if err != nil || !slices.Equal(pids, want) {
		t.Fatalf("pids=%v err=%v; want %v", pids, err, want)
	}
	_, err = legacyCandidatePIDs(teardownTestDir, current, func() ([]legacyProcessEntry, error) {
		return nil, windows.ERROR_ACCESS_DENIED
	})
	if !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("lost enumeration failure: %v", err)
	}
}

func TestStopLegacyDaemonBoundedCandidatesRemainFailClosed(t *testing.T) {
	owned := legacyProcessIdentity{
		command: teardownTestCommand(t, teardownOldExe, teardownTestDir),
		image:   teardownOldExe, sameUser: true,
	}
	for _, scenario := range []string{"unrelated inaccessible", "candidate open denied", "candidate owner unknown", "candidate image mismatch", "candidate other role", "candidate other user", "ambiguous"} {
		t.Run(scenario, func(t *testing.T) {
			selected := &fakeLegacyTeardownProcess{info: owned}
			candidate := &fakeLegacyTeardownProcess{info: owned}
			switch scenario {
			case "candidate owner unknown":
				candidate.inspectErr = windows.ERROR_ACCESS_DENIED
			case "candidate image mismatch":
				candidate.info.image = `C:\Other\herdr-mesh.exe`
			case "candidate other role":
				candidate.info.command = strings.Replace(owned.command, "managed-run", "server", 1)
			case "candidate other user":
				candidate.info.sameUser = false
			}
			entries := []legacyProcessEntry{
				{pid: 236, name: "System"},
				{pid: 1364, name: "arbitrary-protected-system-process.exe"},
				{pid: 1365, name: "mesh-retired.exe"},
				{pid: 1000, name: "herdr-mesh.exe"},
			}
			if scenario != "unrelated inaccessible" {
				entries = append(entries, legacyProcessEntry{pid: 2000, name: "HERDR-MESH.EXE"})
			}
			err := stopLegacyDaemon(context.Background(), teardownTestDir, legacyProcessSource{
				list: func() ([]uint32, error) {
					return legacyCandidatePIDs(teardownTestDir, teardownOldExe, func() ([]legacyProcessEntry, error) { return entries, nil })
				},
				open: func(pid uint32) (legacyTeardownProcess, error) {
					switch pid {
					case 1000:
						return selected, nil
					case 2000:
						if scenario == "candidate open denied" {
							return nil, windows.ERROR_ACCESS_DENIED
						}
						return candidate, nil
					default:
						t.Fatalf("attempted to inspect unrelated process %d", pid)
						return nil, windows.ERROR_ACCESS_DENIED
					}
				},
			})
			wantStop := scenario == "unrelated inaccessible" || scenario == "candidate other role" || scenario == "candidate other user"
			wantStops := 0
			if wantStop {
				wantStops = 1
			}
			if (err == nil) != wantStop || selected.stops != wantStops || selected.closes != 1 || candidate.stops != 0 {
				t.Fatalf("err=%v selected stops=%d closes=%d other stops=%d", err, selected.stops, selected.closes, candidate.stops)
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
	// or terminate the user's running mesh.
	err = stopLegacyDaemon(context.Background(), dir, legacyProcessSource{
		list: func() ([]uint32, error) {
			return legacyCandidatePIDs(dir, executable, func() ([]legacyProcessEntry, error) {
				return []legacyProcessEntry{{pid: pid, name: filepath.Base(executable)}}, nil
			})
		},
		open: func(candidate uint32) (legacyTeardownProcess, error) {
			if candidate != pid {
				t.Fatalf("attempted to open a process not owned by this test: %d", candidate)
			}
			return openLegacyProcess(candidate)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := held.identity(); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("stop returned before the process handle signaled: %v", err)
	}
}
