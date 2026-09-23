//go:build windows

package onboard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Paths are compared lexically, not resolved through links or short-name
// aliases.
func teardownStateDir(dir string) (string, error) {
	if !filepath.IsAbs(dir) || !utf8.ValidString(dir) || len(dir) > 4096 ||
		strings.ContainsFunc(dir, unicode.IsControl) || strings.ContainsAny(dir, `"<>|?*`) ||
		strings.HasPrefix(dir, `\\`) || strings.HasPrefix(dir, "//") ||
		strings.Contains(strings.TrimPrefix(dir, filepath.VolumeName(dir)), ":") {
		return "", errors.New("managed teardown requires an absolute canonical local state directory")
	}
	for _, part := range strings.FieldsFunc(dir, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == ".." || (part != "." && strings.TrimRight(part, " .") != part) {
			return "", errors.New("managed teardown state directory contains traversal or an ambiguous path component")
		}
	}
	dir = filepath.Clean(dir)
	if filepath.Dir(dir) == dir {
		return "", errors.New("managed teardown state directory cannot be a filesystem root")
	}
	return dir, nil
}

func managedTeardownArgs(args []string, dir string) bool {
	if len(args) != 4 || args[1] != "managed-run" || args[2] != "--state-dir" ||
		!filepath.IsAbs(args[0]) || strings.ContainsAny(args[0], "\"\x00\r\n") {
		return false
	}
	stateDir, err := teardownStateDir(args[3])
	return err == nil && strings.EqualFold(stateDir, dir)
}

func matchesLegacyDaemon(command, image, dir string) (bool, error) {
	args, err := windows.DecomposeCommandLine(command)
	if err != nil {
		return false, fmt.Errorf("parse process command line: %w", err)
	}
	if !managedTeardownArgs(args, dir) {
		return false, nil
	}
	if !filepath.IsAbs(image) || !strings.EqualFold(filepath.Clean(image), filepath.Clean(args[0])) {
		return false, errors.New("managed-run command executable does not match its process image; refusing stop")
	}
	return true, nil
}

type legacyProcessIdentity struct {
	command, image string
	sameUser       bool
}

type legacyTeardownProcess interface {
	identity() (legacyProcessIdentity, error)
	stop(context.Context) error
	close() error
}

type legacyProcessSource struct {
	list func() ([]uint32, error)
	open func(uint32) (legacyTeardownProcess, error)
}

// StopLegacyDaemon force-stops only the current user's single, exact managed-run
// process for dir. Call the runtime stop RPC first; this fallback does not stop
// descendants (Herdr/provider jobs) and never uses process-name termination.
// Discovery considers only the shipped/current executable basenames. An old
// renamed executable matching neither basename is deliberately not inspected.
// No match is a no-op, not proof of shutdown: callers must verify IsRunning
// before claiming success or purging state. Candidate inspection failures error.
func StopLegacyDaemon(ctx context.Context, dir string) error {
	return stopLegacyDaemon(ctx, dir, legacyProcessSource{
		list: func() ([]uint32, error) {
			executable, err := os.Executable()
			if err != nil {
				return nil, err
			}
			return legacyCandidatePIDs(dir, executable, snapshotLegacyProcesses)
		},
		open: openLegacyProcess,
	})
}

func stopLegacyDaemon(ctx context.Context, dir string, source legacyProcessSource) (result error) {
	dir, err := teardownStateDir(dir)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pids, err := source.list()
	if err != nil {
		return fmt.Errorf("enumerate legacy managed daemon candidates: %w", err)
	}
	var selected legacyTeardownProcess
	seen := make(map[uint32]bool, len(pids))
	for _, pid := range pids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if seen[pid] {
			return errors.New("process enumeration returned a duplicate PID; refusing stop")
		}
		seen[pid] = true
		process, err := source.open(pid)
		if errors.Is(err, os.ErrProcessDone) {
			continue
		}
		if err != nil {
			return fmt.Errorf("open user process %d for inspection: %w", pid, err)
		}
		identity, inspectErr := process.identity()
		match := false
		if inspectErr == nil && identity.sameUser {
			match, inspectErr = matchesLegacyDaemon(identity.command, identity.image, dir)
		}
		if inspectErr != nil || !match {
			closeErr := process.close()
			if errors.Is(inspectErr, os.ErrProcessDone) {
				inspectErr = nil
			}
			if err := errors.Join(inspectErr, closeErr); err != nil {
				return fmt.Errorf("inspect user process %d: %w", pid, err)
			}
			continue
		}
		// Keep the inspection handle alive through selection and termination.
		// Reopening an unreferenced PID after inspection would permit PID reuse.
		defer func() { result = errors.Join(result, process.close()) }()
		if selected != nil {
			return errors.New("multiple managed-run processes match this state directory; refusing to stop any")
		}
		selected = process
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if selected == nil {
		return nil
	}
	identity, err := selected.identity()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("recheck selected managed daemon: %w", err)
	}
	match, err := matchesLegacyDaemon(identity.command, identity.image, dir)
	if err != nil {
		return err
	}
	if !identity.sameUser || !match {
		return errors.New("selected managed daemon identity changed; refusing stop")
	}
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := stopCtx.Err(); err != nil {
		return err
	}
	if err := selected.stop(stopCtx); err != nil {
		return fmt.Errorf("stop legacy managed daemon: %w", err)
	}
	return nil
}

type legacyProcessEntry struct {
	pid  uint32
	name string
}

func legacyCandidateNames(dir, executable string) ([]string, error) {
	if _, err := teardownStateDir(dir); err != nil {
		return nil, err
	}
	executable = filepath.Clean(executable)
	if !filepath.IsAbs(executable) || filepath.Dir(executable) == executable ||
		strings.ContainsAny(executable, "\"\x00\r\n") {
		return nil, errors.New("legacy daemon discovery requires an absolute current executable path")
	}
	names := []string{"herdr-mesh.exe"}
	if name := filepath.Base(executable); !strings.EqualFold(name, names[0]) {
		names = append(names, name)
	}
	return names, nil
}

func legacyCandidatePIDs(dir, executable string, snapshot func() ([]legacyProcessEntry, error)) ([]uint32, error) {
	names, err := legacyCandidateNames(dir, executable)
	if err != nil {
		return nil, err
	}
	entries, err := snapshot()
	if err != nil {
		return nil, err
	}
	var pids []uint32
	for _, entry := range entries {
		for _, name := range names {
			if strings.EqualFold(entry.name, name) {
				pids = append(pids, entry.pid)
				break
			}
		}
	}
	return pids, nil
}

// Toolhelp reads only PID/image names here. Owner, command line and image path
// are queried on held process handles only AFTER bounded candidate narrowing.
func snapshotLegacyProcesses() (entries []legacyProcessEntry, result error) {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return nil, err
	}
	defer func() { result = errors.Join(result, windows.CloseHandle(snapshot)) }()
	entry := windows.ProcessEntry32{Size: uint32(unsafe.Sizeof(windows.ProcessEntry32{}))}
	for err := windows.Process32First(snapshot, &entry); ; err = windows.Process32Next(snapshot, &entry) {
		if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, legacyProcessEntry{pid: entry.ProcessID, name: windows.UTF16ToString(entry.ExeFile[:])})
	}
}

type nativeLegacyProcess struct {
	handle windows.Handle
	pid    uint32
}

func openLegacyProcess(pid uint32) (legacyTeardownProcess, error) {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, pid)
	if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
		return nil, os.ErrProcessDone
	}
	if err != nil {
		return nil, err
	}
	return &nativeLegacyProcess{handle: handle, pid: pid}, nil
}

func (p *nativeLegacyProcess) close() error { return windows.CloseHandle(p.handle) }

func legacyProcessExited(handle windows.Handle) (bool, error) {
	event, err := windows.WaitForSingleObject(handle, 0)
	if err != nil {
		return false, err
	}
	switch event {
	case windows.WAIT_OBJECT_0:
		return true, nil
	case uint32(windows.WAIT_TIMEOUT):
		return false, nil
	default:
		return false, fmt.Errorf("unexpected process wait result %#x", event)
	}
}

func (p *nativeLegacyProcess) identity() (identity legacyProcessIdentity, result error) {
	exited, err := legacyProcessExited(p.handle)
	if err != nil {
		return identity, err
	}
	if exited {
		return identity, os.ErrProcessDone
	}
	// A process may exit between any two queries; only a signaled handle
	// establishes disappearance, rather than treating access denial as absence.
	defer func() {
		if result != nil {
			exited, err := legacyProcessExited(p.handle)
			if err == nil && exited {
				result = os.ErrProcessDone
			} else {
				result = errors.Join(result, err)
			}
		}
	}()
	var token windows.Token
	if err := windows.OpenProcessToken(p.handle, windows.TOKEN_QUERY, &token); err != nil {
		return identity, err
	}
	defer func() { result = errors.Join(result, token.Close()) }()
	owner, err := token.GetTokenUser()
	if err != nil {
		return identity, err
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return identity, err
	}
	identity.sameUser = user.User.Sid.Equals(owner.User.Sid)
	if !identity.sameUser {
		return identity, nil
	}
	image := make([]uint16, 32768)
	size := uint32(len(image))
	if err := windows.QueryFullProcessImageName(p.handle, 0, &image[0], &size); err != nil {
		return identity, err
	}
	identity.image = windows.UTF16ToString(image[:size])
	identity.command, err = legacyProcessCommandLine(p.handle)
	return identity, err
}

func legacyProcessCommandLine(handle windows.Handle) (string, error) {
	// ProcessCommandLineInformation returns a local UNICODE_STRING plus its
	// UTF-16 data, avoiding architecture-specific remote PEB reads.
	var size uint32
	err := windows.NtQueryInformationProcess(handle, windows.ProcessCommandLineInformation, nil, 0, &size)
	if err != nil && !errors.Is(err, windows.STATUS_INFO_LENGTH_MISMATCH) && !errors.Is(err, windows.STATUS_BUFFER_TOO_SMALL) {
		return "", err
	}
	headerSize := uint32(unsafe.Sizeof(windows.NTUnicodeString{}))
	if size < headerSize || size > 1<<20 {
		return "", errors.New("invalid process command-line buffer size")
	}
	buffer := make([]byte, size)
	var returned uint32
	if err := windows.NtQueryInformationProcess(handle, windows.ProcessCommandLineInformation, unsafe.Pointer(&buffer[0]), size, &returned); err != nil {
		return "", err
	}
	if returned < headerSize || returned > size {
		return "", errors.New("invalid process command-line response size")
	}
	value := (*windows.NTUnicodeString)(unsafe.Pointer(&buffer[0]))
	start := uintptr(unsafe.Pointer(&buffer[0]))
	data := uintptr(unsafe.Pointer(value.Buffer))
	if value.Length%2 != 0 || value.Length > value.MaximumLength ||
		data < start+uintptr(headerSize) || data > start+uintptr(returned) ||
		uintptr(value.Length) > start+uintptr(returned)-data {
		return "", errors.New("invalid process command-line string")
	}
	// Derive the slice from the allocation, not the returned integer address.
	offset := int(data - start)
	if value.Length == 0 {
		return "", nil
	}
	text := unsafe.Slice((*uint16)(unsafe.Pointer(&buffer[offset])), int(value.Length)/2)
	for _, ch := range text {
		if ch == 0 {
			return "", errors.New("process command line contains an embedded NUL")
		}
	}
	return windows.UTF16ToString(text), nil
}

func (p *nativeLegacyProcess) stop(ctx context.Context) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	exited, err := legacyProcessExited(p.handle)
	if err != nil || exited {
		return err
	}
	// The original handle fences PID reuse. Compare creation times as an
	// additional guard before using the termination-capable handle.
	handle, err := windows.OpenProcess(windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, p.pid)
	if err != nil {
		exited, waitErr := legacyProcessExited(p.handle)
		if waitErr == nil && exited {
			return nil
		}
		return errors.Join(err, waitErr)
	}
	defer func() { result = errors.Join(result, windows.CloseHandle(handle)) }()
	var original, reopened, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(p.handle, &original, &exit, &kernel, &user); err != nil {
		return err
	}
	if err := windows.GetProcessTimes(handle, &reopened, &exit, &kernel, &user); err != nil {
		return err
	}
	if original != reopened {
		return errors.New("process identity changed; refusing stop")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := windows.TerminateProcess(handle, 1); err != nil {
		exited, waitErr := legacyProcessExited(handle)
		if waitErr == nil && exited {
			return nil
		}
		return errors.Join(err, waitErr)
	}
	for {
		exited, err := legacyProcessExited(handle)
		if err != nil || exited {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for terminated process to signal: %w", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
