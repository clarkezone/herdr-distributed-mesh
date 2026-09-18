package meshlocal

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/protobuf/types/known/emptypb"
)

var ErrLegacyShutdown = errors.New("running daemon does not support cooperative shutdown")

type RuntimeControl struct {
	Nonce string
}

type ManagedIdentity struct {
	DeviceID string
	DNSName  string
	Tailnet  string
}

type DestroyState struct {
	Configuration Config
	Identity      ManagedIdentity
	RemovePolicy  bool
	DeviceRemoved bool
	PolicyRemoved bool
}

func LoadForCleanup(dir string) (Config, error) {
	config, err := Load(dir)
	if errors.Is(err, os.ErrNotExist) {
		record, recordErr := ReadDestroyState(dir)
		if recordErr == nil && record.Configuration.validate() == nil {
			return record.Configuration, nil
		}
	}
	return config, err
}

var deviceIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func writePrivateJSON(dir, name string, value any) (result error) {
	file, err := os.CreateTemp(dir, ".managed-write-*")
	if err != nil {
		return err
	}
	path := file.Name()
	defer func() {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}()
	if err := state.ProtectPrivatePath(path, false); err != nil {
		return errors.Join(err, file.Close())
	}
	err = json.NewEncoder(file).Encode(value)
	if err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return err
	}
	return replaceStatus(path, filepath.Join(dir, name))
}

func CheckNotDestroying(dir string) error {
	if _, err := os.Lstat(filepath.Join(dir, "destroy.json")); err == nil {
		return errors.New("managed installation is being destroyed; resume shutdown --destroy, not init/join")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func startShutdownMonitor(ctx context.Context, dir string, cancel context.CancelFunc) (func() error, error) {
	control := RuntimeControl{Nonce: rand.Text()}
	if err := writePrivateJSON(dir, "runtime-control.json", control); err != nil {
		return nil, err
	}
	child, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-child.Done():
				done <- nil
				return
			case <-ticker.C:
				var request RuntimeControl
				err := readJSON(filepath.Join(dir, "shutdown-request.json"), &request)
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				if err != nil {
					cancel()
					done <- fmt.Errorf("read managed shutdown request: %w", err)
					return
				}
				if request.Nonce == control.Nonce {
					cancel()
					done <- nil
					return
				}
			}
		}
	}()
	return func() error { stop(); return <-done }, nil
}

// Shutdown never signals an operating-system process or removes durable state.
// A nonce pins the request to the observed daemon, not a replacement.
func Shutdown(ctx context.Context, dir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := privateDir(dir, false)
	if err != nil {
		return err
	}
	running, err := IsRunning(root)
	if err != nil || !running {
		return err
	}
	var control RuntimeControl
	if err := readJSON(filepath.Join(root, "runtime-control.json"), &control); errors.Is(err, os.ErrNotExist) {
		return ErrLegacyShutdown
	} else if err != nil {
		return err
	}
	if len(control.Nonce) < 16 || len(control.Nonce) > 128 {
		return errors.New("invalid managed runtime control identity")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writePrivateJSON(root, "shutdown-request.json", control); err != nil {
		return err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		running, err := IsRunning(root)
		if err != nil {
			return err
		}
		if !running {
			return nil
		}
		var current RuntimeControl
		if err := readJSON(filepath.Join(root, "runtime-control.json"), &current); err != nil {
			return err
		}
		if current != control {
			return errors.New("managed daemon was replaced during shutdown; replacement was not targeted")
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("daemon shutdown not confirmed; state retained: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func RecordStopped(ctx context.Context, dir string) (result error) {
	root, err := privateDir(dir, false)
	if err != nil {
		return err
	}
	guard, err := state.AcquireRoleState(ctx, root, "client")
	if err != nil {
		return fmt.Errorf("daemon is active or ownership changed before shutdown could be recorded: %w", err)
	}
	defer func() { result = errors.Join(result, guard.Close()) }()
	status, err := ReadStatus(root)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writeStatus(root, Status{State: "stopped", DNSName: status.DNSName, Server: status.Server})
}

func RetainIdentity(dir string, identity ManagedIdentity) error {
	if !deviceIDPattern.MatchString(identity.DeviceID) || identity.DNSName == "" || len(identity.DNSName) > 253 ||
		len(identity.Tailnet) > 253 || strings.ContainsAny(identity.DNSName+identity.Tailnet, "\x00\r\n") {
		return errors.New("invalid managed network identity")
	}
	var existing ManagedIdentity
	err := readJSON(filepath.Join(dir, "mesh-identity.json"), &existing)
	if err == nil && existing.DeviceID != identity.DeviceID {
		return errors.New("managed Tailscale identity changed; refusing to replace retained teardown identity")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writePrivateJSON(dir, "mesh-identity.json", identity)
}

func ReadDestroyState(dir string) (DestroyState, error) {
	var value DestroyState
	err := readJSON(filepath.Join(dir, "destroy.json"), &value)
	if err == nil && !deviceIDPattern.MatchString(value.Identity.DeviceID) {
		err = errors.New("destroy record has no valid pinned device identity")
	}
	return value, err
}

func SaveDestroyState(dir string, value DestroyState) error {
	if !deviceIDPattern.MatchString(value.Identity.DeviceID) {
		return errors.New("destroy requires a pinned device identity")
	}
	return writePrivateJSON(dir, "destroy.json", value)
}

// ResolveManagedIdentity supports old installations via their exact retained
// mesh instance ID. It never selects a device by hostname.
func ResolveManagedIdentity(ctx context.Context, dir string) (ManagedIdentity, error) {
	root, err := privateDir(dir, false)
	if err != nil {
		return ManagedIdentity{}, err
	}
	if record, err := ReadDestroyState(root); err == nil {
		return record.Identity, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ManagedIdentity{}, err
	}
	var identity ManagedIdentity
	if err := readJSON(filepath.Join(root, "mesh-identity.json"), &identity); err == nil {
		if !deviceIDPattern.MatchString(identity.DeviceID) {
			return ManagedIdentity{}, errors.New("invalid retained managed device identity")
		}
		return identity, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ManagedIdentity{}, err
	}
	file, err := openPrivateRead(filepath.Join(root, "node", "instance-id"))
	if err != nil {
		return identity, fmt.Errorf("cannot identify legacy managed node; preserve state and use its original daemon: %w", err)
	}
	content, readErr := io.ReadAll(io.LimitReader(file, 129))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return identity, err
	}
	instanceID := strings.TrimSpace(string(content))
	if len(content) > 128 || !deviceIDPattern.MatchString(instanceID) {
		return identity, errors.New("invalid retained managed node instance ID")
	}
	conn, err := Dial(ctx, root)
	if err != nil {
		return identity, fmt.Errorf("legacy identity discovery requires its original running daemon; state preserved: %w", err)
	}
	defer conn.Close()
	nodes, err := pb.NewFleetClient(conn).ListNodes(ctx, &emptypb.Empty{})
	if err != nil {
		return identity, err
	}
	matches := 0
	for _, node := range nodes.GetNodes() {
		if node.GetInstanceId() == instanceID {
			matches++
			identity.DeviceID = node.TailscaleStableId
		}
	}
	if matches != 1 || !deviceIDPattern.MatchString(identity.DeviceID) {
		return identity, errors.New("no unique authenticated device matches the retained local node instance; refusing hostname-based cleanup")
	}
	status, err := ReadStatus(root)
	if err != nil {
		return identity, err
	}
	config, err := Load(root)
	if err != nil {
		return identity, err
	}
	identity.DNSName, identity.Tailnet = status.DNSName, config.Tailnet
	return identity, nil
}

// PurgeManaged removes only an explicitly confirmed managed root, with all
// runtime guards held. Configuration is deleted before releasing those guards.
func PurgeManaged(ctx context.Context, dir string) (result error) {
	root, err := privateDir(dir, false)
	if err != nil {
		return err
	}
	if _, err := LoadForCleanup(root); err != nil {
		return err
	}
	record, err := ReadDestroyState(root)
	if err != nil || !record.DeviceRemoved || (record.RemovePolicy && !record.PolicyRemoved) {
		return errors.New("local purge requires confirmed device removal and a durable destroy record")
	}
	var guards []*state.RoleStateLock
	defer func() {
		for _, guard := range guards {
			result = errors.Join(result, guard.Close())
		}
	}()
	for _, entry := range []struct{ path, role string }{{root, "client"}, {filepath.Join(root, "server"), "server"}, {filepath.Join(root, "node"), "node"}} {
		if _, err := os.Lstat(entry.path); errors.Is(err, os.ErrNotExist) && entry.path != root {
			continue
		}
		guard, err := state.AcquireRoleState(ctx, entry.path, entry.role)
		if err != nil {
			return fmt.Errorf("cannot purge active or invalid managed state: %w", err)
		}
		guards = append(guards, guard)
	}
	// Refuse aliases anywhere, even where RemoveAll would usually unlink them.
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil || !samePath(resolved, path) || entry.Type()&os.ModeSymlink != 0 {
			return errors.New("managed purge refuses linked or aliased descendants")
		}
		return nil
	}); err != nil {
		return err
	}
	// Locks and recovery metadata survive a partial purge.
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() == "role-state.lock" || path == filepath.Join(root, "config.json") || path == filepath.Join(root, "destroy.json") {
			return nil
		}
		return os.Remove(path)
	}); err != nil {
		return fmt.Errorf("managed purge incomplete; destroy record retained: %w", err)
	}
	if err := os.Remove(filepath.Join(root, "config.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, guard := range guards {
		if err := guard.Close(); err != nil {
			return err
		}
	}
	guards = nil
	var directories []string
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			directories = append(directories, path)
			return nil
		}
		if path == filepath.Join(root, "destroy.json") {
			return nil
		}
		return os.Remove(path)
	}); err != nil {
		return err
	}
	sort.Slice(directories, func(i, j int) bool { return len(directories[i]) > len(directories[j]) })
	for _, directory := range directories {
		if directory != root {
			if err := os.Remove(directory); err != nil {
				return err
			}
		}
	}
	if err := os.Remove(filepath.Join(root, "destroy.json")); err != nil {
		return err
	}
	return os.Remove(root)
}

func ReadPrivateArtifact(root, path string, limit int64) ([]byte, error) {
	canonical, err := privateDir(root, false)
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(canonical, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return nil, errors.New("cleanup artifact escapes managed state")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || !samePath(resolved, path) {
		return nil, errors.New("cleanup artifact is missing or aliased")
	}
	file, err := openPrivateRead(path)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, limit+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("cleanup artifact exceeds supported size")
	}
	return data, nil
}
