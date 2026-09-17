package herdrsession

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeRunner struct {
	running  bool
	starts   int
	delay    bool
	protocol int
	output   []byte
	runErr   error
	done     chan error
	commands [][]string
	started  chan struct{}
}

func (f *fakeRunner) run(ctx context.Context, args ...string) ([]byte, error) {
	f.commands = append(f.commands, append([]string{}, args...))
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.runErr != nil {
		return nil, f.runErr
	}
	if f.output != nil {
		return f.output, nil
	}
	if args[0] == "session" {
		return []byte(`{"sessions":[]}`), nil
	}
	name := "default"
	if args[0] == "--session" {
		name = args[1]
	}
	protocol := f.protocol
	if protocol == 0 {
		protocol = 18
	}
	status := "not_running"
	if f.running {
		status = "running"
	}
	return json.Marshal(map[string]any{"status": status, "running": f.running,
		"protocol": protocol, "compatible": protocol == 18, "socket": "local-marker", "session": name})
}

func (f *fakeRunner) start(args ...string) (<-chan error, error) {
	f.starts++
	if f.started != nil {
		close(f.started)
		f.started = nil
	}
	f.commands = append(f.commands, append([]string{}, args...))
	if !f.delay {
		f.running = true
	}
	f.done = make(chan error, 1)
	return f.done, nil
}

func testManager(t *testing.T, f *fakeRunner) *Manager {
	t.Helper()
	m, err := newManager(Config{Timeout: time.Second, PollInterval: time.Millisecond}, f,
		func(path string) (string, error) { return "incarnation-1", nil })
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestEnsureConcurrentReuse(t *testing.T) {
	f := &fakeRunner{}
	m := testManager(t, f)
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			s, err := m.Ensure(context.Background(), "dev-a")
			if err != nil || s.Status != "ready" || s.Incarnation != "incarnation-1" {
				t.Errorf("Ensure = %+v, %v", s, err)
			}
		})
	}
	wg.Wait()
	if f.starts != 1 {
		t.Fatalf("started %d servers", f.starts)
	}
}

func TestEnsureCanceledDoesNotRepeatPendingStart(t *testing.T) {
	f := &fakeRunner{delay: true}
	m := testManager(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	s, err := m.Ensure(ctx, "dev-a")
	if !errors.Is(err, ErrStarting) || s.Status != "starting" {
		t.Fatalf("Ensure = %+v, %v", s, err)
	}

	if _, err := m.Ensure(context.Background(), "dev-a"); !errors.Is(err, ErrStarting) {
		t.Fatalf("pending ensure = %v", err)
	}
	if f.starts != 1 {
		t.Fatalf("pending startup was repeated %d times", f.starts)
	}
	pending, err := m.Status(context.Background(), "dev-a")
	if err != nil || pending.Status != "starting" {
		t.Fatalf("pending discovery = %+v, %v", pending, err)
	}
	list, err := m.List(context.Background())
	if err != nil || len(list) != 1 || list[0].Status != "starting" {
		t.Fatalf("pending session missing from list = %+v, %v", list, err)
	}
	f.running = true
	if _, err := m.Ensure(context.Background(), "dev-a"); err != nil {
		t.Fatal(err)
	}
}

func TestPendingEnsureDoesNotBlockDiscovery(t *testing.T) {
	started := make(chan struct{})
	f := &fakeRunner{delay: true, started: started}
	m := testManager(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := m.Ensure(ctx, "slow")
		done <- err
	}()
	defer func() { cancel(); <-done }()
	<-started
	discovery, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stop()
	if _, err := m.Status(discovery, "other"); err != nil {
		t.Fatalf("pending native start blocked other-session status: %v", err)
	}
	if _, err := m.List(discovery); err != nil {
		t.Fatalf("pending native start blocked list: %v", err)
	}
	if _, err := m.Ensure(discovery, "slow"); !errors.Is(err, ErrStarting) {
		t.Fatalf("concurrent pending ensure did not return uncertainty: %v", err)
	}
}

func TestEnsureDiscoveryFailureNeverStarts(t *testing.T) {
	for _, output := range []string{`{}`, `null`, `[]`, `{"running":false}`, `{} {}`,
		`{"status":"running","running":true,"socket":"marker","session":"other","protocol":18,"compatible":true}`} {
		t.Run(output, func(t *testing.T) {
			f := &fakeRunner{output: []byte(output)}
			m := testManager(t, f)
			if _, err := m.Ensure(context.Background(), "dev-a"); err == nil {
				t.Fatal("invalid discovery accepted")
			}
			if f.starts != 0 {
				t.Fatal("started despite invalid discovery")
			}
		})
	}
}

func TestUnsupportedAndMissingIdentity(t *testing.T) {
	f := &fakeRunner{running: true, protocol: 19}
	m := testManager(t, f)
	if _, err := m.Ensure(context.Background(), "dev-a"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unsupported = %v", err)
	}
	f.protocol = 18
	m.identity = func(string) (string, error) { return "", errors.New("marker missing") }
	if _, err := m.Ensure(context.Background(), "dev-a"); !errors.Is(err, ErrIdentity) {
		t.Fatalf("identity = %v", err)
	}
	if f.starts != 0 {
		t.Fatal("started over unsupported/unidentified server")
	}
}

func TestSessionIdentityNotSocketPath(t *testing.T) {
	f := &fakeRunner{running: true}
	m := testManager(t, f)
	current := "first"
	m.identity = func(string) (string, error) { return current, nil }
	first, err := m.Status(context.Background(), "dev-a")
	if err != nil {
		t.Fatal(err)
	}
	current = "second"
	second, err := m.Status(context.Background(), "dev-a")
	if err != nil || first.Incarnation == second.Incarnation {
		t.Fatalf("replacement identity %+v -> %+v, %v", first, second, err)
	}
	encoded, err := json.Marshal(second)
	if err != nil || strings.Contains(string(encoded), "local-marker") {
		t.Fatal("endpoint leaked through session JSON")
	}
}

func TestNamesAndNoRemoteArguments(t *testing.T) {
	for _, name := range []string{"", "../x", `a\b`, "-x", "A", "aux", "com1", "nul", "a b", "dev;cmd", "é", strings.Repeat("a", 65)} {
		if ValidateName(name) == nil {
			t.Errorf("accepted %q", name)
		}
	}
	f := &fakeRunner{}
	m := testManager(t, f)
	if _, err := m.Ensure(context.Background(), "dev-a"); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, args := range f.commands {
		if fmt.Sprint(args) == "[--session dev-a server]" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing native named headless invocation: %v", f.commands)
	}
}

func TestCanceledLockAndEnvironment(t *testing.T) {
	f := &fakeRunner{}
	m := testManager(t, f)
	m.gate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.List(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("lock error = %v", err)
	}
	if len(f.commands) != 0 {
		t.Fatal("executed canceled command")
	}
	clean := cleanEnvironment([]string{"PATH=x", "HERDR_CONFIG_PATH=y", "HERDR_SOCKET=z", "herdr_session=a", "HERDR_PANE_ID=b"})
	if fmt.Sprint(clean) != "[PATH=x HERDR_CONFIG_PATH=y]" {
		t.Fatalf("environment = %v", clean)
	}
}

func TestBoundedNativeOutput(t *testing.T) {
	var b boundedBuffer
	if _, err := b.Write([]byte(strings.Repeat("x", maxOutput))); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write([]byte("x")); !errors.Is(err, ErrOutputLimit) || !b.overflow || b.Len() != maxOutput {
		t.Fatal("native output not bounded")
	}
}

type listRunner struct {
	fakeRunner
	list []byte
}

func (f *listRunner) run(ctx context.Context, args ...string) ([]byte, error) {
	if args[0] == "session" {
		return f.list, nil
	}
	return f.fakeRunner.run(ctx, args...)
}

func TestListPerSessionUnavailableAndBounds(t *testing.T) {
	f := &listRunner{fakeRunner: fakeRunner{running: true}, list: []byte(`{"sessions":[{"name":"dev-a","default":false}]}`)}
	m, _ := newManager(Config{}, f, func(string) (string, error) { return "", ErrIdentity })
	sessions, err := m.List(context.Background())
	if err != nil || len(sessions) != 1 || sessions[0].ErrorCode != "incarnation_unavailable" {
		t.Fatalf("List = %+v, %v", sessions, err)
	}
	f.list = []byte(`{"sessions":[{"name":"dev-a"},{"name":"dev-a"}]}`)
	if _, err := m.List(context.Background()); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("duplicate sessions = %v", err)
	}
	entries := make([]map[string]any, MaxSessions+1)
	for i := range entries {
		entries[i] = map[string]any{"name": fmt.Sprintf("dev-%d", i)}
	}
	f.list, _ = json.Marshal(map[string]any{"sessions": entries})
	if _, err := m.List(context.Background()); !errors.Is(err, ErrCapacity) {
		t.Fatalf("oversized sessions = %v", err)
	}
}
