// Package herdrsession manages installed Herdr named headless servers. Endpoint
// paths are local routing data, not fleet inventory.
package herdrsession

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	MaxSessions = 64
	maxOutput   = 1 << 20
)

var (
	ErrInvalidName     = errors.New("herdr session: invalid portable session name")
	ErrUnavailable     = errors.New("herdr session: native discovery unavailable")
	ErrInvalidResponse = errors.New("herdr session: invalid native discovery response")
	ErrUnsupported     = errors.New("herdr session: unsupported protocol")
	ErrIdentity        = errors.New("herdr session: incarnation unavailable")
	ErrStarting        = errors.New("herdr session: startup outcome uncertain; inspect before retrying")
	ErrStart           = errors.New("herdr session: headless startup failed")
	ErrCapacity        = errors.New("herdr session: session capacity reached")
	ErrOutputLimit     = errors.New("herdr session: native output limit exceeded")
)

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// ValidateName deliberately uses a portable subset of native session names.
func ValidateName(name string) error {
	if !namePattern.MatchString(name) {
		return ErrInvalidName
	}

	switch name {
	case "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5",
		"com6", "com7", "com8", "com9", "lpt1", "lpt2", "lpt3", "lpt4",
		"lpt5", "lpt6", "lpt7", "lpt8", "lpt9":
		return ErrInvalidName
	}
	return nil
}

// SocketIdentity refreshes the configured endpoint's incarnation without using
// installed-session discovery or substituting a different default endpoint.
func SocketIdentity(path string) (string, error) { return socketIdentity(path) }

type Config struct {
	Executable   string
	Timeout      time.Duration
	PollInterval time.Duration
}

type Session struct {
	Name        string `json:"name"`
	Default     bool   `json:"default"`
	Status      string `json:"status"`
	Incarnation string `json:"incarnation,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
	SocketPath  string `json:"-"`
}

type nativeStatus struct {
	Status     string  `json:"status"`
	Running    *bool   `json:"running"`
	Protocol   *int    `json:"protocol"`
	Compatible *bool   `json:"compatible"`
	Socket     string  `json:"socket"`
	Session    *string `json:"session"`
}

type nativeList struct {
	Sessions []struct {
		Name    string `json:"name"`
		Default bool   `json:"default"`
		Running bool   `json:"running"`
	} `json:"sessions"`
}

type runner interface {
	run(context.Context, ...string) ([]byte, error)
	start(...string) (<-chan error, error)
}

// Manager serializes native check/start operations. A canceled ensure never
// cancels its server or launches a replacement while the first start is pending.
type Manager struct {
	config   Config
	runner   runner
	identity func(string) (string, error)
	gate     chan struct{}
	starting map[string]<-chan error
}

func New(config Config) (*Manager, error) {
	if config.Executable == "" {
		config.Executable = "herdr"
	}
	path, err := exec.LookPath(config.Executable)
	if err != nil {
		return nil, fmt.Errorf("%w: executable not found", ErrUnavailable)
	}
	return newManager(config, commandRunner{executable: path}, socketIdentity)
}

func newManager(config Config, r runner, identity func(string) (string, error)) (*Manager, error) {
	if config.Timeout < 0 || config.Timeout > 5*time.Minute || config.PollInterval < 0 {
		return nil, errors.New("herdr session: invalid timeout or poll interval")
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.PollInterval == 0 {
		config.PollInterval = 100 * time.Millisecond
	}
	return &Manager{config: config, runner: r, identity: identity,
		gate: make(chan struct{}, 1), starting: make(map[string]<-chan error)}, nil
}

func (m *Manager) lock(ctx context.Context) error {
	select {
	case m.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-m.gate
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func argsFor(name string, args ...string) []string {
	if name == "default" {
		return args
	}
	return append([]string{"--session", name}, args...)
}

func decode(data []byte, out any) error {
	if len(data) > maxOutput || len(data) == 0 {
		return ErrInvalidResponse
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(out); err != nil {
		return ErrInvalidResponse
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrInvalidResponse
	}
	return nil
}

func (m *Manager) status(ctx context.Context, name string) (Session, error) {
	s := Session{Name: name, Default: name == "default", Status: "unavailable"}
	data, err := m.runner.run(ctx, argsFor(name, "status", "server", "--json")...)
	if err != nil {
		return s, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	var status nativeStatus
	if err := decode(data, &status); err != nil {
		return s, err
	}
	if status.Running == nil || status.Socket == "" ||
		(name != "default" && (status.Session == nil || *status.Session != name)) ||
		(name == "default" && status.Session != nil && *status.Session != "default") {
		return s, ErrInvalidResponse
	}
	s.SocketPath = status.Socket
	if !*status.Running {
		if status.Status != "not_running" {
			return s, ErrInvalidResponse
		}
		s.Status = "stopped"
		if pending := m.starting[name]; pending != nil {
			select {
			case <-pending:
				delete(m.starting, name)
			default:
				s.Status, s.ErrorCode = "starting", "startup_pending"
			}
		}
		return s, nil
	}
	if status.Status != "running" || status.Protocol == nil || status.Compatible == nil {
		return s, ErrInvalidResponse
	}
	if *status.Protocol != 18 || !*status.Compatible {
		s.Status, s.ErrorCode = "unsupported", "unsupported_protocol"
		return s, ErrUnsupported
	}
	identity, err := m.identity(s.SocketPath)
	if err != nil || identity == "" {
		return s, ErrIdentity
	}
	s.Status, s.Incarnation = "ready", identity
	delete(m.starting, name)
	return s, nil
}

func (m *Manager) Status(ctx context.Context, name string) (Session, error) {
	if err := ValidateName(name); err != nil {
		return Session{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, m.config.Timeout)
	defer cancel()
	if err := m.lock(ctx); err != nil {
		return Session{}, err
	}
	defer func() { <-m.gate }()
	return m.status(ctx, name)
}

func (m *Manager) list(ctx context.Context) ([]Session, error) {
	data, err := m.runner.run(ctx, "session", "list", "--json")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	var response nativeList
	if err := decode(data, &response); err != nil {
		return nil, err
	}
	if response.Sessions == nil {
		return nil, ErrInvalidResponse
	}
	if len(response.Sessions) > MaxSessions {
		return nil, ErrCapacity
	}
	sessions := make([]Session, 0, len(response.Sessions))
	seen := make(map[string]bool)
	for _, entry := range response.Sessions {
		if ValidateName(entry.Name) != nil || entry.Default != (entry.Name == "default") || seen[entry.Name] {
			return nil, ErrInvalidResponse
		}
		seen[entry.Name] = true
		s, err := m.status(ctx, entry.Name)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			s.ErrorCode = errorCode(err)
		}
		sessions = append(sessions, s)
	}
	for name := range m.starting {
		if seen[name] {
			continue
		}
		if len(sessions) >= MaxSessions {
			return nil, ErrCapacity
		}
		s, err := m.status(ctx, name)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			s.ErrorCode = errorCode(err)
		}
		sessions = append(sessions, s)
	}
	slices.SortFunc(sessions, func(a, b Session) int { return strings.Compare(a.Name, b.Name) })
	return sessions, nil
}

func errorCode(err error) string {
	switch {
	case errors.Is(err, ErrUnsupported):
		return "unsupported_protocol"
	case errors.Is(err, ErrIdentity):
		return "incarnation_unavailable"
	case errors.Is(err, ErrInvalidResponse):
		return "invalid_discovery_response"
	default:
		return "discovery_unavailable"
	}
}

func (m *Manager) List(ctx context.Context) ([]Session, error) {
	ctx, cancel := context.WithTimeout(ctx, m.config.Timeout)
	defer cancel()
	if err := m.lock(ctx); err != nil {
		return nil, err
	}
	defer func() { <-m.gate }()
	return m.list(ctx)
}

func (m *Manager) Ensure(ctx context.Context, name string) (Session, error) {
	if err := ValidateName(name); err != nil {
		return Session{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, m.config.Timeout)
	defer cancel()
	if err := m.lock(ctx); err != nil {
		return Session{}, err
	}
	locked := true
	defer func() {
		if locked {
			<-m.gate
		}
	}()
	s, err := m.status(ctx, name)
	if err != nil || s.Status == "ready" {
		return s, err
	}
	if pending, exists := m.starting[name]; exists {
		select {
		case <-pending:
			delete(m.starting, name)
		default:
			return s, ErrStarting
		}
	}
	sessions, err := m.list(ctx)
	if err != nil {
		return s, err
	}
	exists := false
	for _, session := range sessions {
		if session.Name == name {
			exists = true
		}
	}
	if !exists && len(sessions) >= MaxSessions {
		return s, ErrCapacity
	}
	if err := ctx.Err(); err != nil {
		return s, err
	}
	done, err := m.runner.start(argsFor(name, "server")...)
	if err != nil {
		return s, fmt.Errorf("%w: %w", ErrStart, err)
	}
	m.starting[name] = done
	// Waiting for startup must not hold discovery for other sessions hostage.
	// starting remains protected by gate and prevents duplicate native starts.
	<-m.gate
	locked = false
	ticker := time.NewTicker(m.config.PollInterval)
	defer ticker.Stop()
	for {
		s, err = m.Status(ctx, name)
		if err == nil && s.Status == "ready" {
			return s, nil
		}
		if errors.Is(err, ErrUnsupported) {
			return s, err
		}
		select {
		case <-ctx.Done():
			s.Status, s.ErrorCode = "starting", "startup_uncertain"
			return s, fmt.Errorf("%w: %w", ErrStarting, ctx.Err())
		case processErr := <-done:
			// Native duplicate-start protection can lose a race to another
			// manager; accept only a fresh compatible API/identity check.
			s, err = m.Status(ctx, name)
			if err == nil && s.Status == "ready" {
				return s, nil
			}
			if processErr != nil {
				return s, fmt.Errorf("%w: %w", ErrStart, processErr)
			}
			return s, ErrStart
		case <-ticker.C:
		}
	}
}

type boundedBuffer struct {
	bytes.Buffer
	overflow bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(p) > maxOutput-b.Len() {
		b.overflow = true
		return 0, ErrOutputLimit
	}
	return b.Buffer.Write(p)
}

type commandRunner struct{ executable string }

func (r commandRunner) command(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, r.executable, args...)
	configureCommand(cmd)
	cmd.WaitDelay = time.Second
	return cmd
}

func (r commandRunner) run(ctx context.Context, args ...string) ([]byte, error) {
	cmd := r.command(ctx, args...)
	var stdout, stderr boundedBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if stdout.overflow || stderr.overflow {
		return nil, ErrOutputLimit
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, errors.New("native command failed")
	}
	return stdout.Bytes(), nil
}

func (r commandRunner) start(args ...string) (<-chan error, error) {
	// Server lifetime is independent of an individual ensure request.
	cmd := exec.Command(r.executable, args...)
	configureCommand(cmd)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		return nil, errors.New("native server process could not start")
	}
	done := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		if err != nil {
			err = errors.New("native server process exited unsuccessfully")
		}
		done <- err
		close(done)
	}()
	return done, nil
}

func cleanEnvironment(env []string) []string {
	clean := make([]string, 0, len(env))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		switch strings.ToUpper(key) {
		case "HERDR_SOCKET", "HERDR_API_SOCKET", "HERDR_SESSION", "HERDR_SESSION_NAME",
			"HERDR_PANE_ID", "HERDR_TAB_ID", "HERDR_WORKSPACE_ID":
			continue
		}
		clean = append(clean, entry)
	}
	return clean
}
