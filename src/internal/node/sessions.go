package node

import (
	"context"
	"errors"
	"log"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrsession"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SessionManager is the installed native headless manager, injectable in
// authenticated RunSession fixtures. Production Run always constructs its own.
type SessionManager interface {
	List(context.Context) ([]herdrsession.Session, error)
	Status(context.Context, string) (herdrsession.Session, error)
	Ensure(context.Context, string) (herdrsession.Session, error)
}

const maxSessionInventoryBytes = 256 * 1024
const sessionSnapshotStaleAfter = 30 * time.Second

type sessionFailure string

func (e sessionFailure) Error() string { return string(e) }

func sessionError(err error) string {
	var failure sessionFailure
	if errors.As(err, &failure) {
		return string(failure)
	}
	switch {
	case errors.Is(err, herdrsession.ErrStarting):
		return "startup_uncertain"
	case errors.Is(err, herdrsession.ErrUnsupported):
		return "unsupported_protocol"
	case errors.Is(err, herdrsession.ErrCapacity):
		return "session_capacity"
	case errors.Is(err, herdrsession.ErrStart):
		return "session_start_failed"
	case errors.Is(err, herdrsession.ErrUnavailable):
		return "session_manager_unavailable"
	default:
		return "session_unavailable"
	}
}

func sameEndpoint(a, b string) bool {
	a, b = filepath.Clean(a), filepath.Clean(b)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}
	return a == b
}

func (h *commandHandler) configuredDefault(session herdrsession.Session) bool {
	return session.Name != "default" || h.herdrConfig.SocketPath == "" ||
		sameEndpoint(session.SocketPath, h.herdrConfig.SocketPath)
}

func (h *commandHandler) sessionsReady() bool {
	return h.sessionsNegotiated && h.sessions != nil && h.journal != nil && h.verifyCoordinator != nil
}

// resolveSession never treats a focused or installed default as the configured
// default. Named resolution refreshes API readiness and native incarnation.
func (h *commandHandler) resolveSession(ctx context.Context, name, expected string) (herdr.Config, string, error) {
	if name == "" {
		if expected == "" {
			return h.herdrConfig, "", nil
		}
		actual, err := herdrsession.SocketIdentity(h.herdrConfig.SocketPath)
		if err != nil {
			return herdr.Config{}, "", sessionFailure("session_unavailable")
		}
		if expected != "" && actual != expected {
			return herdr.Config{}, "", sessionFailure("session_replaced")
		}
		return h.pinSessionConfig(h.herdrConfig, name, actual), actual, nil
	}
	if !h.sessionsReady() {
		return herdr.Config{}, "", sessionFailure("session_manager_unavailable")
	}
	session, err := h.sessions.Status(ctx, name)
	if err != nil {
		return herdr.Config{}, "", sessionFailure(sessionError(err))
	}
	if !h.configuredDefault(session) {
		return herdr.Config{}, "", sessionFailure("session_default_unmanaged")
	}
	if session.Name != name || session.Status != "ready" || !protocol.ValidSessionIncarnation(session.Incarnation) || session.SocketPath == "" {
		return herdr.Config{}, "", sessionFailure("session_unavailable")
	}
	if expected != "" && expected != session.Incarnation {
		return herdr.Config{}, "", sessionFailure("session_replaced")
	}
	return h.pinSessionConfig(herdr.Config{SocketPath: session.SocketPath}, name, session.Incarnation), session.Incarnation, nil
}

func (h *commandHandler) pinSessionConfig(config herdr.Config, name, incarnation string) herdr.Config {
	config.CheckSession = func(ctx context.Context) error {
		current, _, err := h.resolveSession(ctx, name, incarnation)
		if err == nil && !sameEndpoint(current.SocketPath, config.SocketPath) {
			return sessionFailure("session_replaced")
		}
		return err
	}
	return config
}

func (h *commandHandler) ensureSession(ctx context.Context, command *pb.Command, receivedAt time.Time) *pb.CommandResult {
	result := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_REJECTED, Detail: "session_manager_unavailable"}
	if !h.sessionsReady() {
		return result
	}
	deadline := command.ExpiresAt.AsTime()
	if remaining := receivedAt.Add(command.Ttl.AsDuration()); remaining.Before(deadline) {
		deadline = remaining
	}
	effect, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	verification, stop := context.WithTimeout(effect, workspacePeerVerificationTimeout)
	err := h.verifyCoordinator(verification)
	verificationErr := verification.Err()
	stop()
	if effect.Err() != nil {
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_TIMED_OUT, "deadline_expired"
		return result
	}
	if err != nil || verificationErr != nil {
		result.Detail = "authorization_changed"
		return result
	}
	if command.SessionEnsure.Name == "default" && h.herdrConfig.SocketPath != "" {
		current, err := h.sessions.Status(effect, "default")
		if err != nil {
			result.Detail = sessionError(err)
			return result
		}
		if current.Name != "default" || current.SocketPath == "" {
			result.Detail = "session_unavailable"
			return result
		}
		if !h.configuredDefault(current) {
			result.Detail = "session_default_unmanaged"
			return result
		}
	}
	value, err := h.sessions.Ensure(effect, command.SessionEnsure.Name)
	if err != nil {
		result.Detail = sessionError(err)
		if errors.Is(err, herdrsession.ErrStarting) || effect.Err() != nil {
			result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, "startup_uncertain"
		}
		return result
	}
	if value.Name != command.SessionEnsure.Name || value.Status != "ready" || !protocol.ValidSessionIncarnation(value.Incarnation) {
		result.Detail = "session_unavailable"
		return result
	}
	result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, "session_ready"
	result.SessionEnsure = &pb.SessionView{Name: value.Name, Incarnation: value.Incarnation, Status: value.Status}
	return result
}

type sessionObservation struct {
	name        string
	incarnation string
	state       *pb.HerdrState
	observedAt  time.Time
}

type sessionWatch struct {
	incarnation string
	socket      string
	cancel      context.CancelFunc
	done        chan struct{}
}

// Each ready session owns at most one observer. Replacements are canceled and
// joined before their new observer starts; queued observations are fenced by
// native incarnation. All watchers are joined before a stream reconnects.
func (h *commandHandler) observeSessions(ctx context.Context, publish func(*pb.SessionInventory) error) error {
	ctx, cancel := context.WithCancel(ctx)
	discovery := make(chan struct {
		sessions []herdrsession.Session
		err      error
	}, 1)
	refreshDone := make(chan struct{})
	go func() {
		defer close(refreshDone)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			sessions, err := h.sessions.List(ctx)
			select {
			case discovery <- struct {
				sessions []herdrsession.Session
				err      error
			}{sessions, err}:
			case <-ctx.Done():
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() { cancel(); <-refreshDone }()
	watches := make(map[string]*sessionWatch)
	defer func() {
		for _, watch := range watches {
			watch.cancel()
		}
		for _, watch := range watches {
			<-watch.done
		}
	}()
	updates := make(chan sessionObservation, herdrsession.MaxSessions)
	views := make(map[string]*pb.SessionView)
	observed := make(map[string]time.Time)
	expiryTicker := time.NewTicker(time.Second)
	defer expiryTicker.Stop()
	var sequence uint64
	var inventoryError string
	diagnostics := sessionDiagnostics{logf: log.Printf}
	emit := func() error {
		expireSessionSnapshots(views, observed, time.Now())
		sequence++
		inventory := &pb.SessionInventory{Sequence: sequence, ObservedAt: timestamppb.Now(), ErrorCode: inventoryError}
		names := make([]string, 0, len(views))
		for name := range views {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			inventory.Sessions = append(inventory.Sessions, proto.Clone(views[name]).(*pb.SessionView))
		}
		if proto.Size(inventory) > maxSessionInventoryBytes {
			// Retain every session identity while explicitly suppressing an
			// over-budget topology; never send a success-shaped partial tree.
			inventory.ErrorCode = "session_capacity"
			for _, value := range inventory.Sessions {
				value.Herdr = nil
			}
		}
		return publish(inventory)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-expiryTicker.C:
			if expireSessionSnapshots(views, observed, now) {
				if err := emit(); err != nil {
					return err
				}
			}
		case result := <-discovery:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			diagnostics.update(result.sessions, result.err)
			inventoryError = ""
			if result.err != nil || len(result.sessions) > herdrsession.MaxSessions {
				inventoryError = sessionError(result.err)
				if len(result.sessions) > herdrsession.MaxSessions {
					inventoryError = "session_capacity"
				}
				result.sessions = nil
			}
			next := make(map[string]*pb.SessionView, len(result.sessions))
			for _, session := range result.sessions {
				value := &pb.SessionView{Name: session.Name, Incarnation: session.Incarnation, Status: session.Status}
				if session.ErrorCode != "" {
					value.ErrorCode = "session_unavailable"
					if session.Status == "unsupported" {
						value.ErrorCode = "unsupported_protocol"
					}
				}
				if !h.configuredDefault(session) {
					value.Status, value.ErrorCode, value.Incarnation = "unavailable", "session_default_unmanaged", ""
				}
				if previous := views[session.Name]; previous != nil && previous.Incarnation == value.Incarnation && value.Status == "ready" {
					value.Herdr = previous.Herdr
				}
				if value.Herdr == nil {
					delete(observed, session.Name)
				}
				next[session.Name] = value
				if existing := watches[session.Name]; existing != nil &&
					(existing.incarnation != session.Incarnation || existing.socket != session.SocketPath || value.Status != "ready") {
					existing.cancel()
					<-existing.done
					delete(watches, session.Name)
				}
				if value.Status == "ready" && watches[session.Name] == nil {
					child, cancel := context.WithCancel(ctx)
					watch := &sessionWatch{incarnation: session.Incarnation, socket: session.SocketPath, cancel: cancel, done: make(chan struct{})}
					watches[session.Name] = watch
					go func(session herdrsession.Session) {
						defer close(watch.done)
						err := herdr.Observe(child, herdr.Config{SocketPath: session.SocketPath, ProjectResolver: h.managedProjects}, func(state *pb.HerdrState) error {
							observedAt := time.Now()
							// The local API does not offer atomic identity CAS;
							// freshly verify the marker before labeling a tree.
							current, err := h.sessions.Status(child, session.Name)
							if err != nil || current.Incarnation != session.Incarnation {
								state = &pb.HerdrState{Status: "unavailable", ErrorCode: "io_error", Sequence: state.Sequence, ObservedAt: state.ObservedAt}
							}
							select {
							case updates <- sessionObservation{session.Name, session.Incarnation, state, observedAt}:
								return nil
							case <-child.Done():
								return child.Err()
							}
						})
						if err != nil && child.Err() == nil {
							select {
							case updates <- sessionObservation{session.Name, session.Incarnation, &pb.HerdrState{Status: "unavailable", ErrorCode: "io_error", Sequence: 1, ObservedAt: timestamppb.Now()}, time.Now()}:
							case <-child.Done():
							}
						}
					}(session)
				}
			}
			for name, watch := range watches {
				if next[name] == nil {
					watch.cancel()
					<-watch.done
					delete(watches, name)
				}
			}
			for name := range observed {
				if next[name] == nil {
					delete(observed, name)
				}
			}
			views = next
			if err := emit(); err != nil {
				return err
			}
		case observation := <-updates:
			if current := views[observation.name]; current != nil && current.Status == "ready" && current.Incarnation == observation.incarnation {
				current.Herdr = observation.state
				observed[observation.name] = observation.observedAt
				if err := emit(); err != nil {
					return err
				}
			}
		}
	}
}

func expireSessionSnapshots(views map[string]*pb.SessionView, observed map[string]time.Time, now time.Time) bool {
	changed := false
	for name, value := range views {
		if value.Herdr.GetStatus() != "ready" {
			continue
		}
		age := now.Sub(observed[name])
		if !observed[name].IsZero() && age >= 0 && age < sessionSnapshotStaleAfter {
			continue
		}
		value.Herdr = &pb.HerdrState{Status: "unavailable", ErrorCode: "observation_stale",
			Sequence: value.Herdr.Sequence, ObservedAt: value.Herdr.ObservedAt}
		changed = true
	}
	return changed
}

// Keep errors from independent command lanes race-free.
type commandWorkerErrors struct {
	sync.Mutex
	errors []error
}
