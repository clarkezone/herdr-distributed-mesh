// Package herdr observes a local Herdr session without modifying it. Only
// validated identifiers, focus flags, agent statuses, and version information
// are exposed; event payloads are never applied to the published state.
package herdr

import (
	"context"
	"errors"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const minEventRefresh = 250 * time.Millisecond

// Config selects an explicit local socket marker or fully qualified local
// Windows pipe. Zero durations default to 5s refresh, 1s retry, and 3s per
// request (including dialing). Negative durations are invalid.
type Config struct {
	SocketPath      string
	RefreshInterval time.Duration
	RetryDelay      time.Duration
	RequestTimeout  time.Duration
}

type dialFunc func(context.Context, string) (net.Conn, error)

type observer struct {
	config Config
	dial   dialFunc
	nextID uint64
}

// Observe emits complete ready snapshots or entity-free unavailable states.
// Sequence numbers start at one and increase across reconnections. API failures
// are logged and emitted using fixed categories, then retried until cancellation.
// Ready states exceeding 256 KiB serialized (including sequence and timestamp)
// are rejected and reported as unavailable, without transmitting their entities.
// Both ping and every snapshot must report the supported local protocol 18;
// other positive protocols produce unavailable with unsupported_protocol.
// Invalid configuration returns without dialing. Cancellation returns ctx.Err().
// emit is called synchronously and must return promptly (or honor ctx itself);
// its errors are terminal and returned unchanged. No observer goroutine survives
// this function's return. The observer only sends ping, events.subscribe, and
// session.snapshot, with one request per connection.
func Observe(ctx context.Context, config Config, emit func(*agentflowv1.HerdrState) error) error {
	return observe(ctx, config, emit, dialLocal)
}

func observe(ctx context.Context, config Config, emit func(*agentflowv1.HerdrState) error, dial dialFunc) error {
	address, err := localAddress(config.SocketPath)
	if err != nil {
		return err
	}
	if emit == nil {
		return errors.New("herdr: emit is required")
	}
	if config.RefreshInterval < 0 || config.RetryDelay < 0 || config.RequestTimeout < 0 {
		return errors.New("herdr: durations must not be negative")
	}
	if config.RefreshInterval == 0 {
		config.RefreshInterval = 5 * time.Second
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = time.Second
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 3 * time.Second
	}
	config.SocketPath = address
	o := observer{config: config, dial: dial}
	var sequence uint64
	var emitErr error
	publish := func(state *agentflowv1.HerdrState) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		state.Sequence = sequence + 1
		state.ObservedAt = timestamppb.Now()
		if err := checkStateSize(state); err != nil {
			return err
		}
		sequence++
		emitErr = emit(state)
		return emitErr
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := o.session(ctx, publish)
		if emitErr != nil {
			return emitErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		code := category(err)
		log.Printf("herdr observer unavailable: %s", code)
		if err := publish(&agentflowv1.HerdrState{Status: "unavailable", ErrorCode: code}); err != nil {
			return err
		}
		if err := wait(ctx, config.RetryDelay); err != nil {
			return err
		}
	}
}

func (o *observer) requestID() string {
	o.nextID++
	return "herdr-" + strconv.FormatUint(o.nextID, 10)
}

func (o *observer) session(ctx context.Context, publish func(*agentflowv1.HerdrState) error) error {
	pong, err := o.rpc(ctx, "ping", "pong")
	if err != nil {
		return err
	}
	_, protocol, err := versionProtocol(pong)
	if err != nil {
		return apiError("invalid_response")
	}
	if protocol != supportedProtocol {
		return apiError("unsupported_protocol")
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	params := struct {
		Subscriptions []subscription `json:"subscriptions"`
	}{Subscriptions: topologySubscriptions()}
	conn, reader, cleanup, _, err := o.request(sessionCtx, "events.subscribe", "subscription_started", params)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return apiError("io_error")
	}

	hints := make(chan struct{}, 1)
	done := make(chan struct{})
	var failureMu sync.Mutex
	var failure error
	getFailure := func() error {
		failureMu.Lock()
		defer failureMu.Unlock()
		return failure
	}
	go func() {
		defer close(done)
		for {
			event, err := reader.object()
			if err == nil {
				err = validateEvent(event)
			}
			if err != nil {
				failureMu.Lock()
				failure = err
				failureMu.Unlock()
				cancel()
				return
			}
			select {
			case hints <- struct{}{}:
			default:
			}
		}
	}()
	defer func() {
		cancel()
		cleanup()
		<-done
	}()

	periodic := time.NewTicker(o.config.RefreshInterval)
	defer periodic.Stop()
	for {
		started := time.Now()
		result, err := o.rpc(sessionCtx, "session.snapshot", "session_snapshot")
		// A failed subscription invalidates even a successful in-flight RPC.
		if subErr := getFailure(); subErr != nil {
			return subErr
		}
		if err != nil {
			return err
		}
		state, err := sanitizeSnapshot(result["snapshot"])
		if err != nil {
			return err
		}
		if subErr := getFailure(); subErr != nil {
			return subErr
		}
		if err := sessionCtx.Err(); err != nil {
			return err
		}
		if err := publish(state); err != nil {
			return err
		}
		select {
		case <-sessionCtx.Done():
			if subErr := getFailure(); subErr != nil {
				return subErr
			}
			return sessionCtx.Err()
		case <-periodic.C:
		case <-hints:
			// Do not clear hints after a snapshot: events received while that
			// RPC was pending must cause another authoritative reconciliation.
			if err := wait(sessionCtx, time.Until(started.Add(minEventRefresh))); err != nil {
				if subErr := getFailure(); subErr != nil {
					return subErr
				}
				return err
			}
		}
	}
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(max(0, delay))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
