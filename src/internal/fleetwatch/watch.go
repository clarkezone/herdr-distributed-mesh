// Package fleetwatch serves bounded replacement snapshots, not replayable deltas.
// Every new subscription starts with a complete current snapshot.
package fleetwatch

import (
	"context"
	"errors"
	"log"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	MaxDuration      = 5 * time.Minute
	MaxSubscriptions = 8
	MaxSnapshotBytes = 8 * 1024 * 1024
)

// Reader returns an owned snapshot, as the ordinary Fleet.ListNodes path does.
type Reader func(context.Context) (*pb.NodeList, error)

type Watcher struct {
	read     Reader
	slots    chan struct{}
	interval time.Duration
	maxBytes int
}

func New(read Reader) (*Watcher, error) {
	if read == nil {
		return nil, errors.New("fleet watch requires a snapshot reader")
	}
	return &Watcher{
		read: read, slots: make(chan struct{}, MaxSubscriptions),
		interval: time.Second, maxBytes: MaxSnapshotBytes,
	}, nil
}

// Watch requires the actual transport context to carry a bounded deadline, so
// blocked sends are interrupted by the transport itself. emit is synchronous:
// slow readers retain no event queue and receive a fresh replacement next time.
func (w *Watcher) Watch(ctx context.Context, emit func(*pb.NodeList) error) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > MaxDuration || emit == nil {
		return status.Error(codes.InvalidArgument, "fleet subscriptions require an emitter and a deadline within five minutes")
	}
	select {
	case w.slots <- struct{}{}:
		defer func() { <-w.slots }()
	default:
		return status.Error(codes.ResourceExhausted, "too many fleet subscriptions")
	}
	var previous *pb.NodeList
	for {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		snapshot, err := w.read(ctx)
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		if err != nil {
			if code := status.Code(err); code == codes.PermissionDenied || code == codes.Unauthenticated {
				return status.Error(code, "fleet subscription peer is not authorized")
			}
			log.Printf("fleet subscription snapshot unavailable")
			return status.Error(codes.Unavailable, "fleet snapshot unavailable")
		}
		if snapshot == nil {
			return status.Error(codes.Internal, "fleet snapshot is missing")
		}
		if proto.Size(snapshot) > w.maxBytes {
			return status.Error(codes.ResourceExhausted, "fleet snapshot exceeds subscription limit")
		}
		if previous == nil || !proto.Equal(previous, snapshot) {
			previous = proto.Clone(snapshot).(*pb.NodeList)
			if err := emit(snapshot); err != nil {
				return err
			}
		}
		timer := time.NewTimer(w.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
	}
}
