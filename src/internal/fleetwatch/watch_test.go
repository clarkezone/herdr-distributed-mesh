package fleetwatch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWatchReplacesAndSuppressesUnchangedSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	w, err := New(func(context.Context) (*pb.NodeList, error) {
		calls++
		if calls < 3 {
			return &pb.NodeList{}, nil
		}
		return &pb.NodeList{Nodes: []*pb.NodeView{{Connected: true}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	w.interval = time.Millisecond
	var received []*pb.NodeList
	err = w.Watch(ctx, func(snapshot *pb.NodeList) error {
		received = append(received, snapshot)
		if len(received) == 2 {
			cancel()
		}
		return nil
	})
	if status.Code(err) != codes.Canceled || calls != 3 || len(received) != 2 ||
		len(received[0].Nodes) != 0 || len(received[1].Nodes) != 1 {
		t.Fatalf("replacement snapshots: calls=%d, frames=%d, err=%v", calls, len(received), err)
	}
}

func TestWatchRequiresBoundedTransportContext(t *testing.T) {
	w, err := New(func(context.Context) (*pb.NodeList, error) {
		t.Fatal("invalid subscription reached reader")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	long, cancel := context.WithTimeout(context.Background(), MaxDuration+time.Minute)
	defer cancel()
	for _, ctx := range []context.Context{context.Background(), long} {
		if err := w.Watch(ctx, func(*pb.NodeList) error { return nil }); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("unbounded context accepted: %v", err)
		}
	}
	if _, err := New(nil); err == nil {
		t.Fatal("nil reader accepted")
	}
}

func TestWatchErrorsNeverBecomeEmptySuccess(t *testing.T) {
	for _, test := range []struct {
		name string
		read Reader
		code codes.Code
	}{
		{"missing", func(context.Context) (*pb.NodeList, error) { return nil, nil }, codes.Internal},
		{"read failure", func(context.Context) (*pb.NodeList, error) { return nil, errors.New("PRIVATE") }, codes.Unavailable},
		{"role removed", func(context.Context) (*pb.NodeList, error) {
			return nil, status.Error(codes.PermissionDenied, "PRIVATE")
		}, codes.PermissionDenied},
		{"identity removed", func(context.Context) (*pb.NodeList, error) {
			return nil, status.Error(codes.Unauthenticated, "PRIVATE")
		}, codes.Unauthenticated},
		{"oversized", func(context.Context) (*pb.NodeList, error) {
			return &pb.NodeList{Nodes: []*pb.NodeView{{Connected: true}}}, nil
		}, codes.ResourceExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			w, err := New(test.read)
			if err != nil {
				t.Fatal(err)
			}
			w.maxBytes = 1
			err = w.Watch(ctx, func(*pb.NodeList) error {
				t.Fatal("invalid snapshot emitted")
				return nil
			})
			if status.Code(err) != test.code || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("unsafe error result: %v", err)
			}
		})
	}
}

func TestWatchHasNoProducerQueueWhileSendBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	w, err := New(func(context.Context) (*pb.NodeList, error) {
		calls++
		return &pb.NodeList{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	w.interval = time.Millisecond
	sentinel := errors.New("transport closed")
	err = w.Watch(ctx, func(*pb.NodeList) error {
		timer := time.AfterFunc(5*time.Millisecond, cancel)
		defer timer.Stop()
		<-ctx.Done()
		return sentinel
	})
	if !errors.Is(err, sentinel) || calls != 1 || len(w.slots) != 0 {
		t.Fatalf("backpressure/release failed: calls=%d, slots=%d, err=%v", calls, len(w.slots), err)
	}
}

func TestWatchDoesNotEmitAfterCanceledRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w, err := New(func(context.Context) (*pb.NodeList, error) {
		cancel()
		return &pb.NodeList{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = w.Watch(ctx, func(*pb.NodeList) error {
		t.Fatal("canceled read emitted a snapshot")
		return nil
	})
	if status.Code(err) != codes.Canceled || len(w.slots) != 0 {
		t.Fatalf("canceled read did not release subscription: %v", err)
	}
}

func TestWatchCapacityAndResubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w, err := New(func(context.Context) (*pb.NodeList, error) { return &pb.NodeList{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	for range cap(w.slots) {
		w.slots <- struct{}{}
	}
	if err := w.Watch(ctx, func(*pb.NodeList) error { return nil }); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("capacity not enforced: %v", err)
	}
	for range cap(w.slots) {
		<-w.slots
	}
	sentinel := errors.New("disconnect")
	for range 2 {
		frames := 0
		err := w.Watch(ctx, func(*pb.NodeList) error {
			frames++
			return sentinel
		})
		if !errors.Is(err, sentinel) || frames != 1 {
			t.Fatalf("reconnect did not get full snapshot: frames=%d, err=%v", frames, err)
		}
	}
}
