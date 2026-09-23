package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/fleetwatch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fleetWatchEvent struct {
	Type      string          `json:"type"`
	Fleet     json.RawMessage `json:"fleet,omitempty"`
	ErrorCode string          `json:"error_code,omitempty"`
}

func WatchNodes(ctx context.Context, options Options) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("fleet watch requires an overall deadline")
	}
	if options.Output == nil {
		return errors.New("fleet watch requires an output writer")
	}
	return WithFleet(ctx, options, func(client pb.FleetClient) error {
		return watchNodesWithClient(ctx, options, client, fleetwatch.MaxDuration-time.Second, 500*time.Millisecond)
	})
}

func watchNodesWithClient(ctx context.Context, options Options, client pb.FleetClient, duration, retryDelay time.Duration) error {
	gap := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		streamCtx, cancel := context.WithTimeout(ctx, duration)
		stream, err := client.WatchNodes(streamCtx, &emptypb.Empty{}, grpc.MaxCallRecvMsgSize(fleetwatch.MaxSnapshotBytes))
		for err == nil {
			var snapshot *pb.NodeList
			snapshot, err = stream.Recv()
			if err != nil {
				break
			}
			if err := ctx.Err(); err != nil {
				cancel()
				return err
			}
			if snapshot == nil {
				cancel()
				return errors.New("fleet subscription returned no snapshot")
			}
			data, encodeErr := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(snapshot)
			if encodeErr == nil {
				encodeErr = writeFleetWatchEvent(options, fleetWatchEvent{Type: "snapshot", Fleet: data})
			}
			if encodeErr != nil {
				cancel()
				return fmt.Errorf("write fleet snapshot: %w", encodeErr)
			}
			gap = false
		}
		renew := errors.Is(streamCtx.Err(), context.DeadlineExceeded)
		cancel()
		if err := ctx.Err(); err != nil {
			return err
		}
		code := status.Code(err)
		if code != codes.Unavailable && code != codes.DeadlineExceeded &&
			!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, io.EOF) {
			return fmt.Errorf("watch fleet: %w", err)
		}
		if renew {
			continue
		}
		if !gap {
			label := "unavailable"
			if errors.Is(err, io.EOF) {
				label = "stream_closed"
			} else if code == codes.DeadlineExceeded {
				label = "deadline_exceeded"
			}
			if err := writeFleetWatchEvent(options, fleetWatchEvent{Type: "gap", ErrorCode: label}); err != nil {
				return err
			}
			gap = true
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func writeFleetWatchEvent(options Options, event fleetWatchEvent) error {
	if options.JSON {
		return json.NewEncoder(options.Output).Encode(event)
	}
	if event.Type == "gap" {
		_, err := fmt.Fprintf(options.Output, "fleet gap=%s; retained inventory is not fresh\n", event.ErrorCode)
		return err
	}
	_, err := fmt.Fprintf(options.Output, "fleet snapshot %s\n", event.Fleet)
	return err
}
