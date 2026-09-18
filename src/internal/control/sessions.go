package control

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/protobuf/types/known/durationpb"
)

func Sessions(ctx context.Context, options Options, nodeID string) error {
	if strings.TrimSpace(options.RequiredServerTag) == "" {
		return errors.New("session requests require an expected server tag")
	}
	if strings.TrimSpace(nodeID) == "" {
		return errors.New("session listing requires a node")
	}
	return withFleet(ctx, options, func(client pb.FleetClient, _ transport.SelfStatus) error {
		list, err := retryUnavailable(ctx, func() (*pb.SessionList, error) {
			return client.ListSessions(ctx, &pb.ListSessionsRequest{NodeInstanceId: nodeID})
		})
		if err != nil {
			return fmt.Errorf("list sessions: %w", err)
		}
		return writeSessions(options, list)
	})
}

func EnsureSession(ctx context.Context, options Options, nodeID, key, name string, ttl time.Duration) error {
	if strings.TrimSpace(options.RequiredServerTag) == "" {
		return errors.New("session requests require an expected server tag")
	}
	if key == "" {
		key = protocol.NewCommandID()
	}
	request, err := protocol.NormalizeCommandRequest(&pb.SubmitCommandRequest{
		NodeInstanceId: nodeID, IdempotencyKey: key, CommandType: protocol.SessionEnsureCommandType,
		Ttl: durationpb.New(ttl), SessionEnsure: &pb.SessionEnsure{Name: name},
	})
	if err != nil {
		return err
	}
	return submitAndWait(ctx, options, request)
}

func writeSessions(options Options, list *pb.SessionList) error {
	if list == nil || (list.ErrorCode != "" && !protocol.ValidSessionError(list.ErrorCode)) {
		return errors.New("server returned an invalid session list")
	}
	for _, value := range list.Sessions {
		if value == nil || protocol.ValidateSessionEnsure(&pb.SessionEnsure{Name: value.Name}) != nil ||
			protocol.ValidateSessionSelector(value.Name, value.Incarnation, value.Status == "ready") != nil ||
			(value.ErrorCode != "" && !protocol.ValidSessionError(value.ErrorCode)) {
			return errors.New("server returned an invalid session view")
		}
		switch value.Status {
		case "stopped", "starting", "ready", "unavailable", "unsupported":
		default:
			return errors.New("server returned an invalid session status")
		}
		if stamp := value.HerdrReceivedAt; stamp != nil &&
			(value.Herdr == nil || stamp.CheckValid() != nil || len(stamp.ProtoReflect().GetUnknown()) != 0) {
			return errors.New("server returned an invalid session receipt timestamp")
		}
	}
	if options.JSON {
		if err := writeProjectJSON(options, list); err != nil {
			return err
		}
	} else {
		if len(list.Sessions) == 0 && list.ErrorCode == "" {
			if _, err := fmt.Fprintln(options.Output, "no sessions registered"); err != nil {
				return err
			}
		}
		for _, value := range list.Sessions {
			if _, err := fmt.Fprintf(options.Output, "session=%s session_incarnation=%s status=%s herdr=%s stale=%t error=%s\n",
				value.Name, value.Incarnation, value.Status, value.Herdr.GetStatus(), value.Stale, value.ErrorCode); err != nil {
				return err
			}
		}
	}
	if list.ErrorCode != "" {
		return fmt.Errorf("list sessions: %s", list.ErrorCode)
	}
	return nil
}
