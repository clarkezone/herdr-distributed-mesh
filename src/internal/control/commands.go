package control

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

func Ping(ctx context.Context, options Options, nodeID, key string, ttl time.Duration) error {
	if options.RequiredServerTag == "" {
		return errors.New("command requests require an expected server tag")
	}
	request, err := protocol.NormalizeProbeRequest(&agentflowv1.SubmitCommandRequest{
		NodeInstanceId: nodeID, IdempotencyKey: key, CommandType: protocol.ProbeCommandType, Ttl: durationpb.New(ttl),
	})
	if err != nil {
		return err
	}
	return submitAndWait(ctx, options, request)
}

func EnsureWorkspace(ctx context.Context, options Options, nodeID, key, projectID, revision string, ttl time.Duration) error {
	if options.RequiredServerTag == "" {
		return errors.New("command requests require an expected server tag")
	}
	request, err := protocol.NormalizeCommandRequest(&agentflowv1.SubmitCommandRequest{
		NodeInstanceId: nodeID, IdempotencyKey: key, CommandType: protocol.WorkspaceEnsureCommandType, Ttl: durationpb.New(ttl),
		WorkspaceEnsure: &agentflowv1.WorkspaceEnsure{ProjectId: projectID, BindingRevision: revision},
	})
	if err != nil {
		return err
	}
	return submitAndWait(ctx, options, request)
}

func submitAndWait(ctx context.Context, options Options, request *agentflowv1.SubmitCommandRequest) error {
	key := request.IdempotencyKey
	log.Printf("command type=%s idempotency_key=%s target_node=%s", request.CommandType, key, request.NodeInstanceId)
	return withFleet(ctx, options, func(client agentflowv1.FleetClient, _ transport.SelfStatus) error {
		return submitAndWaitWithClient(ctx, options, client, request)
	})
}

func submitAndWaitWithClient(ctx context.Context, options Options, client agentflowv1.FleetClient, request *agentflowv1.SubmitCommandRequest) error {
	key := request.IdempotencyKey
	record, err := retryUnavailable(ctx, func() (*agentflowv1.CommandRecord, error) { return client.SubmitCommand(ctx, request) })
	if err != nil {
		return fmt.Errorf("submit command (retry with the same key %q): %w", key, err)
	}
	record, err = waitCommand(ctx, client, record)
	if err != nil {
		return fmt.Errorf("wait for command %s (retry with the same key %q): %w", record.GetCommand().GetCommandId(), key, err)
	}
	if err := writeCommand(options, record); err != nil {
		return err
	}
	if record.Status != agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		return fmt.Errorf("command ended with %s; use the same key or command ID to inspect this operation", record.Status)
	}
	return nil
}

func CommandStatus(ctx context.Context, options Options, id string) error {
	if options.RequiredServerTag == "" {
		return errors.New("command requests require an expected server tag")
	}
	return withFleet(ctx, options, func(client agentflowv1.FleetClient, _ transport.SelfStatus) error {
		record, err := retryUnavailable(ctx, func() (*agentflowv1.CommandRecord, error) {
			return client.GetCommand(ctx, &agentflowv1.GetCommandRequest{CommandId: id})
		})
		if err != nil {
			return fmt.Errorf("get command: %w", err)
		}
		return writeCommand(options, record)
	})
}

func waitCommand(ctx context.Context, client agentflowv1.FleetClient, record *agentflowv1.CommandRecord) (*agentflowv1.CommandRecord, error) {
	for {
		if record == nil || record.Command == nil {
			return record, errors.New("server returned an invalid command record")
		}
		if protocol.IsTerminalCommand(record.Status) {
			return record, nil
		}
		if record.Status != agentflowv1.CommandStatus_COMMAND_STATUS_ACCEPTED && record.Status != agentflowv1.CommandStatus_COMMAND_STATUS_RUNNING {
			return record, errors.New("server returned an invalid command status")
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return record, ctx.Err()
		case <-timer.C:
		}
		next, err := retryUnavailable(ctx, func() (*agentflowv1.CommandRecord, error) {
			return client.GetCommand(ctx, &agentflowv1.GetCommandRequest{CommandId: record.Command.CommandId})
		})
		if err != nil {
			return record, err
		}
		record = next
	}
}

func writeCommand(options Options, record *agentflowv1.CommandRecord) error {
	if record == nil || record.Command == nil {
		return errors.New("server returned an invalid command record")
	}
	if options.JSON {
		data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(record)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(options.Output, string(data))
		return err
	}
	_, err := fmt.Fprintf(options.Output, "command=%s node=%s key=%s status=%s detail=%s audit_events=%d\n",
		record.Command.CommandId, record.Command.TargetId, record.Command.IdempotencyKey, record.Status, record.Detail, len(record.Audit))
	if err != nil {
		return err
	}
	if workspace := record.WorkspaceEnsure; workspace != nil {
		_, err = fmt.Fprintf(options.Output, "project=%s binding_revision=%s workspace=%s created=%t\n",
			workspace.ProjectId, workspace.BindingRevision, workspace.WorkspaceId, workspace.Created)
		if err != nil {
			return err
		}
	}
	if worktree := record.WorktreeCreate; worktree != nil {
		_, err = fmt.Fprintf(options.Output, "project=%s binding_revision=%s workspace=%s name=%s branch=%s base_commit=%s\n",
			worktree.ProjectId, worktree.BindingRevision, worktree.WorkspaceId, worktree.Name, worktree.Branch, worktree.BaseCommit)
	}
	if err == nil && record.AgentControl != nil {
		value := record.AgentControl
		_, err = fmt.Fprintf(options.Output, "agent=%s terminal=%s observed_status=%s state_change_seq=%d (input receipt, not task completion)\n",
			value.Target.GetPaneId(), value.Target.GetTerminalId(), value.ObservedStatus, value.StateChangeSeq)
	}
	return err
}
