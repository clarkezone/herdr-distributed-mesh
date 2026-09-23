package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
	return EnsureWorkspaceInSession(ctx, options, nodeID, key, &agentflowv1.WorkspaceEnsure{ProjectId: projectID, BindingRevision: revision}, ttl)
}

func EnsureWorkspaceInSession(ctx context.Context, options Options, nodeID, key string, workspace *agentflowv1.WorkspaceEnsure, ttl time.Duration) error {
	if options.RequiredServerTag == "" {
		return errors.New("command requests require an expected server tag")
	}
	request, err := protocol.NormalizeCommandRequest(&agentflowv1.SubmitCommandRequest{
		NodeInstanceId: nodeID, IdempotencyKey: key, CommandType: protocol.WorkspaceEnsureCommandType, Ttl: durationpb.New(ttl),
		WorkspaceEnsure: workspace,
	})
	if err != nil {
		return err
	}
	return submitAndWait(ctx, options, request)
}

func submitAndWait(ctx context.Context, options Options, request *agentflowv1.SubmitCommandRequest) error {
	return withFleet(ctx, options, func(client agentflowv1.FleetClient, _ transport.SelfStatus) error {
		return submitAndWaitWithClient(ctx, options, client, request)
	})
}

func submitAndWaitWithClient(ctx context.Context, options Options, client agentflowv1.FleetClient, request *agentflowv1.SubmitCommandRequest) error {
	key := request.IdempotencyKey
	if err := writeCommandRetryIdentity(options, request); err != nil {
		return fmt.Errorf("write retry identity before submission: %w", err)
	}
	record, err := retryUnavailable(ctx, func() (*agentflowv1.CommandRecord, error) { return client.SubmitCommand(ctx, request) })
	if err != nil {
		return fmt.Errorf("submit command (retry with the same key %q): %w", key, err)
	}
	if protocol.IsLifecycleCommand(request.CommandType) &&
		(record == nil || record.Command == nil || protocol.ValidateCommand(record.Command, request.NodeInstanceId) != nil ||
			!proto.Equal(record.Command.SubmittedRequest, request)) {
		return errors.New("server returned a mismatched lifecycle request fingerprint")
	}
	record, err = waitCommand(ctx, client, record)
	if err != nil {
		if protocol.IsLifecycleCommand(request.CommandType) && record != nil && record.Command != nil {
			err = errors.Join(err, writeCommand(options, record))
		}
		return fmt.Errorf("wait for command %s (retry with the same key %q): %w", record.GetCommand().GetCommandId(), key, err)
	}
	if err := writeCommand(options, record); err != nil {
		return err
	}
	if record.Status != agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		return fmt.Errorf("command %s: %s (%s); inspect with ctl command -id %s; preserve retry key %q and the original request", record.Command.CommandId, HumanCommandStatus(record.Status), record.Status, record.Command.CommandId, key)
	}
	return nil
}

func writeCommandRetryIdentity(options Options, request *agentflowv1.SubmitCommandRequest) error {
	if options.RetryOutput == nil {
		return nil
	}
	if _, err := fmt.Fprintf(options.RetryOutput, "Retry identity: -node %q -idempotency-key %q -ttl %s; reuse with the original request\n",
		request.NodeInstanceId, request.IdempotencyKey, request.Ttl.AsDuration()); err != nil {
		return err
	}
	target := request.GetAgentControl().GetTarget()
	if target == nil {
		target = request.GetAgentStop().GetTarget()
	}
	if target != nil {
		_, err := fmt.Fprintf(options.RetryOutput, "Pinned target: -agent %q -terminal %q -agent-session %q -session %q -session-incarnation %q\n",
			target.PaneId, target.TerminalId, target.AgentSessionId, target.SessionName, target.SessionIncarnation)
		return err
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
		if protocol.IsLifecycleCommand(record.Command.CommandType) {
			if err := protocol.ValidateCommand(record.Command, record.Command.TargetId); err != nil {
				return record, err
			}
			if record.AgentLifecycle != nil {
				if err := protocol.ValidateLifecycleReceipt(record.AgentLifecycle, record.Command); err != nil {
					return record, err
				}
			}
		}
		if protocol.IsTerminalCommand(record.Status) {
			if protocol.IsLifecycleCommand(record.Command.CommandType) && record.Status != agentflowv1.CommandStatus_COMMAND_STATUS_NODE_UNAVAILABLE {
				if err := protocol.ValidateResultForCommand(&agentflowv1.CommandResult{CommandId: record.Command.CommandId,
					Status: record.Status, Detail: record.Detail, AgentLifecycle: record.AgentLifecycle}, record.Command); err != nil {
					return record, fmt.Errorf("invalid lifecycle receipt: %w", err)
				}
			}
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
		if protocol.IsLifecycleCommand(record.Command.CommandType) {
			if next == nil || !proto.Equal(next.Command, record.Command) {
				return record, errors.New("server changed the lifecycle command identity while waiting")
			}
			if next.AgentLifecycle != nil {
				if err := protocol.ValidateLifecycleAdvance(record.AgentLifecycle, next.AgentLifecycle, record.Command); err != nil {
					return record, err
				}
			} else if record.AgentLifecycle != nil {
				return record, errors.New("server discarded a partial lifecycle handle")
			}
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
	var view humanView
	view.field("Command", record.Command.CommandId)
	view.field("Status", HumanCommandStatus(record.Status))
	view.field("Target node", record.Command.TargetId)
	view.field("Detail", HumanDetail(record.Detail))
	view.field("Retry key", record.Command.IdempotencyKey)
	view.field("Retry safety", "reuse this exact key and original request/target; a new key creates a new operation")
	switch record.Status {
	case agentflowv1.CommandStatus_COMMAND_STATUS_INDETERMINATE:
		view.field("Next", "effects may already have occurred; inspect this command and its target before further changes")
	case agentflowv1.CommandStatus_COMMAND_STATUS_NODE_UNAVAILABLE:
		view.field("Next", "check the node connection and inspect this command before deciding whether to retry")
	case agentflowv1.CommandStatus_COMMAND_STATUS_FAILED, agentflowv1.CommandStatus_COMMAND_STATUS_REJECTED, agentflowv1.CommandStatus_COMMAND_STATUS_TIMED_OUT:
		view.field("Next", "inspect this command and the reported issue before submitting another operation")
	}
	if workspace := record.WorkspaceEnsure; workspace != nil {
		view.field("Project", workspace.ProjectId)
		view.field("Binding revision", workspace.BindingRevision)
		view.field("Workspace", workspace.WorkspaceId)
		view.field("Created", fmt.Sprint(workspace.Created))
		view.session(workspace.SessionName, workspace.SessionIncarnation)
	}
	if worktree := record.WorktreeCreate; worktree != nil {
		view.field("Project", worktree.ProjectId)
		view.field("Binding revision", worktree.BindingRevision)
		view.field("Workspace", worktree.WorkspaceId)
		view.field("Worktree name", worktree.Name)
		view.field("Branch", worktree.Branch)
		view.field("Base commit", worktree.BaseCommit)
		view.session(worktree.SessionName, worktree.SessionIncarnation)
	}
	if record.AgentControl != nil {
		value := record.AgentControl
		view.target(value.Target)
		view.field("Observed status", readable(value.ObservedStatus))
		view.field("Receipt", "input receipt, not task completion; read or wait on the agent to observe progress")
	}
	if record.SessionEnsure != nil {
		value := record.SessionEnsure
		view.session(value.Name, value.Incarnation)
		view.field("Session status", readable(value.Status))
		view.field("Session issue", HumanDetail(value.ErrorCode))
	}
	if record.AgentLifecycle != nil {
		value := record.AgentLifecycle
		handle := value.GetHandle()
		target := handle.GetTarget()
		view.field("Workspace", handle.GetWorkspaceId())
		view.field("Tab", handle.GetTabId())
		view.target(target)
		view.field("Provider", handle.GetProvider())
		view.field("Pane creation", readable(value.PaneOutcome))
		view.field("Agent launch", readable(value.LaunchOutcome))
		view.field("Initial prompt", readable(value.PromptOutcome))
		view.field("Agent stop", readable(value.StopOutcome))
		view.field("Observed status", readable(value.ObservedStatus))
		view.field("Receipt", "readiness/input receipt, not task completion; read or wait on the agent to observe progress")
	}
	_, err := fmt.Fprint(options.Output, view.String())
	return err
}
