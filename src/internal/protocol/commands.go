package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"regexp"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	ProbeCommandType  = "node.ping.v1"
	ProbeCapability   = "commands.node-ping.v1"
	DefaultCommandTTL = 10 * time.Second
	MaxCommandTTL     = 30 * time.Second
)

var commandToken = regexp.MustCompile(`^[A-Za-z0-9:_-]{1,128}$`)
var commandID = regexp.MustCompile(`^[a-f0-9]{32}$`)

func NewCommandID() string {
	var value [16]byte
	// crypto/rand.Read terminates the process if the platform CSPRNG fails.
	rand.Read(value[:])
	return hex.EncodeToString(value[:])
}

func ValidCommandID(id string) bool { return commandID.MatchString(id) }

func ValidIdempotencyKey(key string) bool { return commandToken.MatchString(key) }

func NormalizeProbeRequest(request *agentflowv1.SubmitCommandRequest) (*agentflowv1.SubmitCommandRequest, error) {
	if request == nil || request.CommandType != ProbeCommandType {
		return nil, errors.New("only the read-only node.ping.v1 probe is allowed")
	}
	return NormalizeCommandRequest(request)
}

func NormalizeCommandRequest(request *agentflowv1.SubmitCommandRequest) (*agentflowv1.SubmitCommandRequest, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 ||
		!commandToken.MatchString(request.NodeInstanceId) || !ValidIdempotencyKey(request.IdempotencyKey) {
		return nil, errors.New("node ID and idempotency key must be bounded identifier tokens")
	}
	check := proto.Clone(request).(*agentflowv1.SubmitCommandRequest)
	if check.AgentStart != nil && check.AgentStart.BindingRevision == "" {
		check.AgentStart.BindingRevision = "managed"
	}
	if check.WorkspaceEnsure != nil && check.WorkspaceEnsure.BindingRevision == "" {
		check.WorkspaceEnsure.BindingRevision = "managed"
	}
	if check.WorktreeCreate != nil {
		if check.WorktreeCreate.BindingRevision == "" {
			check.WorktreeCreate.BindingRevision = "managed"
		}
		if check.WorktreeCreate.Branch == "" {
			check.WorktreeCreate.Branch = check.WorktreeCreate.Name
		}
		if check.WorktreeCreate.BaseCommit == "" {
			check.WorktreeCreate.BaseCommit = "0000000000000000000000000000000000000000"
		}
	}
	if err := validateCommandBody(check.CommandType, check.WorkspaceEnsure, check.WorktreeCreate, check.AgentControl, check.SessionEnsure, check.AgentStart, check.AgentStop); err != nil {
		return nil, err
	}
	copy := proto.Clone(request).(*agentflowv1.SubmitCommandRequest)
	if copy.AgentStart != nil {
		copy.AgentStart.StartupTimeoutMs, _ = NormalizeAgentStartupTimeout(copy.AgentStart.StartupTimeoutMs)
	}
	if copy.Ttl == nil {
		copy.Ttl = durationpb.New(DefaultCommandTTL)
	}
	if err := validTTL(copy.Ttl); err != nil {
		return nil, err
	}
	return copy, nil
}

func validTTL(ttl *durationpb.Duration) error {
	if ttl == nil || ttl.CheckValid() != nil || len(ttl.ProtoReflect().GetUnknown()) != 0 ||
		ttl.AsDuration() <= 0 || ttl.AsDuration() > MaxCommandTTL {
		return errors.New("command TTL must be greater than zero and at most 30 seconds")
	}
	return nil
}

func ValidateProbeCommand(command *agentflowv1.Command, nodeID string) error {
	if command == nil || command.CommandType != ProbeCommandType {
		return errors.New("invalid probe type")
	}
	return ValidateCommand(command, nodeID)
}

func ValidateCommand(command *agentflowv1.Command, nodeID string) error {
	if command == nil || !ValidCommandID(command.CommandId) ||
		command.TargetId != nodeID || !commandToken.MatchString(command.TargetId) ||
		!commandToken.MatchString(command.IdempotencyKey) || len(command.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("invalid or unsupported node command")
	}
	if IsLifecycleCommand(command.CommandType) && proto.Size(command)+MaxLifecycleReceiptBytes+1024 > 16*1024 {
		return errors.New("lifecycle command exceeds request budget reserved for durable receipt and audit")
	}
	worktree := command.WorktreeCreate
	if command.SubmittedRequest != nil {
		if err := ValidateSubmittedRequest(command); err != nil {
			return err
		}
		if worktree != nil && worktree.BaseCommit == "" {
			worktree = proto.Clone(worktree).(*agentflowv1.WorktreeCreate)
			worktree.BaseCommit = "0000000000000000000000000000000000000000"
		}
	}
	if err := validateCommandBody(command.CommandType, command.WorkspaceEnsure, worktree, command.AgentControl, command.SessionEnsure, command.AgentStart, command.AgentStop); err != nil {
		return err
	}
	if command.Actor == nil || !commandToken.MatchString(command.Actor.ActorId) ||
		command.Actor.Role != agentflowv1.Role_ROLE_CONTROLLER || command.Actor.Origin != agentflowv1.ActorOrigin_ACTOR_ORIGIN_UNSPECIFIED ||
		command.Actor.HopCount != 0 || len(command.Actor.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("invalid command actor")
	}
	if command.Payload != nil || command.Preconditions != nil {
		return errors.New("untyped payloads and preconditions are not supported")
	}
	if command.ExpiresAt == nil || command.ExpiresAt.CheckValid() != nil || len(command.ExpiresAt.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("command expiry is required")
	}
	if command.AgentStart != nil {
		expected, err := LifecycleExecutionDeadline(command.ExpiresAt.AsTime(), command.AgentStart.StartupTimeoutMs)
		if err != nil || command.SubmittedRequest == nil || command.ExecutionExpiresAt == nil ||
			command.ExecutionExpiresAt.CheckValid() != nil || len(command.ExecutionExpiresAt.ProtoReflect().GetUnknown()) != 0 ||
			!command.ExecutionExpiresAt.AsTime().Equal(expected) {
			return errors.New("start requires original request and fixed separate execution deadline")
		}
	} else if command.ExecutionExpiresAt != nil || command.AgentStop != nil && command.SubmittedRequest == nil {
		return errors.New("invalid lifecycle expiry or missing original stop request")
	}
	return validTTL(command.Ttl)
}

func IsTerminalCommand(status agentflowv1.CommandStatus) bool {
	switch status {
	case agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED, agentflowv1.CommandStatus_COMMAND_STATUS_FAILED,
		agentflowv1.CommandStatus_COMMAND_STATUS_TIMED_OUT, agentflowv1.CommandStatus_COMMAND_STATUS_REJECTED,
		agentflowv1.CommandStatus_COMMAND_STATUS_INDETERMINATE, agentflowv1.CommandStatus_COMMAND_STATUS_NODE_UNAVAILABLE:
		return true
	default:
		return false
	}
}

func ValidateProbeResult(result *agentflowv1.CommandResult) error {
	if result == nil || !ValidCommandID(result.CommandId) || result.Payload != nil || result.WorkspaceEnsure != nil || result.WorktreeCreate != nil || result.AgentControl != nil || result.SessionEnsure != nil || result.AgentLifecycle != nil || len(result.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("invalid command result")
	}
	valid := false
	switch result.Status {
	case agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED:
		valid = result.Detail == "pong"
	case agentflowv1.CommandStatus_COMMAND_STATUS_TIMED_OUT:
		valid = result.Detail == "deadline_expired"
	case agentflowv1.CommandStatus_COMMAND_STATUS_REJECTED:
		valid = result.Detail == "journal_full"
	case agentflowv1.CommandStatus_COMMAND_STATUS_INDETERMINATE:
		valid = result.Detail == "node_restarted"
	}
	if !valid {
		return errors.New("invalid probe outcome")
	}
	return nil
}
