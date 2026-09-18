package protocol

import (
	"errors"
	"time"
)

const (
	AgentStartCommandType        = "agent.start.v1"
	AgentStopCommandType         = "agent.stop.v1"
	AgentLifecycleCapability     = "herdr.agent-lifecycle.v1"
	DefaultAgentStartupTimeoutMs = 30000
	MaxAgentStartupTimeoutMs     = 300000
	MaxLifecycleReceiptBytes     = 4096
	MaxLifecycleEvents           = 8
	LifecycleSetupAllowance      = 30 * time.Second
)

func IsLifecycleCommand(kind string) bool {
	return kind == AgentStartCommandType || kind == AgentStopCommandType
}

func NormalizeAgentStartupTimeout(ms uint32) (uint32, error) {
	if ms == 0 {
		return DefaultAgentStartupTimeoutMs, nil
	}
	if ms <= 3000 || ms > MaxAgentStartupTimeoutMs {
		return 0, errors.New("agent startup timeout must be 3001..300000 milliseconds")
	}
	return ms, nil
}

// LifecycleExecutionDeadline is an execution bound, not permission to dispatch
// after the original TTL. Queued commands always keep their dispatch deadline.
func LifecycleExecutionDeadline(dispatchDeadline time.Time, startupMs uint32) (time.Time, error) {
	ms, err := NormalizeAgentStartupTimeout(startupMs)
	if err != nil {
		return time.Time{}, err
	}
	return dispatchDeadline.Add(time.Duration(ms)*time.Millisecond + LifecycleSetupAllowance), nil
}
