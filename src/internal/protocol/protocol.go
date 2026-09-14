package protocol

import (
	"errors"
	"fmt"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

const (
	MinimumVersion uint32 = 1
	MaximumVersion uint32 = 1
)

var ServerCapabilities = []string{
	"node.heartbeat.v1",
	"protocol.negotiation.v1",
}

func SupportedRange() *agentflowv1.ProtocolRange {
	return &agentflowv1.ProtocolRange{
		Minimum: MinimumVersion,
		Maximum: MaximumVersion,
	}
}

func Negotiate(remote *agentflowv1.ProtocolRange) (uint32, error) {
	if remote == nil {
		return 0, errors.New("protocol range is required")
	}
	if remote.Minimum == 0 || remote.Maximum == 0 {
		return 0, errors.New("protocol versions must be greater than zero")
	}
	if remote.Minimum > remote.Maximum {
		return 0, fmt.Errorf("invalid protocol range %d-%d", remote.Minimum, remote.Maximum)
	}

	minimum := max(remote.Minimum, MinimumVersion)
	maximum := min(remote.Maximum, MaximumVersion)
	if minimum > maximum {
		return 0, fmt.Errorf(
			"no compatible protocol version: local=%d-%d remote=%d-%d",
			MinimumVersion,
			MaximumVersion,
			remote.Minimum,
			remote.Maximum,
		)
	}
	return maximum, nil
}

func ValidateNodeHello(hello *agentflowv1.Hello) error {
	if hello == nil {
		return errors.New("hello is required")
	}
	if hello.InstanceId == "" {
		return errors.New("instance ID is required")
	}
	if hello.Role != agentflowv1.Role_ROLE_NODE {
		return fmt.Errorf("role %s is not permitted on the node control stream", hello.Role)
	}
	_, err := Negotiate(hello.Protocol)
	return err
}
