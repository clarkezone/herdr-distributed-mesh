package protocol

import (
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testProbe() *agentflowv1.Command {
	return &agentflowv1.Command{CommandId: NewCommandID(), CommandType: ProbeCommandType, TargetId: "node-1",
		IdempotencyKey: "request-1", Actor: &agentflowv1.Actor{ActorId: "client-1", Role: agentflowv1.Role_ROLE_CONTROLLER},
		Ttl: durationpb.New(DefaultCommandTTL), ExpiresAt: timestamppb.New(time.Now().Add(DefaultCommandTTL))}
}

func TestProbeRequestDeadlineAndAllowlist(t *testing.T) {
	request := &agentflowv1.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "request-1", CommandType: ProbeCommandType}
	normalized, err := NormalizeProbeRequest(request)
	if err != nil || normalized.Ttl.AsDuration() != DefaultCommandTTL || request.Ttl != nil {
		t.Fatal("default TTL missing or input mutated")
	}
	for _, ttl := range []time.Duration{time.Nanosecond, MaxCommandTTL} {
		request.Ttl = durationpb.New(ttl)
		if _, err := NormalizeProbeRequest(request); err != nil {
			t.Fatalf("valid boundary %s rejected: %v", ttl, err)
		}
	}
	for _, ttl := range []time.Duration{0, -time.Second, MaxCommandTTL + time.Nanosecond} {
		request.Ttl = durationpb.New(ttl)
		if _, err := NormalizeProbeRequest(request); err == nil {
			t.Fatalf("invalid boundary %s accepted", ttl)
		}
	}
	request.Ttl = nil
	request.CommandType = "workspace.create"
	if _, err := NormalizeProbeRequest(request); err == nil {
		t.Fatal("mutation type accepted")
	}
	request.CommandType = ProbeCommandType
	request.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1})
	if _, err := NormalizeProbeRequest(request); err == nil {
		t.Fatal("unknown request fields accepted")
	}
}

func TestNodeProbeRejectsEverythingOutsideContract(t *testing.T) {
	for name, mutate := range map[string]func(*agentflowv1.Command){
		"mutation":      func(c *agentflowv1.Command) { c.CommandType = "workspace.create" },
		"wrong target":  func(c *agentflowv1.Command) { c.TargetId = "other-node" },
		"missing actor": func(c *agentflowv1.Command) { c.Actor = nil },
		"wrong role":    func(c *agentflowv1.Command) { c.Actor.Role = agentflowv1.Role_ROLE_NODE },
		"origin spoof":  func(c *agentflowv1.Command) { c.Actor.Origin = agentflowv1.ActorOrigin_ACTOR_ORIGIN_HUMAN },
		"fan out":       func(c *agentflowv1.Command) { c.Actor.HopCount = 1 },
		"payload":       func(c *agentflowv1.Command) { c.Payload = &structpb.Struct{} },
		"preconditions": func(c *agentflowv1.Command) { c.Preconditions = &structpb.Struct{} },
		"expiry":        func(c *agentflowv1.Command) { c.ExpiresAt = nil },
		"TTL":           func(c *agentflowv1.Command) { c.Ttl = durationpb.New(MaxCommandTTL + 1) },
		"unknown":       func(c *agentflowv1.Command) { c.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			c := testProbe()
			mutate(c)
			if err := ValidateProbeCommand(c, "node-1"); err == nil {
				t.Fatal("unsafe command accepted")
			}
		})
	}
	if err := ValidateProbeCommand(testProbe(), "node-1"); err != nil {
		t.Fatal(err)
	}
}

func TestProbeResultsAreTypedAndBounded(t *testing.T) {
	result := &agentflowv1.CommandResult{CommandId: NewCommandID(), Status: agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "pong"}
	if err := ValidateProbeResult(result); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*agentflowv1.CommandResult){
		func(r *agentflowv1.CommandResult) { r.Detail = "raw private output" },
		func(r *agentflowv1.CommandResult) { r.Payload = &structpb.Struct{} },
		func(r *agentflowv1.CommandResult) { r.Status = agentflowv1.CommandStatus_COMMAND_STATUS_RUNNING },
	} {
		copy := proto.Clone(result).(*agentflowv1.CommandResult)
		change(copy)
		if err := ValidateProbeResult(copy); err == nil {
			t.Fatal("unsafe result accepted")
		}
	}
}
