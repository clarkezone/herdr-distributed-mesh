package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func agentCommand() *pb.Command {
	command := probe()
	command.CommandType = protocol.AgentControlCommandType
	command.AgentControl = &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT,
		Target: &pb.AgentTarget{PaneId: "p1", TerminalId: "t1", AgentSessionId: "a1"}, Text: "test prompt"}
	return command
}

func readyAgentHandler(t *testing.T) *commandHandler {
	t.Helper()
	h := &commandHandler{journal: testJournal(t), nodeID: "test-node", agentNegotiated: true, verifyCoordinator: func(context.Context) error { return nil }}
	h.agentBaseline.Store(&pb.HerdrState{Status: "ready", ObservedAt: timestamppb.Now()})
	return h
}

func TestAgentControlDurableReplayAndExactPayload(t *testing.T) {
	h := readyAgentHandler(t)
	calls := 0
	h.controlAgent = func(_ context.Context, _ herdr.Config, c *pb.AgentControl) (*pb.AgentControlResult, error) {
		calls++
		return &pb.AgentControlResult{Target: proto.Clone(c.Target).(*pb.AgentTarget), ObservedStatus: "working", StateChangeSeq: 2}, nil
	}
	for _, action := range []pb.AgentControlAction{pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT, pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT} {
		command := agentCommand()
		command.AgentControl.Action = action
		if action != pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT {
			command.AgentControl.Text = ""
		}
		if action == pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT {
			command.AgentControl.Keys = []string{"enter"}
		}
		first, err := h.handle(context.Background(), command, time.Now())
		if err != nil || first.Status != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
			t.Fatalf("control: %v %v", first, err)
		}
		h.verifyCoordinator = func(context.Context) error { return errors.New("revoked") }
		second, err := h.handle(context.Background(), command, time.Now())
		if err != nil || !proto.Equal(first, second) {
			t.Fatalf("replay: %v", err)
		}
		changed := proto.Clone(command).(*pb.Command)
		changed.AgentControl.Target.AgentSessionId = "other"
		if _, err := h.handle(context.Background(), changed, time.Now()); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("changed target: %v", err)
		}
		h.verifyCoordinator = func(context.Context) error { return nil }
	}
	if calls != 3 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestAgentControlReadinessAndPeerRevalidation(t *testing.T) {
	for name, mutate := range map[string]func(*commandHandler){
		"baseline": func(h *commandHandler) { h.agentBaseline.Store(nil) },
		"stale": func(h *commandHandler) {
			h.agentBaseline.Store(&pb.HerdrState{Status: "ready", ObservedAt: timestamppb.New(time.Now().Add(-time.Minute))})
		},
		"verifier": func(h *commandHandler) { h.verifyCoordinator = nil },
		"revoked": func(h *commandHandler) {
			h.verifyCoordinator = func(context.Context) error { return errors.New("revoked") }
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := readyAgentHandler(t)
			mutate(h)
			h.controlAgent = func(context.Context, herdr.Config, *pb.AgentControl) (*pb.AgentControlResult, error) {
				t.Fatal("unauthorized effect")
				return nil, nil
			}
			result, err := h.handle(context.Background(), agentCommand(), time.Now())
			if err != nil || result.Status != pb.CommandStatus_COMMAND_STATUS_REJECTED {
				t.Fatalf("unready result: %v %v", result, err)
			}
		})
	}
	h := readyAgentHandler(t)
	h.agentNegotiated = false
	if _, err := h.handle(context.Background(), agentCommand(), time.Now()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unnegotiated: %v", err)
	}
}

func TestAgentQueryCancellationDeadlineCapacityAndJoin(t *testing.T) {
	h := readyAgentHandler(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var running atomic.Int32
	started := make(chan struct{}, maxAgentQueries)
	q := newAgentQueries(ctx, Options{InstanceID: "test-node", QueryAgent: func(ctx context.Context, _ herdr.Config, _ *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		running.Add(1)
		defer running.Add(-1)
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}}, h)
	query := func() *pb.AgentQuery {
		return &pb.AgentQuery{QueryId: protocol.NewCommandID(), Request: &pb.AgentQueryRequest{
			NodeInstanceId: "test-node", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT, Target: agentCommand().AgentControl.Target, TimeoutMs: 5000}, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}
	}
	var first *pb.AgentQuery
	for range maxAgentQueries {
		value := query()
		if first == nil {
			first = value
		}
		if err := q.start(value); err != nil {
			t.Fatal(err)
		}
		<-started
	}
	overloaded := query()
	if err := q.start(overloaded); err != nil {
		t.Fatal(err)
	}
	if result := <-q.results; result.ErrorCode != "overloaded" {
		t.Fatal("capacity not bounded")
	}
	if err := q.cancelQuery(&pb.AgentQueryCancel{QueryId: first.QueryId}); err != nil {
		t.Fatal(err)
	}
	if result := <-q.results; result.QueryId != first.QueryId || result.ErrorCode != "canceled" {
		t.Fatal("cancellation not propagated")
	}
	cancel()
	q.join()
	if running.Load() != 0 || len(q.active) != 0 {
		t.Fatal("query executors not joined")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	q = newAgentQueries(ctx, Options{InstanceID: "test-node", QueryAgent: func(ctx context.Context, _ herdr.Config, _ *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}, h)
	first = query()
	first.Request.TimeoutMs = 20
	if err := q.start(first); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-q.results:
		if result.ErrorCode != "timeout" {
			t.Fatal("deadline not mapped")
		}
	case <-time.After(time.Second):
		t.Fatal("request timeout ignored")
	}
	q.join()
}

func TestAgentControlInvalidExecutorResultIsInputConflict(t *testing.T) {
	h := readyAgentHandler(t)
	h.controlAgent = func(context.Context, herdr.Config, *pb.AgentControl) (*pb.AgentControlResult, error) {
		return &pb.AgentControlResult{Target: &pb.AgentTarget{PaneId: "other", TerminalId: "t1"}, ObservedStatus: "working"}, nil
	}

	_, err := h.handle(context.Background(), agentCommand(), time.Now())
	var journalFailure *journalError
	if status.Code(err) != codes.InvalidArgument || errors.As(err, &journalFailure) {
		t.Fatalf("invalid executor result poisoned journal: %v", err)
	}
}

func TestAgentControlErrorsAreControlledAndDurable(t *testing.T) {
	for _, test := range []struct {
		err     error
		detail  string
		outcome pb.CommandStatus
	}{
		{herdr.ErrAgentPrecondition, "precondition_failed", pb.CommandStatus_COMMAND_STATUS_REJECTED},
		{herdr.ErrAgentBusy, "agent_busy", pb.CommandStatus_COMMAND_STATUS_REJECTED},
		{herdr.ErrAgentBlocked, "agent_blocked", pb.CommandStatus_COMMAND_STATUS_REJECTED},
		{herdr.ErrAgentChanged, "target_changed", pb.CommandStatus_COMMAND_STATUS_REJECTED},
		{herdr.ErrAgentUnavailable, "herdr_unavailable", pb.CommandStatus_COMMAND_STATUS_REJECTED},
		{herdr.ErrAgentUnsupported, "unsupported", pb.CommandStatus_COMMAND_STATUS_REJECTED},
		{context.DeadlineExceeded, "deadline_expired", pb.CommandStatus_COMMAND_STATUS_TIMED_OUT},
		{errors.Join(herdr.ErrAgentIndeterminate, context.DeadlineExceeded), "herdr_outcome_unknown", pb.CommandStatus_COMMAND_STATUS_INDETERMINATE},
		{errors.New("PRIVATE ADAPTER ERROR"), "herdr_outcome_unknown", pb.CommandStatus_COMMAND_STATUS_INDETERMINATE},
	} {
		t.Run(test.detail, func(t *testing.T) {
			h := readyAgentHandler(t)
			h.controlAgent = func(context.Context, herdr.Config, *pb.AgentControl) (*pb.AgentControlResult, error) {
				return nil, test.err
			}
			command := agentCommand()
			result, err := h.handle(context.Background(), command, time.Now())
			if err != nil || result.Detail != test.detail || result.Status != test.outcome {
				t.Fatalf("mapped result: %v %v", result, err)
			}
			stored, claimed, err := h.journal.Claim(context.Background(), command)
			if err != nil || claimed || !proto.Equal(stored, result) {
				t.Fatal("result not durable")
			}
		})
	}
}

func TestAgentSessionCapabilityOptInAndConfiguration(t *testing.T) {
	for _, mutate := range []func(*Options){
		func(o *Options) { o.CommandJournalPath = "" },
		func(o *Options) { o.HerdrSocket = "" },
		func(o *Options) { o.RequiredServerTag = "" },
	} {
		options := Options{EnableAgentControl: true, CommandJournalPath: filepath.Join(t.TempDir(), "node.db"), HerdrSocket: "absent", RequiredServerTag: DefaultRequiredServerTag}
		mutate(&options)
		if journal, err := openJournal(context.Background(), options); err == nil || journal != nil {
			t.Fatal("incomplete agent configuration accepted")
		}
	}
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			verified := make(chan error, 1)
			api := &scriptedProbeServer{capabilities: []string{protocol.ProbeCapability, protocol.HerdrReadCapability, protocol.AgentControlCapability}}
			api.script = func(stream probeStream, hello *pb.Hello) (err error) {
				defer func() { verified <- err }()
				if slices.Contains(hello.Capabilities, protocol.AgentControlCapability) != enabled {
					return errors.New("agent capability did not follow explicit opt-in")
				}
				for {
					envelope, err := stream.Recv()
					if err != nil {
						return err
					}
					if heartbeat := envelope.GetHeartbeat(); heartbeat != nil {
						if heartbeat.AgentReady {
							return errors.New("agent ready without baseline and verifier")
						}
						return nil
					}
				}
			}
			options := Options{InstanceID: "test-node", EnableAgentControl: enabled, CommandJournalPath: filepath.Join(t.TempDir(), "node.db"),
				RequiredServerTag: DefaultRequiredServerTag, HerdrSocket: filepath.Join(t.TempDir(), "missing.sock")}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := RunSession(ctx, sessionClient(t, api), options)
			select {
			case scriptErr := <-verified:
				if scriptErr != nil {
					t.Fatal(scriptErr)
				}
			default:
				t.Fatalf("script did not finish: %v", err)
			}
		})
	}
	api := &sessionServer{capabilities: []string{protocol.ProbeCapability, protocol.HerdrReadCapability}, messages: make(chan *pb.NodeEnvelope, 1)}
	options := Options{InstanceID: "test-node", EnableAgentControl: true, CommandJournalPath: filepath.Join(t.TempDir(), "node.db"),
		RequiredServerTag: DefaultRequiredServerTag, HerdrSocket: filepath.Join(t.TempDir(), "missing.sock")}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if registered, err := RunSession(ctx, sessionClient(t, api), options); registered || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing server capability accepted: %v", err)
	}
}

func TestAgentQueryValidatesBeforeExecutionAndSanitizesErrors(t *testing.T) {
	h := readyAgentHandler(t)
	var calls atomic.Int32
	q := newAgentQueries(context.Background(), Options{InstanceID: "test-node", QueryAgent: func(context.Context, herdr.Config, *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		calls.Add(1)
		return &pb.AgentQueryResult{Text: "PRIVATE", ErrorCode: "unexpected"}, nil
	}}, h)
	query := &pb.AgentQuery{QueryId: protocol.NewCommandID(), ExpiresAt: timestamppb.New(time.Now().Add(time.Second)), Request: &pb.AgentQueryRequest{
		NodeInstanceId: "test-node", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ, Target: agentCommand().AgentControl.Target, TimeoutMs: 100}}
	for _, mutate := range []func(*pb.AgentQuery){
		func(v *pb.AgentQuery) { v.QueryId = "bad" },
		func(v *pb.AgentQuery) { v.ExpiresAt = nil },
		func(v *pb.AgentQuery) { v.Request.NodeInstanceId = "other-node" },
		func(v *pb.AgentQuery) { v.Request.TimeoutMs = 300001 },
	} {
		bad := proto.Clone(query).(*pb.AgentQuery)
		mutate(bad)
		if err := q.start(bad); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid query accepted: %v", err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid query executed")
	}
	if err := q.start(query); err != nil {
		t.Fatal(err)
	}
	result := <-q.results
	q.join()
	if result.ErrorCode != "invalid_request" || result.Text != "" || result.Agent != nil || result.Truncated {
		t.Fatal("untrusted executor result leaked")
	}
	if pending, err := h.journal.PendingResults(context.Background()); err != nil || len(pending) != 0 {
		t.Fatal("query used command journal")
	}
}

func TestAgentReceiptReplaysAfterReopenWithControlDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")
	journal, err := state.OpenNodeJournal(context.Background(), path, "test-node")
	if err != nil {
		t.Fatal(err)
	}
	command := agentCommand()
	if _, claimed, err := journal.Claim(context.Background(), command); err != nil || !claimed {
		t.Fatalf("claim: %v", err)
	}
	result := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "agent_prompt_sent",
		AgentControl: &pb.AgentControlResult{Target: command.AgentControl.Target, ObservedStatus: "working"}}
	if err := journal.Complete(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	verified := make(chan error, 1)
	api := &scriptedProbeServer{capabilities: []string{protocol.ProbeCapability}}
	api.script = func(stream probeStream, _ *pb.Hello) (err error) {
		defer func() { verified <- err }()
		actual, err := receiveResult(stream, nil, nil)
		if err != nil {
			return err
		}
		if !proto.Equal(actual, result) {
			return errors.New("reopened agent receipt changed")
		}
		return stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_CommandAck{CommandAck: &pb.CommandAck{CommandId: result.CommandId, Status: result.Status}}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = RunSession(ctx, sessionClient(t, api), Options{InstanceID: "test-node", CommandJournalPath: path, RequiredServerTag: DefaultRequiredServerTag})
	select {
	case scriptErr := <-verified:
		if scriptErr != nil {
			t.Fatal(scriptErr)
		}
	default:
		t.Fatalf("replay incomplete: %v", err)
	}
}
