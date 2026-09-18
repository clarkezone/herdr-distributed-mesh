package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFollowPinsTargetReportsGapsAndStopsOnReplacement(t *testing.T) {
	var out bytes.Buffer
	calls := 0
	client := agentClientFixture{query: func(ctx context.Context, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		calls++
		if calls == 1 {
			if request.Kind != pb.AgentQueryKind_AGENT_QUERY_KIND_GET {
				t.Fatal("missing discovery")
			}
		} else if request.Target.TerminalId != "term_original" || request.Target.AgentSessionId != "session-original" {
			t.Fatal("follow resolved a replacement instead of retaining identity")
		}
		if calls == 4 || calls == 5 {
			return nil, status.Error(codes.Unavailable, "offline")
		}
		if calls == 7 {
			return &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), ErrorCode: "target_changed"}, nil
		}
		result := &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: agentClientView()}
		if request.Kind == pb.AgentQueryKind_AGENT_QUERY_KIND_READ {
			result.Text = "same response"
		}
		return result, nil
	}}
	query := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ,
		Target: &pb.AgentTarget{PaneId: "w1:p1"}, TimeoutMs: 1000}
	err := followAgentWithClient(context.Background(), Options{Output: &out, JSON: true}, client, query, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "target_changed") || calls != 7 {
		t.Fatalf("follow calls=%d error=%v", calls, err)
	}
	decoder := json.NewDecoder(&out)
	var kinds []string
	for {
		var event followEvent
		if err := decoder.Decode(&event); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, event.Type)
	}
	if strings.Join(kinds, ",") != "snapshot,gap,resumed,snapshot,error" {
		t.Fatalf("events=%v", kinds)
	}
	if query.Target.TerminalId != "" {
		t.Fatal("caller selection was mutated")
	}
}

func TestFollowPinsNamedSessionEvenWithTerminalSelection(t *testing.T) {
	for _, alreadyPinned := range []bool{false, true} {
		t.Run(map[bool]string{false: "discover-incarnation", true: "retain-incarnation"}[alreadyPinned], func(t *testing.T) {
			incarnation := strings.Repeat("a", 64)
			target := agentClientView().Target
			target.SessionName = "build"
			if alreadyPinned {
				target.SessionIncarnation = incarnation
			}
			calls, discoveries := 0, 0
			client := agentClientFixture{query: func(_ context.Context, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
				calls++
				if request.Target.TerminalId != target.TerminalId || request.Target.SessionName != "build" {
					t.Fatal("named discovery lost existing target constraints")
				}
				if request.Kind == pb.AgentQueryKind_AGENT_QUERY_KIND_GET {
					discoveries++
					if alreadyPinned || request.Target.SessionIncarnation != "" {
						t.Fatal("rediscovered a pinned session")
					}
					view := agentClientView()
					view.Target.SessionName = "build"
					view.Target.SessionIncarnation = incarnation
					return &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: view}, nil
				}
				if request.Target.SessionIncarnation != incarnation {
					t.Fatal("read proceeded without the discovered incarnation")
				}
				return &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), ErrorCode: "session_replaced"}, nil
			}}
			query := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ, Target: target}
			err := followAgentWithClient(context.Background(), Options{Output: &bytes.Buffer{}, JSON: true}, client, query, time.Millisecond)
			wantDiscoveries := 1
			if alreadyPinned {
				wantDiscoveries = 0
			}
			if err == nil || !strings.Contains(err.Error(), "session_replaced") || discoveries != wantDiscoveries || calls != wantDiscoveries+1 {
				t.Fatalf("discovery/replacement: discoveries=%d calls=%d err=%v", discoveries, calls, err)
			}
			if !alreadyPinned && target.SessionIncarnation != "" {
				t.Fatal("caller target was mutated")
			}
		})
	}
}

func TestFollowNamedSessionGapsRetainIncarnation(t *testing.T) {
	var out bytes.Buffer
	target := agentClientView().Target
	target.SessionName, target.SessionIncarnation = "build", strings.Repeat("b", 64)
	calls := 0
	client := agentClientFixture{query: func(_ context.Context, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		calls++
		if request.Kind != pb.AgentQueryKind_AGENT_QUERY_KIND_READ || request.Target.SessionIncarnation != target.SessionIncarnation {
			t.Fatal("follow rediscovered or changed its session during a gap")
		}
		switch calls {
		case 1:
			return &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), ErrorCode: "session_unavailable"}, nil
		case 2:
			return &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), ErrorCode: "session_manager_unavailable"}, nil
		case 3:
			return nil, status.Error(codes.ResourceExhausted, "busy")
		case 4:
			view := agentClientView()
			view.Target.SessionName, view.Target.SessionIncarnation = target.SessionName, target.SessionIncarnation
			return &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: view, Text: "resumed"}, nil
		default:
			return &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), ErrorCode: "session_replaced"}, nil
		}
	}}
	query := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ, Target: target}
	err := followAgentWithClient(context.Background(), Options{Output: &out, JSON: true}, client, query, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "session_replaced") || calls != 5 {
		t.Fatalf("gap recovery failed: calls=%d err=%v", calls, err)
	}
	decoder := json.NewDecoder(&out)
	var kinds []string
	for decoder.More() {
		var event followEvent
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, event.Type)
	}
	if strings.Join(kinds, ",") != "gap,gap,gap,resumed,snapshot,error" {
		t.Fatalf("gap/recovery events=%v", kinds)
	}
}

func TestFollowCancellationNeverSendsControl(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	client := agentClientFixture{query: func(ctx context.Context, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		calls++
		cancel()
		return nil, ctx.Err()
	}, submit: func(context.Context, *pb.SubmitCommandRequest) (*pb.CommandRecord, error) {
		t.Fatal("canceled observer sent a mutation")
		return nil, nil
	}}
	query := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ, Target: agentClientView().Target}
	err := followAgentWithClient(ctx, Options{Output: &bytes.Buffer{}}, client, query, time.Millisecond)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancel = %v, calls %d", err, calls)
	}
}

func TestFollowInvalidResponseStopsAndOutputFailurePropagates(t *testing.T) {
	client := agentClientFixture{query: func(context.Context, *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
		return &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), Agent: agentClientView(), Text: "\x1b[2J"}, nil
	}}
	query := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ, Target: agentClientView().Target}
	var out bytes.Buffer
	if err := followAgentWithClient(context.Background(), Options{Output: &out, JSON: true}, client, query, time.Millisecond); err == nil {
		t.Fatal("unsafe output accepted")
	}
	if strings.Contains(out.String(), "\\u001b") || !strings.Contains(out.String(), "invalid_response") {
		t.Fatalf("unsafe event: %s", &out)
	}
	if err := writeFollowEvent(Options{Output: failedFollowWriter{}, JSON: true}, "gap", "timeout", nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writer failure=%v", err)
	}
}

type failedFollowWriter struct{}

func (failedFollowWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestFollowValidationBeforeTransport(t *testing.T) {
	query := &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ, Target: agentClientView().Target}
	opts := Options{Output: &bytes.Buffer{}, RequiredServerTag: "tag:server"}
	if err := FollowAgent(context.Background(), opts, query, time.Second); err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("unbounded follow=%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := FollowAgent(ctx, opts, query, time.Millisecond); err == nil {
		t.Fatal("unbounded polling rate accepted")
	}
	query.Kind = pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT
	if err := FollowAgent(ctx, opts, query, time.Second); err == nil || !strings.Contains(err.Error(), "only agent read") {
		t.Fatalf("wait follow=%v", err)
	}
}
