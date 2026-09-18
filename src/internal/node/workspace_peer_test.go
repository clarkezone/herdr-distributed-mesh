package node

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestWorkspacePeerVerifierUsesFreshWhoIsAndExactTag(t *testing.T) {
	calls := 0
	tags := []string{DefaultRequiredServerTag}
	var identifyErr error
	verify := workspacePeerTagVerifier(func(_ context.Context, address string) (transport.PeerIdentity, error) {
		calls++
		if address != "100.64.0.1:50052" {
			t.Errorf("WhoIs received substitute identity: %q", address)
		}
		return transport.PeerIdentity{Tags: tags, Name: "ignored-dns-name"}, identifyErr
	}, DefaultRequiredServerTag)
	if err := verify(context.Background(), "100.64.0.1:50052"); err != nil {
		t.Fatal(err)
	}
	tags = []string{DefaultRequiredServerTag + "-other"}
	if err := verify(context.Background(), "100.64.0.1:50052"); err == nil {
		t.Fatal("revoked tag was cached or prefix matched")
	}
	tags = []string{DefaultRequiredServerTag}
	identifyErr = errors.New("WhoIs unavailable")
	if err := verify(context.Background(), "100.64.0.1:50052"); err == nil {
		t.Fatal("WhoIs failure accepted")
	}
	if calls != 3 {
		t.Fatalf("WhoIs calls=%d", calls)
	}
	if err := workspacePeerTagVerifier(nil, DefaultRequiredServerTag)(context.Background(), "peer"); err == nil {
		t.Fatal("missing WhoIs accepted")
	}
	if err := workspacePeerTagVerifier(nil, "")(context.Background(), ""); err == nil {
		t.Fatal("missing peer and tag accepted")
	}
}

func TestWorkspaceStreamPeerFailsClosedWhenUnavailable(t *testing.T) {
	for _, streamContext := range []context.Context{
		context.Background(),
		peer.NewContext(context.Background(), &peer.Peer{}),
	} {
		verify := streamPeerVerifier(streamContext, func(context.Context, string) error {
			t.Fatal("missing actual stream address reached verifier")
			return nil
		})
		if err := verify(context.Background()); err == nil {
			t.Fatal("unavailable stream peer accepted")
		}
	}
	streamContext := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("100.64.0.1"), Port: 50052}})
	if err := streamPeerVerifier(streamContext, nil)(context.Background()); err == nil {
		t.Fatal("missing injected verifier accepted")
	}
}

func TestWorkspaceFreshVerificationIsDurableBoundedAndReplaySafe(t *testing.T) {
	claimed, verified := false, false
	calls := 0
	var verifyErr error
	journal := observedJournal{commandJournal: testJournal(t), afterClaim: func() { claimed = true }}
	handler := &commandHandler{nodeID: "test-node", journal: journal,
		workspacePolicy: workspacePolicy(t), workspaceNegotiated: true,
		verifyCoordinator: func(ctx context.Context) error {
			calls++
			deadline, ok := ctx.Deadline()
			if !claimed || !ok || time.Until(deadline) > workspacePeerVerificationTimeout {
				t.Fatal("verification ran without a durable claim or bounded deadline")
			}
			verified = true
			return verifyErr
		},
		ensureWorkspace: func(_ context.Context, _ herdr.Config, binding projects.Binding) (*pb.WorkspaceEnsureResult, error) {
			if !verified || verifyErr != nil {
				t.Fatal("effect ran without fresh authorization")
			}
			return workspaceSuccess(binding, true), nil
		}}
	command := workspaceCommand()
	result, err := handler.handle(context.Background(), command, time.Now())
	if err != nil || result.GetDetail() != "workspace_created" {
		t.Fatalf("verified effect failed: %v %v", result, err)
	}
	verifyErr = errors.New("coordinator tag revoked")
	replayed, err := handler.handle(context.Background(), command, time.Now())
	if err != nil || !proto.Equal(result, replayed) || calls != 1 {
		t.Fatalf("safe replay reverified or changed: %v %v calls=%d", replayed, err, calls)
	}
	rejected := workspaceCommand()
	result, err = handler.handle(context.Background(), rejected, time.Now())
	if err != nil || result.GetDetail() != "precondition_failed" || calls != 2 {
		t.Fatalf("revoked coordinator executed: %v %v calls=%d", result, err, calls)
	}
	stored, claimedAgain, err := journal.Claim(context.Background(), rejected)
	if err != nil || claimedAgain || !proto.Equal(result, stored) {
		t.Fatalf("authorization rejection not durable: %v %v", stored, err)
	}
}

func TestWorkspaceVerifierUnavailableOrDeadlineExpiredNeverExecutes(t *testing.T) {
	for _, test := range []string{"missing", "no-peer", "deadline"} {
		t.Run(test, func(t *testing.T) {
			command := workspaceCommand()
			handler := &commandHandler{nodeID: "test-node", journal: testJournal(t),
				workspacePolicy: workspacePolicy(t), workspaceNegotiated: true,
				ensureWorkspace: func(context.Context, herdr.Config, projects.Binding) (*pb.WorkspaceEnsureResult, error) {
					t.Fatal("unverified command executed")
					return nil, nil
				}}
			detail := "precondition_failed"
			if test == "no-peer" {
				handler.verifyCoordinator = streamPeerVerifier(context.Background(), func(context.Context, string) error {
					t.Fatal("missing peer reached verifier")
					return nil
				})
			}
			if test == "deadline" {
				command.Ttl = durationpb.New(30 * time.Millisecond)
				detail = "deadline_expired"
				handler.verifyCoordinator = func(ctx context.Context) error {
					<-ctx.Done()
					// A verifier returning nil after its deadline is not authorization.
					return nil
				}
			}
			result, err := handler.handle(context.Background(), command, time.Now())
			if err != nil || result.GetDetail() != detail {
				t.Fatalf("verification did not fail closed: %v %v", result, err)
			}
		})
	}
}
