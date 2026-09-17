package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func TestManagedProjectInvalidRevalidationAcrossStreamsKeepsNodeResponsive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkout")
	config := &pb.ProjectConfig{NodeInstanceId: "test-node", ProjectId: "project",
		Generation: 1, CheckoutPath: path, WorktreeRoot: path}
	options := Options{InstanceID: "test-node", ManagedProjects: projects.NewManaged("test-node"),
		CommandJournalPath: filepath.Join(t.TempDir(), "node.db"), RequiredServerTag: DefaultRequiredServerTag,
		HerdrSocket: filepath.Join(t.TempDir(), "absent.sock"), HeartbeatInterval: 10 * time.Millisecond,
		VerifyServerPeer: func(context.Context, string) error { return nil }}
	api := &scriptedProbeServer{capabilities: []string{protocol.ProbeCapability, protocol.ProjectConfigCapability,
		protocol.HerdrReadCapability, protocol.WorkspaceEnsureCapability, protocol.WorktreeCreateCapability}}
	client := sessionClient(t, api)
	for _, fresh := range []bool{false, true} {
		verified := make(chan error, 1)
		api.script = func(stream probeStream, _ *pb.Hello) (err error) {
			defer func() { verified <- err }()
			apply := func() (*pb.ProjectAck, error) {
				if err := stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_ProjectConfig{ProjectConfig: config}}); err != nil {
					return nil, err
				}
				for {
					envelope, err := stream.Recv()
					if err != nil {
						return nil, err
					}
					if ack := envelope.GetProjectAck(); ack != nil {
						return ack, nil
					}
				}
			}
			ack, err := apply()
			expected := "invalid_path"
			if fresh {
				expected = "configuration_conflict"
			}
			if err != nil || ack.GetStatus() != "invalid" || ack.GetErrorCode() != expected || ack.Generation != 1 {
				return fmt.Errorf("fresh=%t: invalid acknowledgement %v: %w", fresh, ack, err)
			}
			if !fresh {
				if err := os.Mkdir(path, 0700); err != nil {
					return err
				}
				if out, err := exec.Command("git", "-C", path, "init", "--quiet").CombinedOutput(); err != nil {
					return fmt.Errorf("initialize checkout: %s: %w", out, err)
				}
				repeated, err := apply()
				if err != nil || !proto.Equal(ack, repeated) {
					return fmt.Errorf("same-stream observation changed: %v: %w", repeated, err)
				}
				return nil
			}
			if err := sendProbe(stream, managedCommand(1)); err != nil {
				return err
			}
			rejected, err := receiveResult(stream, nil, nil)
			if err != nil || rejected.GetStatus() != pb.CommandStatus_COMMAND_STATUS_REJECTED {
				return fmt.Errorf("invalid project became ready: %v: %w", rejected, err)
			}
			if err := sendProbe(stream, probe()); err != nil {
				return err
			}
			result, err := receiveResult(stream, nil, nil)
			if err != nil || result.GetDetail() != "pong" {
				return fmt.Errorf("node stopped responding after revalidation: %v: %w", result, err)
			}
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, sessionErr := RunSession(ctx, client, options)
		cancel()
		var fatal *journalError
		if errors.As(sessionErr, &fatal) {
			t.Fatalf("revalidation permanently stopped node: %v", fatal)
		}
		select {
		case err := <-verified:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatalf("revalidation script incomplete: %v", sessionErr)
		}
	}
}
