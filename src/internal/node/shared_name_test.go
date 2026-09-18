package node

import (
	"context"
	"io"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc"
)

type nameHelloStream struct {
	grpc.ClientStream
	hello *pb.Hello
}

func (s *nameHelloStream) Send(envelope *pb.NodeEnvelope) error {
	s.hello = envelope.GetHello()
	return nil
}
func (*nameHelloStream) Recv() (*pb.NodeEnvelope, error) { return nil, io.EOF }

type nameHelloClient struct {
	pb.NodeControlClient
	stream *nameHelloStream
}

func (c nameHelloClient) Connect(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[pb.NodeEnvelope, pb.NodeEnvelope], error) {
	return c.stream, nil
}

func TestNodeHelloUsesOptionalLogicalNameWithoutChangingInstanceIdentity(t *testing.T) {
	for _, name := range []string{"desktop", "laptop", ""} {
		stream := &nameHelloStream{}
		_, _ = RunSession(context.Background(), nameHelloClient{stream: stream}, Options{InstanceID: "independent-node-id", Name: name})
		if stream.hello == nil || stream.hello.Hostname != name || stream.hello.InstanceId != "independent-node-id" {
			t.Fatalf("logical name or legacy identity changed: %+v", stream.hello)
		}
	}
}
