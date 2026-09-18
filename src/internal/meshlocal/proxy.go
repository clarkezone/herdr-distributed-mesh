package meshlocal

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/fleetwatch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	maxMessage  = 1024 * 1024
	maxResponse = fleetwatch.MaxSnapshotBytes
)

// Dial connects only to the private gateway. A stopped runtime is an error,
// never an instruction to create another Tailscale identity.
func Dial(ctx context.Context, dir string) (*grpc.ClientConn, error) {
	root, err := privateDir(dir, false)
	if err != nil {
		return nil, err
	}
	connection, err := grpc.NewClient("passthrough:///managed-fleet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxResponse), grpc.MaxCallSendMsgSize(maxMessage)),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return dialIPC(ctx, root) }))
	if err != nil {
		return nil, err
	}
	check, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := pb.NewFleetClient(connection).GetServerInfo(check, &emptypb.Empty{}); err != nil {
		return nil, errors.Join(errors.New("managed mesh IPC unavailable; start the configured runtime (no enrollment attempted)"), err, connection.Close())
	}
	return connection, nil
}

// Descriptor-driven forwarding includes newly generated Fleet unary methods,
// but never exposes NodeControl or accepts identity metadata from local callers.
func newProxy(upstream grpc.ClientConnInterface) (*grpc.Server, error) {
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(pb.Fleet_ServiceDesc.ServiceName))
	if err != nil {
		return nil, err
	}
	fleet, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, errors.New("Fleet protocol descriptor is not a service")
	}
	regular, urgent, watches := make(chan struct{}, 64), make(chan struct{}, 8), make(chan struct{}, 16)
	service := grpc.ServiceDesc{ServiceName: string(fleet.FullName()), HandlerType: (*interface{})(nil)}
	for i := 0; i < fleet.Methods().Len(); i++ {
		method := fleet.Methods().Get(i)
		full := "/" + string(fleet.FullName()) + "/" + string(method.Name())
		if method.IsStreamingClient() {
			return nil, errors.New("Fleet client-streaming RPCs require an explicit gateway contract")
		}
		if method.IsStreamingServer() {
			service.Streams = append(service.Streams, grpc.StreamDesc{StreamName: string(method.Name()), ServerStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					if !acquire(watches) {
						return status.Error(codes.ResourceExhausted, "managed watch capacity reached")
					}
					defer release(watches)
					request := dynamicpb.NewMessage(method.Input())
					if err := stream.RecvMsg(request); err != nil {
						return err
					}
					ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(stream.Context(), metadata.MD{}))
					defer cancel()
					remote, err := upstream.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, full,
						grpc.MaxCallRecvMsgSize(maxResponse), grpc.MaxCallSendMsgSize(maxMessage))
					if err != nil {
						return err
					}
					if err := remote.SendMsg(request); err != nil {
						return err
					}
					if err := remote.CloseSend(); err != nil {
						return err
					}
					for {
						response := dynamicpb.NewMessage(method.Output())
						if err := remote.RecvMsg(response); err != nil {
							if errors.Is(err, io.EOF) {
								return nil
							}
							return err
						}
						if err := stream.SendMsg(response); err != nil {
							return err
						}
					}
				}})
			continue
		}
		service.Methods = append(service.Methods, grpc.MethodDesc{MethodName: string(method.Name()),
			Handler: func(_ any, ctx context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				request := dynamicpb.NewMessage(method.Input())
				if err := decode(request); err != nil {
					return nil, err
				}
				gate := regular
				stopField := method.Input().Fields().ByName("agent_stop")
				if strings.Contains(string(method.Name()), "Stop") || (stopField != nil && request.Has(stopField)) {
					gate = urgent
				}
				if !acquire(gate) {
					return nil, status.Error(codes.ResourceExhausted, "managed request capacity reached")
				}
				defer release(gate)
				ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(ctx, metadata.MD{}), 2*time.Minute)
				defer cancel()
				response := dynamicpb.NewMessage(method.Output())
				err := upstream.Invoke(ctx, full, request, response, grpc.MaxCallRecvMsgSize(maxResponse), grpc.MaxCallSendMsgSize(maxMessage))
				return response, err
			}})
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessage), grpc.MaxSendMsgSize(maxResponse), grpc.MaxConcurrentStreams(128))
	server.RegisterService(&service, struct{}{})
	return server, nil
}

func acquire(gate chan struct{}) bool {
	select {
	case gate <- struct{}{}:
		return true
	default:
		return false
	}
}
func release(gate chan struct{}) { <-gate }

type limitedListener struct {
	net.Listener
	gate chan struct{}
}

func (l limitedListener) Accept() (net.Conn, error) {
	for {
		connection, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if !acquire(l.gate) {
			_ = connection.Close()
			continue
		}
		return &limitedConnection{Conn: connection, release: func() { release(l.gate) }}, nil
	}
}

type limitedConnection struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitedConnection) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
