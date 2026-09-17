package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

const pingServiceName = "spike.v1.PingService"

type pingMessage struct {
	ID     string
	Kind   string
	SentAt time.Time
	Text   string
}

func encodeMessage(message pingMessage) (*structpb.Struct, error) {
	if message.ID == "" {
		return nil, errors.New("message id is required")
	}
	if message.Kind != "ping" && message.Kind != "pong" {
		return nil, fmt.Errorf("unsupported message kind %q", message.Kind)
	}
	if message.SentAt.IsZero() {
		return nil, errors.New("sent_at is required")
	}

	return structpb.NewStruct(map[string]any{
		"id":      message.ID,
		"kind":    message.Kind,
		"sent_at": message.SentAt.UTC().Format(time.RFC3339Nano),
		"text":    message.Text,
	})
}

func decodeMessage(value *structpb.Struct) (pingMessage, error) {
	if value == nil {
		return pingMessage{}, errors.New("message is nil")
	}

	message := pingMessage{
		ID:   value.GetFields()["id"].GetStringValue(),
		Kind: value.GetFields()["kind"].GetStringValue(),
		Text: value.GetFields()["text"].GetStringValue(),
	}
	if message.ID == "" {
		return pingMessage{}, errors.New("message id is required")
	}
	if message.Kind != "ping" && message.Kind != "pong" {
		return pingMessage{}, fmt.Errorf("unsupported message kind %q", message.Kind)
	}

	sentAt := value.GetFields()["sent_at"].GetStringValue()
	if sentAt == "" {
		return pingMessage{}, errors.New("sent_at is required")
	}
	var err error
	message.SentAt, err = time.Parse(time.RFC3339Nano, sentAt)
	if err != nil {
		return pingMessage{}, fmt.Errorf("parse sent_at: %w", err)
	}
	return message, nil
}

type pingServiceClient interface {
	Ping(context.Context, ...grpc.CallOption) (pingServicePingClient, error)
}

type pingServicePingClient interface {
	Send(*structpb.Struct) error
	Recv() (*structpb.Struct, error)
	CloseSend() error
	grpc.ClientStream
}

type grpcPingServiceClient struct {
	connection grpc.ClientConnInterface
}

type grpcPingServicePingClient struct {
	grpc.ClientStream
}

func newPingServiceClient(connection grpc.ClientConnInterface) pingServiceClient {
	return &grpcPingServiceClient{connection: connection}
}

func (client *grpcPingServiceClient) Ping(ctx context.Context, options ...grpc.CallOption) (pingServicePingClient, error) {
	stream, err := client.connection.NewStream(ctx, &pingServiceDescription.Streams[0], "/"+pingServiceName+"/Ping", options...)
	if err != nil {
		return nil, err
	}
	return &grpcPingServicePingClient{ClientStream: stream}, nil
}

func (stream *grpcPingServicePingClient) Send(message *structpb.Struct) error {
	return stream.SendMsg(message)
}

func (stream *grpcPingServicePingClient) Recv() (*structpb.Struct, error) {
	message := new(structpb.Struct)
	if err := stream.RecvMsg(message); err != nil {
		return nil, err
	}
	return message, nil
}

type pingServiceServer interface {
	Ping(pingServicePingServer) error
}

type pingServicePingServer interface {
	Send(*structpb.Struct) error
	Recv() (*structpb.Struct, error)
	grpc.ServerStream
}

type grpcPingServicePingServer struct {
	grpc.ServerStream
}

func (stream *grpcPingServicePingServer) Send(message *structpb.Struct) error {
	return stream.SendMsg(message)
}

func (stream *grpcPingServicePingServer) Recv() (*structpb.Struct, error) {
	message := new(structpb.Struct)
	if err := stream.RecvMsg(message); err != nil {
		return nil, err
	}
	return message, nil
}

func registerPingServiceServer(registrar grpc.ServiceRegistrar, server pingServiceServer) {
	registrar.RegisterService(&pingServiceDescription, server)
}

func pingServiceHandler(server any, stream grpc.ServerStream) error {
	return server.(pingServiceServer).Ping(&grpcPingServicePingServer{ServerStream: stream})
}

var pingServiceDescription = grpc.ServiceDesc{
	ServiceName: pingServiceName,
	HandlerType: (*pingServiceServer)(nil),
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Ping",
			Handler:       pingServiceHandler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
}
