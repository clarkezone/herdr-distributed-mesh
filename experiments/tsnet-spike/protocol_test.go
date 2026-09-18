package main

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestPingRoundTrip(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	registerPingServiceServer(server, pingServer{})
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("Serve() error = %v", err)
		}
	}()
	t.Cleanup(server.Stop)

	connection, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := newPingServiceClient(connection).Ping(ctx)
	if err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	sentAt := time.Now().UTC()
	request, err := encodeMessage(pingMessage{
		ID:     "test-1",
		Kind:   "ping",
		SentAt: sentAt,
		Text:   "hello",
	})
	if err != nil {
		t.Fatalf("encodeMessage() error = %v", err)
	}
	if err := stream.Send(request); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	responseValue, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv() error = %v", err)
	}
	response, err := decodeMessage(responseValue)
	if err != nil {
		t.Fatalf("decodeMessage() error = %v", err)
	}
	if response.ID != "test-1" {
		t.Errorf("response ID = %q, want test-1", response.ID)
	}
	if response.Kind != "pong" {
		t.Errorf("response Kind = %q, want pong", response.Kind)
	}
	if response.Text != "hello" {
		t.Errorf("response Text = %q, want hello", response.Text)
	}

	secondRequest, err := encodeMessage(pingMessage{
		ID:     "test-2",
		Kind:   "ping",
		SentAt: time.Now().UTC(),
		Text:   "same stream",
	})
	if err != nil {
		t.Fatalf("encodeMessage() second error = %v", err)
	}
	if err := stream.Send(secondRequest); err != nil {
		t.Fatalf("Send() second error = %v", err)
	}
	secondResponseValue, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv() second error = %v", err)
	}
	secondResponse, err := decodeMessage(secondResponseValue)
	if err != nil {
		t.Fatalf("decodeMessage() second error = %v", err)
	}
	if secondResponse.ID != "test-2" || secondResponse.Text != "same stream" {
		t.Errorf("second response = %#v, want id test-2 and text same stream", secondResponse)
	}
}

func TestDecodeMessageRejectsInvalidTimestamp(t *testing.T) {
	value, err := encodeMessage(pingMessage{
		ID:     "test-1",
		Kind:   "ping",
		SentAt: time.Now().UTC(),
		Text:   "hello",
	})
	if err != nil {
		t.Fatalf("encodeMessage() error = %v", err)
	}
	value.Fields["sent_at"].Kind = nil

	if _, err := decodeMessage(value); err == nil {
		t.Fatal("decodeMessage() error = nil, want error")
	}
}
