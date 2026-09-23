package control

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type retryFleetClient struct {
	calls int
}

func (client *retryFleetClient) GetServerInfo(
	context.Context,
	*emptypb.Empty,
	...grpc.CallOption,
) (*agentflowv1.ServerInfo, error) {
	client.calls++
	if client.calls == 1 {
		return nil, status.Error(codes.Unavailable, "starting")
	}
	return &agentflowv1.ServerInfo{InstanceId: "server-1"}, nil
}

func TestGetServerInfoRetriesUnavailable(t *testing.T) {
	client := &retryFleetClient{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	info, err := getServerInfo(ctx, client)
	if err != nil {
		t.Fatalf("getServerInfo() error = %v", err)
	}
	if info.InstanceId != "server-1" || client.calls != 2 {
		t.Fatalf("getServerInfo() info=%v calls=%d, want server-1 after 2 calls", info, client.calls)
	}
}

type deniedFleetClient struct{}

func (*deniedFleetClient) GetServerInfo(
	context.Context,
	*emptypb.Empty,
	...grpc.CallOption,
) (*agentflowv1.ServerInfo, error) {
	return nil, status.Error(codes.PermissionDenied, "wrong role")
}

func TestGetServerInfoDoesNotRetryPermissionDenied(t *testing.T) {
	_, err := getServerInfo(context.Background(), &deniedFleetClient{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("getServerInfo() code = %s, want %s", status.Code(err), codes.PermissionDenied)
	}
}

type serverInfoFixture struct {
	agentflowv1.FleetClient
	info *agentflowv1.ServerInfo
}

func (f serverInfoFixture) GetServerInfo(context.Context, *emptypb.Empty, ...grpc.CallOption) (*agentflowv1.ServerInfo, error) {
	return f.info, nil
}

func TestServerInfoHumanAndJSONContracts(t *testing.T) {
	info := &agentflowv1.ServerInfo{InstanceId: "coordinator-1", ImplementationVersion: "1.2.3",
		Protocol: protocol.SupportedRange(), Capabilities: []string{"fleet.nodes.v1"}}
	expiry := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	local := transport.SelfStatus{StableID: "local-id", DNSName: "client.example",
		IPs: []string{"100.64.0.1"}, Tags: []string{"tag:client"}, StateDir: `C:\private\client`, KeyExpiry: &expiry}
	for _, diagnose := range []bool{false, true} {
		for _, asJSON := range []bool{false, true} {
			var out bytes.Buffer
			options := Options{Output: &out, JSON: asJSON, Diagnose: diagnose, ServerAddress: "coordinator:50052",
				Transport: transport.Config{Tags: []string{"tag:client"}}}
			if err := writeServerInfo(context.Background(), options, serverInfoFixture{info: info}, local); err != nil {
				t.Fatal(err)
			}
			if asJSON {
				expected := map[string]any{"server": map[string]any{
					"capabilities": info.Capabilities, "implementation_version": info.ImplementationVersion,
					"instance_id": info.InstanceId, "protocol_maximum": info.Protocol.Maximum,
					"protocol_minimum": info.Protocol.Minimum, "protocol_selected": protocol.MaximumVersion,
				}}
				if diagnose {
					expected["local_tsnet"] = local
				}
				data, err := json.Marshal(expected)
				if err != nil {
					t.Fatal(err)
				}
				var got, want any
				if json.Unmarshal(out.Bytes(), &got) != nil || json.Unmarshal(data, &want) != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("JSON changed or contains prose: %s", &out)
				}
			} else {
				for _, text := range []string{"Coordinator: reachable; compatible with this client", "Address: coordinator:50052",
					"Version: 1.2.3", "Coordinator ID: coordinator-1", "Execution readiness: not checked here"} {
					if !strings.Contains(out.String(), text) {
						t.Fatalf("missing %q: %s", text, &out)
					}
				}
				if strings.Contains(out.String(), "Capability codes:") != diagnose || strings.Contains(out.String(), "Protocol:") != diagnose {
					t.Fatalf("diagnostic details in wrong view: %s", &out)
				}
				if diagnose {
					for _, text := range []string{"Local Tailscale ID: local-id", "Local DNS name: client.example",
						"Local IP addresses: 100.64.0.1", "Assigned role tags: tag:client", "Requested role tags: tag:client",
						"Enrollment key expiry: 2027-01-02T03:04:05Z", "Local transport state directory: C:\\private\\client",
						"Local transport health: no issues reported"} {
						if !strings.Contains(out.String(), text) {
							t.Fatalf("missing diagnostic %q: %s", text, &out)
						}
					}
				}
			}
			options.Output = failedWriter{}
			if err := writeServerInfo(context.Background(), options, serverInfoFixture{info: info}, local); err == nil {
				t.Fatal("output failure hidden")
			}
		}
	}
}
