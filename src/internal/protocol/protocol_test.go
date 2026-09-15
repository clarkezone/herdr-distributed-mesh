package protocol

import (
	"testing"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

func TestNegotiate(t *testing.T) {
	tests := []struct {
		name    string
		remote  *agentflowv1.ProtocolRange
		want    uint32
		wantErr bool
	}{
		{name: "exact", remote: &agentflowv1.ProtocolRange{Minimum: 1, Maximum: 1}, want: 1},
		{name: "overlap", remote: &agentflowv1.ProtocolRange{Minimum: 1, Maximum: 2}, want: 1},
		{name: "future only", remote: &agentflowv1.ProtocolRange{Minimum: 2, Maximum: 3}, wantErr: true},
		{name: "invalid", remote: &agentflowv1.ProtocolRange{Minimum: 2, Maximum: 1}, wantErr: true},
		{name: "missing", remote: nil, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Negotiate(test.remote)
			if test.wantErr {
				if err == nil {
					t.Fatal("Negotiate() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Negotiate() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("Negotiate() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestValidateNodeHelloRejectsController(t *testing.T) {
	err := ValidateNodeHello(&agentflowv1.Hello{
		Protocol:   SupportedRange(),
		InstanceId: "controller",
		Role:       agentflowv1.Role_ROLE_CONTROLLER,
	})
	if err == nil {
		t.Fatal("ValidateNodeHello() error = nil, want error")
	}
}
