package server

import (
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

func TestHerdrMetadataFieldsRemainBoundedIdentifiers(t *testing.T) {
	state := readyState(1)
	entity := &pb.HerdrEntity{Id: "p1", AgentStatus: "idle", Provider: "copilot",
		TerminalId: "terminal-1", ProviderSessionId: "provider-session-1", ProjectId: "project-1",
		InteractiveReady: proto.Bool(false)}
	state.Agents = []*pb.HerdrEntity{entity}
	if err := validateHerdrState(state); err != nil {
		t.Fatalf("typed optional metadata rejected: %v", err)
	}

	for _, set := range []func(*pb.HerdrEntity, string){
		func(v *pb.HerdrEntity, value string) { v.Provider = value },
		func(v *pb.HerdrEntity, value string) { v.TerminalId = value },
		func(v *pb.HerdrEntity, value string) { v.ProviderSessionId = value },
		func(v *pb.HerdrEntity, value string) { v.ProjectId = value },
	} {
		for _, value := range []string{`C:\private\path`, "raw diagnostic text", strings.Repeat("a", 129)} {
			copy := proto.Clone(state).(*pb.HerdrState)
			set(copy.Agents[0], value)
			if err := validateHerdrState(copy); err == nil {
				t.Fatal("new metadata field bypassed bounded allowlist")
			}
		}
	}
}

func TestHerdrDisplayMetadataIsValidatedSeparatelyFromIdentity(t *testing.T) {
	state := readyState(1)
	entity := state.Panes[0]
	entity.DisplayName, entity.Directory = "Shell \u754c", `C:\src\space in path`
	if err := validateHerdrState(state); err != nil {
		t.Fatal("configured display metadata rejected", err)
	}
	for _, mutate := range []func(*pb.HerdrEntity){
		func(v *pb.HerdrEntity) { v.DisplayName = strings.Repeat("x", 257) },
		func(v *pb.HerdrEntity) { v.Directory = strings.Repeat("x", 4097) },
		func(v *pb.HerdrEntity) { v.DisplayName = "bad\nname" },
		func(v *pb.HerdrEntity) { v.Directory = "bad\u0085path" },
		func(v *pb.HerdrEntity) { v.Directory = "bad\xff" },
	} {
		copy := proto.Clone(state).(*pb.HerdrState)
		mutate(copy.Panes[0])
		if err := validateHerdrState(copy); err == nil {
			t.Fatal("invalid display metadata accepted")
		}
	}
}
