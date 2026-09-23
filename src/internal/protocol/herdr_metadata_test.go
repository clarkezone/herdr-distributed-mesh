package protocol

import (
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestHerdrEntityInteractiveReadyPreservesPresence(t *testing.T) {
	for _, ready := range []*bool{nil, proto.Bool(false), proto.Bool(true)} {
		entity := &pb.HerdrEntity{Id: "p1", Provider: "copilot", TerminalId: "terminal-1",
			ProviderSessionId: "provider-session-1", ProjectId: "project-1", InteractiveReady: ready}
		data, err := proto.Marshal(entity)
		if err != nil {
			t.Fatal(err)
		}
		var decoded pb.HerdrEntity
		if err := proto.Unmarshal(data, &decoded); err != nil || !proto.Equal(entity, &decoded) {
			t.Fatalf("metadata or readiness presence lost: %v %v", &decoded, err)
		}
		data, err = (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(&decoded)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), `"interactive_ready"`) != (ready != nil) {
			t.Fatalf("unreported readiness confused with false: %s", data)
		}
	}
}
