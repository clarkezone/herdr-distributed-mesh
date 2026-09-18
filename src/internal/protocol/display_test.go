package protocol

import (
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

func TestDisplayMetadataBoundsAndRoundtrip(t *testing.T) {
	for _, test := range []struct {
		name, directory string
		valid           bool
	}{
		{"", "", true},
		{"Review <one> & two", `C:\src\space in path`, true},
		{strings.Repeat("\u754c", 85), "/src/\u754c", true},
		{strings.Repeat("n", MaxDisplayNameBytes), strings.Repeat("p", MaxDirectoryBytes), true},
		{strings.Repeat("n", MaxDisplayNameBytes+1), "", false},
		{strings.Repeat("\u754c", 86), "", false},
		{"", strings.Repeat("p", MaxDirectoryBytes+1), false},
		{"name\nother", "", false},
		{"", "/path\tother", false},
		{"bad\x7f", "", false},
		{"", "/path\u0085other", false},
		{"bad\xff", "", false},
	} {
		if ValidDisplayMetadata(test.name, test.directory) != test.valid {
			t.Fatalf("unexpected display metadata validation: %+v", test)
		}
	}
	entity := &pb.HerdrEntity{Id: "w1", DisplayName: "Project \u754c", Directory: `C:\src\demo`}
	data, err := proto.Marshal(entity)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pb.HerdrEntity
	if err := proto.Unmarshal(data, &decoded); err != nil || !proto.Equal(entity, &decoded) {
		t.Fatalf("display metadata lost in protocol: %v", err)
	}
	view := agentTestView()
	view.DisplayName, view.Directory = entity.DisplayName, entity.Directory
	if err := ValidateAgentView(view); err != nil {
		t.Fatal(err)
	}
	view.Directory = "bad\npath"
	if err := ValidateAgentView(view); err == nil {
		t.Fatal("agent query bypassed display bounds")
	}
}
