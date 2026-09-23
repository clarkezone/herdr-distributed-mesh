package control

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

func TestProjectPagesProduceOneCompleteJSONObject(t *testing.T) {
	var out bytes.Buffer
	calls := 0
	client := projectClient{list: func(request *pb.ListProjectsRequest) (*pb.ProjectList, error) {
		calls++
		next := ""
		if calls == 1 {
			next = "next"
		} else if request.PageToken != "next" {
			t.Fatal("missing continuation")
		}
		return &pb.ProjectList{Projects: []*pb.ProjectRecord{projectFixture()}, NextPageToken: next}, nil
	}}
	request := &pb.ListProjectsRequest{NodeInstanceId: "node-1"}
	if err := projectsWithClient(context.Background(), Options{JSON: true, Output: &out}, client, request); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Projects []json.RawMessage `json:"projects"`
	}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || len(result.Projects) != 2 || calls != 2 {
		t.Fatalf("result: %s %v", out.String(), err)
	}
	if request.PageToken != "" {
		t.Fatal("caller request mutated")
	}
}

func TestProjectRepeatedContinuationFailsExplicitly(t *testing.T) {
	client := projectClient{list: func(*pb.ListProjectsRequest) (*pb.ProjectList, error) {
		return &pb.ProjectList{Projects: []*pb.ProjectRecord{projectFixture()}, NextPageToken: "same"}, nil
	}}
	var out bytes.Buffer
	if err := projectsWithClient(context.Background(), Options{Output: &out}, client, &pb.ListProjectsRequest{}); err == nil || out.Len() != 0 {
		t.Fatal("invalid continuation produced success-shaped output")
	}
}
