package meshmcp

import (
	"context"
	"reflect"
	"testing"
)

func TestMixedCaseSessionToolPreservesName(t *testing.T) {
	want := EnsureHerdrSessionInput{NodeInput: NodeInput{NodeInstanceID: "node-1"},
		HerdrSessionID: "QEI", IdempotencyKey: "request-1"}
	calls := 0
	client := connect(t, func(_ context.Context, op Operation, input any) (any, error) {
		calls++
		if op != EnsureHerdrSession || !reflect.DeepEqual(input, want) {
			t.Errorf("session selector changed: %s, %+v", op, input)
		}
		return map[string]string{"status": "accepted"}, nil
	}, Options{})
	result := callTool(t, client, EnsureHerdrSession, want)
	if result.IsError || calls != 1 {
		t.Fatalf("mixed-case name rejected: %+v, calls=%d", result, calls)
	}
}
