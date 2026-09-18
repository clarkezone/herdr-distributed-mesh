package state

import (
	"strings"
	"testing"
)

func TestLifecycleQuarantineScopeUsesCanonicalNativeIdentity(t *testing.T) {
	incarnation := strings.Repeat("a", 64)
	first, err := lifecycleResourceKey("node", incarnation, "terminal", "term_1")
	if err != nil || len(first) != 64 {
		t.Fatal(err)
	}
	alias, err := lifecycleResourceKey("node", incarnation, "terminal", "term_1")
	if err != nil || first != alias {
		t.Fatal("the same physical target did not converge")
	}
	for _, parts := range [][4]string{
		{"other-node", incarnation, "terminal", "term_1"},
		{"node", strings.Repeat("b", 64), "terminal", "term_1"},
		{"node", incarnation, "workspace", "term_1"},
		{"node", incarnation, "terminal", "term_2"},
	} {
		key, err := lifecycleResourceKey(parts[0], parts[1], parts[2], parts[3])
		if err != nil || key == first {
			t.Fatal("distinct scoped targets collided")
		}
	}
	if _, err := lifecycleResourceKey("node", "", "terminal", "term_1"); err == nil {
		t.Fatal("unresolved native identity was treated as canonical")
	}
}
