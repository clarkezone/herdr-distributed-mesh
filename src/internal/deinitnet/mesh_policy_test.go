package deinitnet

import (
	"bytes"
	"errors"
	"net/http"
	"testing"
)

const meshPolicy = `{"tagowners":{"tag:herdr-mesh-node":[],"tag:herdr-mesh-client":[],"tag:herdr-mesh-server":[],"tag:unrelated":["autogroup:admin"]},"grants":[{"src":["tag:herdr-mesh-node"],"dst":["tag:herdr-mesh-server"],"ip":["tcp:50052"]},{"src":["tag:herdr-mesh-client"],"dst":["tag:herdr-mesh-server"],"ip":["tcp:50052"]},{"src":["*"],"dst":["tag:herdr-mesh-server"],"ip":["tcp:8787"]},{"src":["tag:unrelated"],"dst":["tag:unrelated"],"ip":["tcp:443"]}],"acls":[{"action":"accept","src":["*"],"dst":["*:*"]}]}`
const cleanMeshPolicy = `{"tagowners":{"tag:unrelated":["autogroup:admin"]},"grants":[{"src":["tag:unrelated"],"dst":["tag:unrelated"],"ip":["tcp:443"]}],"acls":[{"action":"accept","src":["*"],"dst":["*:*"]}]}`

func TestMeshPolicyRemovalIncludesPreexistingEntries(t *testing.T) {
	post := exchange{method: http.MethodPost, path: "/tailnet/" + testTailnet + "/acl", status: 200, body: cleanMeshPolicy,
		check: func(t *testing.T, r *http.Request) {
			if r.Header.Get("If-Match") != testETag {
				t.Fatal("missing ETag guard")
			}
			var body bytes.Buffer
			if _, err := body.ReadFrom(r.Body); err != nil {
				t.Fatal(err)
			}
			_, got, err := parsePolicy(body.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			_, want, _ := parsePolicy([]byte(cleanMeshPolicy))
			if !bytes.Equal(got, want) {
				t.Fatal("wrong policy cleanup")
			}
		}}
	unrelated := `{"devices":[{"nodeId":"nOTHER","tags":["tag:unrelated"]}]}`
	c := scriptedClient(t, getPolicy(meshPolicy),
		listDevices(`{"devices":[{"nodeId":"n292kg92CNTRL","tags":["tag:herdr-mesh-server"]},{"nodeId":"nOTHER","tags":["tag:unrelated"]}]}`),
		listDevices(unrelated), post)
	plan, err := c.PrepareMeshPolicyRemoval(t.Context(), testTailnet)
	if err != nil || !plan.HasChanges() {
		t.Fatal(err)
	}
	if err := c.CheckPolicyDependencies(t.Context(), plan, testID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplyPolicy(t.Context(), plan); err != nil {
		t.Fatal(err)
	}
}

func TestMeshPolicyRemovalBlocksOtherMeshRoleDevice(t *testing.T) {
	c := scriptedClient(t, getPolicy(meshPolicy), listDevices(`{"devices":[{"nodeId":"n292kg92CNTRL","tags":["tag:herdr-mesh-server"]},{"nodeId":"nOTHER","tags":["tag:herdr-mesh-node"]}]}`))
	plan, err := c.PrepareMeshPolicyRemoval(t.Context(), testTailnet)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CheckPolicyDependencies(t.Context(), plan, testID); !errors.Is(err, ErrPolicyInUse) {
		t.Fatal(err)
	}
}

func TestStateFreeMeshPolicyRemovalAllowsUnrelatedDevices(t *testing.T) {
	unrelated := listDevices(`{"devices":[{"nodeId":"nOTHER","tags":["tag:unrelated"]},{"nodeId":"nUNTAGGED"}]}`)
	c := scriptedClient(t, getPolicy(meshPolicy), unrelated, unrelated,
		exchange{method: http.MethodPost, path: "/tailnet/" + testTailnet + "/acl", status: 200, body: cleanMeshPolicy})
	plan, err := c.PrepareMeshPolicyRemoval(t.Context(), testTailnet)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CheckPolicyDependencies(t.Context(), plan, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplyPolicy(t.Context(), plan); err != nil {
		t.Fatal(err)
	}
}

func TestMeshPolicyRemovalRejectsMixedAndUnsupportedReferences(t *testing.T) {
	for _, body := range []string{
		`{"grants":[{"src":["tag:herdr-mesh-node","tag:unrelated"],"dst":["tag:herdr-mesh-server"],"ip":["tcp:50052"]}]}`,
		`{"nodeAttrs":[{"target":["tag:herdr-mesh-node"],"attr":["funnel"]}]}`,
	} {
		c := scriptedClient(t, getPolicy(body))
		if _, err := c.PrepareMeshPolicyRemoval(t.Context(), testTailnet); !errors.Is(err, ErrAmbiguousMeshPolicy) {
			t.Fatal(err)
		}
	}
}
