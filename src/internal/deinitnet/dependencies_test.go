package deinitnet

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func listDevices(body string) exchange {
	return exchange{method: http.MethodGet, path: "/tailnet/" + testTailnet + "/devices", status: 200, body: body}
}

func emptyDevices() exchange { return listDevices(`{"devices":[]}`) }

func TestDependencyPreflightExcludesOnlyExactRetainedIdentity(t *testing.T) {
	own := `{"devices":[{"id":"92960230385","nodeId":"n292kg92CNTRL","tags":["tag:herdr-mesh-server"]}]}`
	for _, id := range []string{testID, "92960230385", "", "nOTHER"} {
		t.Run(id, func(t *testing.T) {
			c := scriptedClient(t, getPolicy(appliedJSON), listDevices(own))
			plan := prepare(t, c)
			err := c.CheckPolicyDependencies(t.Context(), plan, id)
			if id == testID || id == "92960230385" {
				if err != nil {
					t.Fatalf("retained identity was not excluded: %v", err)
				}
			} else if !errors.Is(err, ErrPolicyInUse) {
				t.Fatalf("nonexcluded role device was ignored: %v", err)
			}
		})
	}
}

func TestOtherRoleDevicesBlockEvenWhenOfflineExpiredOrUnauthorized(t *testing.T) {
	for _, tag := range []string{"tag:herdr-mesh-server", "tag:herdr-mesh-node", "tag:herdr-mesh-client"} {
		t.Run(tag, func(t *testing.T) {
			body := fmt.Sprintf(`{"devices":[
				{"nodeId":"%s","tags":["tag:herdr-mesh-server"]},
				{"nodeId":"nOTHER","hostname":"same-as-own","authorized":false,"online":false,"isExpired":true,"tags":[%q]}
			]}`, testID, tag)
			c := scriptedClient(t, getPolicy(appliedJSON), listDevices(body))
			plan := prepare(t, c)
			if err := c.CheckPolicyDependencies(t.Context(), plan, testID); !errors.Is(err, ErrPolicyInUse) {
				t.Fatalf("shared role was not protected: %v", err)
			}
		})
	}
}

func TestUnrelatedAndUntaggedDevicesDoNotBlock(t *testing.T) {
	body := `{"devices":[
		{"nodeId":"nUNTAGGED"},
		{"nodeId":"nNULL","tags":null},
		{"nodeId":"nEMPTY","tags":[]},
		{"id":"42","tags":["tag:unrelated"]}
	]}`
	c := scriptedClient(t, getPolicy(appliedJSON), listDevices(body))
	if err := c.CheckPolicyDependencies(t.Context(), prepare(t, c), testID); err != nil {
		t.Fatalf("unrelated device blocked policy: %v", err)
	}
}

func TestApplyRepeatsDependencyCheckAfterPreflight(t *testing.T) {
	newMember := `{"devices":[{"nodeId":"nNEW","tags":["tag:herdr-mesh-node"]}]}`
	c := scriptedClient(t, getPolicy(appliedJSON), emptyDevices(), listDevices(newMember))
	plan := prepare(t, c)
	if err := c.CheckPolicyDependencies(t.Context(), plan, testID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplyPolicy(t.Context(), plan); !errors.Is(err, ErrPolicyInUse) {
		t.Fatalf("membership change did not block POST: %v", err)
	}
}

func TestApplyDoesNotExcludeRetainedDevice(t *testing.T) {
	own := `{"devices":[{"nodeId":"n292kg92CNTRL","tags":["tag:herdr-mesh-server"]}]}`
	c := scriptedClient(t, getPolicy(appliedJSON), listDevices(own), listDevices(own))
	plan := prepare(t, c)
	if err := c.CheckPolicyDependencies(t.Context(), plan, testID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplyPolicy(t.Context(), plan); !errors.Is(err, ErrPolicyInUse) {
		t.Fatalf("POST allowed while retained role device still exists: %v", err)
	}
}

func TestPolicyNoChangesNeedsNoDeviceListPermission(t *testing.T) {
	c := scriptedClient(t, getPolicy(beforeJSON), getPolicy(beforeJSON))
	plan := prepare(t, c)
	if err := c.CheckPolicyDependencies(t.Context(), plan, ""); err != nil {
		t.Fatal(err)
	}
	result, err := c.ApplyPolicy(t.Context(), plan)
	if err != nil || !result.AlreadyClean {
		t.Fatalf("no-op unnecessarily required listing devices: %+v %v", result, err)
	}
}

func TestDependencyFailuresAreClosedAndSanitized(t *testing.T) {
	for _, status := range []int{401, 403, 404, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			e := listDevices("SECRET " + testToken)
			e.status = status
			c := scriptedClient(t, getPolicy(appliedJSON), e)
			_, err := c.ApplyPolicy(t.Context(), prepare(t, c))
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.StatusCode != status ||
				errors.Is(err, ErrDeviceAbsent) || errors.Is(err, ErrOutcomeUnknown) ||
				strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), testToken) {
				t.Fatalf("incorrect list failure: %v", err)
			}
		})
	}
	e := listDevices("")
	e.err = errors.New("SECRET " + testToken)
	c := scriptedClient(t, getPolicy(appliedJSON), e)
	if _, err := c.ApplyPolicy(t.Context(), prepare(t, c)); err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("list transport error was not sanitized: %v", err)
	}
}

func TestInvalidDeviceListsRefusePolicyMutation(t *testing.T) {
	for _, body := range []string{
		`{}`, `null`, `[]`, `{"devices":null}`, `{"devices":{}}`,
		`{"devices":[],"next":"another-page"}`,
		`{"devices":[],"devices":[{"nodeId":"nHIDDEN"}]}`,
		`{"devices":[null]}`,
		`{"devices":[{}]}`,
		`{"devices":[{"hostname":"not-an-identity"}]}`,
		`{"devices":[{"nodeId":"nodekey:SECRET"}]}`,
		`{"devices":[{"nodeId":1}]}`,
		`{"devices":[{"nodeId":"123"}]}`,
		`{"devices":[{"id":"nOTHER"}]}`,
		`{"devices":[{"nodeId":"nSAME"},{"nodeId":"nSAME"}]}`,
		`{"devices":[{"id":"42","nodeId":"nONE"},{"id":"42","nodeId":"nTWO"}]}`,
		`{"devices":[{"nodeId":"nONE","tags":"tag:herdr-mesh-node"}]}`,
		`{"devices":[{"nodeId":"nONE","tags":[1]}]}`,
		`{"devices":[{"nodeId":"nONE","tags":["invalid"]}]}`,
		`{"devices":[{"nodeId":"nONE","tags":["tag:"]}]}`,
		`{"devices":[{"nodeId":"nONE","tags":[],"tags":["tag:herdr-mesh-node"]}]}`,
		`{"devices":[]} {}`,
		"{\"devices\":[{\"nodeId\":\"nONE\",\"tags\":[\"tag:\xff\"]}]}",
	} {
		t.Run(body, func(t *testing.T) {
			c := scriptedClient(t, getPolicy(appliedJSON), listDevices(body))
			if _, err := c.ApplyPolicy(t.Context(), prepare(t, c)); err == nil || strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("malformed listing allowed mutation or leaked data: %v", err)
			}
		})
	}
}

func TestDeviceListBounds(t *testing.T) {
	exact := `{"devices":[]}` + strings.Repeat(" ", maxDeviceListBytes-len(`{"devices":[]}`))
	for _, body := range []string{exact, exact + " "} {
		c := scriptedClient(t, getPolicy(appliedJSON), listDevices(body))
		err := c.CheckPolicyDependencies(t.Context(), prepare(t, c), "")
		if (err == nil) != (len(body) == maxDeviceListBytes) {
			t.Fatalf("incorrect byte limit handling: %v", err)
		}
	}
	for _, count := range []int{maxListedDevices, maxListedDevices + 1} {
		devices := make([]map[string]string, count)
		for i := range devices {
			devices[i] = map[string]string{"id": fmt.Sprint(i + 1)}
		}
		body, err := json.Marshal(map[string]any{"devices": devices})
		if err != nil {
			t.Fatal(err)
		}
		c := scriptedClient(t, getPolicy(appliedJSON), listDevices(string(body)))
		err = c.CheckPolicyDependencies(t.Context(), prepare(t, c), "")
		if (err == nil) != (count == maxListedDevices) {
			t.Fatalf("incorrect device count limit handling: %v", err)
		}
	}
}

func TestDependencyPlanAndIdentityValidation(t *testing.T) {
	c := scriptedClient(t, getPolicy(appliedJSON))
	plan := prepare(t, c)
	other := scriptedClient(t)
	if err := other.CheckPolicyDependencies(t.Context(), plan, testID); err == nil {
		t.Fatal("cross-client plan accepted")
	}
	if err := c.CheckPolicyDependencies(t.Context(), nil, testID); err == nil {
		t.Fatal("nil plan accepted")
	}
	for _, id := range []string{"host.tailnet.ts.net", "../other", "nONE?query", "nodekey:SECRET"} {
		if err := c.CheckPolicyDependencies(t.Context(), plan, id); err == nil || strings.Contains(err.Error(), "SECRET") {
			t.Fatalf("unsafe retained ID accepted or disclosed: %v", err)
		}
	}
}
