package deinitnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
)

const (
	testTailnet = "example.com"
	testETag    = `"original-etag"`
	beforeJSON  = `{
		"acls": [{"action":"accept","src":["*"],"dst":["*:*"]}],
		"tagOwners": {"tag:existing":["autogroup:admin"],"tag:herdr-mesh-node":["group:existing"]},
		"grants": [{"src":["group:existing"],"dst":["tag:existing"],"ip":["tcp:443"]}],
		"groups": {"group:existing":["owner@example.com"]},
		"unknownFutureField": {"exactInteger":9007199254740993}
	}`
	appliedJSON = `{
		"acls": [{"action":"accept","src":["*"],"dst":["*:*"]}],
		"tagOwners": {
			"tag:existing":["autogroup:admin"],
			"tag:herdr-mesh-node":["group:existing","autogroup:admin"],
			"tag:herdr-mesh-server":["autogroup:admin"],
			"tag:herdr-mesh-client":["autogroup:admin"]
		},
		"grants": [
			{"src":["group:existing"],"dst":["tag:existing"],"ip":["tcp:443"]},
			{"src":["tag:herdr-mesh-node"],"dst":["tag:herdr-mesh-server"],"ip":["tcp:50052"]},
			{"src":["tag:herdr-mesh-client"],"dst":["tag:herdr-mesh-server"],"ip":["tcp:50052"]},
			{"src":["tag:herdr-mesh-client"],"dst":["tag:herdr-mesh-server"],"ip":["tcp:8080"]}
		],
		"groups": {"group:existing":["owner@example.com"]},
		"unknownFutureField": {"exactInteger":9007199254740993}
	}`
)

func getPolicy(body string) exchange {
	return exchange{method: http.MethodGet, path: "/tailnet/" + testTailnet + "/acl", status: 200, body: body, etag: testETag}
}

func postPolicy(status int, body string) exchange {
	return exchange{method: http.MethodPost, path: "/tailnet/" + testTailnet + "/acl", status: status, body: body, check: func(t *testing.T, r *http.Request) {
		t.Helper()
		if r.Header.Get("If-Match") != testETag || r.Header.Get("Content-Type") != "application/json" {
			t.Error("policy POST lacks conditional update headers")
		}
		if r.GetBody != nil {
			t.Error("mutation can be replayed")
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		_, got, err := parsePolicy(data)
		if err != nil {
			t.Fatal(err)
		}
		_, want, err := parsePolicy([]byte(beforeJSON))
		if err != nil || !bytes.Equal(got, want) {
			t.Error("POST removed more than init additions")
		}
	}}
}

func prepare(t *testing.T, c *Client) *PolicyPlan {
	t.Helper()
	plan, err := c.PreparePolicy(t.Context(), testTailnet, []byte(beforeJSON), []byte(appliedJSON))
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestOwnedPolicyAllowsWildcardDashboardPortButNotRPCPort(t *testing.T) {
	before := []byte(`{"grants":[]}`)
	old, _, _ := parsePolicy(before)
	for _, tc := range []struct {
		port string
		want bool
	}{{"8787", true}, {"50052", false}} {
		applied := []byte(`{"grants":[{"src":["*"],"dst":["tag:herdr-mesh-server"],"ip":["tcp:` + tc.port + `"]}]}`)
		next, _, _ := parsePolicy(applied)
		if got := supportedAdditions(old, next); got != tc.want {
			t.Fatalf("wildcard grant on %s accepted=%v, want %v", tc.port, got, tc.want)
		}
	}
}

func TestPolicyPreviewAndApplyExactAdditions(t *testing.T) {
	c := scriptedClient(t, getPolicy(appliedJSON), emptyDevices(), postPolicy(200, beforeJSON))
	plan := prepare(t, c)
	if !plan.HasChanges() {
		t.Fatal("missing cleanup preview")
	}
	want, _, err := parsePolicy([]byte(beforeJSON))
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := parsePolicy(plan.DesiredPolicy())
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("preview did not preserve preexisting policy and precise numbers")
	}
	copy := plan.DesiredPolicy()
	clear(copy)
	if len(plan.DesiredPolicy()) == 0 || plan.DesiredPolicy()[0] != '{' {
		t.Fatal("preview copy mutated plan")
	}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		text := fmt.Sprintf(format, plan)
		if strings.Contains(text, "owner@example") || strings.Contains(text, testToken) {
			t.Fatal("plan formatting disclosed policy or credentials")
		}
	}
	result, err := c.ApplyPolicy(t.Context(), plan)
	if err != nil || result != (PolicyResult{}) {
		t.Fatalf("apply = %+v, %v", result, err)
	}
	if _, err := c.ApplyPolicy(t.Context(), plan); !errors.Is(err, ErrPlanUsed) {
		t.Fatalf("plan was reusable: %v", err)
	}
}

func TestSupportedAdditionsWithLowercaseTagOwners(t *testing.T) {
	old, _, err := parsePolicy([]byte(strings.ReplaceAll(beforeJSON, `"tagOwners"`, `"tagowners"`)))
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := parsePolicy([]byte(strings.ReplaceAll(appliedJSON, `"tagOwners"`, `"tagowners"`)))
	if err != nil {
		t.Fatal(err)
	}
	if !supportedAdditions(old, next) {
		t.Fatal("lowercase tagowners from Tailscale API was rejected")
	}
	next["tagOwners"] = next["tagowners"]
	if supportedAdditions(old, next) {
		t.Fatal("ambiguous case-variant tag owner sections were accepted")
	}
}

func TestPolicyAlreadyClean(t *testing.T) {
	c := scriptedClient(t, getPolicy(beforeJSON), getPolicy(beforeJSON))
	plan := prepare(t, c)
	if plan.HasChanges() {
		t.Fatal("already removed additions were scheduled for mutation")
	}
	result, err := c.ApplyPolicy(t.Context(), plan)
	if err != nil || !result.AlreadyClean {
		t.Fatalf("no-op = %+v, %v", result, err)
	}
}

func TestPolicyNoopDoesNotClaimStaleCleanState(t *testing.T) {
	c := scriptedClient(t, getPolicy(beforeJSON), getPolicy(appliedJSON))
	plan := prepare(t, c)
	if _, err := c.ApplyPolicy(t.Context(), plan); !errors.Is(err, ErrPolicyConflict) {
		t.Fatalf("stale no-op incorrectly claimed clean: %v", err)
	}
}

func TestPolicyHuJSONAndFormatting(t *testing.T) {
	before := []byte("{ // saved before\n \"groups\": {\"group:a\": [\"a@example.com\",],},\n}")
	applied := []byte(`{"groups":{"group:a":["a@example.com"]},"tagOwners":{"tag:herdr-mesh-server":["autogroup:admin"]},"grants":[]}`)
	current := "{ /* newer formatting only */ \"grants\": [], \"tagOwners\":{\"tag:herdr-mesh-server\":[\"autogroup:admin\",],}, \"groups\":{\"group:a\":[\"a@example.com\"]},}"
	beforeCopy, appliedCopy := bytes.Clone(before), bytes.Clone(applied)
	c := scriptedClient(t, getPolicy(current))
	plan, err := c.PreparePolicy(t.Context(), testTailnet, before, applied)
	if err != nil || !plan.HasChanges() {
		t.Fatalf("HuJSON prepare: %v", err)
	}
	if !bytes.Equal(before, beforeCopy) || !bytes.Equal(applied, appliedCopy) {
		t.Fatal("preparation mutated retained input artifacts")
	}
	if got := string(plan.DesiredPolicy()); got != `{"groups":{"group:a":["a@example.com"]}}` {
		t.Fatalf("unexpected desired JSON: %s", got)
	}
}

func TestCurrentPolicyChangesNeverOverwritten(t *testing.T) {
	for _, current := range []string{
		strings.Replace(appliedJSON, `"owner@example.com"`, `"new-owner@example.com"`, 1),
		strings.Replace(appliedJSON, `"tcp:8080"`, `"tcp:8081"`, 1),
		strings.Replace(appliedJSON, `"tag:herdr-mesh-server":["autogroup:admin"]`, `"tag:herdr-mesh-server":["group:reused"]`, 1),
		strings.Replace(appliedJSON, `"groups":`, `"ssh":[{"src":["tag:herdr-mesh-server"]}], "groups":`, 1),
		strings.Replace(appliedJSON, `"groups":`, `"newField":true, "groups":`, 1),
	} {
		t.Run(fmt.Sprint(len(current)), func(t *testing.T) {
			c := scriptedClient(t, getPolicy(current))
			if plan, err := c.PreparePolicy(t.Context(), testTailnet, []byte(beforeJSON), []byte(appliedJSON)); plan != nil || !errors.Is(err, ErrPolicyConflict) {
				t.Fatalf("changed/reused policy was not refused: %v", err)
			}
		})
	}
}

func TestInvalidArtifactsNeverReachNetwork(t *testing.T) {
	tests := []struct{ name, before, applied string }{
		{"empty", "", appliedJSON},
		{"syntax", `{"SECRET"`, appliedJSON},
		{"array root", `[]`, `[]`},
		{"null root", `null`, `null`},
		{"duplicate top", `{"grants":[],"grants":[]}`, appliedJSON},
		{"duplicate nested", `{"groups":{"group:a":[],"group:a":[]}}`, appliedJSON},
		{"duplicate escaped", `{"groups":{},"\u0067roups":{}}`, appliedJSON},
		{"second document", `{}` + `{}`, appliedJSON},
		{"invalid UTF8", "{\"x\":\"\xff\"}", appliedJSON},
		{"removed ACL", beforeJSON, strings.Replace(appliedJSON, `"action":"accept"`, `"action":"deny"`, 1)},
		{"unrelated replacement", beforeJSON, strings.Replace(appliedJSON, `"owner@example.com"`, `"other@example.com"`, 1)},
		{"unrelated addition", beforeJSON, strings.Replace(appliedJSON, `"groups":`, `"newField":1,"groups":`, 1)},
		{"unrelated removal", `{"preserve":null}`, `{}`},
		{"null owners", `{}`, `{"tagOwners":null}`},
		{"null old owners", `{"tagOwners":null}`, `{"tagOwners":{}}`},
		{"other tag", `{}`, `{"tagOwners":{"tag:not-owned":["autogroup:admin"]}}`},
		{"removed owners", `{"tagOwners":{"tag:herdr-mesh-server":["old"]}}`, `{"tagOwners":{}}`},
		{"replaced owners", beforeJSON, strings.Replace(appliedJSON, `["group:existing","autogroup:admin"]`, `["replacement","autogroup:admin"]`, 1)},
		{"nonarray owner", `{}`, `{"tagOwners":{"tag:herdr-mesh-server":"autogroup:admin"}}`},
		{"empty owner", `{}`, `{"tagOwners":{"tag:herdr-mesh-server":[""]}}`},
		{"duplicate owner", `{"tagOwners":{"tag:herdr-mesh-server":["admin"]}}`, `{"tagOwners":{"tag:herdr-mesh-server":["admin","admin"]}}`},
		{"owners append two", `{}`, `{"tagOwners":{"tag:herdr-mesh-server":["a","b"]}}`},
		{"null grants", `{}`, `{"grants":null}`},
		{"removed grants", beforeJSON, strings.Replace(appliedJSON, `{"src":["group:existing"],"dst":["tag:existing"],"ip":["tcp:443"]},`, "", 1)},
		{"new broad grant", beforeJSON, strings.Replace(appliedJSON, `"tcp:8080"`, `"*"`, 1)},
		{"new external grant", beforeJSON, strings.Replace(appliedJSON, `["tag:herdr-mesh-client"]`, `["tag:somebody-else"]`, 1)},
		{"node wrong port", beforeJSON, strings.Replace(appliedJSON, `"tcp:50052"`, `"tcp:80"`, 1)},
		{"invalid port", beforeJSON, strings.Replace(appliedJSON, `"tcp:8080"`, `"tcp:65536"`, 1)},
		{"zero port", beforeJSON, strings.Replace(appliedJSON, `"tcp:8080"`, `"tcp:0"`, 1)},
		{"noncanonical port", beforeJSON, strings.Replace(appliedJSON, `"tcp:8080"`, `"tcp:08080"`, 1)},
		{"duplicate grant", beforeJSON, strings.Replace(appliedJSON, `"tcp:8080"`, `"tcp:50052"`, 1)},
		{"unexpected grant property", beforeJSON, strings.Replace(appliedJSON, `"tcp:8080"]}`, `"tcp:8080"],"via":["tag:router"]}`, 1)},
		{"too deep", `{"x":` + strings.Repeat("[", 101) + "0" + strings.Repeat("]", 101) + "}", appliedJSON},
		{"oversize", strings.Repeat(" ", MaxPolicyBytes) + "{}", appliedJSON},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := scriptedClient(t)
			plan, err := c.PreparePolicy(t.Context(), testTailnet, []byte(tt.before), []byte(tt.applied))
			if plan != nil || !errors.Is(err, ErrInvalidPolicy) || strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("invalid artifact accepted or disclosed: %v", err)
			}
		})
	}
}

func TestPolicyHTTPReadGuards(t *testing.T) {
	for _, status := range []int{401, 403, 404, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			e := getPolicy("SECRET")
			e.status = status
			c := scriptedClient(t, e)
			_, err := c.PreparePolicy(t.Context(), testTailnet, []byte(beforeJSON), []byte(appliedJSON))
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.StatusCode != status || errors.Is(err, ErrDeviceAbsent) || strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("policy error is not a sanitized status: %v", err)
			}

		})
	}
	for _, etag := range []string{"", "*", `"a","b"`, `W/"weak"`, "\"line\r\ninjection\"", strings.Repeat("x", 257)} {
		t.Run("etag "+etag, func(t *testing.T) {
			e := getPolicy(appliedJSON)
			e.etag = etag
			c := scriptedClient(t, e)
			if _, err := c.PreparePolicy(t.Context(), testTailnet, []byte(beforeJSON), []byte(appliedJSON)); err == nil {
				t.Fatal("unsafe/missing ETag accepted")
			}
		})
	}
	for _, body := range []string{"SECRET-invalid", strings.Repeat(" ", MaxPolicyBytes+1)} {
		t.Run("bounded invalid response", func(t *testing.T) {
			c := scriptedClient(t, getPolicy(body))
			if _, err := c.PreparePolicy(t.Context(), testTailnet, []byte(beforeJSON), []byte(appliedJSON)); err == nil || strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("unsafe policy response accepted: %v", err)
			}
		})
	}
}

func TestPolicyExactSizeBoundary(t *testing.T) {
	data := "{}" + strings.Repeat(" ", MaxPolicyBytes-2)
	c := scriptedClient(t, getPolicy(data))
	plan, err := c.PreparePolicy(t.Context(), testTailnet, []byte(data), []byte(data))
	if err != nil || plan.HasChanges() {
		t.Fatalf("exactly maximum-sized artifacts/response rejected: %v", err)
	}
	c = scriptedClient(t)
	if _, err := c.PreparePolicy(t.Context(), testTailnet, []byte("{}"), []byte(data+" ")); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("oversize applied artifact accepted: %v", err)
	}
}

func TestPolicyUnknownOutcomeAndPreconditions(t *testing.T) {
	tests := []struct {
		name       string
		post       exchange
		reconcile  *exchange
		want       PolicyResult
		wantErr    error
		wantStatus int
	}{
		{name: "precondition failed", post: postPolicy(412, "SECRET"), wantErr: ErrPolicyConflict},
		{name: "forbidden", post: postPolicy(403, "SECRET"), wantStatus: 403},
		{name: "tailnet missing", post: postPolicy(404, "SECRET"), wantStatus: 404},
		{name: "invalid policy", post: postPolicy(400, "SECRET"), wantStatus: 400},
		{name: "uncertain applied", post: postPolicy(500, ""), reconcile: ptr(getPolicy(beforeJSON)), want: PolicyResult{Reconciled: true}},
		{name: "uncertain unchanged", post: postPolicy(500, ""), reconcile: ptr(getPolicy(appliedJSON)), wantErr: ErrOutcomeUnknown},
		{name: "uncertain newer policy", post: postPolicy(500, ""), reconcile: ptr(getPolicy(`{"newer":true}`)), wantErr: ErrOutcomeUnknown},
		{name: "bad success payload applied", post: postPolicy(200, "SECRET"), reconcile: ptr(getPolicy(beforeJSON)), want: PolicyResult{Reconciled: true}},
		{name: "bad success payload unknown", post: postPolicy(200, "{}"), reconcile: ptr(getPolicy(appliedJSON)), wantErr: ErrOutcomeUnknown},
		{name: "oversize success applied", post: postPolicy(200, strings.Repeat("x", MaxPolicyBytes+1)), reconcile: ptr(getPolicy(beforeJSON)), want: PolicyResult{Reconciled: true}},
		{name: "accepted not finished", post: postPolicy(202, ""), reconcile: ptr(getPolicy(appliedJSON)), wantErr: ErrOutcomeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exchanges := []exchange{getPolicy(appliedJSON), emptyDevices(), tt.post}
			if tt.reconcile != nil {
				exchanges = append(exchanges, *tt.reconcile)
			}
			c := scriptedClient(t, exchanges...)
			plan := prepare(t, c)
			got, err := c.ApplyPolicy(t.Context(), plan)
			if tt.wantStatus != 0 {
				var httpErr *HTTPError
				if !errors.As(err, &httpErr) || httpErr.StatusCode != tt.wantStatus {
					t.Fatalf("error = %v; want HTTP %d", err, tt.wantStatus)
				}
			} else if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v; want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("result = %+v; want %+v", got, tt.want)
			}
			if _, err := c.ApplyPolicy(t.Context(), plan); !errors.Is(err, ErrPlanUsed) {
				t.Fatal("attempted mutation was retried")
			}
		})
	}
}

func ptr(e exchange) *exchange { return &e }

func TestPolicyLostResponseAndLaterReadOnlyReconciliation(t *testing.T) {
	mutation := postPolicy(0, "")
	mutation.err = errors.New("SECRET " + testToken)
	c := scriptedClient(t, getPolicy(appliedJSON), emptyDevices(), mutation, getPolicy(appliedJSON), getPolicy(beforeJSON))
	plan := prepare(t, c)
	if _, err := c.ApplyPolicy(t.Context(), plan); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("expected unknown: %v", err)
	}
	clean, err := c.ReconcilePolicy(t.Context(), plan)
	if err != nil || !clean {
		t.Fatalf("read-only reconciliation failed: %v", err)
	}
}

func TestPolicyCanceledMutationRetainsUnknown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	mutation := postPolicy(0, "")
	mutation.check = func(*testing.T, *http.Request) { cancel() }
	mutation.err = context.Canceled
	c := scriptedClient(t, getPolicy(appliedJSON), emptyDevices(), mutation, getPolicy(beforeJSON))
	plan := prepare(t, c)
	if _, err := c.ApplyPolicy(ctx, plan); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("canceled mutation not marked unknown: %v", err)
	}
	if clean, err := c.ReconcilePolicy(t.Context(), plan); err != nil || !clean {
		t.Fatalf("fresh context did not reconcile: %v", err)
	}
}

func TestPolicyPlanClientBindingAndConcurrentUse(t *testing.T) {
	c := scriptedClient(t, getPolicy(appliedJSON), emptyDevices(), postPolicy(200, beforeJSON))
	plan := prepare(t, c)
	other := scriptedClient(t)
	if _, err := other.ApplyPolicy(t.Context(), plan); err == nil {
		t.Fatal("cross-client plan accepted")
	}
	if _, err := other.ReconcilePolicy(t.Context(), plan); err == nil {
		t.Fatal("cross-client reconciliation accepted")
	}
	if _, err := c.ApplyPolicy(t.Context(), nil); err == nil {
		t.Fatal("nil plan accepted")
	}
	if _, err := c.ReconcilePolicy(t.Context(), nil); err == nil {
		t.Fatal("nil reconciliation accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.ApplyPolicy(ctx, plan); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled plan invoked network")
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Go(func() {
			_, err := c.ApplyPolicy(t.Context(), plan)
			results <- err
		})
	}
	wg.Wait()
	close(results)
	var successful, used int
	for err := range results {
		switch {
		case err == nil:
			successful++
		case errors.Is(err, ErrPlanUsed):
			used++
		default:
			t.Errorf("unexpected concurrent result: %v", err)
		}
	}
	if successful != 1 || used != 1 {
		t.Fatalf("single-attempt invariant failed: success=%d used=%d", successful, used)
	}
}

func TestTailnetValidationAndExistingPolicyNoChanges(t *testing.T) {
	for _, tailnet := range []string{"", "-", ".", "..", "../other", "a/b", "a?query", "a#fragment", "tskey-" + "api-secret", "a\r\n", strings.Repeat("a", 254)} {
		c := scriptedClient(t)
		if _, err := c.PreparePolicy(t.Context(), tailnet, []byte("{}"), []byte("{}")); err == nil {
			t.Fatalf("accepted unsafe tailnet")
		}
	}
	for _, policy := range []string{"{}", beforeJSON, appliedJSON} {
		c := scriptedClient(t, getPolicy(policy))
		plan, err := c.PreparePolicy(t.Context(), testTailnet, []byte(policy), []byte(policy))
		if err != nil || plan.HasChanges() {
			t.Fatalf("unchanged snapshots not preserved: %v", err)
		}
	}
}

func FuzzPolicyParsing(f *testing.F) {
	for _, seed := range []string{beforeJSON, appliedJSON, `{"x":1,"x":2}`, "{//comment\n\"x\":[],}"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		value, canonical, err := parsePolicy(data)
		if err != nil {
			if !errors.Is(err, ErrInvalidPolicy) {
				t.Fatal("unsanitized parsing error")
			}
			return
		}
		if !json.Valid(canonical) {
			t.Fatal("canonical policy is not JSON")
		}
		roundtrip, again, err := parsePolicy(canonical)
		if err != nil || !reflect.DeepEqual(value, roundtrip) || !bytes.Equal(canonical, again) {
			t.Fatal("canonicalization is not stable")
		}
	})
}
