package setup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testAPIToken = "tskey-api-TEST-DO-NOT-USE"
const testPolicy = "{\n // keep this policy section\n \"acls\":[{\"action\":\"accept\",\"src\":[\"*\"],\"dst\":[\"*:*\"]}], \"ssh\":[]\n}"

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func apiClientFor(f roundTripFunc) *http.Client { return &http.Client{Transport: f} }

func apiReply(status int, body, etag string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Etag": []string{etag}}, Body: io.NopCloser(strings.NewReader(body))}
}

func nativeOptions(t *testing.T) Options {
	t.Helper()
	o := DefaultOptions()
	o.Tailnet = "example.com"
	o.OutputDirectory = filepath.Join(t.TempDir(), "artifacts")
	return o
}

func checkAPIRequest(t *testing.T, request *http.Request) {
	t.Helper()
	if request.URL.Scheme != "https" || request.URL.Host != "api.tailscale.com" ||
		request.Header.Get("Authorization") != "Bearer "+testAPIToken || request.URL.RawQuery != "" {
		t.Fatal("unsafe or incorrect API request")
	}
}

func setupErrorCode(t *testing.T, err error, code string, unknown bool) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code || e.RemoteEffectsUnknown != unknown ||
		strings.Contains(err.Error(), testAPIToken) {
		t.Fatalf("error=%v, want code=%s unknown=%v", err, code, unknown)
	}
}

func TestNativePolicyOnlyPreviewAndApply(t *testing.T) {
	for _, apply := range []bool{false, true} {
		t.Run(map[bool]string{false: "preview", true: "apply"}[apply], func(t *testing.T) {
			o := nativeOptions(t)
			o.PolicyOnly, o.Apply = true, apply
			hash := sha256.Sum256([]byte(testPolicy))
			if apply {
				o.ExpectedPolicySHA256 = hex.EncodeToString(hash[:])
			}
			reads, writes := 0, 0
			client := apiClientFor(func(request *http.Request) (*http.Response, error) {
				checkAPIRequest(t, request)
				if request.URL.Path != "/api/v2/tailnet/example.com/acl" {
					t.Fatal("unexpected API path")
				}
				switch request.Method {
				case http.MethodGet:
					reads++
					return apiReply(http.StatusOK, testPolicy, `"etag-1"`), nil
				case http.MethodPost:
					writes++
					if !apply || request.Header.Get("If-Match") != `"etag-1"` || request.Header.Get("Content-Type") != "application/json" {
						t.Fatal("unfenced policy mutation")
					}
					body, _ := io.ReadAll(request.Body)
					if !bytes.Contains(body, []byte(`"tag:herdr-mesh-node"`)) || !bytes.Contains(body, []byte(`"ssh"`)) {
						t.Fatal("policy additions or existing section missing")
					}
					return apiReply(http.StatusOK, "{}", ""), nil
				}
				t.Fatal("unexpected API method")
				return nil, nil
			})
			report, err := runNative(context.Background(), o, testAPIToken, client)
			if err != nil || reads != 1 || writes != map[bool]int{false: 0, true: 1}[apply] {
				t.Fatalf("report=%+v err=%v reads=%d writes=%d", report, err, reads, writes)
			}
			if report.KeysCreated != 0 || report.Mode != map[bool]string{false: "preview", true: "applied"}[apply] ||
				!containsString(report.Warnings, "wildcard_allow_preserved") {
				t.Fatalf("wrong policy-only report: %+v", report)
			}
			backup, err := os.ReadFile(report.PolicyBackup)
			if err != nil || string(backup) != testPolicy {
				t.Fatal("original policy backup lost")
			}
			entries, err := os.ReadDir(o.OutputDirectory)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				path := filepath.Join(o.OutputDirectory, entry.Name())
				info, err := entry.Info()
				if err != nil || strings.Contains(entry.Name(), "-key") || !info.Mode().IsRegular() {
					t.Fatal("policy-only created key or unsafe artifact")
				}
				if info.Mode().Perm()&0077 != 0 && os.PathSeparator == '/' {
					t.Fatal("policy artifact is not private")
				}
				data, _ := os.ReadFile(path)
				if bytes.Contains(data, []byte(testAPIToken)) {
					t.Fatal("API token was saved")
				}
			}
			if !pathAbsent(filepath.Join(o.OutputDirectory, ".configure-tailnet.lock")) {
				t.Fatal("lock leaked")
			}
		})
	}
}

func TestNativeDashboardGrantAllowsPolicyWildcardSources(t *testing.T) {
	o := nativeOptions(t)
	o.PolicyOnly = true
	port := 8787
	o.DashboardPort = &port
	client := apiClientFor(func(request *http.Request) (*http.Response, error) {
		return apiReply(http.StatusOK, `{"grants":[]}`, `"etag-1"`), nil
	})
	report, err := runNative(context.Background(), o, testAPIToken, client)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := os.ReadFile(report.PolicyProposal)
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		Grants []struct {
			Src []string `json:"src"`
			Dst []string `json:"dst"`
			IP  []string `json:"ip"`
		} `json:"grants"`
	}
	if err := json.Unmarshal(proposal, &policy); err != nil {
		t.Fatal(err)
	}
	for _, grant := range policy.Grants {
		if len(grant.Src) == 1 && grant.Src[0] == "*" &&
			len(grant.Dst) == 1 && grant.Dst[0] == "tag:herdr-mesh-server" &&
			len(grant.IP) == 1 && grant.IP[0] == "tcp:8787" {
			return
		}
	}
	t.Fatal("dashboard wildcard grant missing")
}

func TestNativeApplyRecognizesExistingLowercaseTagOwners(t *testing.T) {
	policy := `{"tagowners":{"tag:herdr-mesh-server":["autogroup:admin"],"tag:herdr-mesh-node":["autogroup:admin"],"tag:herdr-mesh-client":["autogroup:admin"]},"grants":[{"src":["tag:herdr-mesh-node"],"dst":["tag:herdr-mesh-server"],"ip":["tcp:50052"]},{"src":["tag:herdr-mesh-client"],"dst":["tag:herdr-mesh-server"],"ip":["tcp:50052"]}]}`
	o := nativeOptions(t)
	o.PolicyOnly, o.Apply = true, true
	writes := 0
	client := apiClientFor(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			writes++
			t.Fatal("unchanged policy was posted")
		}
		return apiReply(http.StatusOK, policy, `"etag-1"`), nil
	})
	report, err := runNative(context.Background(), o, testAPIToken, client)
	if err != nil || report.Mode != "applied" || writes != 0 {
		t.Fatalf("report=%+v err=%v writes=%d", report, err, writes)
	}
	proposal, err := os.ReadFile(report.PolicyProposal)
	if err != nil || bytes.Contains(proposal, []byte(`"tagOwners"`)) || !bytes.Contains(proposal, []byte(`"tagowners"`)) {
		t.Fatal("existing tag owner spelling was not preserved")
	}
}

func TestNativeMergeAddsToExistingLowercaseTagOwners(t *testing.T) {
	o := nativeOptions(t)
	o.PolicyOnly = true
	policy := `{"tagowners":{"tag:other":[]}}`
	client := apiClientFor(func(request *http.Request) (*http.Response, error) {
		return apiReply(http.StatusOK, policy, `"etag-1"`), nil
	})
	report, err := runNative(context.Background(), o, testAPIToken, client)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := os.ReadFile(report.PolicyProposal)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(proposal, &object); err != nil {
		t.Fatal(err)
	}
	owners, ok := object["tagowners"].(map[string]any)
	if !ok || len(owners) != 4 {
		t.Fatalf("missing merged tag owners: %v", owners)
	}
	if _, duplicate := object["tagOwners"]; duplicate {
		t.Fatal("duplicate case-variant section")
	}
}

func TestNativeAdvancedSetupCreatesScopedKeys(t *testing.T) {
	o := nativeOptions(t)
	o.Apply, o.KeysPerRole = true, 1
	keys := 0
	client := apiClientFor(func(request *http.Request) (*http.Response, error) {
		checkAPIRequest(t, request)
		switch {
		case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/acl"):
			return apiReply(http.StatusOK, testPolicy, `"etag-1"`), nil
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/acl"):
			return apiReply(http.StatusOK, "{}", ""), nil
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/keys"):
			keys++
			body, _ := io.ReadAll(request.Body)
			if !bytes.Contains(body, []byte(`"preauthorized":true`)) || !bytes.Contains(body, []byte(`"reusable":false`)) {
				t.Fatal("key capabilities changed")
			}
			return apiReply(http.StatusOK, `{"id":"key-`+string(rune('0'+keys))+`","key":"tskey-auth-fake-`+string(rune('0'+keys))+`"}`, ""), nil
		}
		t.Fatal("unexpected API request")
		return nil, nil
	})
	report, err := runNative(context.Background(), o, testAPIToken, client)
	if err != nil || keys != 3 || report.KeysCreated != 3 || !containsString(report.Warnings, "auth_key_secrets") {
		t.Fatalf("report=%+v err=%v keys=%d", report, err, keys)
	}
	for _, role := range roleTags {
		data, err := os.ReadFile(filepath.Join(o.OutputDirectory, role.name+"-key-1.ps1"))
		if err != nil || !bytes.Contains(data, []byte("TS_AUTHKEY_"+strings.ToUpper(role.name))) || bytes.Contains(data, []byte(testAPIToken)) {
			t.Fatal("scoped key file missing or unsafe")
		}
		if _, err := os.Stat(filepath.Join(o.OutputDirectory, role.name+"-key.ps1")); err != nil {
			t.Fatal("primary alias missing")
		}
	}
}

func TestNativeKeyFailureRevokesKnownKeys(t *testing.T) {
	o := nativeOptions(t)
	o.Apply, o.KeysPerRole = true, 1
	created, revoked := 0, 0
	client := apiClientFor(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet:
			return apiReply(http.StatusOK, testPolicy, `"etag-1"`), nil
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/acl"):
			return apiReply(http.StatusOK, "{}", ""), nil
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/keys"):
			created++
			if created == 2 {
				return apiReply(http.StatusInternalServerError, "remote-secret", ""), nil
			}
			return apiReply(http.StatusOK, `{"id":"key-1","key":"tskey-auth-fake-1"}`, ""), nil
		case request.Method == http.MethodDelete && strings.HasSuffix(request.URL.Path, "/keys/key-1"):
			revoked++
			return apiReply(http.StatusNoContent, "", ""), nil
		}
		t.Fatal("unexpected API request")
		return nil, nil
	})
	_, err := runNative(context.Background(), o, testAPIToken, client)
	setupErrorCode(t, err, "key_creation_failed", true)
	if created != 2 || revoked != 1 || !pathAbsent(filepath.Join(o.OutputDirectory, "server-key-1.ps1")) ||
		!pathAbsent(filepath.Join(o.OutputDirectory, "server-key.ps1")) {
		t.Fatal("known key was not revoked and cleaned up")
	}
}

func TestNativeKeyCleanupSurvivesSetupCancellation(t *testing.T) {
	o := nativeOptions(t)
	o.Apply, o.KeysPerRole = true, 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	created, revoked := 0, 0
	client := apiClientFor(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet:
			return apiReply(http.StatusOK, testPolicy, `"etag-1"`), nil
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/acl"):
			return apiReply(http.StatusOK, "{}", ""), nil
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/keys"):
			created++
			if created == 2 {
				cancel()
				return nil, context.Canceled
			}
			return apiReply(http.StatusOK, `{"id":"key-1","key":"tskey-auth-fake-1"}`, ""), nil
		case request.Method == http.MethodDelete && strings.HasSuffix(request.URL.Path, "/keys/key-1"):
			if request.Context().Err() != nil {
				t.Fatal("revocation inherited the expired setup context")
			}
			revoked++
			return apiReply(http.StatusNoContent, "", ""), nil
		}
		t.Fatal("unexpected API request")
		return nil, nil
	})
	_, err := runNative(ctx, o, testAPIToken, client)
	setupErrorCode(t, err, "key_creation_failed", true)
	if created != 2 || revoked != 1 || !pathAbsent(filepath.Join(o.OutputDirectory, "server-key-1.ps1")) {
		t.Fatalf("cleanup after cancellation: created=%d revoked=%d", created, revoked)
	}
}

func TestNativeSetupRejectsBadInputBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name, token, policy, etag, hash, code string
		status                                int
	}{
		{"missing token", "", testPolicy, `"etag-1"`, "", "token_missing", 200},
		{"wrong token", "tskey-auth-wrong", testPolicy, `"etag-1"`, "", "token_kind", 200},
		{"rejected token", testAPIToken, testPolicy, `"etag-1"`, "", "token_rejected", 401},
		{"missing etag", testAPIToken, testPolicy, "", "", "etag_missing", 200},
		{"invalid policy", testAPIToken, `{"acls":`, `"etag-1"`, "", "policy_invalid", 200},
		{"changed policy", testAPIToken, testPolicy, `"etag-1"`, strings.Repeat("0", 64), "policy_changed", 200},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := nativeOptions(t)
			o.PolicyOnly, o.Apply, o.ExpectedPolicySHA256 = true, true, test.hash
			writes := 0
			client := apiClientFor(func(request *http.Request) (*http.Response, error) {
				if request.Method != http.MethodGet {
					writes++
				}
				return apiReply(test.status, test.policy, test.etag), nil
			})
			_, err := runNative(context.Background(), o, test.token, client)
			setupErrorCode(t, err, test.code, false)
			if writes != 0 {
				t.Fatal("invalid input caused a remote mutation")
			}
		})
	}
}

func TestNativeApplyPreservesETagAndUnknownOutcome(t *testing.T) {
	for _, test := range []struct {
		name, code string
		response   *http.Response
		transport  error
	}{
		{"concurrent policy change", "policy_changed", apiReply(http.StatusPreconditionFailed, "", ""), nil},
		{"lost update response", "api_update_failed", nil, errors.New("remote response contained a secret")},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := nativeOptions(t)
			o.PolicyOnly, o.Apply = true, true
			writes := 0
			client := apiClientFor(func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodGet {
					return apiReply(http.StatusOK, testPolicy, `"etag-1"`), nil
				}
				writes++
				if request.Header.Get("If-Match") != `"etag-1"` {
					t.Fatal("policy update lost its ETag fence")
				}
				return test.response, test.transport
			})
			_, err := runNative(context.Background(), o, testAPIToken, client)
			setupErrorCode(t, err, test.code, true)
			if writes != 1 || strings.Contains(err.Error(), "remote response contained") {
				t.Fatalf("unsafe update result: %v writes=%d", err, writes)
			}
		})
	}
}

func TestNativeUpdateErrorReportsStatusWithoutResponseBody(t *testing.T) {
	o := nativeOptions(t)
	o.PolicyOnly, o.Apply = true, true
	client := apiClientFor(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			return apiReply(http.StatusOK, testPolicy, `"etag-1"`), nil
		}
		return apiReply(http.StatusForbidden, `{"message":"private policy detail"}`, ""), nil
	})
	_, err := runNative(context.Background(), o, testAPIToken, client)
	var setupErr *Error
	if !errors.As(err, &setupErr) || setupErr.Code != "api_update_failed" || setupErr.HTTPStatus != http.StatusForbidden ||
		!strings.Contains(err.Error(), "HTTP 403") || strings.Contains(err.Error(), "private policy detail") {
		t.Fatalf("unsafe or ambiguous error: %v", err)
	}
}

func TestCurrentPolicyRecoveryOnlyReads(t *testing.T) {
	reads := 0
	client := apiClientFor(func(request *http.Request) (*http.Response, error) {
		checkAPIRequest(t, request)
		if request.Method != http.MethodGet || request.URL.Path != "/api/v2/tailnet/example.com/acl" {
			t.Fatal("recovery attempted a mutation or read a different resource")
		}
		reads++
		return apiReply(http.StatusOK, testPolicy, ""), nil
	})
	current, err := readCurrentPolicy(context.Background(), "example.com", testAPIToken, client)
	if err != nil || reads != 1 || !json.Valid(current) {
		t.Fatalf("read-only recovery failed: reads=%d err=%v", reads, err)
	}
}

func TestNativeSetupRejectsLinkedOutputAndExistingProposal(t *testing.T) {
	o := nativeOptions(t)
	if err := prepareOutputDirectory(o.OutputDirectory); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(o.OutputDirectory, "policy-proposed.json"), []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := runNative(context.Background(), o, testAPIToken, apiClientFor(func(*http.Request) (*http.Response, error) {
		t.Fatal("existing artifact caused a request")
		return nil, nil
	}))
	setupErrorCode(t, err, "output_exists", false)
	if err := os.Symlink(o.OutputDirectory, filepath.Join(filepath.Dir(o.OutputDirectory), "linked")); err == nil {
		o.OutputDirectory = filepath.Join(filepath.Dir(o.OutputDirectory), "linked")
		_, err = runNative(context.Background(), o, testAPIToken, nil)
		setupErrorCode(t, err, "output_unavailable", false)
	}
}

func TestPromptedTokenNormalizationAndGuidance(t *testing.T) {
	if got := normalizePromptedToken([]byte("\x1b[200~" + testAPIToken + "\x1b[201~")); got != testAPIToken {
		t.Fatal("bracketed paste was not normalized")
	}
	if got := normalizePromptedToken([]byte("\x1b[200~" + testAPIToken)); got == testAPIToken {
		t.Fatal("incomplete paste wrapper accepted")
	}
	o := nativeOptions(t)
	_, err := RunWithToken(context.Background(), o, []byte("tskey-auth-wrong-kind"))
	setupErrorCode(t, err, "token_kind", false)
	if !strings.Contains(err.Error(), "hidden prompt") || strings.Contains(err.Error(), "environment variable") {
		t.Fatal("prompted credential guidance names the wrong input")
	}
	t.Setenv(o.ApiTokenEnvironmentVariable, "tskey-auth-wrong-kind")
	_, err = Run(context.Background(), o)
	setupErrorCode(t, err, "token_kind", false)
	if !strings.Contains(err.Error(), "environment variable") {
		t.Fatal("advanced credential guidance names the wrong input")
	}
}

func TestNativeSetupOptions(t *testing.T) {
	o := nativeOptions(t)
	o.PolicyOnly = true
	o.KeysPerRole = 99
	normalized, err := o.Normalize()
	if err != nil || normalized.KeysPerRole != 0 {
		t.Fatalf("policy-only normalization: %+v %v", normalized, err)
	}
	o.Tailnet = "tskey-api-secret"
	if _, err := o.Normalize(); err == nil {
		t.Fatal("token accepted as tailnet")
	}
}
