package setup

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/deinitnet"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

const policyLimit = 1 << 20

var apiTokenPattern = regexp.MustCompile(`^tskey-api-[A-Za-z0-9_-]+$`)
var keyIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)
var authKeyPattern = regexp.MustCompile(`^tskey-auth-[A-Za-z0-9_-]+$`)

var roleTags = []struct{ name, tag string }{
	{"server", "tag:herdr-mesh-server"},
	{"node", "tag:herdr-mesh-node"},
	{"client", "tag:herdr-mesh-client"},
}

type apiClient interface {
	Do(*http.Request) (*http.Response, error)
}

func nativeHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSHandshakeTimeout: timeout, ResponseHeaderTimeout: timeout,
			MaxResponseHeaderBytes: 32 << 10, MaxIdleConnsPerHost: 1,
		},
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// CurrentPolicy reads the live policy for guided recovery of an uncertain
// apply. It never creates artifacts or sends a mutation request.
func CurrentPolicy(ctx context.Context, tailnet string, promptedToken []byte) (policy []byte, resultErr error) {
	defer func() {
		var setupErr *Error
		if errors.As(resultErr, &setupErr) {
			setupErr.PromptedToken = true
		}
	}()
	return readCurrentPolicy(ctx, tailnet, normalizePromptedToken(promptedToken), nil)
}

func readCurrentPolicy(ctx context.Context, tailnet, token string, client apiClient) ([]byte, error) {
	o := DefaultOptions()
	o.Tailnet, o.PolicyOnly = tailnet, true
	if _, err := o.Normalize(); err != nil {
		return nil, err
	}
	if token == "" {
		return nil, &Error{Code: "token_missing"}
	}
	if !apiTokenPattern.MatchString(token) || len(token) > 4096 {
		return nil, &Error{Code: "token_kind"}
	}
	if client == nil {
		native := nativeHTTPClient(o.Timeout)
		defer native.CloseIdleConnections()
		client = native
	}
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	endpoint := "https://api.tailscale.com/api/v2/tailnet/" + url.PathEscape(tailnet) + "/acl"
	read, err := requestAPI(ctx, client, token, http.MethodGet, endpoint, nil, "", policyLimit)
	if err != nil {
		return nil, &Error{Code: requestFailureCode(ctx, "api_read_failed")}
	}
	if read.status == http.StatusUnauthorized {
		return nil, &Error{Code: "token_rejected", HTTPStatus: read.status}
	}
	if read.status != http.StatusOK {
		return nil, &Error{Code: "api_read_failed", HTTPStatus: read.status}
	}
	_, canonical, err := deinitnet.ParsePolicy(read.body)
	if err != nil {
		return nil, &Error{Code: "policy_invalid"}
	}
	return canonical, nil
}

// runNative uses the public Tailscale HTTPS API before this machine has a
// tsnet identity. The injectable client is only for tests; production always
// uses the fixed API origin and a client that does not inherit proxy settings.
func runNative(ctx context.Context, options Options, token string, client apiClient) (report Report, resultErr error) {
	o, err := options.Normalize()
	if err != nil {
		return Report{}, err
	}
	if token == "" {
		return Report{}, &Error{Code: "token_missing"}
	}
	if !apiTokenPattern.MatchString(token) || len(token) > 4096 {
		return Report{}, &Error{Code: "token_kind"}
	}
	if client == nil {
		native := nativeHTTPClient(o.Timeout)
		defer native.CloseIdleConnections()
		client = native
	}
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	if err := prepareOutputDirectory(o.OutputDirectory); err != nil {
		return Report{}, &Error{Code: "output_unavailable"}
	}
	lockPath := filepath.Join(o.OutputDirectory, ".configure-tailnet.lock")
	lock, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return Report{}, &Error{Code: "output_unavailable"}
	}
	if err := state.ProtectPrivatePath(lockPath, false); err != nil {
		lock.Close()
		os.Remove(lockPath)
		return Report{}, &Error{Code: "output_unavailable"}
	}
	_ = lock.Close()
	mutating := false
	defer func() {
		if err := os.Remove(lockPath); err != nil {
			resultErr = &Error{Code: "cleanup_failed", RemoteEffectsUnknown: mutating}
		}
		var setupErr *Error
		if mutating && errors.As(resultErr, &setupErr) {
			setupErr.RemoteEffectsUnknown = true
		}
	}()
	proposalPath := filepath.Join(o.OutputDirectory, "policy-proposed.json")
	if !pathAbsent(proposalPath) {
		return Report{}, &Error{Code: "output_exists"}
	}
	if o.Apply && !o.PolicyOnly {
		for _, role := range roleTags {
			for number := 1; number <= o.KeysPerRole; number++ {
				if !pathAbsent(filepath.Join(o.OutputDirectory, fmt.Sprintf("%s-key-%d.ps1", role.name, number))) {
					return Report{}, &Error{Code: "output_exists"}
				}
			}
		}
	}
	base := "https://api.tailscale.com/api/v2/tailnet/" + url.PathEscape(o.Tailnet)
	read, err := requestAPI(ctx, client, token, http.MethodGet, base+"/acl", nil, "", policyLimit)
	if err != nil {
		return Report{}, &Error{Code: requestFailureCode(ctx, "api_read_failed")}
	}
	if read.status == http.StatusUnauthorized {
		return Report{}, &Error{Code: "token_rejected"}
	}
	if read.status != http.StatusOK {
		return Report{}, &Error{Code: "api_read_failed"}
	}
	if !strongETag(read.etag) {
		return Report{}, &Error{Code: "etag_missing"}
	}
	if o.ExpectedPolicySHA256 != "" {
		hash := sha256.Sum256(read.body)
		if hex.EncodeToString(hash[:]) != o.ExpectedPolicySHA256 {
			return Report{}, &Error{Code: "policy_changed"}
		}
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return Report{}, &Error{Code: "local_failure"}
	}
	backupPath := filepath.Join(o.OutputDirectory, "policy-before-"+time.Now().UTC().Format("20060102-150405000")+"-"+hex.EncodeToString(nonce[:])+".json")
	if err := writePrivateFile(backupPath, read.body); err != nil {
		return Report{}, &Error{Code: "local_failure"}
	}
	policy, beforeCanonical, err := deinitnet.ParsePolicy(read.body)
	if err != nil {
		return Report{}, &Error{Code: "policy_invalid"}
	}
	warnings, err := mergePolicy(policy, o)
	if err != nil {
		return Report{}, &Error{Code: "policy_invalid"}
	}
	proposal, err := json.MarshalIndent(policy, "", "  ")
	if err != nil || len(proposal) > 2*policyLimit {
		return Report{}, &Error{Code: "policy_invalid"}
	}
	afterCanonical, err := json.Marshal(policy)
	if err != nil {
		return Report{}, &Error{Code: "policy_invalid"}
	}
	if err := writePrivateFile(proposalPath, proposal); err != nil {
		return Report{}, &Error{Code: "local_failure"}
	}
	report = Report{Mode: "preview", PolicyBackup: backupPath, PolicyProposal: proposalPath, Warnings: warnings}
	if !o.Apply {
		return report, nil
	}
	if !bytes.Equal(beforeCanonical, afterCanonical) {
		mutating = true
		update, err := requestAPI(ctx, client, token, http.MethodPost, base+"/acl", proposal, read.etag, policyLimit)
		if err != nil {
			return Report{}, &Error{Code: requestFailureCode(ctx, "api_update_failed")}
		}
		if update.status == http.StatusPreconditionFailed {
			return Report{}, &Error{Code: "policy_changed", HTTPStatus: update.status}
		}
		if update.status != http.StatusOK && update.status != http.StatusNoContent {
			return Report{}, &Error{Code: "api_update_failed", HTTPStatus: update.status}
		}
	}
	if !o.PolicyOnly {
		mutating = true
		count, keyWarnings, err := createRoleKeys(ctx, client, token, base, o)
		if err != nil {
			return Report{}, err
		}
		report.KeysCreated = count
		report.Warnings = append(report.Warnings, keyWarnings...)
	}
	report.Mode = "applied"
	return report, nil
}

func prepareOutputDirectory(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
		info, err = os.Lstat(path)
		if err == nil {
			return state.ProtectPrivatePath(path, true)
		}
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe output directory")
	}
	return verifyPrivateDirectory(path, info)
}

func pathAbsent(path string) bool {
	_, err := os.Lstat(path)
	return errors.Is(err, os.ErrNotExist)
}

func writePrivateFile(path string, data []byte) (resultErr error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if resultErr != nil {
			_ = os.Remove(path)
		}
	}()
	if err := state.ProtectPrivatePath(path, false); err != nil {
		_ = file.Close()
		return err
	}
	if n, err := file.Write(data); err != nil || n != len(data) {
		_ = file.Close()
		if err != nil {
			return err
		}
		return io.ErrShortWrite
	}
	resultErr = errors.Join(file.Sync(), file.Close())
	return resultErr
}

type apiResponse struct {
	status int
	etag   string
	body   []byte
}

func requestAPI(ctx context.Context, client apiClient, token, method, endpoint string, body []byte, etag string, limit int64) (apiResponse, error) {
	var reader io.Reader
	if body != nil {
		// Suppress GetBody so net/http cannot replay a mutation.
		reader = struct{ io.Reader }{bytes.NewReader(body)}
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return apiResponse{}, errors.New("invalid API request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if etag != "" {
		req.Header.Set("If-Match", etag)
	}
	res, err := client.Do(req)
	if err != nil {
		return apiResponse{}, errors.New("API transport failure")
	}
	defer res.Body.Close()
	result := apiResponse{status: res.StatusCode, etag: res.Header.Get("ETag")}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return result, nil
	}
	result.body, err = io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil || int64(len(result.body)) > limit {
		return apiResponse{}, errors.New("invalid API response")
	}
	return result, nil
}

func requestFailureCode(ctx context.Context, fallback string) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return "canceled"
	}
	return fallback
}

func strongETag(value string) bool {
	if len(value) < 2 || len(value) > 256 || value[0] != '"' || value[len(value)-1] != '"' {
		return false
	}
	for _, b := range []byte(value[1 : len(value)-1]) {
		if b < 0x21 || b == '"' || b > 0x7e {
			return false
		}
	}
	return true
}

func mergePolicy(policy map[string]any, options Options) ([]string, error) {
	warnings := []string{"policy_round_trip"}
	if hasWildcardAllow(policy) {
		warnings = append(warnings, "wildcard_allow_preserved")
	}
	// Tailscale's JSON API can return the historical spelling "tagowners".
	// Keep the existing field: adding "tagOwners" alongside it makes two
	// case-insensitive aliases of the same policy section.
	ownersKey := "tagOwners"
	if _, present := policy["tagowners"]; present {
		if _, duplicate := policy[ownersKey]; duplicate {
			return nil, errors.New("ambiguous tag owners")
		}
		ownersKey = "tagowners"
	}
	owners := map[string]any{}
	if existing, present := policy[ownersKey]; present {
		var ok bool
		owners, ok = existing.(map[string]any)
		if !ok {
			return nil, errors.New("invalid tag owners")
		}
	}
	for _, role := range roleTags {
		value, present := owners[role.tag]
		if !present {
			owners[role.tag] = []any{options.TagOwner}
			continue
		}
		list, ok := value.([]any)
		if !ok {
			return nil, errors.New("invalid tag owners")
		}
		found := false
		for _, owner := range list {
			if owner == options.TagOwner {
				found = true
			}
		}
		if !found {
			owners[role.tag] = append(list, options.TagOwner)
		}
	}
	policy[ownersKey] = owners
	grants := []any{}
	if existing, present := policy["grants"]; present {
		var ok bool
		grants, ok = existing.([]any)
		if !ok {
			return nil, errors.New("invalid grants")
		}
	}
	add := func(source, destination string, port int) {
		ip := "tcp:" + strconv.Itoa(port)
		for _, value := range grants {
			grant, ok := value.(map[string]any)
			if ok && oneString(grant["src"]) == source && oneString(grant["dst"]) == destination && oneString(grant["ip"]) == ip {
				return
			}
		}
		grants = append(grants, map[string]any{"src": []any{source}, "dst": []any{destination}, "ip": []any{ip}})
	}
	add(roleTags[1].tag, roleTags[0].tag, 50052)
	add(roleTags[2].tag, roleTags[0].tag, 50052)
	if options.DashboardPort != nil {
		add("*", roleTags[0].tag, *options.DashboardPort)
	}
	policy["grants"] = grants
	return warnings, nil
}

func oneString(value any) string {
	list, ok := value.([]any)
	if !ok || len(list) != 1 {
		return ""
	}
	text, _ := list[0].(string)
	return text
}

func contains(list any, value string) bool {
	items, ok := list.([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}

func hasWildcardAllow(policy map[string]any) bool {
	if acls, ok := policy["acls"].([]any); ok {
		for _, value := range acls {
			acl, ok := value.(map[string]any)
			if ok && acl["action"] == "accept" && contains(acl["src"], "*") &&
				(contains(acl["dst"], "*") || contains(acl["dst"], "*:*")) {
				return true
			}
		}
	}
	if grants, ok := policy["grants"].([]any); ok {
		for _, value := range grants {
			grant, ok := value.(map[string]any)
			if !ok || !contains(grant["src"], "*") || !(contains(grant["dst"], "*") || contains(grant["dst"], "*:*")) {
				continue
			}
			if _, present := grant["ip"]; !present || contains(grant["ip"], "*") || contains(grant["ip"], "*:*") {
				return true
			}
		}
	}
	return false
}

type createdKey struct {
	id, path string
}

func createRoleKeys(ctx context.Context, client apiClient, token, base string, options Options) (int, []string, error) {
	var created []createdKey
	var aliases []string
	var warnings []string
	cleanup := func(original error) (int, []string, error) {
		// The setup deadline may already have expired. Give known keys a bounded
		// chance to be revoked before returning the original failure.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		revocationFailed, cleanupFailed := false, false
		for _, key := range created {
			response, err := requestAPI(cleanupCtx, client, token, http.MethodDelete, base+"/keys/"+url.PathEscape(key.id), nil, "", 64<<10)
			if err != nil || response.status != http.StatusOK && response.status != http.StatusNoContent {
				revocationFailed = true
			}
			if err := os.Remove(key.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				cleanupFailed = true
			}
		}
		for _, alias := range aliases {
			if err := os.Remove(alias); err != nil && !errors.Is(err, os.ErrNotExist) {
				cleanupFailed = true
			}
		}
		if revocationFailed {
			return 0, nil, &Error{Code: "key_revocation_unconfirmed"}
		}
		if cleanupFailed {
			return 0, nil, &Error{Code: "key_cleanup_failed"}
		}
		return 0, nil, original
	}
	for _, role := range roleTags {
		for number := 1; number <= options.KeysPerRole; number++ {
			requestBody, _ := json.Marshal(map[string]any{
				"keyType": "auth", "description": fmt.Sprintf("herdr mesh %s hackathon %d", role.name, number),
				"expirySeconds": options.KeyExpirySeconds,
				"capabilities": map[string]any{"devices": map[string]any{"create": map[string]any{
					"reusable": false, "ephemeral": false, "preauthorized": true, "tags": []string{role.tag},
				}}},
			})
			response, err := requestAPI(ctx, client, token, http.MethodPost, base+"/keys", requestBody, "", 64<<10)
			if err != nil || response.status != http.StatusOK && response.status != http.StatusCreated {
				return cleanup(&Error{Code: "key_creation_failed"})
			}
			var key struct{ ID, Key string }
			if json.Unmarshal(response.body, &key) != nil || !keyIDPattern.MatchString(key.ID) {
				return cleanup(&Error{Code: "key_response_invalid"})
			}
			path := filepath.Join(options.OutputDirectory, fmt.Sprintf("%s-key-%d.ps1", role.name, number))
			created = append(created, createdKey{id: key.ID, path: path})
			if !authKeyPattern.MatchString(key.Key) {
				return cleanup(&Error{Code: "key_response_invalid"})
			}
			variable := "TS_AUTHKEY_" + strings.ToUpper(role.name)
			if err := writePrivateFile(path, []byte("$env:"+variable+" = '"+key.Key+"'")); err != nil {
				return cleanup(&Error{Code: "local_failure"})
			}
		}
		alias := filepath.Join(options.OutputDirectory, role.name+"-key.ps1")
		if !pathAbsent(alias) {
			if !containsString(warnings, "primary_alias_preserved") {
				warnings = append(warnings, "primary_alias_preserved")
			}
			continue
		}
		if err := writePrivateFile(alias, []byte(". \"$PSScriptRoot\\"+role.name+"-key-1.ps1\"")); err != nil {
			return cleanup(&Error{Code: "local_failure"})
		}
		aliases = append(aliases, alias)
	}
	warnings = append(warnings, "auth_key_secrets")
	return 3 * options.KeysPerRole, warnings, nil
}

func containsString(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}
