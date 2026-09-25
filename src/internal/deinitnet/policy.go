package deinitnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/tailscale/hujson"
)

var tailnetPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9@.-]{0,252}$`)

// PolicyPlan is an opaque, client-bound, single-attempt cleanup plan.
// Do not persist or log the plan; the caller retains its original artifacts.
type PolicyPlan struct {
	mu        sync.Mutex
	client    *Client
	tailnet   string
	etag      string
	desired   []byte
	unchanged bool
	attempted bool
}

func (*PolicyPlan) String() string   { return "deinitnet.PolicyPlan{redacted}" }
func (*PolicyPlan) GoString() string { return "deinitnet.PolicyPlan{redacted}" }

// HasChanges reports whether this preview requires a policy POST.
func (p *PolicyPlan) HasChanges() bool { return p != nil && !p.unchanged }

// DesiredPolicy returns a copy for explicit confirmation. It contains private
// tailnet policy data; callers must not log it unintentionally.
func (p *PolicyPlan) DesiredPolicy() []byte {
	if p == nil {
		return nil
	}
	return bytes.Clone(p.desired)
}

// PreparePolicy reads but does not mutate the policy. Supply the original
// policy-before-*.json and policy-proposed.json from the successful
// policy-preview-*/apply directory created by onboard.configurePolicy, not an
// arbitrary/latest preview. The caller must establish artifact provenance:
// legacy builds need policy-complete and exactly one unambiguous apply pair.
// CheckPolicyDependencies can check other devices before deleting this device.
//
// This deliberately uses a conservative whole-policy equality guard: it
// validates that init only appended supported grants/tag owners, and refuses
// cleanup if the current policy differs semantically from both saved snapshots.
// Thus newer unrelated changes are never overwritten. HuJSON comments and key
// order are ignored for comparison; posted output is canonical JSON.
func (c *Client) PreparePolicy(ctx context.Context, tailnet string, before, applied []byte) (*PolicyPlan, error) {
	if !validTailnet(tailnet) {
		return nil, errors.New("invalid retained Tailscale tailnet")
	}
	old, oldJSON, err := parsePolicy(before)
	if err != nil {
		return nil, err
	}
	next, nextJSON, err := parsePolicy(applied)
	if err != nil {
		return nil, err
	}
	if !supportedAdditions(old, next) {
		return nil, ErrInvalidPolicy
	}
	current, etag, err := c.getPolicy(ctx, tailnet)
	if err != nil {
		return nil, err
	}
	unchanged := bytes.Equal(current, oldJSON)
	if !unchanged && !bytes.Equal(current, nextJSON) {
		return nil, ErrPolicyConflict
	}
	return &PolicyPlan{client: c, tailnet: tailnet, etag: etag, desired: oldJSON, unchanged: unchanged}, nil
}

// PrepareMeshPolicyRemoval removes Herdr mesh role ownership and grants from
// the current policy, including entries that predate this installation. It
// refuses mixed grants and any mesh references it cannot safely classify.
// The plan requires every other Herdr mesh role device to be absent before
// teardown; unrelated tailnet devices may remain.
func (c *Client) PrepareMeshPolicyRemoval(ctx context.Context, tailnet string) (*PolicyPlan, error) {
	if !validTailnet(tailnet) {
		return nil, errors.New("invalid Tailscale tailnet")
	}
	current, etag, err := c.getPolicy(ctx, tailnet)
	if err != nil {
		return nil, err
	}
	policy, _, err := parsePolicy(current)
	if err != nil {
		return nil, err
	}
	key, valid := tagOwnersKey(policy)
	if !valid {
		return nil, ErrInvalidPolicy
	}
	if key != "" {
		owners, ok := policy[key].(map[string]any)
		if !ok {
			return nil, ErrInvalidPolicy
		}
		for tag := range owners {
			if managedTag(tag) {
				delete(owners, tag)
			}
		}
	}
	for _, section := range []string{"grants", "acls"} {
		entries, exists := policy[section]
		if !exists {
			continue
		}
		list, ok := entries.([]any)
		if !ok {
			return nil, ErrInvalidPolicy
		}
		kept := make([]any, 0, len(list))
		for _, item := range list {
			rule, ok := item.(map[string]any)
			if !ok {
				return nil, ErrInvalidPolicy
			}
			remove := false
			for _, field := range []string{"src", "dst"} {
				selectors, ok := rule[field].([]any)
				if !ok {
					return nil, ErrInvalidPolicy
				}
				mesh, other := false, false
				for _, value := range selectors {
					selector, ok := value.(string)
					if !ok {
						return nil, ErrInvalidPolicy
					}
					if managedPolicySelector(selector) {
						mesh = true
					} else {
						other = true
					}
				}
				if mesh && other {
					return nil, ErrAmbiguousMeshPolicy
				}
				remove = remove || mesh
			}
			if !remove {
				kept = append(kept, item)
			}
		}
		policy[section] = kept
	}
	if containsMeshReference(policy) {
		return nil, ErrAmbiguousMeshPolicy
	}
	desired, err := json.Marshal(policy)
	if err != nil {
		return nil, ErrInvalidPolicy
	}
	return &PolicyPlan{client: c, tailnet: tailnet, etag: etag, desired: desired,
		unchanged: bytes.Equal(current, desired)}, nil
}

func containsMeshReference(value any) bool {
	switch item := value.(type) {
	case string:
		return strings.Contains(item, "tag:herdr-mesh-")
	case []any:
		for _, child := range item {
			if containsMeshReference(child) {
				return true
			}
		}
	case map[string]any:
		for key, child := range item {
			if containsMeshReference(key) || containsMeshReference(child) {
				return true
			}
		}
	}
	return false
}

func managedPolicySelector(selector string) bool {
	if managedTag(selector) {
		return true
	}
	for _, tag := range []string{"tag:herdr-mesh-server", "tag:herdr-mesh-node", "tag:herdr-mesh-client"} {
		if strings.HasPrefix(selector, tag+":") {
			return true // ACL destinations carry a port suffix.
		}
	}
	return false
}

// PolicyResult indicates either no work or a confirmed application.
type PolicyResult struct {
	AlreadyClean bool
	Reconciled   bool
}

// ApplyPolicy posts the reviewed plan exactly once, with its GET ETag in
// If-Match. A concurrent edit (412) is an error, not an overwrite or retry.
// After an uncertain result one GET may confirm the desired policy; otherwise
// ErrOutcomeUnknown is returned. Never create another plan to automatically
// retry an unknown mutation. ReconcilePolicy can inspect it without mutation.
// Before a changing POST, it lists devices and refuses if any mesh role is
// still in use, including by this device: delete the retained device first.
// Device membership and ACL updates are not atomic; prevent concurrent role
// enrollment during teardown. A list check cannot lock out later enrollments.
func (c *Client) ApplyPolicy(ctx context.Context, plan *PolicyPlan) (PolicyResult, error) {
	if plan == nil || plan.client != c {
		return PolicyResult{}, errors.New("invalid Tailscale policy plan")
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	if plan.attempted {
		return PolicyResult{}, ErrPlanUsed
	}
	if err := ctx.Err(); err != nil {
		return PolicyResult{}, err
	}
	plan.attempted = true
	if plan.unchanged {
		clean, err := c.ReconcilePolicy(ctx, plan)
		if err != nil {
			return PolicyResult{}, err
		}
		if !clean {
			return PolicyResult{}, ErrPolicyConflict
		}
		return PolicyResult{AlreadyClean: true}, nil
	}
	if err := c.CheckPolicyDependencies(ctx, plan, ""); err != nil {
		return PolicyResult{}, err
	}
	r, err := c.request(ctx, http.MethodPost, policyPath(plan.tailnet), plan.desired, plan.etag, MaxPolicyBytes, "update policy")
	if err == nil && r.status == http.StatusPreconditionFailed {
		return PolicyResult{}, ErrPolicyConflict
	}
	if err == nil && definiteRejection(r.status) {
		return PolicyResult{}, &HTTPError{"update policy", r.status}
	}
	if errors.Is(err, ErrClosed) {
		return PolicyResult{}, err
	}
	if err == nil && r.status == http.StatusOK {
		_, actual, parseErr := parsePolicy(r.body)
		if parseErr == nil && bytes.Equal(actual, plan.desired) {
			return PolicyResult{}, nil
		}
	}
	if clean, checkErr := c.ReconcilePolicy(ctx, plan); checkErr == nil && clean {
		return PolicyResult{Reconciled: true}, nil
	}
	return PolicyResult{}, ErrOutcomeUnknown
}

// ReconcilePolicy performs only a GET and compares the desired policy. A false
// result does not prove a prior write failed: it might still be in flight or
// have been followed by another writer. Retain pending state in that case.
// This can be called with a fresh context after an ApplyPolicy timeout.
func (c *Client) ReconcilePolicy(ctx context.Context, plan *PolicyPlan) (bool, error) {
	if plan == nil || plan.client != c {
		return false, errors.New("invalid Tailscale policy plan")
	}
	current, _, err := c.getPolicy(ctx, plan.tailnet)
	if err != nil {
		return false, err
	}
	return bytes.Equal(current, plan.desired), nil
}

func validTailnet(tailnet string) bool {
	return tailnetPattern.MatchString(tailnet) && tailnet != "." && tailnet != ".." &&
		!strings.HasPrefix(tailnet, "tskey-")
}

func policyPath(tailnet string) string { return "/tailnet/" + tailnet + "/acl" }

func (c *Client) getPolicy(ctx context.Context, tailnet string) ([]byte, string, error) {
	r, err := c.request(ctx, http.MethodGet, policyPath(tailnet), nil, "", MaxPolicyBytes, "get policy")
	if err != nil {
		return nil, "", err
	}
	if r.status != http.StatusOK {
		return nil, "", &HTTPError{"get policy", r.status}
	}
	if !validETag(r.etag) {
		return nil, "", errors.New("Tailscale policy response lacks a valid strong ETag")
	}
	_, canonical, err := parsePolicy(r.body)
	return canonical, r.etag, err
}

func validETag(etag string) bool {
	if len(etag) < 2 || len(etag) > 256 || etag[0] != '"' || etag[len(etag)-1] != '"' {
		return false
	}
	for _, b := range []byte(etag[1 : len(etag)-1]) {
		if b < 0x21 || b == '"' || b > 0x7e {
			return false
		}
	}
	return true
}

func parsePolicy(data []byte) (map[string]any, []byte, error) {
	if len(data) == 0 || len(data) > MaxPolicyBytes || !utf8.Valid(data) {
		return nil, nil, ErrInvalidPolicy
	}
	standard, err := hujson.Standardize(bytes.Clone(data))
	if err != nil {
		return nil, nil, ErrInvalidPolicy
	}
	decoder := json.NewDecoder(bytes.NewReader(standard))
	decoder.UseNumber()
	value, err := decodeValue(decoder, 0)
	if err != nil {
		return nil, nil, ErrInvalidPolicy
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, nil, ErrInvalidPolicy
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, nil, ErrInvalidPolicy
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, nil, ErrInvalidPolicy
	}
	return object, canonical, nil
}

// ParsePolicy validates a Tailscale policy and returns its object and canonical
// JSON. Setup uses the same strict parser as later owned-policy cleanup.
func ParsePolicy(data []byte) (map[string]any, []byte, error) {
	return parsePolicy(data)
}

// Reject duplicate keys instead of using encoding/json's last-key-wins
// behavior; ownership comparisons must have only one interpretation.
func decodeValue(d *json.Decoder, depth int) (any, error) {
	if depth > 100 {
		return nil, ErrInvalidPolicy
	}
	token, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch token {
	case json.Delim('{'):
		object := make(map[string]any)
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, ErrInvalidPolicy
			}
			if _, duplicate := object[name]; duplicate {
				return nil, ErrInvalidPolicy
			}
			value, err := decodeValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		if end, err := d.Token(); err != nil || end != json.Delim('}') {
			return nil, ErrInvalidPolicy
		}
		return object, nil
	case json.Delim('['):
		array := make([]any, 0)
		for d.More() {
			value, err := decodeValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if end, err := d.Token(); err != nil || end != json.Delim(']') {
			return nil, ErrInvalidPolicy
		}
		return array, nil
	default:
		if _, delimiter := token.(json.Delim); delimiter {
			return nil, ErrInvalidPolicy
		}
		return token, nil
	}
}

func supportedAdditions(before, applied map[string]any) bool {
	for key, value := range before {
		if key != "tagOwners" && key != "tagowners" && key != "grants" && !reflect.DeepEqual(value, applied[key]) {
			return false
		}
		if _, exists := applied[key]; !exists {
			return false
		}
	}
	for key := range applied {
		if _, exists := before[key]; !exists && key != "tagOwners" && key != "tagowners" && key != "grants" {
			return false
		}
	}
	return supportedOwners(before, applied) && supportedGrants(before, applied)
}

func supportedOwners(before, applied map[string]any) bool {
	oldKey, oldValid := tagOwnersKey(before)
	nextKey, nextValid := tagOwnersKey(applied)
	if !oldValid || !nextValid || oldKey != "" && oldKey != nextKey {
		return false
	}
	old, oldOK := before[oldKey].(map[string]any)
	next, nextOK := applied[nextKey].(map[string]any)
	oldPresent, nextPresent := oldKey != "", nextKey != ""
	if !nextPresent {
		return !oldPresent
	}
	if !nextOK || oldPresent && !oldOK {
		return false
	}
	for tag, owners := range old {
		if _, exists := next[tag]; !exists {
			return false
		}
		if !managedTag(tag) && !reflect.DeepEqual(owners, next[tag]) {
			return false
		}
	}
	for tag, owners := range next {
		if reflect.DeepEqual(old[tag], owners) {
			continue
		}
		if !managedTag(tag) {
			return false
		}
		added, ok := owners.([]any)
		if !ok {
			return false
		}
		existing, present := old[tag]
		previous, ok := existing.([]any)
		if present && !ok || len(added) != len(previous)+1 {
			return false
		}
		for i, value := range previous {
			if !reflect.DeepEqual(value, added[i]) {
				return false
			}
		}
		owner, ok := added[len(added)-1].(string)
		if !ok || owner == "" {
			return false
		}
		for _, value := range previous {
			if reflect.DeepEqual(value, owner) {
				return false
			}
		}
	}
	return true
}

func tagOwnersKey(policy map[string]any) (string, bool) {
	_, camel := policy["tagOwners"]
	_, lower := policy["tagowners"]
	if camel && lower {
		return "", false
	}
	if camel {
		return "tagOwners", true
	}
	if lower {
		return "tagowners", true
	}
	return "", true
}

func managedTag(tag string) bool {
	return tag == "tag:herdr-mesh-server" || tag == "tag:herdr-mesh-node" || tag == "tag:herdr-mesh-client"
}

func supportedGrants(before, applied map[string]any) bool {
	old, oldOK := before["grants"].([]any)
	next, nextOK := applied["grants"].([]any)
	_, oldPresent := before["grants"]
	_, nextPresent := applied["grants"]
	if !nextPresent {
		return !oldPresent
	}
	if !nextOK || oldPresent && !oldOK || len(next) < len(old) || len(next)-len(old) > 3 {
		return false
	}
	for i := range old {
		if !reflect.DeepEqual(old[i], next[i]) {
			return false
		}
	}
	dashboardAdded := false
	for i := len(old); i < len(next); i++ {
		grant, ok := next[i].(map[string]any)
		if !ok || len(grant) != 3 {
			return false
		}
		source, srcOK := singleString(grant["src"])
		dest, dstOK := singleString(grant["dst"])
		ip, ipOK := singleString(grant["ip"])
		if !srcOK || !dstOK || !ipOK ||
			(source != "tag:herdr-mesh-node" && source != "tag:herdr-mesh-client") ||
			dest != "tag:herdr-mesh-server" || !strings.HasPrefix(ip, "tcp:") {
			return false
		}
		port, err := strconv.Atoi(strings.TrimPrefix(ip, "tcp:"))
		if err != nil || port < 1 || port > 65535 || ip != "tcp:"+strconv.Itoa(port) {
			return false
		}
		if source == "tag:herdr-mesh-node" && port != 50052 {
			return false
		}
		if port != 50052 {
			if dashboardAdded {
				return false
			}
			dashboardAdded = true
		}
		for j := 0; j < i; j++ {
			other, ok := next[j].(map[string]any)
			if ok && reflect.DeepEqual(grant["src"], other["src"]) &&
				reflect.DeepEqual(grant["dst"], other["dst"]) && reflect.DeepEqual(grant["ip"], other["ip"]) {
				return false
			}
		}
	}
	return true
}

func singleString(value any) (string, bool) {
	list, ok := value.([]any)
	if !ok || len(list) != 1 {
		return "", false
	}
	text, ok := list[0].(string)
	return text, ok
}
