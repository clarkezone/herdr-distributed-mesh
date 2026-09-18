package deinitnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"
)

const (
	maxDeviceListBytes = 2 << 20
	maxListedDevices   = 10000
)

// CheckPolicyDependencies performs a bounded, read-only check of the plan's
// tailnet. It refuses changing plans if another device has any herdr mesh role
// tag, even when the policy still exactly equals the saved applied artifact.
// No-op plans need no device-list permission and perform no request.
//
// For preflight before destructive work, exclude only the exact retained
// managed ID via retainedID (stable nodeId or legacy numeric id). Empty means
// exclude nothing. Hostnames, online/offline state, expiry, and authorization
// never establish that a device is safe to disregard. Errors contain no device
// details. This does not select a deletion target or authorize any mutation.
//
// ApplyPolicy independently repeats the check without exclusions immediately
// before its POST; delete the retained device before applying policy cleanup.
// Neither this check nor the ACL ETag can prevent concurrent device enrollment.
func (c *Client) CheckPolicyDependencies(ctx context.Context, plan *PolicyPlan, retainedID string) error {
	if plan == nil || plan.client != c {
		return errors.New("invalid Tailscale policy plan")
	}
	if !plan.HasChanges() {
		return nil
	}
	if retainedID != "" && !deviceIDPattern.MatchString(retainedID) {
		return errors.New("invalid retained Tailscale device ID")
	}
	r, err := c.request(ctx, http.MethodGet, "/tailnet/"+plan.tailnet+"/devices", nil, "", maxDeviceListBytes, "check policy dependencies")
	if err != nil {
		return err
	}
	if r.status != http.StatusOK {
		return &HTTPError{"check policy dependencies", r.status}
	}
	return checkDeviceList(r.body, retainedID)
}

func checkDeviceList(body []byte, retainedID string) error {
	invalid := errors.New("Tailscale device list is incomplete or invalid; policy cleanup refused")
	if len(body) > maxDeviceListBytes || !utf8.Valid(body) || !json.Valid(body) {
		return invalid
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	value, err := decodeValue(decoder, 0)
	if err != nil {
		return invalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return invalid
	}
	object, ok := value.(map[string]any)
	// The official endpoint currently has no pagination. Refuse an unfamiliar
	// envelope rather than accidentally accepting a future partial result.
	if !ok || len(object) != 1 {
		return invalid
	}
	devices, ok := object["devices"].([]any)
	if !ok || len(devices) > maxListedDevices {
		return invalid
	}
	seen := make(map[string]bool, len(devices)*2)
	inUse := false
	for _, item := range devices {
		device, ok := item.(map[string]any)
		if !ok {
			return invalid
		}
		hasID, excluded := false, false
		for _, field := range []string{"id", "nodeId"} {
			value, exists := device[field]
			if !exists {
				continue
			}
			id, ok := value.(string)
			if !ok || !deviceIDPattern.MatchString(id) ||
				(field == "nodeId") != strings.HasPrefix(id, "n") || seen[id] {
				return invalid
			}
			seen[id] = true
			hasID = true
			excluded = excluded || id == retainedID
		}
		if !hasID {
			return invalid
		}
		// Untagged devices can omit tags or encode them as null.
		tagsValue := device["tags"]
		if tagsValue == nil {
			continue
		}
		tags, ok := tagsValue.([]any)
		if !ok {
			return invalid
		}
		for _, value := range tags {
			tag, ok := value.(string)
			if !ok || !strings.HasPrefix(tag, "tag:") || len(tag) <= len("tag:") {
				return invalid
			}
			if !excluded && managedTag(tag) {
				inUse = true
			}
		}
	}
	if inUse {
		return ErrPolicyInUse
	}
	return nil
}
