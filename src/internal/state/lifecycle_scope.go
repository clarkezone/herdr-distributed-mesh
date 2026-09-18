package state

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

// Canonical native incarnation, not the spelling of a session selector, scopes
// quarantine. The configured default and native "default" can be the same
// server. Actors, request keys, providers and optional conversation IDs must
// not provide a way around an unresolved mutation against that server.
func lifecycleResourceKey(node, incarnation, resourceKind, resourceID string) (string, error) {
	if !protocol.ValidIdempotencyKey(node) || !protocol.ValidSessionIncarnation(incarnation) ||
		!protocol.ValidIdempotencyKey(resourceID) || (resourceKind != "workspace" && resourceKind != "terminal") {
		return "", errors.New("invalid lifecycle quarantine identity")
	}
	value := sha256.Sum256([]byte(strings.Join([]string{node, incarnation, resourceKind, resourceID}, "\x00")))
	return hex.EncodeToString(value[:]), nil
}
