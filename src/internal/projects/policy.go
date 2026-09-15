// Package projects loads an explicit, bounded project allowlist. Node policies
// contain local paths and must never be published as shared state.
package projects

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	maxPolicyBytes = 64 * 1024
	maxBindings    = 128
	maxActors      = 128
)

var (
	ErrDenied        = errors.New("projects: binding denied")
	ErrInvalidPolicy = errors.New("projects: invalid policy")
	ErrPolicyRead    = errors.New("projects: policy unavailable")
	ErrInvalidPath   = errors.New("projects: checkout precondition failed")
	tokenPattern     = regexp.MustCompile(`^[A-Za-z0-9:_-]{1,128}$`)
)

// Binding is a locally authorized checkout. Path is canonical for node policies
// and empty for coordinator policies. Only Load can mint a path-valid binding.
type Binding struct {
	ProjectID string
	NodeID    string
	Revision  string
	Path      string

	actors map[string]struct{}
	pin    *pathPin
}

type pathPin struct {
	source, canonical, projectID, nodeID, revision string
	info                                           os.FileInfo
}

type bindingKey struct{ nodeID, projectID string }

// Policy is immutable after loading; Resolve returns copies of its bindings.
type Policy struct {
	bindings map[bindingKey]Binding
}

// Load rejects ambiguous JSON and unknown fields. An empty nodeID selects a
// coordinator policy, where paths are forbidden. Otherwise every binding must
// belong to that node and pin an existing local Git checkout. IDs use 1..128
// ASCII letters, digits, colons, underscores, or hyphens; actor lists have
// 1..128 distinct entries. Files are limited to 64 KiB and 128 bindings.
func Load(path string, nodeID string) (*Policy, error) {
	if nodeID != "" && !tokenPattern.MatchString(nodeID) {
		return nil, ErrInvalidPolicy
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrPolicyRead
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, ErrPolicyRead
	}
	data, err := io.ReadAll(io.LimitReader(file, maxPolicyBytes+1))
	if err != nil {
		return nil, ErrPolicyRead
	}
	if len(data) > maxPolicyBytes || !utf8.Valid(data) {
		return nil, ErrInvalidPolicy
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if uniqueJSON(decoder, 0) != nil {
		return nil, ErrInvalidPolicy
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, ErrInvalidPolicy
	}
	var root map[string]json.RawMessage
	if json.Unmarshal(data, &root) != nil || len(root) != 1 {
		return nil, ErrInvalidPolicy
	}
	var entries []map[string]json.RawMessage
	if json.Unmarshal(root["projects"], &entries) != nil || len(entries) == 0 || len(entries) > maxBindings {
		return nil, ErrInvalidPolicy
	}
	p := &Policy{bindings: make(map[bindingKey]Binding, len(entries))}
	for _, entry := range entries {
		for key := range entry {
			switch key {
			case "project_id", "node_id", "binding_revision", "actor_ids", "path":
			default:
				return nil, ErrInvalidPolicy
			}
		}
		b := Binding{}
		for key, target := range map[string]*string{
			"project_id": &b.ProjectID, "node_id": &b.NodeID, "binding_revision": &b.Revision,
		} {
			if readString(entry[key], target) != nil || !tokenPattern.MatchString(*target) {
				return nil, ErrInvalidPolicy
			}
		}
		var actors []string
		if json.Unmarshal(entry["actor_ids"], &actors) != nil || len(actors) == 0 || len(actors) > maxActors {
			return nil, ErrInvalidPolicy
		}
		b.actors = make(map[string]struct{}, len(actors))
		for _, actor := range actors {
			if !tokenPattern.MatchString(actor) {
				return nil, ErrInvalidPolicy
			}
			if _, duplicate := b.actors[actor]; duplicate {
				return nil, ErrInvalidPolicy
			}
			b.actors[actor] = struct{}{}
		}
		if raw, ok := entry["path"]; ok && readString(raw, &b.Path) != nil {
			return nil, ErrInvalidPolicy
		}
		if nodeID == "" {
			if b.Path != "" {
				return nil, ErrInvalidPolicy
			}
		} else {
			if b.NodeID != nodeID {
				return nil, ErrInvalidPolicy
			}
			source := b.Path
			canonical, info, err := checkout(source)
			if err != nil {
				return nil, ErrInvalidPath
			}
			for _, previous := range p.bindings {
				if os.SameFile(previous.pin.info, info) {
					return nil, ErrInvalidPolicy
				}
			}
			b.Path = canonical
			b.pin = &pathPin{source, canonical, b.ProjectID, b.NodeID, b.Revision, info}
			if b.ValidatePath() != nil {
				return nil, ErrInvalidPath
			}
		}
		key := bindingKey{b.NodeID, b.ProjectID}
		if _, duplicate := p.bindings[key]; duplicate {
			return nil, ErrInvalidPolicy
		}
		p.bindings[key] = b
	}
	return p, nil
}

// Resolve requires an exact node, project, revision, and actor match. A nil
// policy denies everything; there are no defaults or wildcard actors.
func (p *Policy) Resolve(nodeID, projectID, revision, actorID string) (Binding, error) {
	if p == nil {
		return Binding{}, ErrDenied
	}
	b, ok := p.bindings[bindingKey{nodeID, projectID}]
	if !ok || b.Revision != revision {
		return Binding{}, ErrDenied
	}
	if _, ok := b.actors[actorID]; !ok {
		return Binding{}, ErrDenied
	}
	return b, nil
}

// ValidatePath detects directory replacement and retargeting of the configured
// path or its canonical destination. It cannot eliminate filesystem TOCTOU:
// the local filesystem and Herdr are trusted between this check and IPC.
func (b Binding) ValidatePath() error {
	if b.pin == nil || b.Path != b.pin.canonical || b.ProjectID != b.pin.projectID ||
		b.NodeID != b.pin.nodeID || b.Revision != b.pin.revision {
		return ErrInvalidPath
	}
	for _, path := range []string{b.pin.source, b.pin.canonical} {
		canonical, info, err := checkout(path)
		if err != nil || canonical != b.pin.canonical || !os.SameFile(b.pin.info, info) {
			return ErrInvalidPath
		}
	}
	return nil
}

func checkout(path string) (string, os.FileInfo, error) {
	if !validLocalPath(path) {
		return "", nil, ErrInvalidPath
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || !validLocalPath(canonical) {
		return "", nil, ErrInvalidPath
	}
	canonical = filepath.Clean(canonical)
	file, err := os.Open(canonical)
	if err != nil {
		return "", nil, ErrInvalidPath
	}
	// Stat the open handle: os.Stat on Windows can defer reading file identity
	// until SameFile, which would incorrectly pin a replacement directory.
	info, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil || closeErr != nil || !info.IsDir() {
		return "", nil, ErrInvalidPath
	}
	marker, err := os.Stat(filepath.Join(canonical, ".git"))
	if err != nil || (!marker.IsDir() && !marker.Mode().IsRegular()) {
		return "", nil, ErrInvalidPath
	}
	return canonical, info, nil
}

func validLocalPath(path string) bool {
	if len(path) == 0 || len(path) > 32768 || !utf8.ValidString(path) ||
		!filepath.IsAbs(path) || strings.HasPrefix(path, `\\`) || strings.HasPrefix(path, "//") {
		return false
	}
	for _, r := range path {
		if r < 32 || r == 127 {
			return false
		}
	}
	return localVolume(path)
}

func readString(raw json.RawMessage, into *string) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, into) != nil {
		return ErrInvalidPolicy
	}
	return nil
}

func uniqueJSON(d *json.Decoder, depth int) error {
	if depth > 8 {
		return ErrInvalidPolicy
	}
	token, err := d.Token()
	if err != nil {
		return ErrInvalidPolicy
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for d.More() {
			key, err := d.Token()
			name, ok := key.(string)
			if err != nil || !ok {
				return ErrInvalidPolicy
			}
			if _, duplicate := seen[name]; duplicate {
				return ErrInvalidPolicy
			}
			seen[name] = struct{}{}
			if err := uniqueJSON(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := uniqueJSON(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return ErrInvalidPolicy
	}
	_, err = d.Token()
	return err
}
