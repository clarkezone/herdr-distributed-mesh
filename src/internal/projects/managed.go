package projects

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Managed holds at most 128 current node-local bindings. Central configuration
// supplies revisions; actors and legacy worktree grants are not checked here.
// Filesystem pins are immutable, including in copies returned before an update.
type Managed struct {
	mu       sync.RWMutex
	nodeID   string
	bindings map[string]Binding
}

// NewManaged creates an empty cache. An invalid node ID denies all operations.
func NewManaged(nodeID string) *Managed {
	return &Managed{nodeID: nodeID, bindings: make(map[string]Binding)}
}

// Resolve requires the exact node, project, and current opaque revision.
func (m *Managed) Resolve(nodeID, projectID, revision, actorID string) (Binding, error) {
	if m == nil {
		return Binding{}, ErrDenied
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.bindings[projectID]
	if !ok || m.nodeID != nodeID || b.Revision != revision {
		return Binding{}, ErrDenied
	}
	return b, nil
}

// HasWorktrees reports whether at least one managed binding is available.
func (m *Managed) HasWorktrees() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.bindings) != 0
}

// Apply validates an existing Git checkout without modifying it. An empty root
// selects a sibling named <checkout basename>-worktrees. Only a single missing
// root directory is created; its parent must already exist. Existing roots must
// be empty or still pinned to this project's current binding. A revision cannot
// be reused for different paths. Failed applications leave the cache unchanged.
func (m *Managed) Apply(ctx context.Context, projectID, revision, checkoutPath, root string) (Binding, error) {
	return m.apply(ctx, projectID, revision, checkoutPath, root, nil)
}

// AdoptLegacy explicitly migrates a locally loaded, pinned legacy binding,
// including its nonempty worktree root. Workspace-only bindings get the default
// root. Exported fields alone cannot authorize adoption; the original pins must
// still validate. Adoption never transfers the legacy actor grants.
func (m *Managed) AdoptLegacy(ctx context.Context, legacy Binding) (Binding, error) {
	if m == nil || legacy.NodeID != m.nodeID || legacy.ValidatePath() != nil {
		return Binding{}, ErrInvalidPath
	}

	if legacy.WorktreeRoot != "" && legacy.ValidateWorktreeRoot() != nil {
		return Binding{}, ErrInvalidPath
	}
	return m.apply(ctx, legacy.ProjectID, legacy.Revision, legacy.Path, legacy.WorktreeRoot, &legacy)
}

// RestoreOwnership accepts only implementation-private successful cache records.
// Revalidation does not establish readiness without current-stream delivery.
func (m *Managed) RestoreOwnership(ctx context.Context, projectID, revision, path, root string) error {
	canonical, info, err := realCheckout(ctx, path)
	if err != nil {
		return ErrInvalidPath
	}
	rootCanonical, rootInfo, err := managedRoot(root)
	if err != nil {
		return ErrInvalidPath
	}
	if m == nil {
		return ErrInvalidPath
	}
	b := Binding{ProjectID: projectID, NodeID: m.nodeID, Revision: revision, Path: canonical,
		AllowWorktrees: true, WorktreeRoot: rootCanonical,
		pin:         &pathPin{path, canonical, projectID, m.nodeID, revision, info},
		worktreePin: &pathPin{root, rootCanonical, projectID, m.nodeID, revision, rootInfo}}
	_, err = m.AdoptLegacy(ctx, b)
	return err
}

func (m *Managed) apply(ctx context.Context, projectID, revision, checkoutPath, root string, legacy *Binding) (Binding, error) {
	if m == nil || !tokenPattern.MatchString(m.nodeID) ||
		!tokenPattern.MatchString(projectID) || !tokenPattern.MatchString(revision) {
		return Binding{}, ErrInvalidPolicy
	}
	if ctx == nil {
		return Binding{}, ErrInvalidPath
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil {
		return Binding{}, ErrInvalidPath
	}
	old, exists := m.bindings[projectID]
	if !exists && len(m.bindings) >= maxBindings {
		return Binding{}, ErrInvalidPolicy
	}
	canonical, info, err := realCheckout(ctx, checkoutPath)
	if err != nil {
		return Binding{}, ErrInvalidPath
	}
	b := Binding{
		ProjectID: projectID, NodeID: m.nodeID, Revision: revision,
		Path: canonical, AllowWorktrees: true,
		pin: &pathPin{checkoutPath, canonical, projectID, m.nodeID, revision, info},
	}
	if root == "" {
		root = filepath.Join(filepath.Dir(canonical), filepath.Base(canonical)+"-worktrees")
	}
	if !validLocalPath(root) {
		return Binding{}, ErrInvalidPath
	}
	root = filepath.Clean(root)
	parentSource := filepath.Dir(root)
	if parentSource == root {
		return Binding{}, ErrInvalidPath
	}
	parent, parentInfo, err := directory(parentSource)
	if err != nil {
		return Binding{}, ErrInvalidPath
	}
	rootPath := filepath.Join(parent, filepath.Base(root))
	rootCanonical, rootInfo, err := managedRoot(rootPath)
	missing := os.IsNotExist(err)
	if err != nil && !missing {
		return Binding{}, ErrInvalidPath
	}
	if exists && old.Revision == revision &&
		(old.ValidateWorktreeRoot() != nil || old.Path != b.Path || old.WorktreeRoot != rootPath) {
		return Binding{}, ErrInvalidPolicy
	}
	pins := []*pathPin{b.pin}
	for id, other := range m.bindings {
		if id == projectID {
			continue
		}
		if other.ValidateWorktreeRoot() != nil {
			return Binding{}, ErrInvalidPath
		}
		for _, pin := range []*pathPin{other.pin, other.worktreePin} {
			if overlap, err := directoriesOverlap(b.pin, pin); err != nil || overlap {
				return Binding{}, ErrInvalidPolicy
			}
			pins = append(pins, pin)
		}
	}
	for _, pin := range pins {
		var overlap bool
		if missing {
			// An absent leaf cannot contain an existing pinned directory.
			overlap, err = ancestorHasIdentity(parent, pin.info)
		} else {
			overlap, err = directoriesOverlap(&pathPin{canonical: rootCanonical, info: rootInfo}, pin)
		}
		if err != nil || overlap {
			return Binding{}, ErrInvalidPolicy
		}
	}
	if !missing {
		owned := exists && old.ValidateWorktreeRoot() == nil &&
			os.SameFile(old.worktreePin.info, rootInfo)
		if legacy != nil && legacy.WorktreeRoot != "" {
			owned = owned || (legacy.ValidateWorktreeRoot() == nil &&
				os.SameFile(legacy.worktreePin.info, rootInfo))
		}
		if !owned && !emptyDirectory(rootPath, rootInfo) {
			return Binding{}, ErrInvalidPath
		}
	}
	if ctx.Err() != nil || !sameDirectory(parentSource, parent, parentInfo) || b.ValidatePath() != nil {
		return Binding{}, ErrInvalidPath
	}
	if missing {
		// Never traverse unvalidated missing ancestors or accept a concurrent
		// creator. The filesystem remains trusted between pin checks and use.
		if os.Mkdir(rootPath, 0700) != nil {
			return Binding{}, ErrInvalidPath
		}
		rootCanonical, rootInfo, err = managedRoot(rootPath)
		if err != nil {
			return Binding{}, ErrInvalidPath
		}
	}
	if !sameDirectory(parentSource, parent, parentInfo) ||
		!sameDirectory(rootPath, rootCanonical, rootInfo) || ctx.Err() != nil {
		return Binding{}, ErrInvalidPath
	}
	b.WorktreeRoot = rootCanonical
	b.worktreePin = &pathPin{root, rootCanonical, projectID, m.nodeID, revision, rootInfo}
	if b.ValidateWorktreeRoot() != nil {
		return Binding{}, ErrInvalidPath
	}
	m.bindings[projectID] = b
	return b, nil
}

func managedRoot(path string) (string, os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", nil, ErrInvalidPath
	}
	return directory(path)
}

func sameDirectory(source, canonical string, info os.FileInfo) bool {
	for _, path := range []string{source, canonical} {
		got, current, err := directory(path)
		if err != nil || got != canonical || !os.SameFile(info, current) {
			return false
		}
	}
	return true
}

func ancestorHasIdentity(path string, info os.FileInfo) (bool, error) {
	for {
		_, current, err := directory(path)
		if err != nil {
			return false, err
		}
		if os.SameFile(info, current) {
			return true, nil
		}
		parent := filepath.Dir(path)
		if parent == path {
			return false, nil
		}
		path = parent
	}
}

func emptyDirectory(path string, expected os.FileInfo) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !os.SameFile(expected, info) {
		return false
	}
	_, err = file.Readdirnames(1)
	return err == io.EOF
}

func realCheckout(ctx context.Context, path string) (string, os.FileInfo, error) {
	canonical, info, err := checkout(path)
	if err != nil {
		return "", nil, ErrInvalidPath
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "-C", canonical, "rev-parse", "--show-toplevel")
	cmd.WaitDelay = time.Second
	// Inherited Git overrides must not make a fake directory appear to be a
	// checkout, redirect repository discovery, or trigger optional writes.
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(strings.ToUpper(entry), "GIT_") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0")
	var output boundedGitOutput
	cmd.Stdout = &output
	cmd.Stderr = io.Discard
	if cmd.Run() != nil || ctx.Err() != nil {
		return "", nil, ErrInvalidPath
	}
	top := strings.TrimSuffix(strings.TrimSuffix(output.String(), "\n"), "\r")
	topCanonical, topInfo, err := directory(top)
	if err != nil || topCanonical != canonical || !os.SameFile(info, topInfo) ||
		!sameDirectory(path, canonical, info) {
		return "", nil, ErrInvalidPath
	}
	return canonical, info, nil
}

type boundedGitOutput struct{ buffer bytes.Buffer }

func (b *boundedGitOutput) String() string { return b.buffer.String() }

func (b *boundedGitOutput) Write(p []byte) (int, error) {
	if b.buffer.Len()+len(p) > 32770 {
		return 0, ErrInvalidPath
	}
	return b.buffer.Write(p)
}
