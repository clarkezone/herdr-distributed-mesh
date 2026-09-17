package herdr

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// CreateWorktree creates only an explicitly authorized new linked worktree via
// protocol-18 Herdr. Git subprocesses are bounded, read-only pre/postconditions.
// Callers must serialize mutations and journal every attempted create: any
// unconfirmed effect is indeterminate, never retried, rolled back, or deleted.
// Filesystem/IPC checks are not atomic against other trusted local processes.
func CreateWorktree(ctx context.Context, config Config, binding projects.Binding, request *pb.WorktreeCreate) (*pb.WorktreeCreateResult, error) {
	return createWorktree(ctx, config, binding, request, dialLocal)
}

func createWorktree(ctx context.Context, config Config, binding projects.Binding, request *pb.WorktreeCreate, dial dialFunc) (*pb.WorktreeCreateResult, error) {
	if request != nil && request.BaseCommit == "" {
		request = proto.Clone(request).(*pb.WorktreeCreate)
		git, err := newWorktreeGit(3 * time.Second)
		if err != nil || binding.ValidatePath() != nil {
			return nil, ErrWorkspacePrecondition
		}
		head, err := git.read(ctx, binding.Path, "rev-parse", "--verify", "--end-of-options", "HEAD^{commit}")
		if err != nil {
			return nil, err
		}
		request.BaseCommit = head
	}
	if protocol.ValidateWorktreeCreate(request) != nil ||
		request.ProjectId != binding.ProjectID || request.BindingRevision != binding.Revision {
		return nil, ErrWorkspacePrecondition
	}
	// Snapshot the typed inputs; do not retain caller-owned mutable messages.
	value := &pb.WorktreeCreateResult{
		ProjectId: request.ProjectId, BindingRevision: request.BindingRevision,
		Name: request.Name, Branch: request.Branch, BaseCommit: request.BaseCommit,
	}
	destination, err := binding.WorktreeDestination(value.Name)
	if err != nil {
		return nil, ErrWorkspacePrecondition
	}
	ctx, cancel := context.WithTimeout(ctx, protocol.MaxCommandTTL)
	defer cancel()
	if ctx.Err() != nil {
		return nil, ErrWorkspaceUnavailable
	}
	address, err := localAddress(config.SocketPath)
	if err != nil || config.RefreshInterval < 0 || config.RetryDelay < 0 || config.RequestTimeout < 0 {
		return nil, ErrWorkspaceUnavailable
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 3 * time.Second
	}
	config.SocketPath = address
	git, err := newWorktreeGit(config.RequestTimeout)
	if err != nil {
		return nil, ErrWorkspaceUnavailable
	}
	o := observer{config: config, dial: dial}
	pong, err := o.rpc(ctx, "ping", "pong")
	if err != nil {
		return nil, ErrWorkspaceUnavailable
	}
	if _, version, err := versionProtocol(pong); err != nil || version != supportedProtocol {
		return nil, ErrWorkspaceUnavailable
	}
	_, _, cleanup, list, err := o.request(ctx, "worktree.list", "worktree_list", struct {
		Cwd string `json:"cwd"`
	}{binding.Path})
	if err != nil {
		return nil, ErrWorkspaceUnavailable
	}
	cleanup()
	if err := validateWorktreeList(list, binding, destination, value.Branch); err != nil {
		return nil, err
	}
	before, err := git.preconditions(ctx, binding.Path, value.Branch, value.BaseCommit)
	if err != nil {
		return nil, err
	}
	if current, err := binding.WorktreeDestination(value.Name); err != nil || current != destination {
		return nil, ErrWorkspacePrecondition
	}
	if ctx.Err() != nil {
		return nil, ErrWorkspaceUnavailable
	}
	// Checkout can be much slower than a read. Use the remaining command TTL,
	// not the default three-second read timeout, and never extend its deadline.
	if config.CheckSession != nil {
		if err := config.CheckSession(ctx); err != nil {
			return nil, err
		}
	}
	deadline, _ := ctx.Deadline()
	o.config.RequestTimeout = time.Until(deadline)
	_, _, cleanup, created, err := o.request(ctx, "worktree.create", "worktree_created", struct {
		Cwd    string `json:"cwd"`
		Path   string `json:"path"`
		Branch string `json:"branch"`
		Base   string `json:"base"`
		Focus  bool   `json:"focus"`
	}{binding.Path, destination, value.Branch, value.BaseCommit, false})
	if err != nil {
		return nil, ErrWorkspaceIndeterminate
	}
	cleanup()
	id, checkout, err := workspaceIdentity(created["workspace"])
	if err != nil || checkout == "" {
		return nil, ErrWorkspaceIndeterminate
	}
	tree, err := decodeWorktreeInfo(created["worktree"])
	if err != nil || !tree.linked || tree.bare || tree.detached || tree.prunable ||
		!worktreeBranchMatches(tree.branch, value.Branch) ||
		(tree.workspaceID != "" && tree.workspaceID != id) {
		return nil, ErrWorkspaceIndeterminate
	}
	// Destination() intentionally requires absence, so it cannot be used here.
	if binding.ValidateWorktreeRoot() != nil || !createdWorktreeDirectory(destination, binding.WorktreeRoot) {
		return nil, ErrWorkspaceIndeterminate
	}
	for _, path := range []string{checkout, tree.path} {
		if match, err := workspaceMatches(path, destination); err != nil || !match {
			return nil, ErrWorkspaceIndeterminate
		}
	}
	if git.postconditions(ctx, binding.Path, destination, value.Branch, value.BaseCommit, before) != nil ||
		binding.ValidateWorktreeRoot() != nil || ctx.Err() != nil {
		return nil, ErrWorkspaceIndeterminate
	}
	value.WorkspaceId = id
	return value, nil
}

type worktreeInfo struct {
	path, branch, workspaceID        string
	bare, detached, prunable, linked bool
}

func decodeWorktreeInfo(raw json.RawMessage) (worktreeInfo, error) {
	var value object
	var tree worktreeInfo
	var label string
	if decodeRequired(raw, &value) != nil ||
		required(value, "path", &tree.path) != nil || !validWorkspacePath(tree.path) ||
		required(value, "label", &label) != nil || !worktreeText(label) ||
		required(value, "is_bare", &tree.bare) != nil ||
		required(value, "is_detached", &tree.detached) != nil ||
		required(value, "is_prunable", &tree.prunable) != nil ||
		required(value, "is_linked_worktree", &tree.linked) != nil ||
		optionalWorktreeString(value, "branch", &tree.branch) != nil ||
		optionalWorktreeString(value, "open_workspace_id", &tree.workspaceID) != nil ||
		(tree.workspaceID != "" && !idPattern.MatchString(tree.workspaceID)) {
		return worktreeInfo{}, ErrWorkspaceUnavailable
	}
	return tree, nil
}

func worktreeText(value string) bool {
	if len(value) > 4096 || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

func optionalWorktreeString(value object, key string, into *string) error {
	raw, ok := value[key]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	if decodeRequired(raw, into) != nil || *into == "" || !worktreeText(*into) {
		return ErrWorkspaceUnavailable
	}
	return nil
}

// The adapter accepts the exact short branch or Git's refs/heads/<branch>
// spelling in protocol-18 metadata, not suffixes or other ref types.
func worktreeBranchMatches(actual, requested string) bool {
	return actual == requested || actual == "refs/heads/"+requested
}

func validateWorktreeList(list object, binding projects.Binding, destination, branch string) error {
	var source object
	var checkout, repoRoot, repoKey, repoName, workspaceID string
	var trees []json.RawMessage
	if required(list, "source", &source) != nil ||
		required(source, "source_checkout_path", &checkout) != nil || !validWorkspacePath(checkout) ||
		required(source, "repo_root", &repoRoot) != nil || !validWorkspacePath(repoRoot) ||
		required(source, "repo_key", &repoKey) != nil || repoKey == "" || !worktreeText(repoKey) ||
		required(source, "repo_name", &repoName) != nil || repoName == "" || !worktreeText(repoName) ||
		optionalWorktreeString(source, "source_workspace_id", &workspaceID) != nil ||
		(workspaceID != "" && !idPattern.MatchString(workspaceID)) ||
		required(list, "worktrees", &trees) != nil || len(trees) > maxEntities {
		return ErrWorkspaceUnavailable
	}
	if match, err := workspaceMatches(checkout, binding.Path); err != nil || !match {
		return ErrWorkspacePrecondition
	}
	seenPaths := make(map[string]struct{}, len(trees))
	seenIDs := make(map[string]struct{}, len(trees))
	for _, raw := range trees {
		tree, err := decodeWorktreeInfo(raw)
		if err != nil {
			return ErrWorkspaceUnavailable
		}
		path, err := canonicalWorktreeCandidate(tree.path)
		if err != nil {
			return ErrWorkspaceUnavailable
		}
		if _, duplicate := seenPaths[path]; duplicate {
			return ErrWorkspaceUnavailable
		}
		seenPaths[path] = struct{}{}
		if tree.workspaceID != "" {
			if _, duplicate := seenIDs[tree.workspaceID]; duplicate {
				return ErrWorkspaceUnavailable
			}
			seenIDs[tree.workspaceID] = struct{}{}
		}
		// Conservatively reject case-only conflicts on every platform, including
		// case-insensitive directories and stale/prunable list entries.
		if strings.EqualFold(path, destination) || worktreeBranchMatches(tree.branch, branch) {
			return ErrWorkspacePrecondition
		}
	}
	return nil
}

// A prunable entry may no longer exist. Resolve its nearest existing ancestor
// so aliases cannot conceal a conflicting path without requiring stale entries
// to be present on disk.
func canonicalWorktreeCandidate(path string) (string, error) {
	var leaves []string
	for {
		canonical, err := filepath.EvalSymlinks(path)
		if err == nil {
			for i := len(leaves) - 1; i >= 0; i-- {
				canonical = filepath.Join(canonical, leaves[i])
			}
			return canonical, nil
		}
		if !os.IsNotExist(err) || filepath.Dir(path) == path {
			return "", ErrWorkspaceUnavailable
		}
		leaves = append(leaves, filepath.Base(path))
		path = filepath.Dir(path)
	}
}

func createdWorktreeDirectory(destination, root string) bool {
	info, err := os.Lstat(destination)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	canonical, err := filepath.EvalSymlinks(destination)
	if err != nil || filepath.Base(canonical) != filepath.Base(destination) {
		return false
	}
	match, err := workspaceMatches(filepath.Dir(canonical), root)
	return err == nil && match
}
