package projects

import (
	"os"
	"path/filepath"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

// HasWorktrees reports explicit opt-in, including coordinator bindings. Older
// policies remain workspace-only; a coordinator never receives local roots.
func (p *Policy) HasWorktrees() bool {
	if p != nil {
		for _, b := range p.bindings {
			if b.AllowWorktrees {
				return true
			}
		}
	}
	return false
}

// ValidateWorktreeRoot verifies the original authorization and both configured
// and canonical directory identities. It also revalidates the bound checkout.
// The local filesystem is trusted between these checks and Herdr IPC.
func (b Binding) ValidateWorktreeRoot() error {
	pin := b.worktreePin
	if !b.AllowWorktrees || pin == nil || b.WorktreeRoot != pin.canonical ||
		b.ProjectID != pin.projectID || b.NodeID != pin.nodeID || b.Revision != pin.revision ||
		b.ValidatePath() != nil {
		return ErrInvalidPath
	}
	for _, path := range []string{pin.source, pin.canonical} {
		canonical, info, err := directory(path)
		if err != nil || canonical != pin.canonical || !os.SameFile(pin.info, info) {
			return ErrInvalidPath
		}
	}
	return nil
}

// WorktreeDestination returns an absent portable leaf directly below the pinned
// root. It never reserves or creates directories; only Herdr performs effects.
func (b Binding) WorktreeDestination(name string) (string, error) {
	if !protocol.ValidWorktreeName(name) || b.ValidateWorktreeRoot() != nil {
		return "", ErrInvalidPath
	}
	path := filepath.Join(b.WorktreeRoot, name)
	if filepath.Dir(path) != b.WorktreeRoot {
		return "", ErrInvalidPath
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return "", ErrInvalidPath
	}
	if b.ValidateWorktreeRoot() != nil {
		return "", ErrInvalidPath
	}
	return path, nil
}

func (p *Policy) validateWorktreeOverlaps() error {
	for key, b := range p.bindings {
		if b.worktreePin == nil {
			continue
		}
		for otherKey, other := range p.bindings {
			if overlap, err := directoriesOverlap(b.worktreePin, other.pin); err != nil || overlap {
				return ErrInvalidPolicy
			}
			if key != otherKey && other.worktreePin != nil {
				if overlap, err := directoriesOverlap(b.worktreePin, other.worktreePin); err != nil || overlap {
					return ErrInvalidPolicy
				}
			}
		}
	}
	return nil
}

// Compare identities along both ancestor chains, not case-folded string
// prefixes: aliases (including Windows junctions) must not bypass overlap checks.
func directoriesOverlap(a, b *pathPin) (bool, error) {
	for _, pair := range [][2]*pathPin{{a, b}, {b, a}} {
		for path := pair[0].canonical; ; path = filepath.Dir(path) {
			_, info, err := directory(path)
			if err != nil {
				return false, err
			}
			if os.SameFile(info, pair[1].info) {
				return true, nil
			}
			if filepath.Dir(path) == path {
				break
			}
		}
	}
	return false, nil
}
