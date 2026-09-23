package projects

import (
	"errors"
	"os"
)

var ErrAmbiguousRepository = errors.New("projects: ambiguous repository mapping")

// ProjectForRepository matches a native repository root to a current binding.
// It returns an empty ID for an unregistered repository, never infers ownership
// from a basename or parent directory, and does not expose configured paths.
func (m *Managed) ProjectForRepository(path string) (string, error) {
	if !validLocalPath(path) {
		return "", ErrInvalidPath
	}
	if m == nil {
		return "", nil
	}
	m.mu.RLock()
	bindings := make([]Binding, 0, len(m.bindings))
	for _, binding := range m.bindings {
		bindings = append(bindings, binding)
	}
	m.mu.RUnlock()
	if len(bindings) == 0 {
		return "", nil
	}
	_, info, err := directory(path)
	if err != nil {
		return "", err
	}
	var projectID string
	for _, binding := range bindings {
		if binding.pin == nil || !os.SameFile(info, binding.pin.info) {
			continue
		}
		if err := binding.ValidatePath(); err != nil {
			return "", err
		}
		if projectID != "" && projectID != binding.ProjectID {
			return "", ErrAmbiguousRepository
		}
		projectID = binding.ProjectID
	}
	return projectID, nil
}
