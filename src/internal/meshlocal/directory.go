package meshlocal

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"unicode"
)

type stateDirKey struct{}

func WithStateDir(ctx context.Context, dir string) (context.Context, error) {
	if !filepath.IsAbs(dir) || filepath.Dir(filepath.Clean(dir)) == filepath.Clean(dir) ||
		strings.ContainsFunc(dir, unicode.IsControl) {
		return nil, errors.New("--state-dir requires an absolute non-root directory")
	}
	return context.WithValue(ctx, stateDirKey{}, dir), nil
}

func StateDir(ctx context.Context) (string, error) {
	if dir, ok := ctx.Value(stateDirKey{}).(string); ok {
		return dir, nil
	}
	return DefaultDir()
}
