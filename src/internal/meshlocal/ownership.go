package meshlocal

import (
	"context"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

// IsRunning reports whether the managed lifetime guard is held, including
// interactive login and worker startup. It is not readiness: use Dial for that.
// It never changes state or permissions, activates a guard, or starts a process
// or network. Run still arbitrates races after a stopped observation.
func IsRunning(dir string) (bool, error) {
	return state.RoleStateInUse(context.Background(), dir, "client")
}
