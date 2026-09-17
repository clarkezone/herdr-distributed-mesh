package herdr

import (
	"context"
	"errors"
	"os"
	"time"
)

type lifecyclePreDispatch struct{ err error }

func (e *lifecyclePreDispatch) Error() string { return e.err.Error() }
func (e *lifecyclePreDispatch) Unwrap() error { return e.err }

func lifecycleDirectoryInfo(path string) (os.FileInfo, error) {
	// On Windows, path-based Stat may defer loading file IDs until SameFile,
	// which would reopen a replaced path. Capture identity from a handle now.
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, statErr := file.Stat()
	if err := errors.Join(statErr, file.Close()); err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, ErrAgentPrecondition
	}
	return info, nil
}

func (o *observer) lifecycleCheckSession(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.config.CheckSession != nil {
		if err := o.config.CheckSession(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (o *observer) lifecycleGuardedEffect(ctx context.Context, hook LifecycleHook, stage LifecycleStage,
	result *LifecycleResult, refresh, effect func() error) error {
	return lifecycleEffect(ctx, hook, stage, result, func() error {
		before := *result
		if err := o.lifecycleCheckSession(ctx); err != nil {
			return &lifecyclePreDispatch{err}
		}
		refreshErr := refresh()
		// Fence a restart during the read-only refresh, even when its response
		// happens to reuse the same public pane and terminal identifiers.
		if err := o.lifecycleCheckSession(ctx); err != nil {
			*result = before
			return &lifecyclePreDispatch{err}
		}
		if refreshErr != nil {
			return &lifecyclePreDispatch{refreshErr}
		}
		err := effect()
		if o.config.CheckSession != nil {
			checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()
			if sessionErr := o.lifecycleCheckSession(checkCtx); sessionErr != nil {
				// Do not attribute observations from an unverified incarnation
				// to this command. Preserve all earlier, confirmed evidence.
				*result = before
				return errors.Join(ErrAgentIndeterminate, err, sessionErr)
			}
		}
		return err
	})
}

func (o *observer) lifecycleShellReady(ctx context.Context, handle LifecycleHandle) error {
	pane, err := o.lifecyclePane(ctx, handle)
	if err != nil {
		return err
	}
	if pane.Provider != "unknown" || pane.LaunchPending {
		return ErrAgentPrecondition
	}
	if pane.Status == "working" || pane.Status == "blocked" {
		return ErrAgentBusy
	}
	return nil
}

func (o *observer) lifecyclePromptReady(ctx context.Context, result *LifecycleResult) error {
	agent, err := o.getAgent(ctx, lifecycleTarget(result.Handle))
	if err != nil {
		return err
	}
	if !lifecycleMatches(result.Handle, agent, true) {
		return ErrAgentChanged
	}
	result.ObservedStatus = agent.Status
	switch agent.Status {
	case "working":
		return ErrAgentBusy
	case "blocked":
		return ErrAgentBlocked
	case "idle", "done":
	default:
		return ErrAgentPrecondition
	}
	if !agent.InteractiveReady || agent.LaunchPending {
		return ErrAgentPrecondition
	}
	return nil
}

func (o *observer) lifecycleStopReady(ctx context.Context, result *LifecycleResult) error {
	handle := result.Handle
	if err := o.lifecycleWorkspace(ctx, handle.WorkspaceID); err != nil {
		return err
	}
	panes, err := o.lifecyclePanes(ctx)
	if err != nil {
		return err
	}
	found, workspacePanes := false, 0
	for _, pane := range panes {
		if pane.WorkspaceId == handle.WorkspaceID {
			workspacePanes++
		}
		if pane.Target.PaneId == handle.PaneID {
			if !lifecycleMatches(handle, pane, true) {
				return ErrAgentChanged
			}
			found = true
		}
	}
	if !found {
		return ErrAgentUnavailable
	}
	if workspacePanes < 2 {
		return ErrLifecycleLastPane
	}
	agent, err := o.getAgent(ctx, lifecycleTarget(handle))
	if err != nil {
		return err
	}
	if !lifecycleMatches(handle, agent, true) {
		return ErrAgentChanged
	}
	result.ObservedStatus = agent.Status
	return nil
}
