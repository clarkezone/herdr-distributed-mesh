package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlite3 "modernc.org/sqlite/lib"
)

type sqliteCodeError int

func (e sqliteCodeError) Error() string { return fmt.Sprintf("injected SQLite error %d", e) }
func (e sqliteCodeError) Code() int     { return int(e) }

func TestTransactionInterruptRequiresCanceledContextAndVerifiedRollback(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		for _, kind := range []string{"interrupt", "joined-interrupt", "joined-storage", "storage"} {
			t.Run(fmt.Sprintf("canceled=%t/%s", canceled, kind), func(t *testing.T) {
				s := openTestStore(t, testPath(t))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var original error = sqliteCodeError(sqlite3.SQLITE_INTERRUPT)
				switch kind {
				case "joined-interrupt":
					original = errors.Join(original, context.Canceled)
				case "joined-storage":
					original = errors.Join(original, sqliteCodeError(sqlite3.SQLITE_IOERR))
				case "storage":
					original = sqliteCodeError(sqlite3.SQLITE_IOERR)
				}
				err := s.transaction(ctx, func(tx *sql.Tx) error {
					if err := bind(ctx, tx, Binding{StableID: "temporary", InstanceID: "temporary"}); err != nil {
						return err
					}
					if canceled {
						cancel()
					}
					return original
				})
				wantCanceled := canceled && (kind == "interrupt" || kind == "joined-interrupt")
				if RetryableCancellation(err) != wantCanceled {
					t.Fatalf("incorrect interruption classification: %v", err)
				}
				if wantCanceled && !errors.Is(err, context.Canceled) || !wantCanceled && err != original {
					t.Fatalf("cancellation was not normalized or storage evidence was lost: %v", err)
				}
				if rowCount(t, s, "SELECT count(*) FROM bindings") != 0 {
					t.Fatal("interrupted transaction returned before rollback")
				}
			})
		}
	}
}

func TestTransactionCancellationRollsBackBeforeReturning(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		for _, callbackError := range []bool{false, true} {
			name := "cancel/commit"
			if deadline {
				name = "deadline/commit"
			}
			if callbackError {
				name += "/callback"
			}
			t.Run(name, func(t *testing.T) {
				s := openTestStore(t, testPath(t))
				for i := 0; i < 10; i++ {
					ctx, cancel := context.WithCancel(context.Background())
					if deadline {
						cancel()
						ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
					}
					err := s.transaction(ctx, func(tx *sql.Tx) error {
						if err := bind(ctx, tx, Binding{StableID: "temporary", InstanceID: "temporary"}); err != nil {
							return err
						}
						if !deadline {
							cancel()
						}
						<-ctx.Done()
						if callbackError {
							return ctx.Err()
						}
						return nil
					})
					want := ctx.Err()
					cancel()
					if want == nil || !errors.Is(err, want) || !RetryableCancellation(err) {
						t.Fatalf("iteration %d: canceled transaction returned %v, want %v", i, err, want)
					}
					if count := rowCount(t, s, "SELECT count(*) FROM bindings"); count != 0 {
						t.Fatal("canceled transaction returned before rollback finished")
					}
					requireOK(t, s.transaction(context.Background(), func(tx *sql.Tx) error {
						var value int
						return tx.QueryRowContext(context.Background(), "SELECT 1").Scan(&value)
					}))
				}
			})
		}
	}
}

func TestTransactionCancellationPreservesOtherErrors(t *testing.T) {
	s := openTestStore(t, testPath(t))
	storageErr := errors.New("injected storage failure")
	for _, original := range []error{storageErr, sql.ErrTxDone, errors.Join(sql.ErrTxDone, storageErr)} {
		ctx, cancel := context.WithCancel(context.Background())
		err := s.transaction(ctx, func(tx *sql.Tx) error {
			cancel()
			return original
		})
		cancel()
		if err != original || errors.Is(err, context.Canceled) || RetryableCancellation(err) {
			t.Fatalf("cancellation masked storage error: %v", err)
		}
	}
	err := s.transaction(context.Background(), func(tx *sql.Tx) error {
		return tx.Rollback()
	})
	if err != sql.ErrTxDone {
		t.Fatalf("uncanceled double completion was masked: %v", err)
	}
}

func TestRetryableCancellationRequiresVerifiedRollback(t *testing.T) {
	s := openTestStore(t, testPath(t))
	for _, manualCompletion := range []string{"none", "rollback", "commit", "joined-error"} {
		t.Run(manualCompletion, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := s.transaction(ctx, func(tx *sql.Tx) error {
				if err := bind(ctx, tx, Binding{StableID: manualCompletion, InstanceID: manualCompletion}); err != nil {
					return err
				}
				switch manualCompletion {
				case "rollback":
					if err := tx.Rollback(); err != nil {
						return err
					}
				case "commit":
					if err := tx.Commit(); err != nil {
						return err
					}
				}
				cancel()
				if manualCompletion == "joined-error" {
					return errors.Join(ctx.Err(), errors.New("injected storage error"))
				}
				return ctx.Err()
			})
			if RetryableCancellation(err) != (manualCompletion == "none") {
				t.Fatalf("unsafe retry certification after %s: %v", manualCompletion, err)
			}
		})
	}
}

func TestCanceledWorkspacePollingKeepsStoreUsable(t *testing.T) {
	s := commandStore(t)
	record := admitWorkspace(t, s, workspaceCommand(1, "project1"), false)
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		entered := false
		err := s.transaction(ctx, func(tx *sql.Tx) error {
			entered = true
			if _, err := readCommand(ctx, tx, "WHERE c.actor_id = ? AND c.command_id = ?", "actor", record.Command.CommandId); err != nil {
				return err
			}
			if err := bind(ctx, tx, Binding{StableID: "canceled-poll", InstanceID: "canceled-poll"}); err != nil {
				return err
			}
			cancel()
			_, err := readCommand(ctx, tx, "WHERE c.actor_id = ? AND c.command_id = ?", "actor", record.Command.CommandId)
			return err
		})
		cancel()
		if !entered || !errors.Is(err, context.Canceled) || !RetryableCancellation(err) {
			t.Fatalf("poll %d did not cancel and roll back an active transaction: entered=%t err=%v", i, entered, err)
		}
		if rowCount(t, s, "SELECT count(*) FROM bindings WHERE instance_id = 'canceled-poll'") != 0 {
			t.Fatal("canceled poll returned before its transaction rolled back")
		}

		// Retain naturally racing public polls as additional coverage; the
		// active transaction above guarantees cancellation on every iteration.
		racing, stop := context.WithTimeout(context.Background(), 50*time.Microsecond)
		_, raceErr := s.GetCommand(racing, "actor", record.Command.CommandId)
		stop()
		if raceErr != nil && !errors.Is(raceErr, context.Canceled) && !errors.Is(raceErr, context.DeadlineExceeded) {
			t.Fatalf("racing poll %d poisoned store: %v", i, raceErr)
		}

		got, err := s.GetCommand(context.Background(), "actor", record.Command.CommandId)
		requireOK(t, err)
		if got.Command.CommandId != record.Command.CommandId {
			t.Fatal("healthy lookup after canceled poll returned another command")
		}
	}
}

func TestWorktreeTransactionHonorsCanceledQueries(t *testing.T) {
	s := commandStore(t)
	command := worktreeCommand(1, "project1")
	admitWorkspace(t, s, command, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		called = true
		return nil
	})
	if called || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled transaction began: callback=%v, err=%v", called, err)
	}
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		err := s.transaction(ctx, func(tx *sql.Tx) error {
			if err := bind(ctx, tx, Binding{StableID: "temporary", InstanceID: "temporary"}); err != nil {
				return err
			}
			cancel()
			_, queryErr := readCommands(ctx, tx, "", nil)
			if !errors.Is(queryErr, context.Canceled) {
				t.Fatalf("transaction lifetime context replaced caller query context: %v", queryErr)
			}
			return queryErr
		})
		cancel()
		if !errors.Is(err, context.Canceled) || rowCount(t, s, "SELECT count(*) FROM bindings WHERE stable_id = 'temporary'") != 0 {
			t.Fatalf("canceled query failed to roll back synchronously: %v", err)
		}
		got, err := s.GetCommand(context.Background(), "actor", command.CommandId)
		requireOK(t, err)
		if got.Status != statusAccepted || got.Command.WorktreeCreate.GetName() != command.WorktreeCreate.Name {
			t.Fatal("canceled query damaged worktree journal")
		}
	}
}
