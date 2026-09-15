package state

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

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
					if want == nil || err != want {
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
		if err != original || errors.Is(err, context.Canceled) {
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

func TestCanceledWorkspacePollingKeepsStoreUsable(t *testing.T) {
	s := commandStore(t)
	record := admitWorkspace(t, s, workspaceCommand(1, "project1"), false)
	canceled := 0
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Microsecond)
		_, err := s.GetCommand(ctx, "actor", record.Command.CommandId)
		cancel()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			canceled++
		} else if err != nil {
			t.Fatalf("poll %d poisoned store: %v", i, err)
		}
		got, err := s.GetCommand(context.Background(), "actor", record.Command.CommandId)
		requireOK(t, err)
		if got.Command.CommandId != record.Command.CommandId {
			t.Fatal("healthy lookup after canceled poll returned another command")
		}
	}
	if canceled == 0 {
		t.Fatal("polling did not exercise cancellation")
	}
}
