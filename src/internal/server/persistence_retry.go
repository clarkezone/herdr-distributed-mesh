package server

import (
	"context"
	"log"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

// Only the state store can certify cancellation before commit and successful
// rollback. Retry that local persistence operation once, never a native effect.
func persistCoordinator(operation string, persist func(context.Context) error) error {
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		started := time.Now()
		err := persist(ctx)
		cancel()
		if err == nil {
			if attempt > 1 {
				log.Printf("coordinator persistence recovered operation=%s attempt=%d", operation, attempt)
			}
			return nil
		}
		retry := attempt == 1 && state.RetryableCancellation(err)
		log.Printf("coordinator persistence operation=%s attempt=%d elapsed=%s rolled_back_cancellation=%t retry=%t error=%v",
			operation, attempt, time.Since(started), state.RetryableCancellation(err), retry, err)
		if !retry {
			return err
		}
	}
}
