// Package riverinternaltest contains shared testing utilities for tests
// throughout the rest of the project.
package riverinternaltest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/goleak"

	"github.com/riverqueue/river/rivershared/riversharedtest"
)

// SchedulerShortInterval is an artificially short interval for the scheduler
// that's used in the tests of various components to make sure that errored jobs
// always end up in a `retryable` state rather than `available`. Normally, the
// job executor sets `available` if the retry delay is smaller than the
// scheduler's interval. To simplify things so errors are always `retryable`,
// this time is picked to be smaller than any retry delay that the default
// retry policy will ever produce. It's shared so we can document/explain it all
// in one place.
const SchedulerShortInterval = 500 * time.Millisecond

// DiscardContinuously drains continuously out of the given channel and discards
// anything that comes out of it. Returns a stop function that should be invoked
// to stop draining. Stop must be invoked before tests finish to stop an
// internal goroutine.
func DiscardContinuously[T any](drainChan <-chan T) func() {
	var (
		stop     = make(chan struct{})
		stopped  = make(chan struct{})
		stopOnce sync.Once
	)

	go func() {
		defer close(stopped)

		for {
			select {
			case <-drainChan:
			case <-stop:
				return
			}
		}
	}()

	return func() {
		stopOnce.Do(func() {
			close(stop)
			<-stopped
		})
	}
}

// DrainContinuously drains continuously out of the given channel and
// accumulates items that are received from it. Returns a get function that can
// be called to retrieve the current set of received items, and which will also
// cause the function to shut down and stop draining. This function must be
// invoked before tests finish to stop an internal goroutine. It's safe to call
// it multiple times.
func DrainContinuously[T any](drainChan <-chan T) func() []T {
	var (
		items    []T
		stop     = make(chan struct{})
		stopped  = make(chan struct{})
		stopOnce sync.Once
	)

	go func() {
		defer close(stopped)

		for {
			select {
			case item := <-drainChan:
				items = append(items, item)
			case <-stop:
				// Drain until empty
				for {
					select {
					case item := <-drainChan:
						items = append(items, item)
					default:
						return
					}
				}
			}
		}
	}()

	return func() []T {
		stopOnce.Do(func() {
			close(stop)
			<-stopped
		})

		return items
	}
}

// TestTx starts a test transaction that's rolled back automatically as the test
// case is cleaning itself up. This can be used as a lighter weight alternative
// to `testdb.Manager` in components where it's not necessary to have many
// connections open simultaneously.
//
// TODO(brandur): Get rid of this in favor of `riversharedtest`.
func TestTx(ctx context.Context, tb testing.TB) pgx.Tx {
	tb.Helper()

	return riversharedtest.TestTxPool(ctx, tb, riversharedtest.DBPool(ctx, tb))
}

// WrapTestMain performs some common setup and teardown that should be shared
// amongst all packages. e.g. Checks for no goroutine leaks on teardown.
func WrapTestMain(m *testing.M) {
	status := m.Run()

	if status == 0 {
		if err := goleak.Find(riversharedtest.IgnoredKnownGoroutineLeaks...); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)
			status = 1
		}
	}

	os.Exit(status)
}
