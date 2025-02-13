package startstoptest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/startstop"
)

// Stress is a test helper that puts stress on a service's start and stop
// functions so that we can detect any data races that it might have due to
// improper use of BaseStopStart.
func Stress(ctx context.Context, tb testingT, svc startstop.Service) {
	StressErr(ctx, tb, svc, nil)
}

// StressErr is the same as Stress except that the given allowedStartErr is
// tolerated on start (either no error or an error that is allowedStartErr is
// allowed). This is useful for services that may want to return an error if
// they're shut down as they're still starting up.
func StressErr(ctx context.Context, tb testingT, svc startstop.Service, allowedStartErr error) { //nolint:varnamelen
	tb.Helper()

	var wg sync.WaitGroup

	isAllowedStartError := func(err error) bool {
		if allowedStartErr != nil {
			if errors.Is(err, allowedStartErr) {
				return true
			}
		}

		// Always allow this one because a fairly common intermittent failure is
		// to produce an I/O timeout while trying to connect to Postgres.
		//
		//     write failed: write tcp 127.0.0.1:60976->127.0.0.1:5432: i/o timeout
		//
		if strings.HasSuffix(err.Error(), "i/o timeout") {
			return true
		}

		return false
	}

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 50 {
				err := svc.Start(ctx)
				if err != nil && !isAllowedStartError(err) {
					require.NoError(tb, err)
				}

				stopped := make(chan struct{})

				go func() {
					defer close(stopped)
					svc.Stop()
				}()

				select {
				case <-stopped:
				case <-time.After(5 * time.Second):
					require.FailNow(tb, "Timed out waiting for service to stop")
				}
			}
		}()
	}

	wg.Wait()
}

// Minimal interface for *testing.B/*testing.T that lets us test a failure
// condition for our test helpers above.
type testingT interface {
	Errorf(format string, args ...interface{})
	FailNow()
	Helper()
}
