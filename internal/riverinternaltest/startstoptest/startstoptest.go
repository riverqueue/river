package startstoptest

import (
	"context"
	"sync"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/maintenance/startstop"
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

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < 50; j++ {
				err := svc.Start(ctx)
				if allowedStartErr == nil {
					require.NoError(tb, err)
				} else if err != nil {
					require.ErrorIs(tb, err, allowedStartErr)
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
