package startstoptest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/maintenance/startstop"
)

// Stress is a test helper that puts stress on a service's start and stop
// functions so that we can detect any data races that it might have due to
// improper use of BaseStopStart.
func Stress(ctx context.Context, tb testing.TB, svc startstop.Service) {
	tb.Helper()

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < 50; j++ {
				require.NoError(tb, svc.Start(ctx))

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
