package timeutil_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/util/timeutil"
)

func TestSecondsAsDuration(t *testing.T) {
	t.Parallel()

	require.Equal(t, 1*time.Second, timeutil.SecondsAsDuration(1.0))
}

func TestTickerWithInitialTick(t *testing.T) {
	t.Parallel()

	t.Run("TicksImmediately", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			now := time.Now()
			ticker := timeutil.NewTickerWithInitialTick(ctx, time.Hour)
			synctest.Wait()
			select {
			case tick := <-ticker.C:
				require.Equal(t, now, tick)
			default:
				t.Fatal("Initial tick was not immediate")
			}
		})
	})

	t.Run("TicksPeriodically", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			const interval = 100 * time.Microsecond
			now := time.Now()
			ticker := timeutil.NewTickerWithInitialTick(ctx, interval)
			synctest.Wait()
			require.Equal(t, now, <-ticker.C)

			for range 9 {
				time.Sleep(interval - time.Nanosecond)
				synctest.Wait()
				require.Empty(t, ticker.C)
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				now = now.Add(interval)
				select {
				case tick := <-ticker.C:
					require.Equal(t, now, tick)
				default:
					t.Fatal("Periodic tick was not delivered")
				}
			}
		})
	})
}
