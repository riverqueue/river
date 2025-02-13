package timeutil_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/timeutil"
)

func TestSecondsAsDuration(t *testing.T) {
	t.Parallel()

	require.Equal(t, 1*time.Second, timeutil.SecondsAsDuration(1.0))
}

func TestTickerWithInitialTick(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("TicksImmediately", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)

		ticker := timeutil.NewTickerWithInitialTick(ctx, 1*time.Hour)
		riversharedtest.WaitOrTimeout(t, ticker.C)
	})

	t.Run("TicksPeriodically", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)

		ticker := timeutil.NewTickerWithInitialTick(ctx, 100*time.Microsecond)
		for i := range 10 {
			t.Logf("Waiting on tick %d", i)
			riversharedtest.WaitOrTimeout(t, ticker.C)
		}
	})
}
