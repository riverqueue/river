package timeutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"weavelab.xyz/river/internal/riverinternaltest"
)

func TestSecondsAsDuration(t *testing.T) {
	t.Parallel()

	require.Equal(t, 1*time.Second, SecondsAsDuration(1.0))
}

func TestTickerWithInitialTick(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("TicksImmediately", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)

		ticker := NewTickerWithInitialTick(ctx, 1*time.Hour)
		riverinternaltest.WaitOrTimeout(t, ticker.C)
	})

	t.Run("TicksPeriodically", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)

		ticker := NewTickerWithInitialTick(ctx, 100*time.Microsecond)
		for i := 0; i < 10; i++ {
			t.Logf("Waiting on tick %d", i)
			riverinternaltest.WaitOrTimeout(t, ticker.C)
		}
	})
}
