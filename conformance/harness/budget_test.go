package harness_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// soakFinishMargin covers what a soak does once its duration elapses:
// waiting for the final batch, stopping clients, and shutting adapters down.
// It exceeds one adapterRequestTimeout plus adapterExitTimeout, so a hung
// adapter at the end of a soak still fails on the harness's own bounds.
const soakFinishMargin = 5 * time.Minute

// soakBudgetError reports whether a soak of duration, plus soakFinishMargin,
// fits in the time remaining before the test's deadline.
func soakBudgetError(variable string, duration, remaining time.Duration) error {
	if needed := duration + soakFinishMargin; needed > remaining {
		return fmt.Errorf("%s=%s needs %s including time to finish, but only %s remain before go test's -timeout; raise -timeout (CONFORMANCE_SOAK_TIMEOUT for make) or shorten the soak",
			variable, duration, needed, remaining.Round(time.Second))
	}
	return nil
}

func TestSoakBudgetError(t *testing.T) {
	t.Parallel()

	t.Run("FitsWithinDeadline", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, soakBudgetError("RIVER_CONFORMANCE_SOAK_DURATION", 10*time.Minute, 15*time.Minute))
	})

	t.Run("RejectsSoakOutlastingDeadline", func(t *testing.T) {
		t.Parallel()

		err := soakBudgetError("RIVER_CONFORMANCE_SOAK_DURATION", 6*time.Hour, 6*time.Hour+time.Minute)
		require.EqualError(t, err, "RIVER_CONFORMANCE_SOAK_DURATION=6h0m0s needs 6h5m0s including time to finish, but only 6h1m0s remain before go test's -timeout; raise -timeout (CONFORMANCE_SOAK_TIMEOUT for make) or shorten the soak")
	})
}
