package river

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

// Just proves that DefaultRetryPolicy implements the RetryPolicy interface.
var _ ClientRetryPolicy = &DefaultClientRetryPolicy{}

func TestDefaultClientRetryPolicy_NextRetry(t *testing.T) {
	t.Parallel()

	t.Run("ConfiguredTime", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		retryPolicy := &DefaultClientRetryPolicy{timeNowFunc: func() time.Time { return now }}

		nextRetryAt := retryPolicy.NextRetry(&rivertype.JobRow{})
		require.WithinDuration(t, now.Add(time.Second), nextRetryAt, 150*time.Millisecond)
	})

	t.Run("ZeroValue", func(t *testing.T) {
		t.Parallel()

		retryPolicy := &DefaultClientRetryPolicy{}

		nextRetryAt := retryPolicy.NextRetry(&rivertype.JobRow{})
		require.WithinDuration(t, time.Now().UTC().Add(time.Second), nextRetryAt, 150*time.Millisecond)
	})
}
