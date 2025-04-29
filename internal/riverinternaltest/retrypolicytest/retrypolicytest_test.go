package retrypolicytest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryPolicySlowIntervalSufficientlyLarge(t *testing.T) {
	t.Parallel()

	// Should be a good margin larger than the job rescuer's default interval so
	// that it can easily be used to test the job rescuer with making jobs
	// accidentally eligible to work again right away.
	require.Greater(t, (&RetryPolicySlow{}).Interval(), 1*time.Hour)
}
