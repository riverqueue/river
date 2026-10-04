//go:build riverconformance

package harness_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func performanceJobs(t *testing.T) int {
	t.Helper()

	jobs := 200
	if value := os.Getenv("RIVER_CONFORMANCE_PERFORMANCE_JOBS"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		jobs = parsed
	}
	require.GreaterOrEqual(t, jobs, 20)
	return jobs
}
