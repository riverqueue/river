package river

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobstats"
)

func TestJobStatisticsFromInternal(t *testing.T) {
	t.Parallel()

	require.Equal(t, &JobStatistics{
		CompleteDuration:  1 * time.Second,
		QueueWaitDuration: 2 * time.Second,
		RunDuration:       3 * time.Second,
	}, jobStatisticsFromInternal(&jobstats.JobStatistics{
		CompleteDuration:  1 * time.Second,
		QueueWaitDuration: 2 * time.Second,
		RunDuration:       3 * time.Second,
	}))
}
