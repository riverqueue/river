package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunSQLite(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"fast", "returning"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			// Exercise a partial final insertion batch and the real completion path,
			// including verification of durable rows and cleanup of the temporary DB.
			cfg := config{BatchSize: 10, InsertMode: mode, Jobs: 23, PayloadBytes: 32, Workers: 2}
			var sample trial
			require.NoError(t, runSQLite(t.Context(), cfg, &sample))
			require.Equal(t, cfg.Jobs, sample.Completed)
			require.Positive(t, sample.DrainJobsPerSecond)
			require.Positive(t, sample.InsertJobsPerSecond)
			require.Equal(t, "wal", sample.Settings["journal_mode"])
			require.Equal(t, "2", sample.Settings["synchronous"])
			require.Less(t, sample.DrainSeconds, (30 * time.Second).Seconds())
		})
	}
}

func TestSummarize(t *testing.T) {
	t.Parallel()

	t.Run("Even", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "15 [10, 20]", summarize([]float64{20, 10}))
	})

	t.Run("Odd", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "20 [10, 40]", summarize([]float64{40, 10, 20}))
	})
}
