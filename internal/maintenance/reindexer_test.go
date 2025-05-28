package maintenance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/startstoptest"
)

func TestReindexer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec   riverdriver.Executor
		now    time.Time
		schema string
	}

	setup := func(t *testing.T) (*Reindexer, *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		)

		bundle := &testBundle{
			exec:   riverpgxv5.New(dbPool).GetExecutor(),
			schema: schema,
		}

		archetype := riversharedtest.BaseServiceArchetype(t)
		bundle.now = archetype.Time.StubNowUTC(time.Now())

		fromNow := func(d time.Duration) func(time.Time) time.Time {
			return func(t time.Time) time.Time {
				return t.Add(d)
			}
		}

		svc := NewReindexer(archetype, &ReindexerConfig{
			ScheduleFunc: fromNow(500 * time.Millisecond),
			Schema:       schema,
		}, bundle.exec)
		svc.StaggerStartupDisable(true)
		svc.TestSignals.Init(t)
		t.Cleanup(svc.Stop)

		return svc, bundle
	}

	runImmediatelyThenOnceAnHour := func() func(time.Time) time.Time {
		alreadyRan := false
		return func(t time.Time) time.Time {
			if alreadyRan {
				return t.Add(time.Hour)
			}
			alreadyRan = true
			return t.Add(time.Millisecond)
		}
	}

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)
		svc.Logger = riversharedtest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		svc.TestSignals = ReindexerTestSignals{}   // deinit so channels don't fill

		startstoptest.Stress(ctx, t, svc)
	})

	t.Run("ReindexOneSuccess", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		// Protect against changes in test setup.
		require.NotEmpty(t, svc.Config.IndexNames)

		for _, indexName := range svc.Config.IndexNames {
			didReindex, err := svc.reindexOne(ctx, indexName)
			require.NoError(t, err)
			require.True(t, didReindex)
		}
	})

	t.Run("ReindexSkippedWithReindexArtifact", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		requireReindexOne := func(indexName string) bool {
			didReindex, err := svc.reindexOne(ctx, indexName)
			require.NoError(t, err)
			return didReindex
		}

		indexName := svc.Config.IndexNames[0]

		// With a `_ccnew` index in place, the reindexer refuses to run.
		require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("CREATE INDEX %s_ccnew ON %s.river_job (id)", indexName, bundle.schema)))
		require.False(t, requireReindexOne(indexName))

		// With the index dropped again, reindexing can now occur.
		require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("DROP INDEX %s.%s_ccnew", bundle.schema, indexName)))
		require.True(t, requireReindexOne(indexName))

		// `_ccold` also prevents reindexing.
		require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("CREATE INDEX %s_ccold ON %s.river_job (id)", indexName, bundle.schema)))
		require.False(t, requireReindexOne(indexName))

		// And with `_ccold` dropped, reindexing can proceed.
		require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("DROP INDEX %s.%s_ccold", bundle.schema, indexName)))
		require.True(t, requireReindexOne(indexName))
	})

	t.Run("ReindexesEachIndex", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		svc.Config.IndexNames = []string{
			"river_job_kind",
			"river_job_prioritized_fetching_index",
			"river_job_state_and_finalized_at_index",
		}
		svc.Config.ScheduleFunc = runImmediatelyThenOnceAnHour()

		require.NoError(t, svc.Start(ctx))
		svc.TestSignals.Reindexed.WaitOrTimeout()

		select {
		case <-svc.TestSignals.Reindexed.WaitC():
			require.FailNow(t, "Didn't expect reindexing to occur again")
		case <-time.After(100 * time.Millisecond):
		}

		// Make sure that no `CONCURRENTLY` artifacts exist after reindexing is
		// supposed to be done. Postgres creates a new index suffixed with
		// `_ccnew` before swapping it in as the new index. The existing index
		// is renamed `_ccold` before being dropped concurrently.
		//
		// https://www.postgresql.org/docs/current/sql-reindex.html#SQL-REINDEX-CONCURRENTLY
		for _, indexName := range svc.Config.IndexNames {
			for _, reindexArtifactName := range []string{indexName + "_ccnew", indexName + "_ccold"} {
				indexExists, err := bundle.exec.IndexExists(ctx, &riverdriver.IndexExistsParams{Index: reindexArtifactName, Schema: bundle.schema})
				require.NoError(t, err)
				require.False(t, indexExists)
			}
		}
	})

	t.Run("ReindexDeletesArtifactsWhenCancelledWithStop", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)
		svc.skipReindexArtifactCheck = true

		requireIndexExists := func(indexName string) bool {
			indexExists, err := bundle.exec.IndexExists(ctx, &riverdriver.IndexExistsParams{Index: indexName, Schema: bundle.schema})
			require.NoError(t, err)
			return indexExists
		}

		var (
			indexName    = svc.Config.IndexNames[0]
			indexNameNew = indexName + "_ccnew"
			indexNameOld = indexName + "_ccold"
		)

		require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("CREATE INDEX %s ON %s.river_job (id)", indexNameNew, bundle.schema)))
		require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("CREATE INDEX %s ON %s.river_job (id)", indexNameOld, bundle.schema)))

		require.True(t, requireIndexExists(indexNameNew))
		require.True(t, requireIndexExists(indexNameOld))

		{
			// Pre-cancel context to simulate a reindexer being stopped while
			// building a new index.  This requires use of
			// `skipReindexArtifactCheck` above because checking for reindex
			// artifacts is the first thing the function does upon entry, and
			// normally a cancelled context would error on that step first.
			// Using the flag lets it drop through that to test `REINDEX`.
			ctx, cancel := context.WithCancelCause(ctx)
			cancel(startstop.ErrStop)

			didReindex, err := svc.reindexOne(ctx, indexName)
			require.ErrorIs(t, err, context.Canceled)
			require.False(t, didReindex)
		}

		require.False(t, requireIndexExists(indexNameNew))
		require.False(t, requireIndexExists(indexNameOld))
	})

	t.Run("StopsImmediately", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		require.NoError(t, svc.Start(ctx))
		svc.Stop()
	})

	t.Run("RespectsContextCancellation", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		ctx, cancelFunc := context.WithCancel(ctx)
		require.NoError(t, svc.Start(ctx))

		// To avoid a potential race, make sure to get a reference to the
		// service's stopped channel _before_ cancellation as it's technically
		// possible for the cancel to "win" and remove the stopped channel
		// before we can start waiting on it.
		stopped := svc.Stopped()
		cancelFunc()
		riversharedtest.WaitOrTimeout(t, stopped)
	})

	t.Run("DefaultConfigs", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)
		svc = NewReindexer(&svc.Archetype, &ReindexerConfig{}, bundle.exec)

		require.Equal(t, defaultIndexNames, svc.Config.IndexNames)
		require.Equal(t, ReindexerTimeoutDefault, svc.Config.Timeout)
		require.Equal(t, svc.Config.ScheduleFunc(bundle.now), (&DefaultReindexerSchedule{}).Next(bundle.now))
	})
}

func TestDefaultReindexerSchedule(t *testing.T) {
	t.Parallel()

	t.Run("WithMidnightInputReturnsMidnight24HoursLater", func(t *testing.T) {
		t.Parallel()

		schedule := &DefaultReindexerSchedule{}
		result := schedule.Next(time.Date(2023, 8, 31, 0, 0, 0, 0, time.UTC))
		require.Equal(t, time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC), result)
	})

	t.Run("WithMidnightInputReturnsMidnight24HoursLater", func(t *testing.T) {
		t.Parallel()

		schedule := &DefaultReindexerSchedule{}
		result := schedule.Next(time.Date(2023, 8, 31, 0, 0, 0, 0, time.UTC))
		require.Equal(t, time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC), result)
	})

	t.Run("With1NanosecondBeforeMidnightItReturnsUpcomingMidnight", func(t *testing.T) {
		t.Parallel()

		schedule := &DefaultReindexerSchedule{}
		result := schedule.Next(time.Date(2023, 8, 31, 23, 59, 59, 999999999, time.UTC))
		require.Equal(t, time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC), result)
	})
}
