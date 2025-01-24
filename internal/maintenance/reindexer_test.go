package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
)

func TestReindexer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec riverdriver.Executor
		now  time.Time
	}

	setup := func(t *testing.T) (*Reindexer, *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)
		bundle := &testBundle{
			exec: riverpgxv5.New(dbPool).GetExecutor(),
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
		}, bundle.exec)
		svc.StaggerStartupDisable(true)
		svc.TestSignals.Init()
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

	t.Run("ReindexesEachIndex", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

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
