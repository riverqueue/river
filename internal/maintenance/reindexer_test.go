package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/startstoptest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
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
			now:  time.Now(),
		}

		archetype := riverinternaltest.BaseServiceArchetype(t)
		archetype.TimeNowUTC = func() time.Time { return bundle.now }

		fromNow := func(d time.Duration) func(time.Time) time.Time {
			return func(t time.Time) time.Time {
				return t.Add(d)
			}
		}

		svc := NewReindexer(archetype, &ReindexerConfig{
			ScheduleFunc: fromNow(500 * time.Millisecond),
		}, bundle.exec)
		svc.TestSignals.Init()

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
		svc.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		svc.TestSignals = ReindexerTestSignals{}     // deinit so channels don't fill

		wrapped := baseservice.Init(riverinternaltest.BaseServiceArchetype(t), &maintenanceServiceWrapper{
			service: svc,
		})
		startstoptest.Stress(ctx, t, wrapped)
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

		runMaintenanceService(ctx, t, svc)
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
		MaintenanceServiceStopsImmediately(ctx, t, svc)
	})

	t.Run("DefaultConfigs", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)
		svc = NewReindexer(&svc.Archetype, &ReindexerConfig{}, bundle.exec)

		require.Equal(t, defaultIndexNames, svc.Config.IndexNames)
		require.Equal(t, ReindexerTimeoutDefault, svc.Config.Timeout)
		require.Equal(t, svc.Config.ScheduleFunc(bundle.now), (&defaultReindexerSchedule{}).Next(bundle.now))
	})
}

func TestDefaultReindexerSchedule(t *testing.T) {
	t.Parallel()

	t.Run("WithMidnightInputReturnsMidnight24HoursLater", func(t *testing.T) {
		t.Parallel()

		schedule := &defaultReindexerSchedule{}
		result := schedule.Next(time.Date(2023, 8, 31, 0, 0, 0, 0, time.UTC))
		require.Equal(t, time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC), result)
	})

	t.Run("WithMidnightInputReturnsMidnight24HoursLater", func(t *testing.T) {
		t.Parallel()

		schedule := &defaultReindexerSchedule{}
		result := schedule.Next(time.Date(2023, 8, 31, 0, 0, 0, 0, time.UTC))
		require.Equal(t, time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC), result)
	})

	t.Run("With1NanosecondBeforeMidnightItReturnsUpcomingMidnight", func(t *testing.T) {
		t.Parallel()

		schedule := &defaultReindexerSchedule{}
		result := schedule.Next(time.Date(2023, 8, 31, 23, 59, 59, 999999999, time.UTC))
		require.Equal(t, time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC), result)
	})
}
