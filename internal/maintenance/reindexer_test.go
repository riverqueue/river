package maintenance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

type reindexerExecutorMock struct {
	riverdriver.Executor

	indexesExistCalls  atomic.Int32
	indexesExistFunc   func(ctx context.Context, params *riverdriver.IndexesExistParams) (map[string]bool, error)
	indexesExistSignal chan struct{}
	indexReindexFunc   func(ctx context.Context, params *riverdriver.IndexReindexParams) error
}

func newReindexerExecutorMock(exec riverdriver.Executor) *reindexerExecutorMock {
	return &reindexerExecutorMock{
		Executor:           exec,
		indexesExistFunc:   exec.IndexesExist,
		indexesExistSignal: make(chan struct{}, 10),
		indexReindexFunc:   exec.IndexReindex,
	}
}

func (m *reindexerExecutorMock) IndexesExist(ctx context.Context, params *riverdriver.IndexesExistParams) (map[string]bool, error) {
	m.indexesExistCalls.Add(1)

	select {
	case m.indexesExistSignal <- struct{}{}:
	default:
	}

	return m.indexesExistFunc(ctx, params)
}

func (m *reindexerExecutorMock) IndexReindex(ctx context.Context, params *riverdriver.IndexReindexParams) error {
	return m.indexReindexFunc(ctx, params)
}

func TestReindexer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec       riverdriver.Executor
		indexNames []string
		now        time.Time
		schema     string
	}

	setupWithOpts := func(t *testing.T, testSchemaOpts *riverdbtest.TestSchemaOpts) (*Reindexer, *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, testSchemaOpts)
		)

		bundle := &testBundle{
			exec:       riverpgxv5.New(dbPool).GetExecutor(),
			indexNames: []string{"river_job_kind", "river_job_prioritized_fetching_index", "river_job_state_and_finalized_at_index"},
			schema:     schema,
		}

		archetype := riversharedtest.BaseServiceArchetype(t)
		bundle.now = archetype.Time.StubNow(time.Now())

		fromNow := func(d time.Duration) func(time.Time) time.Time {
			return func(t time.Time) time.Time {
				return t.Add(d)
			}
		}

		svc := NewReindexer(archetype, &ReindexerConfig{
			IndexNames:   bundle.indexNames,
			ScheduleFunc: fromNow(500 * time.Millisecond),
			Schema:       schema,
		}, bundle.exec)
		svc.StaggerStartupDisable(true)
		svc.TestSignals.Init(t)
		t.Cleanup(svc.Stop)

		return svc, bundle
	}

	setup := func(t *testing.T) (*Reindexer, *testBundle) {
		t.Helper()

		return setupWithOpts(t, nil)
	}

	runImmediatelyThenOnceAnHour := func() func(time.Time) time.Time {
		alreadyRan := false
		return func(t time.Time) time.Time {
			if alreadyRan {
				return t.Add(time.Hour)
			}
			// Force the first run immediately, then make the next legitimate
			// schedule far enough away that an immediate retry is clearly wrong.
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

		svc, bundle := setupWithOpts(t, &riverdbtest.TestSchemaOpts{DisableReuse: true})

		mockExec := newReindexerExecutorMock(bundle.exec)
		mockExec.indexReindexFunc = func(ctx context.Context, params *riverdriver.IndexReindexParams) error {
			return nil
		}
		svc.exec = mockExec

		requireReindexOne := func(indexName string) bool {
			didReindex, err := svc.reindexOne(ctx, indexName)
			require.NoError(t, err)
			return didReindex
		}

		indexName := svc.Config.IndexNames[0]

		for _, artifactSuffix := range []string{"_ccnew", "_ccnew1", "_ccnew31", "_ccold", "_ccold1"} {
			artifactName := indexName + artifactSuffix
			require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("CREATE INDEX %s ON %s.river_job (id)", artifactName, bundle.schema)))
			require.False(t, requireReindexOne(indexName))
			require.NoError(t, bundle.exec.IndexDropIfExists(ctx, &riverdriver.IndexDropIfExistsParams{Index: artifactName, Schema: bundle.schema}))
		}

		for _, artifactSuffix := range []string{"_ccnewa", "_ccnew_1", "_ccoldx", "_ccold_1", "x_ccnew1"} {
			artifactName := indexName + artifactSuffix
			require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("CREATE INDEX %s ON %s.river_job (id)", artifactName, bundle.schema)))
			t.Cleanup(func() {
				require.NoError(t, bundle.exec.IndexDropIfExists(ctx, &riverdriver.IndexDropIfExistsParams{Index: artifactName, Schema: bundle.schema}))
			})
		}
		require.True(t, requireReindexOne(indexName))
	})

	t.Run("ReindexableIndexNamesSkipsMissingIndexes", func(t *testing.T) {
		t.Parallel()

		svc, _ := setup(t)

		svc.Config.IndexNames = []string{
			"does_not_exist",
			"river_job_kind",
			"river_job_prioritized_fetching_index",
		}

		indexNames, err := svc.reindexableIndexNames(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"river_job_kind", "river_job_prioritized_fetching_index"}, indexNames)
	})

	t.Run("ReindexesMinimalSubsetofIndexes", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		var (
			// Mock IndexReindex so the test doesn't depend on the speed of real
			// REINDEX CONCURRENTLY operations on the shared CI database. Track
			// which indexes got reindexed so we can verify the expected set.
			mockExec = newReindexerExecutorMock(bundle.exec)

			reindexedNames []string
			reindexedMu    sync.Mutex
		)
		mockExec.indexReindexFunc = func(ctx context.Context, params *riverdriver.IndexReindexParams) error {
			reindexedMu.Lock()
			defer reindexedMu.Unlock()
			reindexedNames = append(reindexedNames, params.Index)
			return nil
		}
		svc.exec = mockExec

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

		reindexedMu.Lock()
		require.ElementsMatch(t, svc.Config.IndexNames, reindexedNames)
		reindexedMu.Unlock()
	})

	t.Run("ReindexesConfiguredIndexes", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		// Mock IndexReindex so the test doesn't depend on the speed of real
		// REINDEX CONCURRENTLY operations on the shared CI database.
		mockExec := newReindexerExecutorMock(bundle.exec)
		mockExec.indexReindexFunc = func(ctx context.Context, params *riverdriver.IndexReindexParams) error {
			return nil
		}
		svc.exec = mockExec

		svc.Config.ScheduleFunc = runImmediatelyThenOnceAnHour()

		require.NoError(t, svc.Start(ctx))
		svc.TestSignals.Reindexed.WaitOrTimeout()
	})

	t.Run("ReindexDeletesArtifactsWhenCancelledWithStop", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setupWithOpts(t, &riverdbtest.TestSchemaOpts{DisableReuse: true})
		svc.skipReindexArtifactCheck = true

		requireIndexExists := func(indexName string) bool {
			indexExists, err := bundle.exec.IndexExists(ctx, &riverdriver.IndexExistsParams{Index: indexName, Schema: bundle.schema})
			require.NoError(t, err)
			return indexExists
		}

		var (
			indexName            = svc.Config.IndexNames[0]
			indexNameNew         = indexName + "_ccnew"
			indexNameNewNumber   = indexName + "_ccnew1"
			indexNameNonArtifact = indexName + "_ccnewa"
			indexNameOld         = indexName + "_ccold"
			indexNameOldNumber   = indexName + "_ccold2"
		)

		require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("CREATE INDEX %s ON %s.river_job (id)", indexNameNew, bundle.schema)))
		require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("CREATE INDEX %s ON %s.river_job (id)", indexNameNewNumber, bundle.schema)))
		require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("CREATE INDEX %s ON %s.river_job (id)", indexNameNonArtifact, bundle.schema)))
		require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("CREATE INDEX %s ON %s.river_job (id)", indexNameOld, bundle.schema)))
		require.NoError(t, bundle.exec.Exec(ctx, fmt.Sprintf("CREATE INDEX %s ON %s.river_job (id)", indexNameOldNumber, bundle.schema)))

		require.True(t, requireIndexExists(indexNameNew))
		require.True(t, requireIndexExists(indexNameNewNumber))
		require.True(t, requireIndexExists(indexNameNonArtifact))
		require.True(t, requireIndexExists(indexNameOld))
		require.True(t, requireIndexExists(indexNameOldNumber))

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
		require.False(t, requireIndexExists(indexNameNewNumber))
		require.True(t, requireIndexExists(indexNameNonArtifact))
		require.False(t, requireIndexExists(indexNameOld))
		require.False(t, requireIndexExists(indexNameOldNumber))
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

	t.Run("CopiesConfiguredIndexNamesAndAppliesOtherDefaults", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)
		input := []string{"river_job_kind"}
		svc = NewReindexer(&svc.Archetype, &ReindexerConfig{IndexNames: input}, bundle.exec)

		require.Equal(t, input, svc.Config.IndexNames)
		input[0] = "mutated"
		require.Equal(t, []string{"river_job_kind"}, svc.Config.IndexNames)
		require.Equal(t, ReindexerTimeoutDefault, svc.Config.Timeout)
		require.Equal(t, svc.Config.ScheduleFunc(bundle.now), (&DefaultReindexerSchedule{}).Next(bundle.now))
	})

	t.Run("PanicsOnNilIndexNames", func(t *testing.T) {
		t.Parallel()

		svc, bundle := setup(t)

		require.PanicsWithValue(t, "ReindexerConfig.IndexNames must be set", func() {
			NewReindexer(&svc.Archetype, &ReindexerConfig{}, bundle.exec)
		})
	})
}

func TestReindexer_DiscoveryErrorSchedulesNextRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var (
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
	)

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

	execMock := newReindexerExecutorMock(driver.GetExecutor())
	execMock.indexesExistFunc = func(ctx context.Context, params *riverdriver.IndexesExistParams) (map[string]bool, error) {
		return nil, errors.New("indexes exist failed")
	}

	svc := NewReindexer(riversharedtest.BaseServiceArchetype(t), &ReindexerConfig{
		IndexNames:   []string{"river_job_kind"},
		ScheduleFunc: runImmediatelyThenOnceAnHour(),
		Schema:       schema,
	}, execMock)
	svc.Logger = riversharedtest.LoggerWarn(t)
	svc.StaggerStartupDisable(true)
	t.Cleanup(svc.Stop)

	require.NoError(t, svc.Start(ctx))
	riversharedtest.WaitOrTimeout(t, execMock.indexesExistSignal)

	select {
	case <-execMock.indexesExistSignal:
		require.FailNowf(t, "unexpected immediate retry", "IndexesExist was called %d times", execMock.indexesExistCalls.Load())
	case <-time.After(100 * time.Millisecond):
	}
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
