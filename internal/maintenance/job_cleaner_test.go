package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/ptrutil"
)

func TestJobCleaner(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		queries = dbsqlc.New()
	)

	type testBundle struct {
		cancelledDeleteHorizon time.Time
		completedDeleteHorizon time.Time
		discardedDeleteHorizon time.Time
		tx                     pgx.Tx
	}

	type insertJobParams struct {
		FinalizedAt *time.Time
		State       dbsqlc.JobState
	}

	insertJob := func(ctx context.Context, dbtx dbsqlc.DBTX, params insertJobParams) *dbsqlc.RiverJob {
		job, err := queries.JobInsert(ctx, dbtx, dbsqlc.JobInsertParams{
			FinalizedAt: params.FinalizedAt,
			Kind:        "test_kind",
			MaxAttempts: int16(rivercommon.DefaultMaxAttempts),
			Priority:    int16(rivercommon.DefaultPriority),
			Queue:       rivercommon.DefaultQueue,
			State:       params.State,
		})
		require.NoError(t, err)
		return job
	}

	setup := func(t *testing.T) (*JobCleaner, *testBundle) {
		t.Helper()

		bundle := &testBundle{
			cancelledDeleteHorizon: time.Now().Add(-DefaultCancelledJobRetentionTime),
			completedDeleteHorizon: time.Now().Add(-DefaultCompletedJobRetentionTime),
			discardedDeleteHorizon: time.Now().Add(-DefaultDiscardedJobRetentionTime),
			tx:                     riverinternaltest.TestTx(ctx, t),
		}

		cleaner := NewJobCleaner(
			riverinternaltest.BaseServiceArchetype(t),
			&JobCleanerConfig{
				CancelledJobRetentionTime: DefaultCancelledJobRetentionTime,
				CompletedJobRetentionTime: DefaultCompletedJobRetentionTime,
				DiscardedJobRetentionTime: DefaultDiscardedJobRetentionTime,
				Interval:                  DefaultJobCleanerInterval,
			},
			bundle.tx)
		cleaner.TestSignals.Init()
		t.Cleanup(cleaner.Stop)

		return cleaner, bundle
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		cleaner := NewJobCleaner(riverinternaltest.BaseServiceArchetype(t), &JobCleanerConfig{}, nil)

		require.Equal(t, cleaner.Config.CancelledJobRetentionTime, DefaultCancelledJobRetentionTime)
		require.Equal(t, cleaner.Config.CompletedJobRetentionTime, DefaultCompletedJobRetentionTime)
		require.Equal(t, cleaner.Config.DiscardedJobRetentionTime, DefaultDiscardedJobRetentionTime)
		require.Equal(t, cleaner.Config.Interval, DefaultJobCleanerInterval)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		cleaner.TestSignals = JobCleanerTestSignals{}    // deinit so channels don't fill

		runStartStopStress(ctx, t, cleaner)
	})

	t.Run("DeletesCompletedJobs", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)

		// none of these get removed
		job1 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateAvailable})
		job2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning})
		job3 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateScheduled})

		cancelledJob1 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCancelled, FinalizedAt: ptrutil.Ptr(bundle.cancelledDeleteHorizon.Add(-1 * time.Hour))})
		cancelledJob2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCancelled, FinalizedAt: ptrutil.Ptr(bundle.cancelledDeleteHorizon.Add(-1 * time.Minute))})
		cancelledJob3 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCancelled, FinalizedAt: ptrutil.Ptr(bundle.cancelledDeleteHorizon.Add(1 * time.Minute))}) // won't be deleted

		completedJob1 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCompleted, FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(-1 * time.Hour))})
		completedJob2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCompleted, FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(-1 * time.Minute))})
		completedJob3 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCompleted, FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(1 * time.Minute))}) // won't be deleted

		discardedJob1 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateDiscarded, FinalizedAt: ptrutil.Ptr(bundle.discardedDeleteHorizon.Add(-1 * time.Hour))})
		discardedJob2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateDiscarded, FinalizedAt: ptrutil.Ptr(bundle.discardedDeleteHorizon.Add(-1 * time.Minute))})
		discardedJob3 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateDiscarded, FinalizedAt: ptrutil.Ptr(bundle.discardedDeleteHorizon.Add(1 * time.Minute))}) // won't be deleted

		require.NoError(t, cleaner.Start(ctx))

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = queries.JobGetByID(ctx, bundle.tx, job1.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there
		_, err = queries.JobGetByID(ctx, bundle.tx, job2.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there
		_, err = queries.JobGetByID(ctx, bundle.tx, job3.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there

		_, err = queries.JobGetByID(ctx, bundle.tx, cancelledJob1.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = queries.JobGetByID(ctx, bundle.tx, cancelledJob2.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = queries.JobGetByID(ctx, bundle.tx, cancelledJob3.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there

		_, err = queries.JobGetByID(ctx, bundle.tx, completedJob1.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = queries.JobGetByID(ctx, bundle.tx, completedJob2.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = queries.JobGetByID(ctx, bundle.tx, completedJob3.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there

		_, err = queries.JobGetByID(ctx, bundle.tx, discardedJob1.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = queries.JobGetByID(ctx, bundle.tx, discardedJob2.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = queries.JobGetByID(ctx, bundle.tx, discardedJob3.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there
	})

	t.Run("DeletesInBatches", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.batchSize = 10 // reduced size for test speed

		// Add one to our chosen batch size to get one extra job and therefore
		// one extra batch, ensuring that we've tested working multiple.
		numJobs := cleaner.batchSize + 1

		jobs := make([]*dbsqlc.RiverJob, numJobs)

		for i := 0; i < int(numJobs); i++ {
			job := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCompleted, FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(-1 * time.Hour))})
			jobs[i] = job
		}

		require.NoError(t, cleaner.Start(ctx))

		// See comment above. Exactly two batches are expected.
		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()
		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		for _, job := range jobs {
			_, err := queries.JobGetByID(ctx, bundle.tx, job.ID)
			require.ErrorIs(t, err, pgx.ErrNoRows)
		}
	})

	t.Run("CustomizableInterval", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Config.Interval = 1 * time.Microsecond

		require.NoError(t, cleaner.Start(ctx))

		// This should trigger ~immediately every time:
		for i := 0; i < 5; i++ {
			t.Logf("Iteration %d", i)
			cleaner.TestSignals.DeletedBatch.WaitOrTimeout()
		}
	})

	t.Run("StopsImmediately", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Config.Interval = time.Minute // should only trigger once for the initial run

		require.NoError(t, cleaner.Start(ctx))
		cleaner.Stop()
	})

	t.Run("RespectsContextCancellation", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Config.Interval = time.Minute // should only trigger once for the initial run

		ctx, cancelFunc := context.WithCancel(ctx)

		require.NoError(t, cleaner.Start(ctx))

		// To avoid a potential race, make sure to get a reference to the
		// service's stopped channel _before_ cancellation as it's technically
		// possible for the cancel to "win" and remove the stopped channel
		// before we can start waiting on it.
		stopped := cleaner.Stopped()
		cancelFunc()
		riverinternaltest.WaitOrTimeout(t, stopped)
	})

	t.Run("CanRunMultipleTimes", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.Config.Interval = time.Minute // should only trigger once for the initial run

		job1 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCompleted, FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(-1 * time.Hour))})

		require.NoError(t, cleaner.Start(ctx))

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		cleaner.Stop()

		job2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCompleted, FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(-1 * time.Minute))})

		require.NoError(t, cleaner.Start(ctx))

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = queries.JobGetByID(ctx, bundle.tx, job1.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = queries.JobGetByID(ctx, bundle.tx, job2.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
	})
}
