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
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func TestJobCleaner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		cancelledDeleteHorizon time.Time
		completedDeleteHorizon time.Time
		exec                   riverdriver.Executor
		discardedDeleteHorizon time.Time
	}

	setup := func(t *testing.T) (*JobCleaner, *testBundle) {
		t.Helper()

		tx := riverinternaltest.TestTx(ctx, t)
		bundle := &testBundle{
			cancelledDeleteHorizon: time.Now().Add(-CancelledJobRetentionPeriodDefault),
			completedDeleteHorizon: time.Now().Add(-CompletedJobRetentionPeriodDefault),
			exec:                   riverpgxv5.New(nil).UnwrapExecutor(tx),
			discardedDeleteHorizon: time.Now().Add(-DiscardedJobRetentionPeriodDefault),
		}

		cleaner := NewJobCleaner(
			riversharedtest.BaseServiceArchetype(t),
			&JobCleanerConfig{
				CancelledJobRetentionPeriod: CancelledJobRetentionPeriodDefault,
				CompletedJobRetentionPeriod: CompletedJobRetentionPeriodDefault,
				DiscardedJobRetentionPeriod: DiscardedJobRetentionPeriodDefault,
				Interval:                    JobCleanerIntervalDefault,
			},
			bundle.exec)
		cleaner.StaggerStartupDisable(true)
		cleaner.TestSignals.Init()
		t.Cleanup(cleaner.Stop)

		return cleaner, bundle
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		cleaner := NewJobCleaner(riversharedtest.BaseServiceArchetype(t), &JobCleanerConfig{}, nil)

		require.Equal(t, CancelledJobRetentionPeriodDefault, cleaner.Config.CancelledJobRetentionPeriod)
		require.Equal(t, CompletedJobRetentionPeriodDefault, cleaner.Config.CompletedJobRetentionPeriod)
		require.Equal(t, DiscardedJobRetentionPeriodDefault, cleaner.Config.DiscardedJobRetentionPeriod)
		require.Equal(t, JobCleanerIntervalDefault, cleaner.Config.Interval)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Logger = riversharedtest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		cleaner.TestSignals = JobCleanerTestSignals{}  // deinit so channels don't fill

		startstoptest.Stress(ctx, t, cleaner)
	})

	t.Run("DeletesCompletedJobs", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)

		// none of these get removed
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled)})

		cancelledJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCancelled), FinalizedAt: ptrutil.Ptr(bundle.cancelledDeleteHorizon.Add(-1 * time.Hour))})
		cancelledJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCancelled), FinalizedAt: ptrutil.Ptr(bundle.cancelledDeleteHorizon.Add(-1 * time.Minute))})
		cancelledJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCancelled), FinalizedAt: ptrutil.Ptr(bundle.cancelledDeleteHorizon.Add(1 * time.Minute))}) // won't be deleted

		completedJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(-1 * time.Hour))})
		completedJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(-1 * time.Minute))})
		completedJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(1 * time.Minute))}) // won't be deleted

		discardedJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateDiscarded), FinalizedAt: ptrutil.Ptr(bundle.discardedDeleteHorizon.Add(-1 * time.Hour))})
		discardedJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateDiscarded), FinalizedAt: ptrutil.Ptr(bundle.discardedDeleteHorizon.Add(-1 * time.Minute))})
		discardedJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateDiscarded), FinalizedAt: ptrutil.Ptr(bundle.discardedDeleteHorizon.Add(1 * time.Minute))}) // won't be deleted

		require.NoError(t, cleaner.Start(ctx))

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = bundle.exec.JobGetByID(ctx, job1.ID)
		require.NotErrorIs(t, err, rivertype.ErrNotFound) // still there
		_, err = bundle.exec.JobGetByID(ctx, job2.ID)
		require.NotErrorIs(t, err, rivertype.ErrNotFound) // still there
		_, err = bundle.exec.JobGetByID(ctx, job3.ID)
		require.NotErrorIs(t, err, rivertype.ErrNotFound) // still there

		_, err = bundle.exec.JobGetByID(ctx, cancelledJob1.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.JobGetByID(ctx, cancelledJob2.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.JobGetByID(ctx, cancelledJob3.ID)
		require.NotErrorIs(t, err, rivertype.ErrNotFound) // still there

		_, err = bundle.exec.JobGetByID(ctx, completedJob1.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.JobGetByID(ctx, completedJob2.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.JobGetByID(ctx, completedJob3.ID)
		require.NotErrorIs(t, err, rivertype.ErrNotFound) // still there

		_, err = bundle.exec.JobGetByID(ctx, discardedJob1.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.JobGetByID(ctx, discardedJob2.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.JobGetByID(ctx, discardedJob3.ID)
		require.NotErrorIs(t, err, rivertype.ErrNotFound) // still there
	})

	t.Run("DeletesInBatches", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.batchSize = 10 // reduced size for test speed

		// Add one to our chosen batch size to get one extra job and therefore
		// one extra batch, ensuring that we've tested working multiple.
		numJobs := cleaner.batchSize + 1

		jobs := make([]*rivertype.JobRow, numJobs)

		for i := 0; i < numJobs; i++ {
			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(-1 * time.Hour))})
			jobs[i] = job
		}

		require.NoError(t, cleaner.Start(ctx))

		// See comment above. Exactly two batches are expected.
		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()
		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		for _, job := range jobs {
			_, err := bundle.exec.JobGetByID(ctx, job.ID)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
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
		riversharedtest.WaitOrTimeout(t, stopped)
	})

	t.Run("CanRunMultipleTimes", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.Config.Interval = time.Minute // should only trigger once for the initial run

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(-1 * time.Hour))})

		require.NoError(t, cleaner.Start(ctx))

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		cleaner.Stop()

		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(bundle.completedDeleteHorizon.Add(-1 * time.Minute))})

		require.NoError(t, cleaner.Start(ctx))

		cleaner.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = bundle.exec.JobGetByID(ctx, job1.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.JobGetByID(ctx, job2.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})
}
