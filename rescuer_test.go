package river

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/ptrutil"
)

type rescuerTestArgs struct{}

func (rescuerTestArgs) Kind() string {
	return "RescuerTest"
}

type rescuerTestWorker struct {
	WorkerDefaults[rescuerTestArgs]
}

func (w *rescuerTestWorker) Work(context.Context, *Job[rescuerTestArgs]) error {
	return nil
}

func (w *rescuerTestWorker) NextRetry(*Job[rescuerTestArgs]) time.Time {
	return time.Now().Add(30 * time.Second)
}

func TestRescuer(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		queries = dbsqlc.New()
	)

	type testBundle struct {
		rescueHorizon time.Time
		tx            pgx.Tx
	}

	type insertJobParams struct {
		Attempt     int16
		AttemptedAt *time.Time
		MaxAttempts int16
		State       dbsqlc.JobState
	}

	insertJob := func(ctx context.Context, dbtx dbsqlc.DBTX, params insertJobParams) *dbsqlc.RiverJob {
		job, err := queries.JobInsert(ctx, dbtx, dbsqlc.JobInsertParams{
			Attempt:     params.Attempt,
			AttemptedAt: params.AttemptedAt,
			Args:        []byte("{}"),
			Kind:        (&rescuerTestArgs{}).Kind(),
			MaxAttempts: 5,
			Priority:    int16(rivercommon.DefaultPriority),
			Queue:       rivercommon.DefaultQueue,
			State:       params.State,
		})
		require.NoError(t, err)
		return job
	}

	setup := func(t *testing.T) (*rescuer, *testBundle) {
		t.Helper()

		bundle := &testBundle{
			rescueHorizon: time.Now().Add(-defaultRescueAfter),
			tx:            riverinternaltest.TestTx(ctx, t),
		}

		workers := NewWorkers()
		AddWorker(workers, &rescuerTestWorker{})

		cleaner := newRescuer(
			riverinternaltest.BaseServiceArchetype(t),
			&rescuerConfig{
				ClientRetryPolicy: &DefaultClientRetryPolicy{},
				Interval:          defaultRescuerInterval,
				RescueAfter:       defaultRescueAfter,
				Workers:           workers,
			},
			bundle.tx)
		cleaner.TestSignals.Init()
		t.Cleanup(cleaner.Stop)

		return cleaner, bundle
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		cleaner := newRescuer(
			riverinternaltest.BaseServiceArchetype(t),
			&rescuerConfig{ClientRetryPolicy: &DefaultClientRetryPolicy{}, Workers: NewWorkers()},
			nil,
		)

		require.Equal(t, cleaner.Config.RescueAfter, defaultRescueAfter)
		require.Equal(t, cleaner.Config.Interval, defaultRescuerInterval)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		cleaner, _ := setup(t)
		cleaner.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		cleaner.TestSignals = rescuerTestSignals{}       // deinit so channels don't fill

		runStartStopStress(ctx, t, cleaner)
	})

	t.Run("RescuesStuckJobs", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		cleaner, bundle := setup(t)

		stuckToRetryJob1 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour))})
		stuckToRetryJob2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Minute))})
		stuckToRetryJob3 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(1 * time.Minute))}) // won't be rescued

		// Already at max attempts:
		stuckToDiscardJob1 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, Attempt: 5, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour))})
		stuckToDiscardJob2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, Attempt: 5, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(1 * time.Minute))}) // won't be rescued

		// these aren't touched:
		notRunningJob1 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCompleted, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour))})
		notRunningJob2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateDiscarded, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour))})

		require.NoError(cleaner.Start(ctx))

		cleaner.TestSignals.FetchedBatch.WaitOrTimeout()
		cleaner.TestSignals.DiscardedJobs.WaitOrTimeout()
		cleaner.TestSignals.RetriedJobs.WaitOrTimeout()

		confirmRetried := func(jobBefore *dbsqlc.RiverJob) {
			jobAfter, err := queries.JobGetByID(ctx, bundle.tx, jobBefore.ID)
			require.NoError(err)
			require.Equal(dbsqlc.JobStateRetryable, jobAfter.State)
		}

		var err error
		confirmRetried(stuckToRetryJob1)
		confirmRetried(stuckToRetryJob2)
		job3After, err := queries.JobGetByID(ctx, bundle.tx, stuckToRetryJob3.ID)
		require.NoError(err)
		require.Equal(stuckToRetryJob3.State, job3After.State) // not rescued

		discard1After, err := queries.JobGetByID(ctx, bundle.tx, stuckToDiscardJob1.ID)
		require.NoError(err)
		require.Equal(dbsqlc.JobStateDiscarded, discard1After.State)
		require.WithinDuration(time.Now(), *discard1After.FinalizedAt, 5*time.Second)
		require.Len(discard1After.Errors, 1)

		discard2After, err := queries.JobGetByID(ctx, bundle.tx, stuckToDiscardJob2.ID)
		require.NoError(err)
		require.Equal(dbsqlc.JobStateRunning, discard2After.State)
		require.Nil(discard2After.FinalizedAt)

		notRunning1After, err := queries.JobGetByID(ctx, bundle.tx, notRunningJob1.ID)
		require.NoError(err)
		require.Equal(notRunning1After.State, notRunningJob1.State)
		notRunning2After, err := queries.JobGetByID(ctx, bundle.tx, notRunningJob2.ID)
		require.NoError(err)
		require.Equal(notRunning2After.State, notRunningJob2.State)
	})

	t.Run("RescuesInBatches", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.batchSize = 10 // reduced size for test speed

		// Add one to our chosen batch size to get one extra job and therefore
		// one extra batch, ensuring that we've tested working multiple.
		numJobs := cleaner.batchSize + 1

		jobs := make([]*dbsqlc.RiverJob, numJobs)

		for i := 0; i < int(numJobs); i++ {
			job := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour))})
			jobs[i] = job
		}

		require.NoError(t, cleaner.Start(ctx))

		// See comment above. Exactly two batches are expected.
		cleaner.TestSignals.FetchedBatch.WaitOrTimeout()
		cleaner.TestSignals.RetriedJobs.WaitOrTimeout()
		cleaner.TestSignals.FetchedBatch.WaitOrTimeout()
		cleaner.TestSignals.RetriedJobs.WaitOrTimeout() // need to wait until after this for the conn to be free

		for _, job := range jobs {
			jobUpdated, err := queries.JobGetByID(ctx, bundle.tx, job.ID)
			require.NoError(t, err)
			require.Equal(t, dbsqlc.JobStateRetryable, jobUpdated.State)
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
			cleaner.TestSignals.FetchedBatch.WaitOrTimeout()
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

		rescuer, bundle := setup(t)
		rescuer.Config.Interval = time.Minute // should only trigger once for the initial run

		job1 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, Attempt: 5, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour))})

		require.NoError(t, rescuer.Start(ctx))

		rescuer.TestSignals.FetchedBatch.WaitOrTimeout()
		rescuer.TestSignals.DiscardedJobs.WaitOrTimeout()

		rescuer.Stop()

		job2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, Attempt: 5, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Minute))})

		require.NoError(t, rescuer.Start(ctx))

		rescuer.TestSignals.FetchedBatch.WaitOrTimeout()
		rescuer.TestSignals.DiscardedJobs.WaitOrTimeout()

		job1After, err := queries.JobGetByID(ctx, bundle.tx, job1.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateDiscarded, job1After.State)
		job2After, err := queries.JobGetByID(ctx, bundle.tx, job2.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateDiscarded, job2After.State)
	})
}

// copied from maintenance package tests because there's no good place to expose it:.
func runStartStopStress(ctx context.Context, tb testing.TB, svc maintenance.Service) {
	tb.Helper()

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 50; j++ {
				require.NoError(tb, svc.Start(ctx))
				svc.Stop()
			}
			wg.Done()
		}()
	}

	wg.Wait()
}
