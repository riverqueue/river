package maintenance

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/rivertype"
)

// callbackWorkUnitFactory wraps a Worker to implement workUnitFactory.
type callbackWorkUnitFactory struct {
	Callback func(ctx context.Context, jobRow *rivertype.JobRow) error
}

func (w *callbackWorkUnitFactory) MakeUnit(jobRow *rivertype.JobRow) workunit.WorkUnit {
	return &callbackWorkUnit{callback: w.Callback, jobRow: jobRow}
}

// callbackWorkUnit implements workUnit for a job and Worker.
type callbackWorkUnit struct {
	callback func(ctx context.Context, jobRow *rivertype.JobRow) error
	jobRow   *rivertype.JobRow
}

func (w *callbackWorkUnit) NextRetry() time.Time           { return time.Now().Add(30 * time.Second) }
func (w *callbackWorkUnit) Timeout() time.Duration         { return 0 }
func (w *callbackWorkUnit) Work(ctx context.Context) error { return w.callback(ctx, w.jobRow) }
func (w *callbackWorkUnit) UnmarshalJob() error            { return nil }

type SimpleClientRetryPolicy struct{}

func (p *SimpleClientRetryPolicy) NextRetry(job *rivertype.JobRow) time.Time {
	errorCount := len(job.Errors) + 1
	retrySeconds := math.Pow(float64(errorCount), 4)
	return job.AttemptedAt.Add(timeutil.SecondsAsDuration(retrySeconds))
}

func TestRescuer(t *testing.T) {
	t.Parallel()

	const rescuerJobKind = "rescuer"

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
		Metadata    []byte
		State       dbsqlc.JobState
	}

	insertJob := func(ctx context.Context, dbtx dbsqlc.DBTX, params insertJobParams) *dbsqlc.RiverJob {
		job, err := queries.JobInsert(ctx, dbtx, dbsqlc.JobInsertParams{
			Attempt:     params.Attempt,
			AttemptedAt: params.AttemptedAt,
			Args:        []byte("{}"),
			Kind:        rescuerJobKind,
			MaxAttempts: 5,
			Metadata:    params.Metadata,
			Priority:    int16(rivercommon.PriorityDefault),
			Queue:       rivercommon.QueueDefault,
			State:       params.State,
		})
		require.NoError(t, err)
		return job
	}

	setup := func(t *testing.T) (*Rescuer, *testBundle) {
		t.Helper()

		bundle := &testBundle{
			rescueHorizon: time.Now().Add(-RescueAfterDefault),
			tx:            riverinternaltest.TestTx(ctx, t),
		}

		rescuer := NewRescuer(
			riverinternaltest.BaseServiceArchetype(t),
			&RescuerConfig{
				ClientRetryPolicy: &SimpleClientRetryPolicy{},
				Interval:          RescuerIntervalDefault,
				RescueAfter:       RescueAfterDefault,
				WorkUnitFactoryFunc: func(kind string) workunit.WorkUnitFactory {
					if kind == rescuerJobKind {
						return &callbackWorkUnitFactory{Callback: func(ctx context.Context, jobRow *rivertype.JobRow) error { return nil }}
					}
					panic("unhandled kind: " + kind)
				},
			},
			bundle.tx)
		rescuer.TestSignals.Init()
		t.Cleanup(rescuer.Stop)

		return rescuer, bundle
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		cleaner := NewRescuer(
			riverinternaltest.BaseServiceArchetype(t),
			&RescuerConfig{
				ClientRetryPolicy:   &SimpleClientRetryPolicy{},
				WorkUnitFactoryFunc: func(kind string) workunit.WorkUnitFactory { return nil },
			},
			nil,
		)

		require.Equal(t, RescueAfterDefault, cleaner.Config.RescueAfter)
		require.Equal(t, RescuerIntervalDefault, cleaner.Config.Interval)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		rescuer, _ := setup(t)
		rescuer.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		rescuer.TestSignals = RescuerTestSignals{}       // deinit so channels don't fill

		runStartStopStress(ctx, t, rescuer)
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

		// Marked as cancelled by query:
		cancelTime := time.Now().UTC().Format(time.RFC3339Nano)
		stuckToCancelJob1 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), Metadata: []byte(fmt.Sprintf(`{"cancel_attempted_at": %q}`, cancelTime))})
		stuckToCancelJob2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(1 * time.Minute)), Metadata: []byte(fmt.Sprintf(`{"cancel_attempted_at": %q}`, cancelTime))}) // won't be rescued

		// these aren't touched:
		notRunningJob1 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCompleted, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour))})
		notRunningJob2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateDiscarded, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour))})
		notRunningJob3 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateCancelled, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour))})

		require.NoError(cleaner.Start(ctx))

		cleaner.TestSignals.FetchedBatch.WaitOrTimeout()
		cleaner.TestSignals.UpdatedBatch.WaitOrTimeout()

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

		cancel1After, err := queries.JobGetByID(ctx, bundle.tx, stuckToCancelJob1.ID)
		require.NoError(err)
		require.Equal(dbsqlc.JobStateCancelled, cancel1After.State)
		require.WithinDuration(time.Now(), *cancel1After.FinalizedAt, 5*time.Second)
		require.Len(cancel1After.Errors, 1)

		cancel2After, err := queries.JobGetByID(ctx, bundle.tx, stuckToCancelJob2.ID)
		require.NoError(err)
		require.Equal(dbsqlc.JobStateRunning, cancel2After.State)
		require.Nil(cancel2After.FinalizedAt)

		notRunning1After, err := queries.JobGetByID(ctx, bundle.tx, notRunningJob1.ID)
		require.NoError(err)
		require.Equal(notRunning1After.State, notRunningJob1.State)
		notRunning2After, err := queries.JobGetByID(ctx, bundle.tx, notRunningJob2.ID)
		require.NoError(err)
		require.Equal(notRunning2After.State, notRunningJob2.State)
		notRunning3After, err := queries.JobGetByID(ctx, bundle.tx, notRunningJob3.ID)
		require.NoError(err)
		require.Equal(notRunning3After.State, notRunningJob3.State)
	})

	t.Run("RescuesInBatches", func(t *testing.T) {
		t.Parallel()

		cleaner, bundle := setup(t)
		cleaner.batchSize = 10 // reduced size for test speed

		// Add one to our chosen batch size to get one extra job and therefore
		// one extra batch, ensuring that we've tested working multiple.
		numJobs := cleaner.batchSize + 1

		jobs := make([]*dbsqlc.RiverJob, numJobs)

		for i := 0; i < numJobs; i++ {
			job := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour))})
			jobs[i] = job
		}

		require.NoError(t, cleaner.Start(ctx))

		// See comment above. Exactly two batches are expected.
		cleaner.TestSignals.FetchedBatch.WaitOrTimeout()
		cleaner.TestSignals.UpdatedBatch.WaitOrTimeout()
		cleaner.TestSignals.FetchedBatch.WaitOrTimeout()
		cleaner.TestSignals.UpdatedBatch.WaitOrTimeout() // need to wait until after this for the conn to be free

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
		rescuer.TestSignals.UpdatedBatch.WaitOrTimeout()

		rescuer.Stop()

		job2 := insertJob(ctx, bundle.tx, insertJobParams{State: dbsqlc.JobStateRunning, Attempt: 5, AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Minute))})

		require.NoError(t, rescuer.Start(ctx))

		rescuer.TestSignals.FetchedBatch.WaitOrTimeout()
		rescuer.TestSignals.UpdatedBatch.WaitOrTimeout()

		job1After, err := queries.JobGetByID(ctx, bundle.tx, job1.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateDiscarded, job1After.State)
		job2After, err := queries.JobGetByID(ctx, bundle.tx, job2.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateDiscarded, job2After.State)
	})
}
