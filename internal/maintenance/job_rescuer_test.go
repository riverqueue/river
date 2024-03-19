package maintenance

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/startstoptest"
	"github.com/riverqueue/river/internal/riverinternaltest/testfactory"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
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

func TestJobRescuer(t *testing.T) {
	t.Parallel()

	const rescuerJobKind = "rescuer"

	ctx := context.Background()

	type testBundle struct {
		exec          riverdriver.Executor
		rescueHorizon time.Time
	}

	setup := func(t *testing.T) (*JobRescuer, *testBundle) {
		t.Helper()

		tx := riverinternaltest.TestTx(ctx, t)
		bundle := &testBundle{
			exec:          riverpgxv5.New(nil).UnwrapExecutor(tx),
			rescueHorizon: time.Now().Add(-JobRescuerRescueAfterDefault),
		}

		rescuer := NewRescuer(
			riverinternaltest.BaseServiceArchetype(t),
			&JobRescuerConfig{
				ClientRetryPolicy: &SimpleClientRetryPolicy{},
				Interval:          JobRescuerIntervalDefault,
				RescueAfter:       JobRescuerRescueAfterDefault,
				WorkUnitFactoryFunc: func(kind string) workunit.WorkUnitFactory {
					if kind == rescuerJobKind {
						return &callbackWorkUnitFactory{Callback: func(ctx context.Context, jobRow *rivertype.JobRow) error { return nil }}
					}
					panic("unhandled kind: " + kind)
				},
			},
			bundle.exec)
		rescuer.StaggerStartupDisable(true)
		rescuer.TestSignals.Init()
		t.Cleanup(rescuer.Stop)

		return rescuer, bundle
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		cleaner := NewRescuer(
			riverinternaltest.BaseServiceArchetype(t),
			&JobRescuerConfig{
				ClientRetryPolicy:   &SimpleClientRetryPolicy{},
				WorkUnitFactoryFunc: func(kind string) workunit.WorkUnitFactory { return nil },
			},
			nil,
		)

		require.Equal(t, JobRescuerRescueAfterDefault, cleaner.Config.RescueAfter)
		require.Equal(t, JobRescuerIntervalDefault, cleaner.Config.Interval)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		rescuer, _ := setup(t)
		rescuer.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		rescuer.TestSignals = JobRescuerTestSignals{}    // deinit so channels don't fill

		startstoptest.Stress(ctx, t, rescuer)
	})

	t.Run("RescuesStuckJobs", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		cleaner, bundle := setup(t)

		stuckToRetryJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})
		stuckToRetryJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Minute)), MaxAttempts: ptrutil.Ptr(5)})
		stuckToRetryJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(1 * time.Minute)), MaxAttempts: ptrutil.Ptr(5)}) // won't be rescued

		// Already at max attempts:
		stuckToDiscardJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), Attempt: ptrutil.Ptr(5), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})
		stuckToDiscardJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), Attempt: ptrutil.Ptr(5), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(1 * time.Minute)), MaxAttempts: ptrutil.Ptr(5)}) // won't be rescued

		// Marked as cancelled by query:
		cancelTime := time.Now().UTC().Format(time.RFC3339Nano)
		stuckToCancelJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), Metadata: []byte(fmt.Sprintf(`{"cancel_attempted_at": %q}`, cancelTime)), MaxAttempts: ptrutil.Ptr(5)})
		stuckToCancelJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(1 * time.Minute)), Metadata: []byte(fmt.Sprintf(`{"cancel_attempted_at": %q}`, cancelTime)), MaxAttempts: ptrutil.Ptr(5)}) // won't be rescued

		// these aren't touched:
		notRunningJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateCompleted), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})
		notRunningJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateDiscarded), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})
		notRunningJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateCancelled), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})

		require.NoError(cleaner.Start(ctx))

		cleaner.TestSignals.FetchedBatch.WaitOrTimeout()
		cleaner.TestSignals.UpdatedBatch.WaitOrTimeout()

		confirmRetried := func(jobBefore *rivertype.JobRow) {
			jobAfter, err := bundle.exec.JobGetByID(ctx, jobBefore.ID)
			require.NoError(err)
			require.Equal(rivertype.JobStateRetryable, jobAfter.State)
		}

		var err error
		confirmRetried(stuckToRetryJob1)
		confirmRetried(stuckToRetryJob2)
		job3After, err := bundle.exec.JobGetByID(ctx, stuckToRetryJob3.ID)
		require.NoError(err)
		require.Equal(stuckToRetryJob3.State, job3After.State) // not rescued

		discard1After, err := bundle.exec.JobGetByID(ctx, stuckToDiscardJob1.ID)
		require.NoError(err)
		require.Equal(rivertype.JobStateDiscarded, discard1After.State)
		require.WithinDuration(time.Now(), *discard1After.FinalizedAt, 5*time.Second)
		require.Len(discard1After.Errors, 1)

		discard2After, err := bundle.exec.JobGetByID(ctx, stuckToDiscardJob2.ID)
		require.NoError(err)
		require.Equal(rivertype.JobStateRunning, discard2After.State)
		require.Nil(discard2After.FinalizedAt)

		cancel1After, err := bundle.exec.JobGetByID(ctx, stuckToCancelJob1.ID)
		require.NoError(err)
		require.Equal(rivertype.JobStateCancelled, cancel1After.State)
		require.WithinDuration(time.Now(), *cancel1After.FinalizedAt, 5*time.Second)
		require.Len(cancel1After.Errors, 1)

		cancel2After, err := bundle.exec.JobGetByID(ctx, stuckToCancelJob2.ID)
		require.NoError(err)
		require.Equal(rivertype.JobStateRunning, cancel2After.State)
		require.Nil(cancel2After.FinalizedAt)

		notRunning1After, err := bundle.exec.JobGetByID(ctx, notRunningJob1.ID)
		require.NoError(err)
		require.Equal(notRunning1After.State, notRunningJob1.State)
		notRunning2After, err := bundle.exec.JobGetByID(ctx, notRunningJob2.ID)
		require.NoError(err)
		require.Equal(notRunning2After.State, notRunningJob2.State)
		notRunning3After, err := bundle.exec.JobGetByID(ctx, notRunningJob3.ID)
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

		jobs := make([]*rivertype.JobRow, numJobs)

		for i := 0; i < numJobs; i++ {
			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})
			jobs[i] = job
		}

		require.NoError(t, cleaner.Start(ctx))

		// See comment above. Exactly two batches are expected.
		cleaner.TestSignals.FetchedBatch.WaitOrTimeout()
		cleaner.TestSignals.UpdatedBatch.WaitOrTimeout()
		cleaner.TestSignals.FetchedBatch.WaitOrTimeout()
		cleaner.TestSignals.UpdatedBatch.WaitOrTimeout() // need to wait until after this for the conn to be free

		for _, job := range jobs {
			jobUpdated, err := bundle.exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
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

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), Attempt: ptrutil.Ptr(5), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})

		require.NoError(t, rescuer.Start(ctx))

		rescuer.TestSignals.FetchedBatch.WaitOrTimeout()
		rescuer.TestSignals.UpdatedBatch.WaitOrTimeout()

		rescuer.Stop()

		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), Attempt: ptrutil.Ptr(5), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Minute)), MaxAttempts: ptrutil.Ptr(5)})

		require.NoError(t, rescuer.Start(ctx))

		rescuer.TestSignals.FetchedBatch.WaitOrTimeout()
		rescuer.TestSignals.UpdatedBatch.WaitOrTimeout()

		job1After, err := bundle.exec.JobGetByID(ctx, job1.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateDiscarded, job1After.State)
		job2After, err := bundle.exec.JobGetByID(ctx, job2.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateDiscarded, job2After.State)
	})
}
