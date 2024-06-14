package maintenance

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/baseservice"
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
	timeout  time.Duration // defaults to 0, which signals default timeout
}

func (w *callbackWorkUnitFactory) MakeUnit(jobRow *rivertype.JobRow) workunit.WorkUnit {
	return &callbackWorkUnit{callback: w.Callback, jobRow: jobRow, timeout: w.timeout}
}

// callbackWorkUnit implements workUnit for a job and Worker.
type callbackWorkUnit struct {
	callback func(ctx context.Context, jobRow *rivertype.JobRow) error
	jobRow   *rivertype.JobRow
	timeout  time.Duration // defaults to 0, which signals default timeout
}

func (w *callbackWorkUnit) NextRetry() time.Time           { return time.Now().Add(30 * time.Second) }
func (w *callbackWorkUnit) Timeout() time.Duration         { return w.timeout }
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

	ctx := context.Background()

	const (
		rescuerJobKind            = "rescuer"
		rescuerJobKindLongTimeout = "rescuer_long_timeout"
	)

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
					emptyCallback := func(ctx context.Context, jobRow *rivertype.JobRow) error { return nil }

					switch kind {
					case rescuerJobKind:
						return &callbackWorkUnitFactory{Callback: emptyCallback}
					case rescuerJobKindLongTimeout:
						return &callbackWorkUnitFactory{Callback: emptyCallback, timeout: JobRescuerRescueAfterDefault + 5*time.Minute}
					}
					panic("unhandled kind: " + kind)
				},
			},
			bundle.exec)
		rescuer.TestSignals.Init()

		return rescuer, bundle
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		rescuer := NewRescuer(
			riverinternaltest.BaseServiceArchetype(t),
			&JobRescuerConfig{
				ClientRetryPolicy:   &SimpleClientRetryPolicy{},
				WorkUnitFactoryFunc: func(kind string) workunit.WorkUnitFactory { return nil },
			},
			nil,
		)

		require.Equal(t, JobRescuerRescueAfterDefault, rescuer.Config.RescueAfter)
		require.Equal(t, JobRescuerIntervalDefault, rescuer.Config.Interval)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		rescuer, _ := setup(t)
		rescuer.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		rescuer.TestSignals = JobRescuerTestSignals{}    // deinit so channels don't fill

		wrapped := baseservice.Init(riverinternaltest.BaseServiceArchetype(t), &maintenanceServiceWrapper{
			service: rescuer,
		})
		startstoptest.Stress(ctx, t, wrapped)
	})

	t.Run("RescuesStuckJobs", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		rescuer, bundle := setup(t)

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

		// these aren't touched because they're in ineligible states
		notRunningJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), FinalizedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), State: ptrutil.Ptr(rivertype.JobStateCompleted), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})
		notRunningJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), FinalizedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), State: ptrutil.Ptr(rivertype.JobStateDiscarded), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})
		notRunningJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), FinalizedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), State: ptrutil.Ptr(rivertype.JobStateCancelled), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})

		// Jobs with worker-specific long timeouts. The first isn't rescued
		// because the difference between its `attempted_at` and now is still
		// within the timeout threshold. The second _is_ rescued because it
		// started earlier and even with the longer timeout, has still timed out.
		longTimeOutJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKindLongTimeout), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Minute)), MaxAttempts: ptrutil.Ptr(5)})
		longTimeOutJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKindLongTimeout), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-6 * time.Minute)), MaxAttempts: ptrutil.Ptr(5)})

		runMaintenanceService(ctx, t, rescuer)

		rescuer.TestSignals.FetchedBatch.WaitOrTimeout()
		rescuer.TestSignals.UpdatedBatch.WaitOrTimeout()

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

		discardJob1After, err := bundle.exec.JobGetByID(ctx, stuckToDiscardJob1.ID)
		require.NoError(err)
		require.Equal(rivertype.JobStateDiscarded, discardJob1After.State)
		require.WithinDuration(time.Now(), *discardJob1After.FinalizedAt, 5*time.Second)
		require.Len(discardJob1After.Errors, 1)

		discardJob2After, err := bundle.exec.JobGetByID(ctx, stuckToDiscardJob2.ID)
		require.NoError(err)
		require.Equal(rivertype.JobStateRunning, discardJob2After.State)
		require.Nil(discardJob2After.FinalizedAt)

		cancelJob1After, err := bundle.exec.JobGetByID(ctx, stuckToCancelJob1.ID)
		require.NoError(err)
		require.Equal(rivertype.JobStateCancelled, cancelJob1After.State)
		require.WithinDuration(time.Now(), *cancelJob1After.FinalizedAt, 5*time.Second)
		require.Len(cancelJob1After.Errors, 1)

		cancelJob2After, err := bundle.exec.JobGetByID(ctx, stuckToCancelJob2.ID)
		require.NoError(err)
		require.Equal(rivertype.JobStateRunning, cancelJob2After.State)
		require.Nil(cancelJob2After.FinalizedAt)

		notRunningJob1After, err := bundle.exec.JobGetByID(ctx, notRunningJob1.ID)
		require.NoError(err)
		require.Equal(notRunningJob1.State, notRunningJob1After.State)
		notRunningJob2After, err := bundle.exec.JobGetByID(ctx, notRunningJob2.ID)
		require.NoError(err)
		require.Equal(notRunningJob2.State, notRunningJob2After.State)
		notRunningJob3After, err := bundle.exec.JobGetByID(ctx, notRunningJob3.ID)
		require.NoError(err)
		require.Equal(notRunningJob3.State, notRunningJob3After.State)

		notTimedOutJob1After, err := bundle.exec.JobGetByID(ctx, longTimeOutJob1.ID)
		require.NoError(err)
		require.Equal(rivertype.JobStateRunning, notTimedOutJob1After.State)
		notTimedOutJob2After, err := bundle.exec.JobGetByID(ctx, longTimeOutJob2.ID)
		require.NoError(err)
		require.Equal(rivertype.JobStateRetryable, notTimedOutJob2After.State)
	})

	t.Run("RescuesInBatches", func(t *testing.T) {
		t.Parallel()

		rescuer, bundle := setup(t)
		rescuer.batchSize = 10 // reduced size for test speed

		// Add one to our chosen batch size to get one extra job and therefore
		// one extra batch, ensuring that we've tested working multiple.
		numJobs := rescuer.batchSize + 1

		jobs := make([]*rivertype.JobRow, numJobs)

		for i := 0; i < numJobs; i++ {
			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})
			jobs[i] = job
		}

		runMaintenanceService(ctx, t, rescuer)

		// See comment above. Exactly two batches are expected.
		rescuer.TestSignals.FetchedBatch.WaitOrTimeout()
		rescuer.TestSignals.UpdatedBatch.WaitOrTimeout()
		rescuer.TestSignals.FetchedBatch.WaitOrTimeout()
		rescuer.TestSignals.UpdatedBatch.WaitOrTimeout() // need to wait until after this for the conn to be free

		for _, job := range jobs {
			jobUpdated, err := bundle.exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
		}
	})

	t.Run("CustomizableInterval", func(t *testing.T) {
		t.Parallel()

		rescuer, _ := setup(t)
		rescuer.Config.Interval = 1 * time.Microsecond

		runMaintenanceService(ctx, t, rescuer)

		// This should trigger ~immediately every time:
		for i := 0; i < 5; i++ {
			t.Logf("Iteration %d", i)
			rescuer.TestSignals.FetchedBatch.WaitOrTimeout()
		}
	})

	t.Run("StopsImmediately", func(t *testing.T) {
		t.Parallel()

		rescuer, _ := setup(t)
		rescuer.Config.Interval = time.Minute // should only trigger once for the initial run

		MaintenanceServiceStopsImmediately(ctx, t, rescuer)
	})

	t.Run("CanRunMultipleTimes", func(t *testing.T) {
		t.Parallel()

		rescuer, bundle := setup(t)
		rescuer.Config.Interval = time.Minute // should only trigger once for the initial run

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), Attempt: ptrutil.Ptr(5), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Hour)), MaxAttempts: ptrutil.Ptr(5)})

		ctx1, cancel1 := context.WithCancel(ctx)
		stopCh1 := make(chan struct{})
		go func() {
			defer close(stopCh1)
			rescuer.Run(ctx1)
		}()
		t.Cleanup(func() { riverinternaltest.WaitOrTimeout(t, stopCh1) })
		t.Cleanup(cancel1)

		rescuer.TestSignals.FetchedBatch.WaitOrTimeout()
		rescuer.TestSignals.UpdatedBatch.WaitOrTimeout()

		cancel1()
		riverinternaltest.WaitOrTimeout(t, stopCh1)

		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(rescuerJobKind), State: ptrutil.Ptr(rivertype.JobStateRunning), Attempt: ptrutil.Ptr(5), AttemptedAt: ptrutil.Ptr(bundle.rescueHorizon.Add(-1 * time.Minute)), MaxAttempts: ptrutil.Ptr(5)})

		ctx2, cancel2 := context.WithCancel(ctx)
		stopCh2 := make(chan struct{})
		go func() {
			defer close(stopCh2)
			rescuer.Run(ctx2)
		}()
		t.Cleanup(func() { riverinternaltest.WaitOrTimeout(t, stopCh2) })
		t.Cleanup(cancel2)

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
