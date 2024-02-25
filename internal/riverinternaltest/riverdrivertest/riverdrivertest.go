package riverdrivertest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest/testfactory" //nolint:depguard
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

type testBundle struct{}

func setupExecutor[TTx any](ctx context.Context, t *testing.T, driver riverdriver.Driver[TTx], beginTx func(ctx context.Context, t *testing.T) TTx) (riverdriver.Executor, *testBundle) {
	t.Helper()

	tx := beginTx(ctx, t)
	return driver.UnwrapExecutor(tx), &testBundle{}
}

// ExerciseExecutorFull exercises a driver that's expected to provide full
// functionality.
func ExerciseExecutorFull[TTx any](ctx context.Context, t *testing.T, driver riverdriver.Driver[TTx], beginTx func(ctx context.Context, t *testing.T) TTx) {
	t.Helper()

	const clientID = "test-client-id"

	// Expect no pool. We'll be using transactions only throughout these tests.
	require.False(t, driver.HasPool())

	// Encompasses all minimal functionality.
	ExerciseExecutorMigrationOnly[TTx](ctx, t, driver, beginTx)

	t.Run("Begin", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		execTx, err := exec.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = execTx.Rollback(ctx) })

		// Job visible in subtransaction, but not parent.
		job := testfactory.Job(ctx, t, execTx, &testfactory.JobOpts{})

		_, err = execTx.JobGetByID(ctx, job.ID)
		require.NoError(t, err)

		require.NoError(t, execTx.Rollback(ctx))

		_, err = exec.JobGetByID(ctx, job.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("JobCancel", func(t *testing.T) {
		t.Parallel()

		for _, startingState := range []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
		} {
			startingState := startingState

			t.Run(fmt.Sprintf("CancelsJobIn%sState", startingState), func(t *testing.T) {
				t.Parallel()

				exec, _ := setupExecutor(ctx, t, driver, beginTx)

				now := time.Now().UTC()
				nowStr := now.Format(time.RFC3339Nano)

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					State: &startingState,
				})
				require.Equal(t, startingState, job.State)

				jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
					ID:                job.ID,
					CancelAttemptedAt: now,
					JobControlTopic:   string(notifier.NotificationTopicJobControl),
				})
				require.NoError(t, err)
				require.NotNil(t, jobAfter)

				require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
				require.WithinDuration(t, time.Now(), *jobAfter.FinalizedAt, 2*time.Second)
				require.JSONEq(t, fmt.Sprintf(`{"cancel_attempted_at":%q}`, nowStr), string(jobAfter.Metadata))
			})
		}

		t.Run("RunningJobIsNotImmediatelyCancelled", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()
			nowStr := now.Format(time.RFC3339Nano)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State: ptrutil.Ptr(rivertype.JobStateRunning),
			})
			require.Equal(t, rivertype.JobStateRunning, job.State)

			jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
				ID:                job.ID,
				CancelAttemptedAt: now,
				JobControlTopic:   string(notifier.NotificationTopicJobControl),
			})
			require.NoError(t, err)
			require.NotNil(t, jobAfter)
			require.Equal(t, rivertype.JobStateRunning, jobAfter.State)
			require.Nil(t, jobAfter.FinalizedAt)
			require.JSONEq(t, fmt.Sprintf(`{"cancel_attempted_at":%q}`, nowStr), string(jobAfter.Metadata))
		})

		for _, startingState := range []rivertype.JobState{
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
		} {
			startingState := startingState

			t.Run(fmt.Sprintf("DoesNotAlterFinalizedJobIn%sState", startingState), func(t *testing.T) {
				t.Parallel()

				exec, _ := setupExecutor(ctx, t, driver, beginTx)

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					FinalizedAt: ptrutil.Ptr(time.Now()),
					State:       &startingState,
				})

				jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
					ID:                job.ID,
					CancelAttemptedAt: time.Now(),
					JobControlTopic:   string(notifier.NotificationTopicJobControl),
				})
				require.NoError(t, err)
				require.Equal(t, startingState, jobAfter.State)
				require.WithinDuration(t, *job.FinalizedAt, *jobAfter.FinalizedAt, time.Microsecond)
				require.JSONEq(t, `{}`, string(jobAfter.Metadata))
			})
		}

		t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
				ID:                1234567890,
				CancelAttemptedAt: time.Now(),
				JobControlTopic:   string(notifier.NotificationTopicJobControl),
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, jobAfter)
		})
	})

	t.Run("JobDeleteBefore", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		var (
			horizon       = time.Now()
			beforeHorizon = horizon.Add(-1 * time.Minute)
			afterHorizon  = horizon.Add(1 * time.Minute)
		)

		deletedJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCancelled)})
		deletedJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
		deletedJob3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

		// Not deleted because not appropriate state.
		notDeletedJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		notDeletedJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		// Not deleted because after the delete horizon.
		notDeletedJob3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateCancelled)})

		// Max two deleted on the first pass.
		numDeleted, err := exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
			CancelledFinalizedAtHorizon: horizon,
			CompletedFinalizedAtHorizon: horizon,
			DiscardedFinalizedAtHorizon: horizon,
			Max:                         2,
		})
		require.NoError(t, err)
		require.Equal(t, 2, numDeleted)

		// And one more pass gets the last one.
		numDeleted, err = exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
			CancelledFinalizedAtHorizon: horizon,
			CompletedFinalizedAtHorizon: horizon,
			DiscardedFinalizedAtHorizon: horizon,
			Max:                         2,
		})
		require.NoError(t, err)
		require.Equal(t, 1, numDeleted)

		// All deleted.
		_, err = exec.JobGetByID(ctx, deletedJob1.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = exec.JobGetByID(ctx, deletedJob2.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = exec.JobGetByID(ctx, deletedJob3.ID)
		require.ErrorIs(t, err, rivertype.ErrNotFound)

		// Not deleted
		_, err = exec.JobGetByID(ctx, notDeletedJob1.ID)
		require.NoError(t, err)
		_, err = exec.JobGetByID(ctx, notDeletedJob2.ID)
		require.NoError(t, err)
		_, err = exec.JobGetByID(ctx, notDeletedJob3.ID)
		require.NoError(t, err)
	})

	t.Run("JobGetAvailable", func(t *testing.T) {
		t.Parallel()

		t.Run("Success", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				AttemptedBy: clientID,
				Max:         100,
				Queue:       rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 1)

			jobRow := jobRows[0]
			require.Equal(t, []string{clientID}, jobRow.AttemptedBy)
		})

		t.Run("ConstrainedToLimit", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			// Two rows inserted but only one found because of the added limit.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				AttemptedBy: clientID,
				Max:         1,
				Queue:       rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 1)
		})

		t.Run("ConstrainedToQueue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Queue: ptrutil.Ptr("other-queue"),
			})

			// Job is in a non-default queue so it's not found.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				AttemptedBy: clientID,
				Max:         100,
				Queue:       rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Empty(t, jobRows)
		})

		t.Run("ConstrainedToScheduledAtBeforeNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt: ptrutil.Ptr(time.Now().Add(1 * time.Minute)),
			})

			// Job is scheduled a while from now so it's not found.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				AttemptedBy: clientID,
				Max:         100,
				Queue:       rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Empty(t, jobRows)
		})

		t.Run("Prioritized", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			// Insert jobs with decreasing priority numbers (3, 2, 1) which means increasing priority.
			for i := 3; i > 0; i-- {
				_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					Priority: &i,
				})
			}

			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				AttemptedBy: clientID,
				Max:         2,
				Queue:       rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 2, "expected to fetch exactly 2 jobs")

			// Because the jobs are ordered within the fetch query's CTE but *not* within
			// the final query, the final result list may not actually be sorted. This is
			// fine, because we've already ensured that we've fetched the jobs we wanted
			// to fetch via that ORDER BY. For testing we'll need to sort the list after
			// fetch to easily assert that the expected jobs are in it.
			sort.Slice(jobRows, func(i, j int) bool { return jobRows[i].Priority < jobRows[j].Priority })

			require.Equal(t, 1, jobRows[0].Priority, "expected first job to have priority 1")
			require.Equal(t, 2, jobRows[1].Priority, "expected second job to have priority 2")

			// Should fetch the one remaining job on the next attempt:
			jobRows, err = exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				AttemptedBy: clientID,
				Max:         1,
				Queue:       rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.NoError(t, err)
			require.Len(t, jobRows, 1, "expected to fetch exactly 1 job")
			require.Equal(t, 3, jobRows[0].Priority, "expected final job to have priority 3")
		})
	})

	t.Run("JobGetByID", func(t *testing.T) {
		t.Parallel()

		t.Run("FetchesAnExistingJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			fetchedJob, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			require.Equal(t, job.ID, fetchedJob.ID)
			require.Equal(t, rivertype.JobStateAvailable, fetchedJob.State)
			require.WithinDuration(t, now, fetchedJob.CreatedAt, 100*time.Millisecond)
			require.WithinDuration(t, now, fetchedJob.ScheduledAt, 100*time.Millisecond)
		})

		t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			job, err := exec.JobGetByID(ctx, 0)
			require.Error(t, err)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, job)
		})
	})

	t.Run("JobGetByIDMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

		// Not returned.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

		jobs, err := exec.JobGetByIDMany(ctx, []int64{job1.ID, job2.ID})
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID},
			sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID }))
	})

	t.Run("JobGetByKindAndUniqueProperties", func(t *testing.T) {
		t.Parallel()

		const uniqueJobKind = "unique_job_kind"

		t.Run("NoOptions", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(uniqueJobKind)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("other_kind")})

			fetchedJob, err := exec.JobGetByKindAndUniqueProperties(ctx, &riverdriver.JobGetByKindAndUniquePropertiesParams{
				Kind: uniqueJobKind,
			})
			require.NoError(t, err)
			require.Equal(t, job.ID, fetchedJob.ID)

			_, err = exec.JobGetByKindAndUniqueProperties(ctx, &riverdriver.JobGetByKindAndUniquePropertiesParams{
				Kind: "does_not_exist",
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})

		t.Run("ByArgs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			args := []byte(`{"unique": "args"}`)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(uniqueJobKind), EncodedArgs: args})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(uniqueJobKind), EncodedArgs: []byte(`{"other": "args"}`)})

			fetchedJob, err := exec.JobGetByKindAndUniqueProperties(ctx, &riverdriver.JobGetByKindAndUniquePropertiesParams{
				Kind:   uniqueJobKind,
				ByArgs: true,
				Args:   args,
			})
			require.NoError(t, err)
			require.Equal(t, job.ID, fetchedJob.ID)

			_, err = exec.JobGetByKindAndUniqueProperties(ctx, &riverdriver.JobGetByKindAndUniquePropertiesParams{
				Kind:   uniqueJobKind,
				ByArgs: true,
				Args:   []byte(`{"does_not_exist": "args"}`),
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})

		t.Run("ByCreatedAt", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			createdAt := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(uniqueJobKind), CreatedAt: &createdAt})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(uniqueJobKind), CreatedAt: ptrutil.Ptr(createdAt.Add(10 * time.Minute))})

			fetchedJob, err := exec.JobGetByKindAndUniqueProperties(ctx, &riverdriver.JobGetByKindAndUniquePropertiesParams{
				Kind:           uniqueJobKind,
				ByCreatedAt:    true,
				CreatedAtBegin: createdAt.Add(-5 * time.Minute),
				CreatedAtEnd:   createdAt.Add(5 * time.Minute),
			})
			require.NoError(t, err)
			require.Equal(t, job.ID, fetchedJob.ID)

			_, err = exec.JobGetByKindAndUniqueProperties(ctx, &riverdriver.JobGetByKindAndUniquePropertiesParams{
				Kind:           uniqueJobKind,
				ByCreatedAt:    true,
				CreatedAtBegin: createdAt.Add(-15 * time.Minute),
				CreatedAtEnd:   createdAt.Add(-5 * time.Minute),
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})

		t.Run("ByQueue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			const queue = "unique_queue"

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(uniqueJobKind), Queue: ptrutil.Ptr(queue)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(uniqueJobKind), Queue: ptrutil.Ptr("other_queue")})

			fetchedJob, err := exec.JobGetByKindAndUniqueProperties(ctx, &riverdriver.JobGetByKindAndUniquePropertiesParams{
				Kind:    uniqueJobKind,
				ByQueue: true,
				Queue:   queue,
			})
			require.NoError(t, err)
			require.Equal(t, job.ID, fetchedJob.ID)

			_, err = exec.JobGetByKindAndUniqueProperties(ctx, &riverdriver.JobGetByKindAndUniquePropertiesParams{
				Kind:    uniqueJobKind,
				ByQueue: true,
				Queue:   "does_not_exist",
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})

		t.Run("ByState", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			const state = rivertype.JobStateCompleted

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(uniqueJobKind), State: ptrutil.Ptr(state)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(uniqueJobKind), State: ptrutil.Ptr(rivertype.JobStateRetryable)})

			fetchedJob, err := exec.JobGetByKindAndUniqueProperties(ctx, &riverdriver.JobGetByKindAndUniquePropertiesParams{
				Kind:    uniqueJobKind,
				ByState: true,
				State:   []string{string(state)},
			})
			require.NoError(t, err)
			require.Equal(t, job.ID, fetchedJob.ID)

			_, err = exec.JobGetByKindAndUniqueProperties(ctx, &riverdriver.JobGetByKindAndUniquePropertiesParams{
				Kind:    uniqueJobKind,
				ByState: true,
				State:   []string{string(rivertype.JobStateScheduled)},
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})
	})

	t.Run("JobGetByKindMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind1")})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind2")})

		// Not returned.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind3")})

		jobs, err := exec.JobGetByKindMany(ctx, []string{job1.Kind, job2.Kind})
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID},
			sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID }))
	})

	t.Run("JobGetStuck", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		var (
			horizon       = time.Now()
			beforeHorizon = horizon.Add(-1 * time.Minute)
			afterHorizon  = horizon.Add(1 * time.Minute)
		)

		stuckJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})
		stuckJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})

		// Not returned because we put a maximum of two.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})

		// Not stuck because not in running state.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})

		// Not stuck because after queried horizon.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})

		// Max two stuck
		stuckJobs, err := exec.JobGetStuck(ctx, &riverdriver.JobGetStuckParams{
			StuckHorizon: horizon,
			Max:          2,
		})
		require.NoError(t, err)
		require.Equal(t, []int64{stuckJob1.ID, stuckJob2.ID},
			sliceutil.Map(stuckJobs, func(j *rivertype.JobRow) int64 { return j.ID }))
	})

	t.Run("JobInsertFast", func(t *testing.T) {
		t.Parallel()

		t.Run("MinimalArgsWithDefaults", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()

			job, err := exec.JobInsertFast(ctx, &riverdriver.JobInsertFastParams{
				EncodedArgs: []byte(`{"encoded": "args"}`),
				Kind:        "test_kind",
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				State:       rivertype.JobStateAvailable,
			})
			require.NoError(t, err)
			require.Equal(t, 0, job.Attempt)
			require.Nil(t, job.AttemptedAt)
			require.WithinDuration(t, now, job.CreatedAt, 2*time.Second)
			require.Equal(t, []byte(`{"encoded": "args"}`), job.EncodedArgs)
			require.Empty(t, job.Errors)
			require.Nil(t, job.FinalizedAt)
			require.Equal(t, "test_kind", job.Kind)
			require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)
			require.Equal(t, []byte(`{}`), job.Metadata)
			require.Equal(t, rivercommon.PriorityDefault, job.Priority)
			require.Equal(t, rivercommon.QueueDefault, job.Queue)
			require.WithinDuration(t, now, job.ScheduledAt, 2*time.Second)
			require.Equal(t, rivertype.JobStateAvailable, job.State)
			require.Equal(t, []string{}, job.Tags)
		})

		t.Run("AllArgs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()

			job, err := exec.JobInsertFast(ctx, &riverdriver.JobInsertFastParams{
				EncodedArgs: []byte(`{"encoded": "args"}`),
				Kind:        "test_kind",
				MaxAttempts: 6,
				Metadata:    []byte(`{"meta": "data"}`),
				Priority:    2,
				Queue:       "queue_name",
				ScheduledAt: &now,
				State:       rivertype.JobStateRunning,
				Tags:        []string{"tag"},
			})
			require.NoError(t, err)
			require.Equal(t, 0, job.Attempt)
			require.Nil(t, job.AttemptedAt)
			require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
			require.Equal(t, []byte(`{"encoded": "args"}`), job.EncodedArgs)
			require.Empty(t, job.Errors)
			require.Nil(t, job.FinalizedAt)
			require.Equal(t, "test_kind", job.Kind)
			require.Equal(t, 6, job.MaxAttempts)
			require.Equal(t, []byte(`{"meta": "data"}`), job.Metadata)
			require.Equal(t, 2, job.Priority)
			require.Equal(t, "queue_name", job.Queue)
			requireEqualTime(t, now, job.ScheduledAt)
			require.Equal(t, rivertype.JobStateRunning, job.State)
			require.Equal(t, []string{"tag"}, job.Tags)
		})
	})

	t.Run("JobInsertFastMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		// This test needs to use a time from before the transaction begins, otherwise
		// the newly-scheduled jobs won't yet show as available because their
		// scheduled_at (which gets a default value from time.Now() in code) will be
		// after the start of the transaction.
		now := time.Now().UTC().Add(-1 * time.Minute)

		insertParams := make([]*riverdriver.JobInsertFastParams, 10)
		for i := 0; i < len(insertParams); i++ {
			insertParams[i] = &riverdriver.JobInsertFastParams{
				EncodedArgs: []byte(`{"encoded": "args"}`),
				Kind:        "test_kind",
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Metadata:    []byte(`{"meta": "data"}`),
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				ScheduledAt: &now,
				State:       rivertype.JobStateAvailable,
				Tags:        []string{"tag"},
			}
			insertParams[i].ScheduledAt = &now
		}

		count, err := exec.JobInsertFastMany(ctx, insertParams)
		require.NoError(t, err)
		require.Len(t, insertParams, int(count))

		jobsAfter, err := exec.JobGetByKindMany(ctx, []string{"test_kind"})
		require.NoError(t, err)
		require.Len(t, jobsAfter, len(insertParams))
		for _, job := range jobsAfter {
			require.Equal(t, 0, job.Attempt)
			require.Nil(t, job.AttemptedAt)
			require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
			require.Equal(t, []byte(`{"encoded": "args"}`), job.EncodedArgs)
			require.Empty(t, job.Errors)
			require.Nil(t, job.FinalizedAt)
			require.Equal(t, "test_kind", job.Kind)
			require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)
			require.Equal(t, []byte(`{"meta": "data"}`), job.Metadata)
			require.Equal(t, rivercommon.PriorityDefault, job.Priority)
			require.Equal(t, rivercommon.QueueDefault, job.Queue)
			requireEqualTime(t, now, job.ScheduledAt)
			require.Equal(t, rivertype.JobStateAvailable, job.State)
			require.Equal(t, []string{"tag"}, job.Tags)
		}
	})

	t.Run("JobInsertFull", func(t *testing.T) {
		t.Parallel()

		t.Run("MinimalArgsWithDefaults", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
				EncodedArgs: []byte(`{"encoded": "args"}`),
				Kind:        "test_kind",
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				State:       rivertype.JobStateAvailable,
			})
			require.NoError(t, err)
			require.Equal(t, 0, job.Attempt)
			require.Nil(t, job.AttemptedAt)
			require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
			require.Equal(t, []byte(`{"encoded": "args"}`), job.EncodedArgs)
			require.Empty(t, job.Errors)
			require.Nil(t, job.FinalizedAt)
			require.Equal(t, "test_kind", job.Kind)
			require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)
			require.Equal(t, rivercommon.QueueDefault, job.Queue)
			require.Equal(t, rivertype.JobStateAvailable, job.State)
		})

		t.Run("AllArgs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()

			job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
				Attempt:     3,
				AttemptedAt: &now,
				CreatedAt:   &now,
				EncodedArgs: []byte(`{"encoded": "args"}`),
				Errors:      [][]byte{[]byte(`{"error": "message"}`)},
				FinalizedAt: &now,
				Kind:        "test_kind",
				MaxAttempts: 6,
				Metadata:    []byte(`{"meta": "data"}`),
				Priority:    2,
				Queue:       "queue_name",
				ScheduledAt: &now,
				State:       rivertype.JobStateCompleted,
				Tags:        []string{"tag"},
			})
			require.NoError(t, err)
			require.Equal(t, 3, job.Attempt)
			requireEqualTime(t, now, *job.AttemptedAt)
			requireEqualTime(t, now, job.CreatedAt)
			require.Equal(t, []byte(`{"encoded": "args"}`), job.EncodedArgs)
			require.Equal(t, "message", job.Errors[0].Error)
			requireEqualTime(t, now, *job.FinalizedAt)
			require.Equal(t, "test_kind", job.Kind)
			require.Equal(t, 6, job.MaxAttempts)
			require.Equal(t, []byte(`{"meta": "data"}`), job.Metadata)
			require.Equal(t, 2, job.Priority)
			require.Equal(t, "queue_name", job.Queue)
			requireEqualTime(t, now, job.ScheduledAt)
			require.Equal(t, rivertype.JobStateCompleted, job.State)
			require.Equal(t, []string{"tag"}, job.Tags)
		})
	})

	t.Run("JobList", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		now := time.Now().UTC()

		job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(3),
			AttemptedAt: &now,
			CreatedAt:   &now,
			EncodedArgs: []byte(`{"encoded": "args"}`),
			Errors:      [][]byte{[]byte(`{"error": "message"}`)},
			FinalizedAt: &now,
			Metadata:    []byte(`{"meta": "data"}`),
			ScheduledAt: &now,
			State:       ptrutil.Ptr(rivertype.JobStateCompleted),
			Tags:        []string{"tag"},
		})

		fetchedJobs, err := exec.JobList(
			ctx,
			fmt.Sprintf("SELECT %s FROM river_job WHERE id = @job_id", exec.JobListFields()),
			map[string]any{"job_id": job.ID},
		)
		require.NoError(t, err)
		require.Len(t, fetchedJobs, 1)

		fetchedJob := fetchedJobs[0]
		require.Equal(t, job.Attempt, fetchedJob.Attempt)
		require.Equal(t, job.AttemptedAt, fetchedJob.AttemptedAt)
		require.Equal(t, job.CreatedAt, fetchedJob.CreatedAt)
		require.Equal(t, job.EncodedArgs, fetchedJob.EncodedArgs)
		require.Equal(t, "message", fetchedJob.Errors[0].Error)
		require.Equal(t, job.FinalizedAt, fetchedJob.FinalizedAt)
		require.Equal(t, job.Kind, fetchedJob.Kind)
		require.Equal(t, job.MaxAttempts, fetchedJob.MaxAttempts)
		require.Equal(t, job.Metadata, fetchedJob.Metadata)
		require.Equal(t, job.Priority, fetchedJob.Priority)
		require.Equal(t, job.Queue, fetchedJob.Queue)
		require.Equal(t, job.ScheduledAt, fetchedJob.ScheduledAt)
		require.Equal(t, job.State, fetchedJob.State)
		require.Equal(t, job.Tags, fetchedJob.Tags)
	})

	t.Run("JobListFields", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		require.Equal(t, "id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags",
			exec.JobListFields())
	})

	t.Run("JobRescueMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		now := time.Now().UTC()

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		_, err := exec.JobRescueMany(ctx, &riverdriver.JobRescueManyParams{
			ID: []int64{
				job1.ID,
				job2.ID,
			},
			Error: [][]byte{
				[]byte(`{"error": "message1"}`),
				[]byte(`{"error": "message2"}`),
			},
			FinalizedAt: []time.Time{
				{},
				now,
			},
			ScheduledAt: []time.Time{
				now,
				now,
			},
			State: []string{
				string(rivertype.JobStateAvailable),
				string(rivertype.JobStateDiscarded),
			},
		})
		require.NoError(t, err)

		updatedJob1, err := exec.JobGetByID(ctx, job1.ID)
		require.NoError(t, err)
		require.Equal(t, "message1", updatedJob1.Errors[0].Error)
		require.Nil(t, updatedJob1.FinalizedAt)
		requireEqualTime(t, now, updatedJob1.ScheduledAt)
		require.Equal(t, rivertype.JobStateAvailable, updatedJob1.State)

		updatedJob2, err := exec.JobGetByID(ctx, job2.ID)
		require.NoError(t, err)
		require.Equal(t, "message2", updatedJob2.Errors[0].Error)
		requireEqualTime(t, now, *updatedJob2.FinalizedAt)
		requireEqualTime(t, now, updatedJob2.ScheduledAt)
		require.Equal(t, rivertype.JobStateDiscarded, updatedJob2.State)
	})

	t.Run("JobRetry", func(t *testing.T) {
		t.Parallel()

		t.Run("DoesNotUpdateARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State: ptrutil.Ptr(rivertype.JobStateRunning),
			})

			jobAfter, err := exec.JobRetry(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, jobAfter.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, jobUpdated.State)
		})

		for _, state := range []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
			// TODO(bgentry): add Pending to this list when it's added:
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
		} {
			state := state

			t.Run(fmt.Sprintf("UpdatesA_%s_JobToBeScheduledImmediately", state), func(t *testing.T) {
				t.Parallel()

				exec, _ := setupExecutor(ctx, t, driver, beginTx)

				now := time.Now().UTC()

				setFinalized := slices.Contains([]rivertype.JobState{
					rivertype.JobStateCancelled,
					rivertype.JobStateCompleted,
					rivertype.JobStateDiscarded,
				}, state)

				var finalizedAt *time.Time
				if setFinalized {
					finalizedAt = &now
				}

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					FinalizedAt: finalizedAt,
					ScheduledAt: ptrutil.Ptr(now.Add(1 * time.Hour)),
					State:       &state,
				})

				jobAfter, err := exec.JobRetry(ctx, job.ID)
				require.NoError(t, err)
				require.Equal(t, rivertype.JobStateAvailable, jobAfter.State)
				require.WithinDuration(t, time.Now().UTC(), jobAfter.ScheduledAt, 100*time.Millisecond)

				jobUpdated, err := exec.JobGetByID(ctx, job.ID)
				require.NoError(t, err)
				require.Equal(t, rivertype.JobStateAvailable, jobUpdated.State)
				require.Nil(t, jobUpdated.FinalizedAt)
			})
		}

		t.Run("AltersScheduledAtForAlreadyCompletedJob", func(t *testing.T) {
			// A job which has already completed will have a ScheduledAt that could be
			// long in the past. Now that we're re-scheduling it, we should update that
			// to the current time to slot it in alongside other recently-scheduled jobs
			// and not skip the line; also, its wait duration can't be calculated
			// accurately if we don't reset the scheduled_at.
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				FinalizedAt: &now,
				ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
				State:       ptrutil.Ptr(rivertype.JobStateCompleted),
			})

			jobAfter, err := exec.JobRetry(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, jobAfter.State)
			require.WithinDuration(t, now, jobAfter.ScheduledAt, 5*time.Second)
		})

		t.Run("DoesNotAlterScheduledAtIfInThePastAndJobAlreadyAvailable", func(t *testing.T) {
			// We don't want to update ScheduledAt if the job was already available
			// because doing so can make it lose its place in line.
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
			})

			jobAfter, err := exec.JobRetry(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, jobAfter.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, jobUpdated.State)
		})

		t.Run("ReturnsErrNotFoundIfJobNotFound", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			_, err := exec.JobRetry(ctx, 0)
			require.Error(t, err)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})
	})

	t.Run("JobSchedule", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		var (
			horizon       = time.Now()
			beforeHorizon = horizon.Add(-1 * time.Minute)
			afterHorizon  = horizon.Add(1 * time.Minute)
		)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRetryable)})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateScheduled)})
		job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateScheduled)})

		// States that aren't scheduled.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

		// Right state, but after horizon.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateRetryable)})
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateScheduled)})

		// First two scheduled because of limit.
		numScheduled, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
			InsertTopic: string(notifier.NotificationTopicInsert),
			Max:         2,
			Now:         horizon,
		})
		require.NoError(t, err)
		require.Equal(t, 2, numScheduled)

		// And then job3 scheduled.
		numScheduled, err = exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
			InsertTopic: string(notifier.NotificationTopicInsert),
			Max:         2,
			Now:         horizon,
		})
		require.NoError(t, err)
		require.Equal(t, 1, numScheduled)

		updatedJob1, err := exec.JobGetByID(ctx, job1.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, updatedJob1.State)

		updatedJob2, err := exec.JobGetByID(ctx, job2.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, updatedJob2.State)

		updatedJob3, err := exec.JobGetByID(ctx, job3.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, updatedJob3.State)
	})

	t.Run("JobSetStateIfRunning_JobSetStateCompleted", func(t *testing.T) {
		t.Parallel()

		t.Run("CompletesARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State: ptrutil.Ptr(rivertype.JobStateRunning),
			})

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateCompleted(job.ID, now))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, jobUpdated.State)
		})

		t.Run("DoesNotCompleteARetryableJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State: ptrutil.Ptr(rivertype.JobStateRetryable),
			})

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateCompleted(job.ID, now))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.Nil(t, jobAfter.FinalizedAt)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
		})
	})

	t.Run("JobSetStateIfRunning_JobSetStateErrored", func(t *testing.T) {
		t.Parallel()

		makeErrPayload := func(t *testing.T, now time.Time) []byte {
			t.Helper()

			errPayload, err := json.Marshal(rivertype.AttemptError{
				Attempt: 1, At: now, Error: "fake error", Trace: "foo.go:123\nbar.go:456",
			})
			require.NoError(t, err)
			return errPayload
		}

		t.Run("SetsARunningJobToRetryable", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State: ptrutil.Ptr(rivertype.JobStateRunning),
			})

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now)))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.WithinDuration(t, now, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)

			// validate error payload:
			require.Len(t, jobAfter.Errors, 1)
			require.Equal(t, now, jobAfter.Errors[0].At)
			require.Equal(t, 1, jobAfter.Errors[0].Attempt)
			require.Equal(t, "fake error", jobAfter.Errors[0].Error)
			require.Equal(t, "foo.go:123\nbar.go:456", jobAfter.Errors[0].Trace)
		})

		t.Run("DoesNotTouchAlreadyRetryableJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:       ptrutil.Ptr(rivertype.JobStateRetryable),
				ScheduledAt: ptrutil.Ptr(now.Add(10 * time.Second)),
			})

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now)))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)
		})

		t.Run("SetsAJobWithCancelAttemptedAtToCancelled", func(t *testing.T) {
			// If a job has cancel_attempted_at in its metadata, it means that the user
			// tried to cancel the job with the Cancel API but that the job
			// finished/errored before the producer received the cancel notification.
			//
			// In this case, we want to move the job to cancelled instead of retryable
			// so that the job is not retried.
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Metadata:    []byte(fmt.Sprintf(`{"cancel_attempted_at":"%s"}`, time.Now().UTC().Format(time.RFC3339))),
				State:       ptrutil.Ptr(rivertype.JobStateRunning),
				ScheduledAt: ptrutil.Ptr(now.Add(-10 * time.Second)),
			})

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now)))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
			require.NotNil(t, jobAfter.FinalizedAt)
			// Loose assertion against FinalizedAt just to make sure it was set (it uses
			// the database's now() instead of a passed-in time):
			require.WithinDuration(t, time.Now().UTC(), *jobAfter.FinalizedAt, 2*time.Second)
			// ScheduledAt should not be touched:
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)
			// Errors should still be appended to:
			require.Len(t, jobAfter.Errors, 1)
			require.Contains(t, jobAfter.Errors[0].Error, "fake error")

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCancelled, jobUpdated.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)
		})
	})

	t.Run("JobUpdate", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

		now := time.Now().UTC()

		updatedJob, err := exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
			ID:                  job.ID,
			AttemptDoUpdate:     true,
			Attempt:             7,
			AttemptedAtDoUpdate: true,
			AttemptedAt:         &now,
			ErrorsDoUpdate:      true,
			Errors:              [][]byte{[]byte(`{"error": "message"}`)},
			FinalizedAtDoUpdate: true,
			FinalizedAt:         &now,
			StateDoUpdate:       true,
			State:               rivertype.JobStateDiscarded,
		})
		require.NoError(t, err)
		require.Equal(t, 7, updatedJob.Attempt)
		requireEqualTime(t, now, *updatedJob.AttemptedAt)
		require.Equal(t, "message", updatedJob.Errors[0].Error)
		requireEqualTime(t, now, *updatedJob.FinalizedAt)
		require.Equal(t, rivertype.JobStateDiscarded, updatedJob.State)
	})

	const (
		leaderInstanceName = "default"
		leaderTTL          = 10 * time.Second
	)

	t.Run("LeaderDeleteExpired", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		now := time.Now().UTC()

		{
			numDeleted, err := exec.LeaderDeleteExpired(ctx, leaderInstanceName)
			require.NoError(t, err)
			require.Zero(t, numDeleted)
		}

		_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
			ElectedAt: ptrutil.Ptr(now.Add(-2 * time.Hour)),
			ExpiresAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
			LeaderID:  ptrutil.Ptr(clientID),
			Name:      ptrutil.Ptr(leaderInstanceName),
		})

		{
			numDeleted, err := exec.LeaderDeleteExpired(ctx, leaderInstanceName)
			require.NoError(t, err)
			require.Equal(t, 1, numDeleted)
		}
	})

	t.Run("LeaderAttemptElect", func(t *testing.T) {
		t.Parallel()

		t.Run("ElectsLeader", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			elected, err := exec.LeaderAttemptElect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: clientID,
				Name:     leaderInstanceName,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.True(t, elected) // won election

			leader, err := exec.LeaderGetElectedLeader(ctx, leaderInstanceName)
			require.NoError(t, err)
			require.WithinDuration(t, time.Now(), leader.ElectedAt, 100*time.Millisecond)
			require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, 100*time.Millisecond)
		})

		t.Run("CannotElectTwiceInARow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(clientID),
				Name:     ptrutil.Ptr(leaderInstanceName),
			})

			elected, err := exec.LeaderAttemptElect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: "different-client-id",
				Name:     leaderInstanceName,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.False(t, elected) // lost election

			// The time should not have changed because we specified that we were not
			// already elected, and the elect query is a no-op if there's already a
			// updatedLeader:
			updatedLeader, err := exec.LeaderGetElectedLeader(ctx, leaderInstanceName)
			require.NoError(t, err)
			require.Equal(t, leader.ExpiresAt, updatedLeader.ExpiresAt)
		})
	})

	t.Run("LeaderAttemptReelect", func(t *testing.T) {
		t.Parallel()

		t.Run("ElectsLeader", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			elected, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: clientID,
				Name:     leaderInstanceName,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.True(t, elected) // won election

			leader, err := exec.LeaderGetElectedLeader(ctx, leaderInstanceName)
			require.NoError(t, err)
			require.WithinDuration(t, time.Now(), leader.ElectedAt, 100*time.Millisecond)
			require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, 100*time.Millisecond)
		})

		t.Run("ReelectsSameLeader", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(clientID),
				Name:     ptrutil.Ptr(leaderInstanceName),
			})

			// Re-elect the same leader. Use a larger TTL to see if time is updated,
			// because we are in a test transaction and the time is frozen at the start of
			// the transaction.
			elected, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: clientID,
				Name:     leaderInstanceName,
				TTL:      30 * time.Second,
			})
			require.NoError(t, err)
			require.True(t, elected) // won re-election

			// expires_at should be incremented because this is the same leader that won
			// previously and we specified that we're already elected:
			updatedLeader, err := exec.LeaderGetElectedLeader(ctx, leaderInstanceName)
			require.NoError(t, err)
			require.Greater(t, updatedLeader.ExpiresAt, leader.ExpiresAt)
		})
	})

	t.Run("LeaderInsert", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		leader, err := exec.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
			LeaderID: clientID,
			Name:     leaderInstanceName,
			TTL:      leaderTTL,
		})
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), leader.ElectedAt, 100*time.Millisecond)
		require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, 100*time.Millisecond)
		require.Equal(t, leaderInstanceName, leader.Name)
		require.Equal(t, clientID, leader.LeaderID)
	})

	t.Run("LeaderGetElectedLeader", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
			LeaderID: ptrutil.Ptr(clientID),
			Name:     ptrutil.Ptr(leaderInstanceName),
		})

		leader, err := exec.LeaderGetElectedLeader(ctx, leaderInstanceName)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), leader.ElectedAt, 100*time.Millisecond)
		require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, 100*time.Millisecond)
		require.Equal(t, leaderInstanceName, leader.Name)
		require.Equal(t, clientID, leader.LeaderID)
	})

	t.Run("LeaderResign", func(t *testing.T) {
		t.Parallel()

		t.Run("Success", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			{
				resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
					LeaderID:        clientID,
					LeadershipTopic: string(notifier.NotificationTopicLeadership),
					Name:            leaderInstanceName,
				})
				require.NoError(t, err)
				require.False(t, resigned)
			}

			_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(clientID),
				Name:     ptrutil.Ptr(leaderInstanceName),
			})

			{
				resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
					LeaderID:        clientID,
					LeadershipTopic: string(notifier.NotificationTopicLeadership),
					Name:            leaderInstanceName,
				})
				require.NoError(t, err)
				require.True(t, resigned)
			}
		})

		t.Run("DoesNotResignWithoutLeadership", func(t *testing.T) {
			t.Parallel()

			exec, _ := setupExecutor(ctx, t, driver, beginTx)

			_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr("other-client-id"),
				Name:     ptrutil.Ptr(leaderInstanceName),
			})

			resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
				LeaderID:        clientID,
				LeadershipTopic: string(notifier.NotificationTopicLeadership),
				Name:            leaderInstanceName,
			})
			require.NoError(t, err)
			require.False(t, resigned)
		})
	})

	t.Run("PGAdvisoryXactLock", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		// Acquire the advisory lock.
		_, err := exec.PGAdvisoryXactLock(ctx, 123456)
		require.NoError(t, err)

		// Open a new transaction and try to acquire the same lock, which should
		// block because the lock can't be acquired. Verify some amount of wait,
		// cancel the lock acquisition attempt, then verify return.
		{
			var (
				otherTx   = beginTx(ctx, t)
				otherExec = driver.UnwrapExecutor(otherTx)
			)

			goroutineDone := make(chan struct{})

			ctx, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)

			go func() {
				defer close(goroutineDone)

				_, err := otherExec.PGAdvisoryXactLock(ctx, 123456)
				require.ErrorIs(t, err, context.Canceled)
			}()

			select {
			case <-goroutineDone:
				require.FailNow(t, "Unexpectedly acquired lock that should've held by other transaction")
			case <-time.After(50 * time.Millisecond):
			}

			cancel()

			select {
			case <-goroutineDone:
			case <-time.After(50 * time.Millisecond):
				require.FailNow(t, "Goroutine didn't finish in a timely manner")
			}
		}
	})
}

// ExerciseExecutorMigrationOnly exercises a driver that's expected to only be
// able to perform database migrations, and not full River functionality.
func ExerciseExecutorMigrationOnly[TTx any](ctx context.Context, t *testing.T, driver riverdriver.Driver[TTx], beginTx func(ctx context.Context, t *testing.T) TTx) {
	t.Helper()

	// Truncates the migration table so we only have to work with test
	// migration data.
	truncateMigrations := func(ctx context.Context, t *testing.T, exec riverdriver.Executor) {
		t.Helper()

		_, err := exec.Exec(ctx, "TRUNCATE TABLE river_migration")
		require.NoError(t, err)
	}

	// Expect no pool. We'll be using transactions only throughout these tests.
	require.False(t, driver.HasPool())

	t.Run("Exec", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		_, err := exec.Exec(ctx, "SELECT 1 + 2")
		require.NoError(t, err)
	})

	t.Run("MigrationDeleteByVersionMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		truncateMigrations(ctx, t, exec)

		migration1 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})
		migration2 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})

		migrations, err := exec.MigrationDeleteByVersionMany(ctx, []int{
			migration1.Version,
			migration2.Version,
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		slices.SortFunc(migrations, func(a, b *riverdriver.Migration) int { return a.Version - b.Version })
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, migration2.Version, migrations[1].Version)
	})

	t.Run("MigrationGetAll", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		truncateMigrations(ctx, t, exec)

		migration1 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})
		migration2 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})

		migrations, err := exec.MigrationGetAll(ctx)
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, migration2.Version, migrations[1].Version)

		// Check the full properties of one of the migrations.
		migration1Fetched := migrations[0]
		require.Equal(t, migration1.ID, migration1Fetched.ID)
		requireEqualTime(t, migration1.CreatedAt, migration1Fetched.CreatedAt)
		require.Equal(t, migration1.Version, migration1Fetched.Version)
	})

	t.Run("MigrationInsertMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		truncateMigrations(ctx, t, exec)

		migrations, err := exec.MigrationInsertMany(ctx, []int{1, 2})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, 1, migrations[0].Version)
		require.Equal(t, 2, migrations[1].Version)
	})

	t.Run("TableExists", func(t *testing.T) {
		t.Parallel()

		exec, _ := setupExecutor(ctx, t, driver, beginTx)

		exists, err := exec.TableExists(ctx, "river_job")
		require.NoError(t, err)
		require.True(t, exists)

		exists, err = exec.TableExists(ctx, "does_not_exist")
		require.NoError(t, err)
		require.False(t, exists)
	})
}

type testListenerBundle[TTx any] struct {
	driver riverdriver.Driver[TTx]
	exec   riverdriver.Executor
}

func setupListener[TTx any](ctx context.Context, t *testing.T, getDriverWithPool func(ctx context.Context, t *testing.T) riverdriver.Driver[TTx]) (riverdriver.Listener, *testListenerBundle[TTx]) {
	t.Helper()

	driver := getDriverWithPool(ctx, t)

	listener := driver.GetListener()
	t.Cleanup(func() { require.NoError(t, listener.Close(ctx)) })

	require.NoError(t, listener.Connect(ctx))

	return listener, &testListenerBundle[TTx]{
		driver: driver,
		exec:   driver.GetExecutor(),
	}
}

func ExerciseListener[TTx any](ctx context.Context, t *testing.T, getDriverWithPool func(ctx context.Context, t *testing.T) riverdriver.Driver[TTx]) {
	t.Helper()

	requireNoNotification := func(ctx context.Context, t *testing.T, listener riverdriver.Listener) {
		t.Helper()

		// Ugh, this is a little sketchy, but hard to test in another way.
		ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		notification, err := listener.WaitForNotification(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded, "Expected no notification, but got: %+v", notification)
	}

	waitForNotification := func(ctx context.Context, t *testing.T, listener riverdriver.Listener) *riverdriver.Notification {
		t.Helper()

		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		notification, err := listener.WaitForNotification(ctx)
		require.NoError(t, err)

		return notification
	}

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		listener, bundle := setupListener(ctx, t, getDriverWithPool)

		require.NoError(t, listener.Listen(ctx, "topic1"))
		require.NoError(t, listener.Listen(ctx, "topic2"))

		require.NoError(t, listener.Ping(ctx)) // still alive

		{
			require.NoError(t, bundle.exec.Notify(ctx, "topic1", "payload1_1"))
			require.NoError(t, bundle.exec.Notify(ctx, "topic2", "payload2_1"))

			notification := waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1_1"}, notification)
			notification = waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic2", Payload: "payload2_1"}, notification)
		}

		require.NoError(t, listener.Unlisten(ctx, "topic2"))

		{
			require.NoError(t, bundle.exec.Notify(ctx, "topic1", "payload1_2"))
			require.NoError(t, bundle.exec.Notify(ctx, "topic2", "payload2_2"))

			notification := waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1_2"}, notification)

			requireNoNotification(ctx, t, listener)
		}

		require.NoError(t, listener.Unlisten(ctx, "topic1"))

		require.NoError(t, listener.Close(ctx))
	})

	t.Run("TransactionGated", func(t *testing.T) {
		t.Parallel()

		listener, bundle := setupListener(ctx, t, getDriverWithPool)

		require.NoError(t, listener.Listen(ctx, "topic1"))

		execTx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)

		require.NoError(t, execTx.Notify(ctx, "topic1", "payload1"))

		// No notification because the transaction hasn't committed yet.
		requireNoNotification(ctx, t, listener)

		require.NoError(t, execTx.Commit(ctx))

		// Notification received now that transaction has committed.
		notification := waitForNotification(ctx, t, listener)
		require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1"}, notification)
	})
}

// requireEqualTime compares to timestamps down the microsecond only. This is
// appropriate for comparing times that might've roundtripped from Postgres,
// which only stores to microsecond precision.
func requireEqualTime(t *testing.T, expected, actual time.Time) {
	t.Helper()

	// Leaving off the nanosecond portion has the effect of truncating it rather
	// than rounding to the nearest microsecond, which functionally matches
	// pgx's behavior while persisting.
	const rfc3339Micro = "2006-01-02T15:04:05.999999Z07:00"

	require.Equal(t,
		expected.Format(rfc3339Micro),
		actual.Format(rfc3339Micro),
	)
}
