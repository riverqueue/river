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
	"github.com/tidwall/gjson"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

// Exercise fully exercises a driver. The driver's listener is exercised if
// supported.
func Exercise[TTx any](ctx context.Context, t *testing.T,
	driverWithPool func(ctx context.Context, t *testing.T) riverdriver.Driver[TTx],
	executorWithTx func(ctx context.Context, t *testing.T) riverdriver.Executor,
) {
	t.Helper()

	if driverWithPool(ctx, t).SupportsListener() {
		exerciseListener(ctx, t, driverWithPool)
	} else {
		t.Logf("Driver does not support listener; skipping listener tests")
	}

	t.Run("GetMigrationFS", func(t *testing.T) {
		driver := driverWithPool(ctx, t)

		for _, line := range driver.GetMigrationLines() {
			migrationFS := driver.GetMigrationFS(line)

			// Directory for the advertised migration line should exist.
			_, err := migrationFS.Open("migration/" + line)
			require.NoError(t, err)
		}
	})

	t.Run("GetMigrationLines", func(t *testing.T) {
		driver := driverWithPool(ctx, t)

		// Should contain at minimum a main migration line.
		require.Contains(t, driver.GetMigrationLines(), riverdriver.MigrationLineMain)
	})

	type testBundle struct{}

	setup := func(ctx context.Context, t *testing.T) (riverdriver.Executor, *testBundle) {
		t.Helper()
		return executorWithTx(ctx, t), &testBundle{}
	}

	const clientID = "test-client-id"

	t.Run("Begin", func(t *testing.T) {
		t.Parallel()

		t.Run("BasicVisibility", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			tx, err := exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback(ctx) })

			// Job visible in subtransaction, but not parent.
			{
				job := testfactory.Job(ctx, t, tx, &testfactory.JobOpts{})

				_, err = tx.JobGetByID(ctx, job.ID)
				require.NoError(t, err)

				require.NoError(t, tx.Rollback(ctx))

				_, err = exec.JobGetByID(ctx, job.ID)
				require.ErrorIs(t, err, rivertype.ErrNotFound)
			}
		})

		t.Run("NestedTransactions", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			tx1, err := exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx1.Rollback(ctx) })

			// Job visible in tx1, but not top level executor.
			{
				job1 := testfactory.Job(ctx, t, tx1, &testfactory.JobOpts{})

				{
					tx2, err := tx1.Begin(ctx)
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx2.Rollback(ctx) })

					// Job visible in tx2, but not top level executor.
					{
						job2 := testfactory.Job(ctx, t, tx2, &testfactory.JobOpts{})

						_, err = tx2.JobGetByID(ctx, job2.ID)
						require.NoError(t, err)

						require.NoError(t, tx2.Rollback(ctx))

						_, err = tx1.JobGetByID(ctx, job2.ID)
						require.ErrorIs(t, err, rivertype.ErrNotFound)
					}

					_, err = tx1.JobGetByID(ctx, job1.ID)
					require.NoError(t, err)
				}

				// Repeat the same subtransaction again.
				{
					tx2, err := tx1.Begin(ctx)
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx2.Rollback(ctx) })

					// Job visible in tx2, but not top level executor.
					{
						job2 := testfactory.Job(ctx, t, tx2, &testfactory.JobOpts{})

						_, err = tx2.JobGetByID(ctx, job2.ID)
						require.NoError(t, err)

						require.NoError(t, tx2.Rollback(ctx))

						_, err = tx1.JobGetByID(ctx, job2.ID)
						require.ErrorIs(t, err, rivertype.ErrNotFound)
					}

					_, err = tx1.JobGetByID(ctx, job1.ID)
					require.NoError(t, err)
				}

				require.NoError(t, tx1.Rollback(ctx))

				_, err = exec.JobGetByID(ctx, job1.ID)
				require.ErrorIs(t, err, rivertype.ErrNotFound)
			}
		})

		t.Run("RollbackAfterCommit", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			tx1, err := exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx1.Rollback(ctx) })

			tx2, err := tx1.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx2.Rollback(ctx) })

			job := testfactory.Job(ctx, t, tx2, &testfactory.JobOpts{})

			require.NoError(t, tx2.Commit(ctx))
			_ = tx2.Rollback(ctx) // "tx is closed" error generally returned, but don't require this

			// Despite rollback being called after commit, the job is still
			// visible from the outer transaction.
			_, err = tx1.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
		})
	})

	t.Run("ColumnExists", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		exists, err := exec.ColumnExists(ctx, "river_job", "id")
		require.NoError(t, err)
		require.True(t, exists)

		exists, err = exec.ColumnExists(ctx, "river_job", "does_not_exist")
		require.NoError(t, err)
		require.False(t, exists)

		exists, err = exec.ColumnExists(ctx, "does_not_exist", "id")
		require.NoError(t, err)
		require.False(t, exists)

		// Will be rolled back by the test transaction.
		_, err = exec.Exec(ctx, "CREATE SCHEMA another_schema_123")
		require.NoError(t, err)

		_, err = exec.Exec(ctx, "SET search_path = another_schema_123")
		require.NoError(t, err)

		// Table with the same name as the main schema, but without the same
		// columns.
		_, err = exec.Exec(ctx, "CREATE TABLE river_job (another_id bigint)")
		require.NoError(t, err)

		exists, err = exec.ColumnExists(ctx, "river_job", "id")
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("Exec", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		_, err := exec.Exec(ctx, "SELECT 1 + 2")
		require.NoError(t, err)
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

				exec, _ := setup(ctx, t)

				now := time.Now().UTC()
				nowStr := now.Format(time.RFC3339Nano)

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					State:     &startingState,
					UniqueKey: []byte("unique-key"),
				})
				require.Equal(t, startingState, job.State)

				jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
					ID:                job.ID,
					CancelAttemptedAt: now,
					ControlTopic:      string(notifier.NotificationTopicControl),
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

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()
			nowStr := now.Format(time.RFC3339Nano)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})
			require.Equal(t, rivertype.JobStateRunning, job.State)

			jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
				ID:                job.ID,
				CancelAttemptedAt: now,
				ControlTopic:      string(notifier.NotificationTopicControl),
			})
			require.NoError(t, err)
			require.NotNil(t, jobAfter)
			require.Equal(t, rivertype.JobStateRunning, jobAfter.State)
			require.Nil(t, jobAfter.FinalizedAt)
			require.JSONEq(t, fmt.Sprintf(`{"cancel_attempted_at":%q}`, nowStr), string(jobAfter.Metadata))
			require.Equal(t, "unique-key", string(jobAfter.UniqueKey))
		})

		for _, startingState := range []rivertype.JobState{
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
		} {
			startingState := startingState

			t.Run(fmt.Sprintf("DoesNotAlterFinalizedJobIn%sState", startingState), func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					FinalizedAt: ptrutil.Ptr(time.Now()),
					State:       &startingState,
				})

				jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
					ID:                job.ID,
					CancelAttemptedAt: time.Now(),
					ControlTopic:      string(notifier.NotificationTopicControl),
				})
				require.NoError(t, err)
				require.Equal(t, startingState, jobAfter.State)
				require.WithinDuration(t, *job.FinalizedAt, *jobAfter.FinalizedAt, time.Microsecond)
				require.JSONEq(t, `{}`, string(jobAfter.Metadata))
			})
		}

		t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
				ID:                1234567890,
				CancelAttemptedAt: time.Now(),
				ControlTopic:      string(notifier.NotificationTopicControl),
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, jobAfter)
		})
	})

	t.Run("JobCountByState", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		// Included because they're the queried state.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})

		// Excluded because they're not.
		finalizedAt := ptrutil.Ptr(time.Now())
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: finalizedAt, State: ptrutil.Ptr(rivertype.JobStateCancelled)})
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: finalizedAt, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: finalizedAt, State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

		numJobs, err := exec.JobCountByState(ctx, rivertype.JobStateAvailable)
		require.NoError(t, err)
		require.Equal(t, 2, numJobs)
	})

	t.Run("JobDelete", func(t *testing.T) {
		t.Parallel()

		t.Run("DoesNotDeleteARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State: ptrutil.Ptr(rivertype.JobStateRunning),
			})

			jobAfter, err := exec.JobDelete(ctx, job.ID)
			require.ErrorIs(t, err, rivertype.ErrJobRunning)
			require.Nil(t, jobAfter)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, jobUpdated.State)
		})

		for _, state := range []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
			rivertype.JobStatePending,
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
		} {
			state := state

			t.Run(fmt.Sprintf("DeletesA_%s_Job", state), func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

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

				jobAfter, err := exec.JobDelete(ctx, job.ID)
				require.NoError(t, err)
				require.NotNil(t, jobAfter)
				require.Equal(t, job.ID, jobAfter.ID)
				require.Equal(t, state, jobAfter.State)

				_, err = exec.JobGetByID(ctx, job.ID)
				require.ErrorIs(t, err, rivertype.ErrNotFound)
			})
		}

		t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			jobAfter, err := exec.JobDelete(ctx, 1234567890)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, jobAfter)
		})
	})

	t.Run("JobDeleteBefore", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

			job, err := exec.JobGetByID(ctx, 0)
			require.Error(t, err)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, job)
		})
	})

	t.Run("JobGetByIDMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

			const state = rivertype.JobStateCompleted

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr(uniqueJobKind), FinalizedAt: ptrutil.Ptr(time.Now()), State: ptrutil.Ptr(state)})
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

		exec, _ := setup(ctx, t)

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

		exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			result, err := exec.JobInsertFast(ctx, &riverdriver.JobInsertFastParams{
				EncodedArgs: []byte(`{"encoded": "args"}`),
				Kind:        "test_kind",
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				State:       rivertype.JobStateAvailable,
			})
			require.NoError(t, err)
			job := result.Job
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

			exec, _ := setup(ctx, t)

			targetTime := time.Now().UTC().Add(-15 * time.Minute)

			result, err := exec.JobInsertFast(ctx, &riverdriver.JobInsertFastParams{
				CreatedAt:    &targetTime,
				EncodedArgs:  []byte(`{"encoded": "args"}`),
				Kind:         "test_kind",
				MaxAttempts:  6,
				Metadata:     []byte(`{"meta": "data"}`),
				Priority:     2,
				Queue:        "queue_name",
				ScheduledAt:  &targetTime,
				State:        rivertype.JobStateRunning,
				Tags:         []string{"tag"},
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})
			require.NoError(t, err)

			require.False(t, result.UniqueSkippedAsDuplicate)
			job := result.Job
			require.Equal(t, 0, job.Attempt)
			require.Nil(t, job.AttemptedAt)
			requireEqualTime(t, targetTime, job.CreatedAt)
			require.Equal(t, []byte(`{"encoded": "args"}`), job.EncodedArgs)
			require.Empty(t, job.Errors)
			require.Nil(t, job.FinalizedAt)
			require.Equal(t, "test_kind", job.Kind)
			require.Equal(t, 6, job.MaxAttempts)
			require.Equal(t, []byte(`{"meta": "data"}`), job.Metadata)
			require.Equal(t, 2, job.Priority)
			require.Equal(t, "queue_name", job.Queue)
			requireEqualTime(t, targetTime, job.ScheduledAt)
			require.Equal(t, rivertype.JobStateRunning, job.State)
			require.Equal(t, []string{"tag"}, job.Tags)
		})
	})

	t.Run("JobInsertFastMany", func(t *testing.T) {
		t.Parallel()

		t.Run("AllArgs", func(t *testing.T) {
			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := 0; i < len(insertParams); i++ {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					EncodedArgs: []byte(`{"encoded": "args"}`),
					Kind:        "test_kind",
					MaxAttempts: rivercommon.MaxAttemptsDefault,
					Metadata:    []byte(`{"meta": "data"}`),
					Priority:    rivercommon.PriorityDefault,
					Queue:       rivercommon.QueueDefault,
					ScheduledAt: ptrutil.Ptr(now.Add(time.Duration(i) * time.Minute)),
					State:       rivertype.JobStateAvailable,
					Tags:        []string{"tag"},
				}
			}

			resultRows, err := exec.JobInsertFastMany(ctx, insertParams)
			require.NoError(t, err)
			require.Len(t, resultRows, len(insertParams))

			for i, result := range resultRows {
				require.False(t, result.UniqueSkippedAsDuplicate)
				job := result.Job
				require.Equal(t, 0, job.Attempt)
				require.Nil(t, job.AttemptedAt)
				require.Empty(t, job.AttemptedBy)
				require.WithinDuration(t, now, job.CreatedAt, 2*time.Second)
				require.Equal(t, []byte(`{"encoded": "args"}`), job.EncodedArgs)
				require.Empty(t, job.Errors)
				require.Nil(t, job.FinalizedAt)
				require.Equal(t, "test_kind", job.Kind)
				require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)
				require.Equal(t, []byte(`{"meta": "data"}`), job.Metadata)
				require.Equal(t, rivercommon.PriorityDefault, job.Priority)
				require.Equal(t, rivercommon.QueueDefault, job.Queue)
				requireEqualTime(t, now.Add(time.Duration(i)*time.Minute), job.ScheduledAt)
				require.Equal(t, rivertype.JobStateAvailable, job.State)
				require.Equal(t, []string{"tag"}, job.Tags)
			}
		})

		t.Run("MissingScheduledAtDefaultsToNow", func(t *testing.T) {
			exec, _ := setup(ctx, t)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := 0; i < len(insertParams); i++ {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					EncodedArgs: []byte(`{"encoded": "args"}`),
					Kind:        "test_kind",
					MaxAttempts: rivercommon.MaxAttemptsDefault,
					Metadata:    []byte(`{"meta": "data"}`),
					Priority:    rivercommon.PriorityDefault,
					Queue:       rivercommon.QueueDefault,
					ScheduledAt: nil, // explicit nil
					State:       rivertype.JobStateAvailable,
					Tags:        []string{"tag"},
				}
			}

			results, err := exec.JobInsertFastMany(ctx, insertParams)
			require.NoError(t, err)
			require.Len(t, results, len(insertParams))

			jobsAfter, err := exec.JobGetByKindMany(ctx, []string{"test_kind"})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for _, job := range jobsAfter {
				require.WithinDuration(t, time.Now().UTC(), job.ScheduledAt, 2*time.Second)
			}
		})
	})

	t.Run("JobInsertFastManyNoReturning", func(t *testing.T) {
		t.Parallel()

		t.Run("AllArgs", func(t *testing.T) {
			exec, _ := setup(ctx, t)

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

			count, err := exec.JobInsertFastManyNoReturning(ctx, insertParams)
			require.NoError(t, err)
			require.Len(t, insertParams, count)

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

		t.Run("MissingScheduledAtDefaultsToNow", func(t *testing.T) {
			exec, _ := setup(ctx, t)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := 0; i < len(insertParams); i++ {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					EncodedArgs: []byte(`{"encoded": "args"}`),
					Kind:        "test_kind",
					MaxAttempts: rivercommon.MaxAttemptsDefault,
					Metadata:    []byte(`{"meta": "data"}`),
					Priority:    rivercommon.PriorityDefault,
					Queue:       rivercommon.QueueDefault,
					ScheduledAt: nil, // explicit nil
					State:       rivertype.JobStateAvailable,
					Tags:        []string{"tag"},
				}
			}

			count, err := exec.JobInsertFastManyNoReturning(ctx, insertParams)
			require.NoError(t, err)
			require.Len(t, insertParams, count)

			jobsAfter, err := exec.JobGetByKindMany(ctx, []string{"test_kind"})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for _, job := range jobsAfter {
				require.WithinDuration(t, time.Now().UTC(), job.ScheduledAt, 2*time.Second)
			}
		})
	})

	t.Run("JobInsertFull", func(t *testing.T) {
		t.Parallel()

		t.Run("MinimalArgsWithDefaults", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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
				UniqueKey:   []byte("unique-key"),
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
			require.Equal(t, []byte("unique-key"), job.UniqueKey)
		})

		t.Run("JobFinalizedAtConstraint", func(t *testing.T) {
			t.Parallel()

			capitalizeJobState := func(state rivertype.JobState) string {
				return cases.Title(language.English, cases.NoLower).String(string(state))
			}

			for _, state := range []rivertype.JobState{
				rivertype.JobStateCancelled,
				rivertype.JobStateCompleted,
				rivertype.JobStateDiscarded,
			} {
				state := state // capture range variable

				t.Run(fmt.Sprintf("CannotSetState%sWithoutFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)
					// Create a job with the target state but without a finalized_at,
					// expect an error:
					params := testfactory.Job_Build(t, &testfactory.JobOpts{
						State: &state,
					})
					params.FinalizedAt = nil
					_, err := exec.JobInsertFull(ctx, params)
					require.ErrorContains(t, err, "violates check constraint \"finalized_or_finalized_at_null\"")
				})

				t.Run(fmt.Sprintf("CanSetState%sWithFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)

					// Create a job with the target state but with a finalized_at, expect
					// no error:
					_, err := exec.JobInsertFull(ctx, testfactory.Job_Build(t, &testfactory.JobOpts{
						FinalizedAt: ptrutil.Ptr(time.Now()),
						State:       &state,
					}))
					require.NoError(t, err)
				})
			}

			for _, state := range []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			} {
				state := state // capture range variable

				t.Run(fmt.Sprintf("CanSetState%sWithoutFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)

					// Create a job with the target state but without a finalized_at,
					// expect no error:
					_, err := exec.JobInsertFull(ctx, testfactory.Job_Build(t, &testfactory.JobOpts{
						State: &state,
					}))
					require.NoError(t, err)
				})

				t.Run(fmt.Sprintf("CannotSetState%sWithFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)

					// Create a job with the target state but with a finalized_at, expect
					// an error:
					_, err := exec.JobInsertFull(ctx, testfactory.Job_Build(t, &testfactory.JobOpts{
						FinalizedAt: ptrutil.Ptr(time.Now()),
						State:       &state,
					}))
					require.ErrorContains(t, err, "violates check constraint \"finalized_or_finalized_at_null\"")
				})
			}
		})
	})

	t.Run("JobList", func(t *testing.T) {
		t.Parallel()

		t.Run("ListsJobs", func(t *testing.T) {
			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Attempt:      ptrutil.Ptr(3),
				AttemptedAt:  &now,
				CreatedAt:    &now,
				EncodedArgs:  []byte(`{"encoded": "args"}`),
				Errors:       [][]byte{[]byte(`{"error": "message1"}`), []byte(`{"error": "message2"}`)},
				FinalizedAt:  &now,
				Metadata:     []byte(`{"meta": "data"}`),
				ScheduledAt:  &now,
				State:        ptrutil.Ptr(rivertype.JobStateCompleted),
				Tags:         []string{"tag"},
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			fetchedJobs, err := exec.JobList(
				ctx,
				fmt.Sprintf("SELECT %s FROM river_job WHERE id = @job_id_123", exec.JobListFields()),
				map[string]any{"job_id_123": job.ID},
			)
			require.NoError(t, err)
			require.Len(t, fetchedJobs, 1)

			fetchedJob := fetchedJobs[0]
			require.Equal(t, job.Attempt, fetchedJob.Attempt)
			require.Equal(t, job.AttemptedAt, fetchedJob.AttemptedAt)
			require.Equal(t, job.CreatedAt, fetchedJob.CreatedAt)
			require.Equal(t, job.EncodedArgs, fetchedJob.EncodedArgs)
			require.Equal(t, "message1", fetchedJob.Errors[0].Error)
			require.Equal(t, "message2", fetchedJob.Errors[1].Error)
			require.Equal(t, job.FinalizedAt, fetchedJob.FinalizedAt)
			require.Equal(t, job.Kind, fetchedJob.Kind)
			require.Equal(t, job.MaxAttempts, fetchedJob.MaxAttempts)
			require.Equal(t, job.Metadata, fetchedJob.Metadata)
			require.Equal(t, job.Priority, fetchedJob.Priority)
			require.Equal(t, job.Queue, fetchedJob.Queue)
			require.Equal(t, job.ScheduledAt, fetchedJob.ScheduledAt)
			require.Equal(t, job.State, fetchedJob.State)
			require.Equal(t, job.Tags, fetchedJob.Tags)
			require.Equal(t, []byte("unique-key"), fetchedJob.UniqueKey)
			require.Equal(t, rivertype.JobStates(), fetchedJob.UniqueStates)
		})

		t.Run("HandlesRequiredArgumentTypes", func(t *testing.T) {
			exec, _ := setup(ctx, t)

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind1")})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind2")})

			{
				fetchedJobs, err := exec.JobList(
					ctx,
					fmt.Sprintf("SELECT %s FROM river_job WHERE kind = @kind", exec.JobListFields()),
					map[string]any{"kind": job1.Kind},
				)
				require.NoError(t, err)
				require.Len(t, fetchedJobs, 1)
			}

			{
				fetchedJobs, err := exec.JobList(
					ctx,
					fmt.Sprintf("SELECT %s FROM river_job WHERE kind = any(@kind::text[])", exec.JobListFields()),
					map[string]any{"kind": []string{job1.Kind, job2.Kind}},
				)
				require.NoError(t, err)
				require.Len(t, fetchedJobs, 2)
			}
		})
	})

	t.Run("JobListFields", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		require.Equal(t, "id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states",
			exec.JobListFields())
	})

	t.Run("JobRescueMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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
			rivertype.JobStatePending,
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
		} {
			state := state

			t.Run(fmt.Sprintf("UpdatesA_%s_JobToBeScheduledImmediately", state), func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

			_, err := exec.JobRetry(ctx, 0)
			require.Error(t, err)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})
	})

	t.Run("JobSchedule", func(t *testing.T) {
		t.Parallel()

		t.Run("BasicScheduling", func(t *testing.T) {
			exec, _ := setup(ctx, t)

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
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

			// Right state, but after horizon.
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateRetryable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateScheduled)})

			// First two scheduled because of limit.
			result, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 2,
				Now: horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 2)

			// And then job3 scheduled.
			result, err = exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 2,
				Now: horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 1)

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

		t.Run("HandlesUniqueConflicts", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var (
				horizon       = time.Now()
				beforeHorizon = horizon.Add(-1 * time.Minute)
			)

			defaultUniqueStates := []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			}
			// The default unique state list, minus retryable to allow for these conflicts:
			nonRetryableUniqueStates := []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			}

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: dbunique.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-2"),
				UniqueStates: dbunique.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			// job3 has no conflict (it's the only one with this key), so it should be
			// scheduled.
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-3"),
				UniqueStates: dbunique.UniqueStatesToBitmask(defaultUniqueStates),
			})

			// This one is a conflict with job1 because it's already running and has
			// the same unique properties:
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: dbunique.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			// This one is *not* a conflict with job2 because it's completed, which
			// isn't in the unique states:
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateCompleted),
				UniqueKey:    []byte("unique-key-2"),
				UniqueStates: dbunique.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})

			result, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 100,
				Now: horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 3)

			updatedJob1, err := exec.JobGetByID(ctx, job1.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, updatedJob1.State)
			require.Equal(t, "scheduler_discarded", gjson.GetBytes(updatedJob1.Metadata, "unique_key_conflict").String())

			updatedJob2, err := exec.JobGetByID(ctx, job2.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob2.State)
			require.False(t, gjson.GetBytes(updatedJob2.Metadata, "unique_key_conflict").Exists())

			updatedJob3, err := exec.JobGetByID(ctx, job3.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob3.State)
			require.False(t, gjson.GetBytes(updatedJob3.Metadata, "unique_key_conflict").Exists())
		})

		t.Run("SchedulingTwoRetryableJobsThatWillConflictWithEachOther", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var (
				horizon       = time.Now()
				beforeHorizon = horizon.Add(-1 * time.Minute)
			)

			// The default unique state list, minus retryable to allow for these conflicts:
			nonRetryableUniqueStates := []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			}

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: dbunique.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: dbunique.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})

			result, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 100,
				Now: horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 2)

			updatedJob1, err := exec.JobGetByID(ctx, job1.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob1.State)
			require.False(t, gjson.GetBytes(updatedJob1.Metadata, "unique_key_conflict").Exists())

			updatedJob2, err := exec.JobGetByID(ctx, job2.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, updatedJob2.State)
			require.Equal(t, "scheduler_discarded", gjson.GetBytes(updatedJob2.Metadata, "unique_key_conflict").String())
		})
	})

	t.Run("JobSetCompleteIfRunningMany", func(t *testing.T) {
		t.Parallel()

		t.Run("CompletesRunningJobs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			finalizedAt1 := time.Now().UTC().Add(-1 * time.Minute)
			finalizedAt2 := time.Now().UTC().Add(-2 * time.Minute)
			finalizedAt3 := time.Now().UTC().Add(-3 * time.Minute)

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning), UniqueKey: []byte("unique-key")})

			// Running, but won't be completed.
			otherJob := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

			jobsAfter, err := exec.JobSetCompleteIfRunningMany(ctx, &riverdriver.JobSetCompleteIfRunningManyParams{
				ID:          []int64{job1.ID, job2.ID, job3.ID},
				FinalizedAt: []time.Time{finalizedAt1, finalizedAt2, finalizedAt3},
			})
			require.NoError(t, err)
			for _, jobAfter := range jobsAfter {
				require.Equal(t, rivertype.JobStateCompleted, jobAfter.State)
			}

			job1Updated, err := exec.JobGetByID(ctx, job1.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, job1Updated.State)
			require.WithinDuration(t, finalizedAt1, *job1Updated.FinalizedAt, time.Microsecond)

			job2Updated, err := exec.JobGetByID(ctx, job2.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, job2Updated.State)
			require.WithinDuration(t, finalizedAt2, *job2Updated.FinalizedAt, time.Microsecond)

			job3Updated, err := exec.JobGetByID(ctx, job3.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, job3Updated.State)
			require.WithinDuration(t, finalizedAt3, *job3Updated.FinalizedAt, time.Microsecond)
			require.Equal(t, "unique-key", string(job3Updated.UniqueKey))

			otherJobUpdated, err := exec.JobGetByID(ctx, otherJob.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, otherJobUpdated.State)
		})

		t.Run("DoesNotCompleteJobsInNonRunningStates", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRetryable)})
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled)})

			jobsAfter, err := exec.JobSetCompleteIfRunningMany(ctx, &riverdriver.JobSetCompleteIfRunningManyParams{
				ID:          []int64{job1.ID, job2.ID, job3.ID},
				FinalizedAt: []time.Time{now, now, now},
			})
			require.NoError(t, err)
			for _, jobAfter := range jobsAfter {
				require.NotEqual(t, rivertype.JobStateCompleted, jobAfter.State)
				require.Nil(t, jobAfter.FinalizedAt)
			}

			job1Updated, err := exec.JobGetByID(ctx, job1.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, job1Updated.State)

			job2Updated, err := exec.JobGetByID(ctx, job2.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, job2Updated.State)

			job3Updated, err := exec.JobGetByID(ctx, job3.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateScheduled, job3Updated.State)
		})

		t.Run("MixOfRunningAndNotRunningStates", func(t *testing.T) {
			exec, _ := setup(ctx, t)

			finalizedAt1 := time.Now().UTC().Add(-1 * time.Minute)
			finalizedAt2 := time.Now().UTC().Add(-2 * time.Minute) // ignored because job is not running
			finalizedAt3 := time.Now().UTC().Add(-3 * time.Minute) // ignored because job is not running
			finalizedAt4 := time.Now().UTC().Add(-3 * time.Minute)

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)}) // not running
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled)}) // not running
			job4 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

			_, err := exec.JobSetCompleteIfRunningMany(ctx, &riverdriver.JobSetCompleteIfRunningManyParams{
				ID:          []int64{job1.ID, job2.ID, job3.ID, job4.ID},
				FinalizedAt: []time.Time{finalizedAt1, finalizedAt2, finalizedAt3, finalizedAt4},
			})
			require.NoError(t, err)

			job1Updated, err := exec.JobGetByID(ctx, job1.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, job1Updated.State) // changed to completed
			require.WithinDuration(t, finalizedAt1, *job1Updated.FinalizedAt, time.Microsecond)

			job2Updated, err := exec.JobGetByID(ctx, job2.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, job2Updated.State) // still available
			require.Nil(t, job2Updated.FinalizedAt)

			job3Updated, err := exec.JobGetByID(ctx, job3.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateScheduled, job3Updated.State) // still scheduled
			require.Nil(t, job3Updated.FinalizedAt)

			job4Updated, err := exec.JobGetByID(ctx, job4.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, job4Updated.State) // changed to completed
			require.WithinDuration(t, finalizedAt4, *job4Updated.FinalizedAt, time.Microsecond)
		})
	})

	t.Run("JobSetStateIfRunning_JobSetStateCompleted", func(t *testing.T) {
		t.Parallel()

		t.Run("CompletesARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateCompleted(job.ID, now))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})

		t.Run("DoesNotCompleteARetryableJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey: []byte("unique-key"),
			})

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateCompleted(job.ID, now))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.Nil(t, jobAfter.FinalizedAt)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})
	})

	makeErrPayload := func(t *testing.T, now time.Time) []byte {
		t.Helper()

		errPayload, err := json.Marshal(rivertype.AttemptError{
			Attempt: 1, At: now, Error: "fake error", Trace: "foo.go:123\nbar.go:456",
		})
		require.NoError(t, err)
		return errPayload
	}

	t.Run("JobSetStateIfRunning_JobSetStateErrored", func(t *testing.T) {
		t.Parallel()

		t.Run("SetsARunningJobToRetryable", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now)))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.WithinDuration(t, now, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))

			// validate error payload:
			require.Len(t, jobAfter.Errors, 1)
			require.Equal(t, now, jobAfter.Errors[0].At)
			require.Equal(t, 1, jobAfter.Errors[0].Attempt)
			require.Equal(t, "fake error", jobAfter.Errors[0].Error)
			require.Equal(t, "foo.go:123\nbar.go:456", jobAfter.Errors[0].Trace)
		})

		t.Run("DoesNotTouchAlreadyRetryableJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

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

			exec, _ := setup(ctx, t)

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

	t.Run("JobSetStateIfRunning_JobSetStateCancelled", func(t *testing.T) {
		t.Parallel()

		t.Run("CancelsARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:        ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateCancelled(job.ID, now, makeErrPayload(t, now)))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCancelled, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})

		t.Run("CancelsARunningV2UniqueJobAndClearsUniqueKey", func(t *testing.T) { //nolint:dupl
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()
			// V2 unique jobs (with no UniqueStates) should not have UniqueKey cleared:
			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})
			// expclitly null out UniqueStates to simulate an old v2 job:
			_, err := exec.Exec(ctx, fmt.Sprintf("UPDATE river_job SET unique_states = NULL WHERE id = %d", job.ID))
			require.NoError(t, err)

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateCancelled(job.ID, now, makeErrPayload(t, now)))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)
			require.Nil(t, jobAfter.UniqueKey)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCancelled, jobUpdated.State)
			require.Nil(t, jobUpdated.UniqueKey)
		})
	})

	t.Run("JobSetStateIfRunning_JobSetStateDiscarded", func(t *testing.T) {
		t.Parallel()

		t.Run("DiscardsARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:        ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateDiscarded(job.ID, now, makeErrPayload(t, now)))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)
			require.Equal(t, "unique-key", string(jobAfter.UniqueKey))
			require.Equal(t, rivertype.JobStates(), jobAfter.UniqueStates)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, jobUpdated.State)
		})

		t.Run("DiscardsARunningV2UniqueJobAndClearsUniqueKey", func(t *testing.T) { //nolint:dupl
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()
			// V2 unique jobs (with no UniqueStates) should not have UniqueKey cleared:
			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})
			// expclitly null out UniqueStates to simulate an old v2 job:
			_, err := exec.Exec(ctx, fmt.Sprintf("UPDATE river_job SET unique_states = NULL WHERE id = %d", job.ID))
			require.NoError(t, err)

			jobAfter, err := exec.JobSetStateIfRunning(ctx, riverdriver.JobSetStateDiscarded(job.ID, now, makeErrPayload(t, now)))
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)
			require.Nil(t, jobAfter.UniqueKey)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, jobUpdated.State)
			require.Nil(t, jobUpdated.UniqueKey)
		})
	})

	setStateManyParams := func(params ...*riverdriver.JobSetStateIfRunningParams) *riverdriver.JobSetStateIfRunningManyParams {
		batchParams := &riverdriver.JobSetStateIfRunningManyParams{}
		// 	ID:          make([]int64, len(params)),
		// 	ErrData:     make([]byte, len(params)),
		// 	FinalizedAt: make([]*time.Time, len(params)),
		// 	MaxAttempts: []*int{maxAttempts},
		// 	ScheduledAt: []*time.Time{scheduledAt},
		// 	State:       []rivertype.JobState{params.State},
		// }
		for _, param := range params {
			var (
				errData     []byte
				finalizedAt *time.Time
				maxAttempts *int
				scheduledAt *time.Time
			)
			if param.ErrData != nil {
				errData = param.ErrData
			}
			if param.FinalizedAt != nil {
				finalizedAt = param.FinalizedAt
			}
			if param.MaxAttempts != nil {
				maxAttempts = param.MaxAttempts
			}
			if param.ScheduledAt != nil {
				scheduledAt = param.ScheduledAt
			}

			batchParams.ID = append(batchParams.ID, param.ID)
			batchParams.ErrData = append(batchParams.ErrData, errData)
			batchParams.FinalizedAt = append(batchParams.FinalizedAt, finalizedAt)
			batchParams.MaxAttempts = append(batchParams.MaxAttempts, maxAttempts)
			batchParams.ScheduledAt = append(batchParams.ScheduledAt, scheduledAt)
			batchParams.State = append(batchParams.State, param.State)
		}

		return batchParams
	}

	t.Run("JobSetStateIfRunningMany_JobSetStateCompleted", func(t *testing.T) {
		t.Parallel()

		t.Run("CompletesARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCompleted(job.ID, now)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateCompleted, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})

		t.Run("DoesNotCompleteARetryableJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCompleted(job.ID, now)))
			jobAfter := jobsAfter[0]
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.Nil(t, jobAfter.FinalizedAt)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})
	})

	t.Run("JobSetStateIfRunningMany_JobSetStateErrored", func(t *testing.T) {
		t.Parallel()

		t.Run("SetsARunningJobToRetryable", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now))))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.WithinDuration(t, now, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))

			// validate error payload:
			require.Len(t, jobAfter.Errors, 1)
			require.Equal(t, now, jobAfter.Errors[0].At)
			require.Equal(t, 1, jobAfter.Errors[0].Attempt)
			require.Equal(t, "fake error", jobAfter.Errors[0].Error)
			require.Equal(t, "foo.go:123\nbar.go:456", jobAfter.Errors[0].Trace)
		})

		t.Run("DoesNotTouchAlreadyRetryableJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:       ptrutil.Ptr(rivertype.JobStateRetryable),
				ScheduledAt: ptrutil.Ptr(now.Add(10 * time.Second)),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now))))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
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

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Metadata:    []byte(fmt.Sprintf(`{"cancel_attempted_at":"%s"}`, time.Now().UTC().Format(time.RFC3339))),
				State:       ptrutil.Ptr(rivertype.JobStateRunning),
				ScheduledAt: ptrutil.Ptr(now.Add(-10 * time.Second)),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now))))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
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

	t.Run("JobSetStateIfRunningMany_JobSetStateCancelled", func(t *testing.T) {
		t.Parallel()

		t.Run("CancelsARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:        ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCancelled(job.ID, now, makeErrPayload(t, now))))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCancelled, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})
	})

	t.Run("JobSetStateIfRunningMany_JobSetStateDiscarded", func(t *testing.T) {
		t.Parallel()

		t.Run("DiscardsARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:        ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateDiscarded(job.ID, now, makeErrPayload(t, now))))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateDiscarded, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)
			require.Equal(t, "unique-key", string(jobAfter.UniqueKey))
			require.Equal(t, rivertype.JobStates(), jobAfter.UniqueStates)

			jobUpdated, err := exec.JobGetByID(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, jobUpdated.State)
		})
	})

	t.Run("JobSetStateIfRunningMany_MultipleJobsAtOnce", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		now := time.Now().UTC()
		future := now.Add(10 * time.Second)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(
			riverdriver.JobSetStateCompleted(job1.ID, now),
			riverdriver.JobSetStateErrorRetryable(job2.ID, future, makeErrPayload(t, now)),
			riverdriver.JobSetStateCancelled(job3.ID, now, makeErrPayload(t, now)),
		))
		require.NoError(t, err)
		completedJob := jobsAfter[0]
		require.Equal(t, rivertype.JobStateCompleted, completedJob.State)
		require.WithinDuration(t, now, *completedJob.FinalizedAt, time.Microsecond)

		retryableJob := jobsAfter[1]
		require.Equal(t, rivertype.JobStateRetryable, retryableJob.State)
		require.WithinDuration(t, future, retryableJob.ScheduledAt, time.Microsecond)
		// validate error payload:
		require.Len(t, retryableJob.Errors, 1)
		require.Equal(t, now, retryableJob.Errors[0].At)
		require.Equal(t, 1, retryableJob.Errors[0].Attempt)
		require.Equal(t, "fake error", retryableJob.Errors[0].Error)
		require.Equal(t, "foo.go:123\nbar.go:456", retryableJob.Errors[0].Trace)

		cancelledJob := jobsAfter[2]
		require.Equal(t, rivertype.JobStateCancelled, cancelledJob.State)
		require.WithinDuration(t, now, *cancelledJob.FinalizedAt, time.Microsecond)
	})

	t.Run("JobUpdate", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

		now := time.Now().UTC()

		updatedJob, err := exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
			ID:                  job.ID,
			AttemptDoUpdate:     true,
			Attempt:             7,
			AttemptedAtDoUpdate: true,
			AttemptedAt:         &now,
			ErrorsDoUpdate:      true,
			Errors:              [][]byte{[]byte(`{"error":"message"}`)},
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

	const leaderTTL = 10 * time.Second

	t.Run("LeaderDeleteExpired", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		now := time.Now().UTC()

		{
			numDeleted, err := exec.LeaderDeleteExpired(ctx)
			require.NoError(t, err)
			require.Zero(t, numDeleted)
		}

		_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
			ElectedAt: ptrutil.Ptr(now.Add(-2 * time.Hour)),
			ExpiresAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
			LeaderID:  ptrutil.Ptr(clientID),
		})

		{
			numDeleted, err := exec.LeaderDeleteExpired(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, numDeleted)
		}
	})

	t.Run("LeaderAttemptElect", func(t *testing.T) {
		t.Parallel()

		t.Run("ElectsLeader", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			elected, err := exec.LeaderAttemptElect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: clientID,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.True(t, elected) // won election

			leader, err := exec.LeaderGetElectedLeader(ctx)
			require.NoError(t, err)
			require.WithinDuration(t, time.Now(), leader.ElectedAt, 100*time.Millisecond)
			require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, 100*time.Millisecond)
		})

		t.Run("CannotElectTwiceInARow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(clientID),
			})

			elected, err := exec.LeaderAttemptElect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: "different-client-id",
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.False(t, elected) // lost election

			// The time should not have changed because we specified that we were not
			// already elected, and the elect query is a no-op if there's already a
			// updatedLeader:
			updatedLeader, err := exec.LeaderGetElectedLeader(ctx)
			require.NoError(t, err)
			require.Equal(t, leader.ExpiresAt, updatedLeader.ExpiresAt)
		})
	})

	t.Run("LeaderAttemptReelect", func(t *testing.T) {
		t.Parallel()

		t.Run("ElectsLeader", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			elected, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: clientID,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.True(t, elected) // won election

			leader, err := exec.LeaderGetElectedLeader(ctx)
			require.NoError(t, err)
			require.WithinDuration(t, time.Now(), leader.ElectedAt, 100*time.Millisecond)
			require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, 100*time.Millisecond)
		})

		t.Run("ReelectsSameLeader", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(clientID),
			})

			// Re-elect the same leader. Use a larger TTL to see if time is updated,
			// because we are in a test transaction and the time is frozen at the start of
			// the transaction.
			elected, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: clientID,
				TTL:      30 * time.Second,
			})
			require.NoError(t, err)
			require.True(t, elected) // won re-election

			// expires_at should be incremented because this is the same leader that won
			// previously and we specified that we're already elected:
			updatedLeader, err := exec.LeaderGetElectedLeader(ctx)
			require.NoError(t, err)
			require.Greater(t, updatedLeader.ExpiresAt, leader.ExpiresAt)
		})
	})

	t.Run("LeaderInsert", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		leader, err := exec.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
			LeaderID: clientID,
			TTL:      leaderTTL,
		})
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), leader.ElectedAt, 500*time.Millisecond)
		require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, 500*time.Millisecond)
		require.Equal(t, clientID, leader.LeaderID)
	})

	t.Run("LeaderGetElectedLeader", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
			LeaderID: ptrutil.Ptr(clientID),
		})

		leader, err := exec.LeaderGetElectedLeader(ctx)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), leader.ElectedAt, 500*time.Millisecond)
		require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, 500*time.Millisecond)
		require.Equal(t, clientID, leader.LeaderID)
	})

	t.Run("LeaderResign", func(t *testing.T) {
		t.Parallel()

		t.Run("Success", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			{
				resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
					LeaderID:        clientID,
					LeadershipTopic: string(notifier.NotificationTopicLeadership),
				})
				require.NoError(t, err)
				require.False(t, resigned)
			}

			_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(clientID),
			})

			{
				resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
					LeaderID:        clientID,
					LeadershipTopic: string(notifier.NotificationTopicLeadership),
				})
				require.NoError(t, err)
				require.True(t, resigned)
			}
		})

		t.Run("DoesNotResignWithoutLeadership", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr("other-client-id"),
			})

			resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
				LeaderID:        clientID,
				LeadershipTopic: string(notifier.NotificationTopicLeadership),
			})
			require.NoError(t, err)
			require.False(t, resigned)
		})
	})

	// Truncates the migration table so we only have to work with test
	// migration data.
	truncateMigrations := func(ctx context.Context, t *testing.T, exec riverdriver.Executor) {
		t.Helper()

		_, err := exec.Exec(ctx, "TRUNCATE TABLE river_migration")
		require.NoError(t, err)
	}

	t.Run("MigrationDeleteAssumingMainMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		truncateMigrations(ctx, t, exec)

		migration1 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})
		migration2 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})

		// This query is designed to work before the `line` column was added to
		// the `river_migration` table. These tests will be operating on a fully
		// migrated database, so drop the column in this transaction to make
		// sure we are really checking that this operation works as expected.
		_, err := exec.Exec(ctx, "ALTER TABLE river_migration DROP COLUMN line")
		require.NoError(t, err)

		migrations, err := exec.MigrationDeleteAssumingMainMany(ctx, []int{
			migration1.Version,
			migration2.Version,
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		slices.SortFunc(migrations, func(a, b *riverdriver.Migration) int { return a.Version - b.Version })
		require.Equal(t, riverdriver.MigrationLineMain, migrations[0].Line)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, riverdriver.MigrationLineMain, migrations[1].Line)
		require.Equal(t, migration2.Version, migrations[1].Version)
	})

	t.Run("MigrationDeleteByLineAndVersionMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		truncateMigrations(ctx, t, exec)

		// not touched
		_ = testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})

		migration1 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: ptrutil.Ptr("alternate")})
		migration2 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: ptrutil.Ptr("alternate")})

		migrations, err := exec.MigrationDeleteByLineAndVersionMany(ctx, "alternate", []int{
			migration1.Version,
			migration2.Version,
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		slices.SortFunc(migrations, func(a, b *riverdriver.Migration) int { return a.Version - b.Version })
		require.Equal(t, "alternate", migrations[0].Line)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, "alternate", migrations[1].Line)
		require.Equal(t, migration2.Version, migrations[1].Version)
	})

	t.Run("MigrationGetAllAssumingMain", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		truncateMigrations(ctx, t, exec)

		migration1 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})
		migration2 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})

		// This query is designed to work before the `line` column was added to
		// the `river_migration` table. These tests will be operating on a fully
		// migrated database, so drop the column in this transaction to make
		// sure we are really checking that this operation works as expected.
		_, err := exec.Exec(ctx, "ALTER TABLE river_migration DROP COLUMN line")
		require.NoError(t, err)

		migrations, err := exec.MigrationGetAllAssumingMain(ctx)
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, migration2.Version, migrations[1].Version)

		// Check the full properties of one of the migrations.
		migration1Fetched := migrations[0]
		requireEqualTime(t, migration1.CreatedAt, migration1Fetched.CreatedAt)
		require.Equal(t, riverdriver.MigrationLineMain, migration1Fetched.Line)
		require.Equal(t, migration1.Version, migration1Fetched.Version)
	})

	t.Run("MigrationGetByLine", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		truncateMigrations(ctx, t, exec)

		// not returned
		_ = testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})

		migration1 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: ptrutil.Ptr("alternate")})
		migration2 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: ptrutil.Ptr("alternate")})

		migrations, err := exec.MigrationGetByLine(ctx, "alternate")
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, migration2.Version, migrations[1].Version)

		// Check the full properties of one of the migrations.
		migration1Fetched := migrations[0]
		requireEqualTime(t, migration1.CreatedAt, migration1Fetched.CreatedAt)
		require.Equal(t, "alternate", migration1Fetched.Line)
		require.Equal(t, migration1.Version, migration1Fetched.Version)
	})

	t.Run("MigrationInsertMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		truncateMigrations(ctx, t, exec)

		migrations, err := exec.MigrationInsertMany(ctx, "alternate", []int{1, 2})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, "alternate", migrations[0].Line)
		require.Equal(t, 1, migrations[0].Version)
		require.Equal(t, "alternate", migrations[1].Line)
		require.Equal(t, 2, migrations[1].Version)
	})

	t.Run("MigrationInsertManyAssumingMain", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		truncateMigrations(ctx, t, exec)

		// This query is designed to work before the `line` column was added to
		// the `river_migration` table. These tests will be operating on a fully
		// migrated database, so drop the column in this transaction to make
		// sure we are really checking that this operation works as expected.
		_, err := exec.Exec(ctx, "ALTER TABLE river_migration DROP COLUMN line")
		require.NoError(t, err)

		migrations, err := exec.MigrationInsertManyAssumingMain(ctx, []int{1, 2})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, riverdriver.MigrationLineMain, migrations[0].Line)
		require.Equal(t, 1, migrations[0].Version)
		require.Equal(t, riverdriver.MigrationLineMain, migrations[1].Line)
		require.Equal(t, 2, migrations[1].Version)
	})

	t.Run("TableExists", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		exists, err := exec.TableExists(ctx, "river_job")
		require.NoError(t, err)
		require.True(t, exists)

		exists, err = exec.TableExists(ctx, "does_not_exist")
		require.NoError(t, err)
		require.False(t, exists)

		// Will be rolled back by the test transaction.
		_, err = exec.Exec(ctx, "CREATE SCHEMA another_schema_123")
		require.NoError(t, err)

		_, err = exec.Exec(ctx, "SET search_path = another_schema_123")
		require.NoError(t, err)

		exists, err = exec.TableExists(ctx, "river_job")
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("PGAdvisoryXactLock", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		// Acquire the advisory lock.
		_, err := exec.PGAdvisoryXactLock(ctx, 123456)
		require.NoError(t, err)

		// Open a new transaction and try to acquire the same lock, which should
		// block because the lock can't be acquired. Verify some amount of wait,
		// cancel the lock acquisition attempt, then verify return.
		{
			otherExec := executorWithTx(ctx, t)

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

	t.Run("QueueCreateOrSetUpdatedAt", func(t *testing.T) {
		t.Run("InsertsANewQueueWithDefaultUpdatedAt", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			metadata := []byte(`{"foo": "bar"}`)
			queue, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Metadata: metadata,
				Name:     "new-queue",
			})
			require.NoError(t, err)
			require.WithinDuration(t, time.Now(), queue.CreatedAt, 500*time.Millisecond)
			require.Equal(t, metadata, queue.Metadata)
			require.Equal(t, "new-queue", queue.Name)
			require.Nil(t, queue.PausedAt)
			require.WithinDuration(t, time.Now(), queue.UpdatedAt, 500*time.Millisecond)
		})

		t.Run("InsertsANewQueueWithCustomPausedAt", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().Add(-5 * time.Minute)
			queue, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Name:     "new-queue",
				PausedAt: ptrutil.Ptr(now),
			})
			require.NoError(t, err)
			require.Equal(t, "new-queue", queue.Name)
			require.WithinDuration(t, now, *queue.PausedAt, time.Millisecond)
		})

		t.Run("UpdatesTheUpdatedAtOfExistingQueue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			metadata := []byte(`{"foo": "bar"}`)
			tBefore := time.Now().UTC()
			queueBefore, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Metadata:  metadata,
				Name:      "updateable-queue",
				UpdatedAt: &tBefore,
			})
			require.NoError(t, err)
			require.WithinDuration(t, tBefore, queueBefore.UpdatedAt, time.Millisecond)

			tAfter := tBefore.Add(2 * time.Second)
			queueAfter, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Metadata:  []byte(`{"other": "metadata"}`),
				Name:      "updateable-queue",
				UpdatedAt: &tAfter,
			})
			require.NoError(t, err)

			// unchanged:
			require.Equal(t, queueBefore.CreatedAt, queueAfter.CreatedAt)
			require.Equal(t, metadata, queueAfter.Metadata)
			require.Equal(t, "updateable-queue", queueAfter.Name)
			require.Nil(t, queueAfter.PausedAt)

			// Timestamp is bumped:
			require.WithinDuration(t, tAfter, queueAfter.UpdatedAt, time.Millisecond)
		})

		t.Run("QueueDeleteExpired", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now()
			_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now)})
			queue2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-25 * time.Hour))})
			queue3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-26 * time.Hour))})
			queue4 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-48 * time.Hour))})
			_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-23 * time.Hour))})

			horizon := now.Add(-24 * time.Hour)
			deletedQueueNames, err := exec.QueueDeleteExpired(ctx, &riverdriver.QueueDeleteExpiredParams{Max: 2, UpdatedAtHorizon: horizon})
			require.NoError(t, err)

			// queue2 and queue3 should be deleted, with queue4 being skipped due to max of 2:
			require.Equal(t, []string{queue2.Name, queue3.Name}, deletedQueueNames)

			// Try again, make sure queue4 gets deleted this time:
			deletedQueueNames, err = exec.QueueDeleteExpired(ctx, &riverdriver.QueueDeleteExpiredParams{Max: 2, UpdatedAtHorizon: horizon})
			require.NoError(t, err)

			require.Equal(t, []string{queue4.Name}, deletedQueueNames)
		})

		t.Run("QueueGet", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Metadata: []byte(`{"foo": "bar"}`)})

			queueFetched, err := exec.QueueGet(ctx, queue.Name)
			require.NoError(t, err)

			require.WithinDuration(t, queue.CreatedAt, queueFetched.CreatedAt, time.Millisecond)
			require.Equal(t, queue.Metadata, queueFetched.Metadata)
			require.Equal(t, queue.Name, queueFetched.Name)
			require.Nil(t, queueFetched.PausedAt)
			require.WithinDuration(t, queue.UpdatedAt, queueFetched.UpdatedAt, time.Millisecond)

			queueFetched, err = exec.QueueGet(ctx, "nonexistent-queue")
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, queueFetched)
		})

		t.Run("QueueList", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			requireQueuesEqual := func(t *testing.T, target, actual *rivertype.Queue) {
				t.Helper()
				require.WithinDuration(t, target.CreatedAt, actual.CreatedAt, time.Millisecond)
				require.Equal(t, target.Metadata, actual.Metadata)
				require.Equal(t, target.Name, actual.Name)
				if target.PausedAt == nil {
					require.Nil(t, actual.PausedAt)
				} else {
					require.NotNil(t, actual.PausedAt)
					require.WithinDuration(t, *target.PausedAt, *actual.PausedAt, time.Millisecond)
				}
			}

			queues, err := exec.QueueList(ctx, 10)
			require.NoError(t, err)
			require.Empty(t, queues)

			// Make queue1, already paused:
			queue1 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Metadata: []byte(`{"foo": "bar"}`), PausedAt: ptrutil.Ptr(time.Now())})
			require.NoError(t, err)

			queue2 := testfactory.Queue(ctx, t, exec, nil)
			queue3 := testfactory.Queue(ctx, t, exec, nil)

			queues, err = exec.QueueList(ctx, 2)
			require.NoError(t, err)

			require.Len(t, queues, 2)
			requireQueuesEqual(t, queue1, queues[0])
			requireQueuesEqual(t, queue2, queues[1])

			queues, err = exec.QueueList(ctx, 3)
			require.NoError(t, err)

			require.Len(t, queues, 3)
			requireQueuesEqual(t, queue3, queues[2])
		})

		t.Run("QueuePause", func(t *testing.T) {
			t.Parallel()

			t.Run("ExistingPausedQueue", func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{
					PausedAt: ptrutil.Ptr(time.Now()),
				})

				require.NoError(t, exec.QueuePause(ctx, queue.Name))

				queueFetched, err := exec.QueueGet(ctx, queue.Name)
				require.NoError(t, err)
				require.NotNil(t, queueFetched.PausedAt)
				requireEqualTime(t, *queue.PausedAt, *queueFetched.PausedAt) // paused_at stays unchanged
				requireEqualTime(t, queue.UpdatedAt, queueFetched.UpdatedAt) // updated_at stays unchanged
			})

			t.Run("ExistingUnpausedQueue", func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				queue := testfactory.Queue(ctx, t, exec, nil)
				require.Nil(t, queue.PausedAt)

				require.NoError(t, exec.QueuePause(ctx, queue.Name))

				queueFetched, err := exec.QueueGet(ctx, queue.Name)
				require.NoError(t, err)
				require.NotNil(t, queueFetched.PausedAt)
				require.WithinDuration(t, time.Now(), *(queueFetched.PausedAt), 500*time.Millisecond)
			})

			t.Run("NonExistentQueue", func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				err := exec.QueuePause(ctx, "queue1")
				require.ErrorIs(t, err, rivertype.ErrNotFound)
			})

			t.Run("AllQueuesExistingQueues", func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				queue1 := testfactory.Queue(ctx, t, exec, nil)
				require.Nil(t, queue1.PausedAt)
				queue2 := testfactory.Queue(ctx, t, exec, nil)
				require.Nil(t, queue2.PausedAt)

				require.NoError(t, exec.QueuePause(ctx, rivercommon.AllQueuesString))

				now := time.Now()

				queue1Fetched, err := exec.QueueGet(ctx, queue1.Name)
				require.NoError(t, err)
				require.NotNil(t, queue1Fetched.PausedAt)
				require.WithinDuration(t, now, *(queue1Fetched.PausedAt), 500*time.Millisecond)

				queue2Fetched, err := exec.QueueGet(ctx, queue2.Name)
				require.NoError(t, err)
				require.NotNil(t, queue2Fetched.PausedAt)
				require.WithinDuration(t, now, *(queue2Fetched.PausedAt), 500*time.Millisecond)
			})

			t.Run("AllQueuesNoQueues", func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				require.NoError(t, exec.QueuePause(ctx, rivercommon.AllQueuesString))
			})
		})

		t.Run("QueueResume", func(t *testing.T) {
			t.Parallel()

			t.Run("ExistingPausedQueue", func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{
					PausedAt: ptrutil.Ptr(time.Now()),
				})

				require.NoError(t, exec.QueueResume(ctx, queue.Name))

				queueFetched, err := exec.QueueGet(ctx, queue.Name)
				require.NoError(t, err)
				require.Nil(t, queueFetched.PausedAt)
			})

			t.Run("ExistingUnpausedQueue", func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				queue := testfactory.Queue(ctx, t, exec, nil)

				require.NoError(t, exec.QueueResume(ctx, queue.Name))

				queueFetched, err := exec.QueueGet(ctx, queue.Name)
				require.NoError(t, err)
				require.Nil(t, queueFetched.PausedAt)
				requireEqualTime(t, queue.UpdatedAt, queueFetched.UpdatedAt) // updated_at stays unchanged
			})

			t.Run("NonExistentQueue", func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				err := exec.QueueResume(ctx, "queue1")
				require.ErrorIs(t, err, rivertype.ErrNotFound)
			})

			t.Run("AllQueuesExistingQueues", func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				queue1 := testfactory.Queue(ctx, t, exec, nil)
				require.Nil(t, queue1.PausedAt)
				queue2 := testfactory.Queue(ctx, t, exec, nil)
				require.Nil(t, queue2.PausedAt)

				require.NoError(t, exec.QueuePause(ctx, rivercommon.AllQueuesString))
				require.NoError(t, exec.QueueResume(ctx, rivercommon.AllQueuesString))

				queue1Fetched, err := exec.QueueGet(ctx, queue1.Name)
				require.NoError(t, err)
				require.Nil(t, queue1Fetched.PausedAt)

				queue2Fetched, err := exec.QueueGet(ctx, queue2.Name)
				require.NoError(t, err)
				require.Nil(t, queue2Fetched.PausedAt)
			})

			t.Run("AllQueuesNoQueues", func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				require.NoError(t, exec.QueueResume(ctx, rivercommon.AllQueuesString))
			})
		})
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

	return listener, &testListenerBundle[TTx]{
		driver: driver,
		exec:   driver.GetExecutor(),
	}
}

func exerciseListener[TTx any](ctx context.Context, t *testing.T, driverWithPool func(ctx context.Context, t *testing.T) riverdriver.Driver[TTx]) {
	t.Helper()

	connectListener := func(ctx context.Context, t *testing.T, listener riverdriver.Listener) {
		t.Helper()

		require.NoError(t, listener.Connect(ctx))
		t.Cleanup(func() { require.NoError(t, listener.Close(ctx)) })
	}

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

	t.Run("Close_NoOpIfNotConnected", func(t *testing.T) {
		t.Parallel()

		listener, _ := setupListener(ctx, t, driverWithPool)
		require.NoError(t, listener.Close(ctx))
	})

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		listener, bundle := setupListener(ctx, t, driverWithPool)

		connectListener(ctx, t, listener)

		require.NoError(t, listener.Listen(ctx, "topic1"))
		require.NoError(t, listener.Listen(ctx, "topic2"))

		require.NoError(t, listener.Ping(ctx)) // still alive

		{
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic1", Payload: []string{"payload1_1"}}))
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic2", Payload: []string{"payload2_1"}}))

			notification := waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1_1"}, notification)
			notification = waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic2", Payload: "payload2_1"}, notification)
		}

		require.NoError(t, listener.Unlisten(ctx, "topic2"))

		{
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic1", Payload: []string{"payload1_2"}}))
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic2", Payload: []string{"payload2_2"}}))

			notification := waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1_2"}, notification)

			requireNoNotification(ctx, t, listener)
		}

		require.NoError(t, listener.Unlisten(ctx, "topic1"))

		require.NoError(t, listener.Close(ctx))
	})

	t.Run("TransactionGated", func(t *testing.T) {
		t.Parallel()

		listener, bundle := setupListener(ctx, t, driverWithPool)

		connectListener(ctx, t, listener)

		require.NoError(t, listener.Listen(ctx, "topic1"))

		tx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)

		require.NoError(t, tx.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic1", Payload: []string{"payload1"}}))

		// No notification because the transaction hasn't committed yet.
		requireNoNotification(ctx, t, listener)

		require.NoError(t, tx.Commit(ctx))

		// Notification received now that transaction has committed.
		notification := waitForNotification(ctx, t, listener)
		require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1"}, notification)
	})

	t.Run("MultipleReuse", func(t *testing.T) {
		t.Parallel()

		listener, _ := setupListener(ctx, t, driverWithPool)

		connectListener(ctx, t, listener)

		require.NoError(t, listener.Listen(ctx, "topic1"))
		require.NoError(t, listener.Unlisten(ctx, "topic1"))

		require.NoError(t, listener.Close(ctx))
		require.NoError(t, listener.Connect(ctx))

		require.NoError(t, listener.Listen(ctx, "topic1"))
		require.NoError(t, listener.Unlisten(ctx, "topic1"))
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
