package riverdrivertest

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

func exerciseJobDelete[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	type testBundle struct {
		driver riverdriver.Driver[TTx]
	}

	setup := func(ctx context.Context, t *testing.T) (riverdriver.Executor, *testBundle) {
		t.Helper()

		exec, driver := executorWithTx(ctx, t)

		return exec, &testBundle{
			driver: driver,
		}
	}

	t.Run("JobDelete", func(t *testing.T) {
		t.Parallel()

		t.Run("DoesNotDeleteARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State: new(rivertype.JobStateRunning),
			})

			jobAfter, err := exec.JobDelete(ctx, &riverdriver.JobDeleteParams{
				ID: job.ID,
			})
			require.ErrorIs(t, err, rivertype.ErrJobRunning)
			require.Nil(t, jobAfter)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
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
					ScheduledAt: new(now.Add(1 * time.Hour)),
					State:       &state,
				})

				jobAfter, err := exec.JobDelete(ctx, &riverdriver.JobDeleteParams{
					ID: job.ID,
				})
				require.NoError(t, err)
				require.NotNil(t, jobAfter)
				require.Equal(t, job.ID, jobAfter.ID)
				require.Equal(t, state, jobAfter.State)

				_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
				require.ErrorIs(t, err, rivertype.ErrNotFound)
			})
		}

		t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			jobAfter, err := exec.JobDelete(ctx, &riverdriver.JobDeleteParams{
				ID: 1234567890,
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, jobAfter)
		})

		t.Run("AlternateSchema", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			_, err := exec.JobDelete(ctx, &riverdriver.JobDeleteParams{
				ID:     job.ID,
				Schema: "custom_schema",
			})
			requireMissingRelation(t, err, "custom_schema", "river_job")
		})
	})

	t.Run("JobDeleteBefore", func(t *testing.T) {
		t.Parallel()

		var (
			horizon       = time.Now()
			beforeHorizon = horizon.Add(-1 * time.Minute)
			afterHorizon  = horizon.Add(1 * time.Minute)
		)

		t.Run("RespectsDisabledStates", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			cancelledJob := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateCancelled)})
			completedJob := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateCompleted)})
			discardedJob := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateDiscarded)})

			numDeleted, err := exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CompletedDoDelete:           true,
				CompletedFinalizedAtHorizon: horizon,
				Max:                         1_000,
			})
			require.NoError(t, err)
			require.Equal(t, 1, numDeleted)

			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: cancelledJob.ID})
			require.NoError(t, err)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: completedJob.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: discardedJob.ID})
			require.NoError(t, err)
		})

		t.Run("Success", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			deletedJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateCancelled)})
			deletedJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateCompleted)})
			deletedJob3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateDiscarded)})

			// Not deleted because not appropriate state.
			notDeletedJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: new(rivertype.JobStateAvailable)})
			notDeletedJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: new(rivertype.JobStateRunning)})

			// Not deleted because after the delete horizon.
			notDeletedJob3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &afterHorizon, State: new(rivertype.JobStateCancelled)})

			// Max two deleted on the first pass.
			numDeleted, err := exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledDoDelete:           true,
				CancelledFinalizedAtHorizon: horizon,
				CompletedDoDelete:           true,
				CompletedFinalizedAtHorizon: horizon,
				DiscardedDoDelete:           true,
				DiscardedFinalizedAtHorizon: horizon,
				Max:                         2,
			})
			require.NoError(t, err)
			require.Equal(t, 2, numDeleted)

			// And one more pass gets the last one.
			numDeleted, err = exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledDoDelete:           true,
				CancelledFinalizedAtHorizon: horizon,
				CompletedDoDelete:           true,
				CompletedFinalizedAtHorizon: horizon,
				DiscardedDoDelete:           true,
				DiscardedFinalizedAtHorizon: horizon,
				Max:                         2,
			})
			require.NoError(t, err)
			require.Equal(t, 1, numDeleted)

			// All deleted
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: deletedJob1.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: deletedJob2.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: deletedJob3.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)

			// Not deleted
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: notDeletedJob1.ID})
			require.NoError(t, err)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: notDeletedJob2.ID})
			require.NoError(t, err)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: notDeletedJob3.ID})
			require.NoError(t, err)
		})

		t.Run("DoDeleteFalse", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var (
				cancelledJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateCancelled)})
				completedJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateCompleted)})
				discardedJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateDiscarded)})
			)

			// Discarded jobs are kept even though they're older than their
			// horizon because their deletion is disabled.
			numDeleted, err := exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledDoDelete:           true,
				CancelledFinalizedAtHorizon: horizon,
				CompletedDoDelete:           true,
				CompletedFinalizedAtHorizon: horizon,
				DiscardedDoDelete:           false,
				DiscardedFinalizedAtHorizon: horizon,
				Max:                         1_000,
			})
			require.NoError(t, err)
			require.Equal(t, 2, numDeleted)

			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: cancelledJob.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: completedJob.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: discardedJob.ID})
			require.NoError(t, err)
		})

		t.Run("QueuesExcluded", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var (
				excludedQueue1 = "excluded1"
				excludedQueue2 = "excluded2"

				// Insert excluded jobs first to verify that they don't consume the
				// limited candidate batch and starve later eligible jobs.
				notDeletedJob1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, Queue: &excludedQueue1, State: new(rivertype.JobStateCompleted)})
				notDeletedJob2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, Queue: &excludedQueue2, State: new(rivertype.JobStateCompleted)})

				cancelledJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateCancelled)})
				completedJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateCompleted)})
				discardedJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateDiscarded)})
			)

			numDeleted, err := exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledDoDelete:           true,
				CancelledFinalizedAtHorizon: horizon,
				CompletedDoDelete:           true,
				CompletedFinalizedAtHorizon: horizon,
				DiscardedDoDelete:           true,
				DiscardedFinalizedAtHorizon: horizon,
				Max:                         3,
				QueuesExcluded:              []string{excludedQueue1, excludedQueue2},
			})
			require.NoError(t, err)
			require.Equal(t, 3, numDeleted)

			// All deleted
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: cancelledJob.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: completedJob.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: discardedJob.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)

			// Not deleted
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: notDeletedJob1.ID})
			require.NoError(t, err)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: notDeletedJob2.ID})
			require.NoError(t, err)
		})

		// Each case inserts a retained backlog in queues `kept1`/`kept2`
		// *before* jobs in `deleted1`/`deleted2`, so retained jobs hold the
		// lowest IDs. With `Max: 2`, a query that limits candidates before
		// applying queue filters selects only retained jobs, deletes nothing,
		// and the cleaner stops making progress. Jobs rotate through every
		// finalized state so each state's branch of the query is exercised.
		for _, testCase := range []struct {
			name              string
			queuesExcluded    []string
			queuesIncluded    []string
			wantBatches       []int    // rows deleted by each successive call
			wantDeletedQueues []string // queues whose jobs are eligible
		}{
			// `kept1` appears in both lists; exclusion takes precedence.
			{name: "Both", queuesExcluded: []string{"kept1", "kept2"}, queuesIncluded: []string{"deleted1", "deleted2", "kept1"}, wantBatches: []int{2, 2, 1, 0}, wantDeletedQueues: []string{"deleted1", "deleted2"}},
			// An empty exclusion list excludes nothing, same as nil.
			{name: "EmptyExcluded", queuesExcluded: []string{}, wantBatches: []int{2, 2, 2, 2, 2, 1, 0}, wantDeletedQueues: []string{"deleted1", "deleted2", "kept1", "kept2"}},
			// A non-nil empty inclusion list matches no queues, unlike nil.
			{name: "EmptyIncluded", queuesIncluded: []string{}, wantBatches: []int{0}},
			{name: "Excluded", queuesExcluded: []string{"kept1", "kept2"}, wantBatches: []int{2, 2, 1, 0}, wantDeletedQueues: []string{"deleted1", "deleted2"}},
			{name: "Included", queuesIncluded: []string{"deleted1", "deleted2"}, wantBatches: []int{2, 2, 1, 0}, wantDeletedQueues: []string{"deleted1", "deleted2"}},
			{name: "MissingIncluded", queuesIncluded: []string{"missing"}, wantBatches: []int{0}},
			{name: "NilLists", wantBatches: []int{2, 2, 2, 2, 2, 1, 0}, wantDeletedQueues: []string{"deleted1", "deleted2", "kept1", "kept2"}},
		} {
			t.Run("QueuesFilteredBeforeLimit/"+testCase.name, func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				finalizedStates := []rivertype.JobState{rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded}

				queues := []string{"kept1", "kept2", "kept1", "kept2", "kept1", "kept2", "deleted1", "deleted2", "deleted1", "deleted2", "deleted1"}

				allJobIDs := make([]int64, 0, len(queues))
				var eligibleJobIDs []int64
				for i, queue := range queues {
					job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
						FinalizedAt: &beforeHorizon,
						Queue:       &queue,
						State:       &finalizedStates[i%len(finalizedStates)],
					})
					allJobIDs = append(allJobIDs, job.ID)
					if slices.Contains(testCase.wantDeletedQueues, queue) {
						eligibleJobIDs = append(eligibleJobIDs, job.ID)
					}
				}

				var numDeletedTotal int
				for _, wantDeleted := range testCase.wantBatches {
					numDeleted, err := exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
						CancelledDoDelete:           true,
						CancelledFinalizedAtHorizon: horizon,
						CompletedDoDelete:           true,
						CompletedFinalizedAtHorizon: horizon,
						DiscardedDoDelete:           true,
						DiscardedFinalizedAtHorizon: horizon,
						Max:                         2,
						QueuesExcluded:              testCase.queuesExcluded,
						QueuesIncluded:              testCase.queuesIncluded,
					})
					require.NoError(t, err)
					require.Equal(t, wantDeleted, numDeleted)
					numDeletedTotal += numDeleted

					// Batches delete the oldest eligible jobs first, so after
					// each call exactly the first numDeletedTotal eligible IDs
					// are gone and every other job remains.
					remainingJobs, err := exec.JobGetByIDMany(ctx, &riverdriver.JobGetByIDManyParams{ID: allJobIDs})
					require.NoError(t, err)
					require.Equal(t,
						slices.DeleteFunc(slices.Clone(allJobIDs), func(id int64) bool {
							return slices.Contains(eligibleJobIDs[:numDeletedTotal], id)
						}),
						sliceutil.Map(remainingJobs, func(job *rivertype.JobRow) int64 { return job.ID }),
					)
				}
				require.Len(t, eligibleJobIDs, numDeletedTotal)
			})
		}

		t.Run("QueuesIncluded", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var (
				cancelledJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateCancelled)})
				completedJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateCompleted)})
				discardedJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: new(rivertype.JobStateDiscarded)})

				includedQueue1 = "included1"
				includedQueue2 = "included2"

				// Deleted because in an included queue.
				deletedJob1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, Queue: &includedQueue1, State: new(rivertype.JobStateCompleted)})
				deletedJob2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, Queue: &includedQueue2, State: new(rivertype.JobStateCompleted)})
			)

			// The three older non-included jobs must not consume this batch.
			numDeleted, err := exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledDoDelete:           true,
				CancelledFinalizedAtHorizon: horizon,
				CompletedDoDelete:           true,
				CompletedFinalizedAtHorizon: horizon,
				DiscardedDoDelete:           true,
				DiscardedFinalizedAtHorizon: horizon,
				Max:                         2,
				QueuesIncluded:              []string{includedQueue1, includedQueue2},
			})
			require.NoError(t, err)
			require.Equal(t, 2, numDeleted)

			// Not deleted
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: cancelledJob.ID})
			require.NoError(t, err)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: completedJob.ID})
			require.NoError(t, err)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: discardedJob.ID})
			require.NoError(t, err)

			// Deleted as part of included queues
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: deletedJob1.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: deletedJob2.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})
	})

	t.Run("JobDeleteMany", func(t *testing.T) {
		t.Parallel()

		t.Run("DeletesJobs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Attempt:      new(3),
				AttemptedAt:  &now,
				CreatedAt:    &now,
				EncodedArgs:  []byte(`{"encoded": "args"}`),
				Errors:       [][]byte{[]byte(`{"error": "message1"}`), []byte(`{"error": "message2"}`)},
				FinalizedAt:  &now,
				Metadata:     []byte(`{"meta": "data"}`),
				ScheduledAt:  &now,
				State:        new(rivertype.JobStateCompleted),
				Tags:         []string{"tag"},
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			// Does not match predicate (makes sure where clause is working).
			otherJob := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			deletedJobs, err := exec.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
				Max:           100,
				NamedArgs:     map[string]any{"job_id_123": job.ID},
				OrderByClause: "id",
				WhereClause:   "id = @job_id_123",
			})
			require.NoError(t, err)
			require.Len(t, deletedJobs, 1)

			deletedJob := deletedJobs[0]
			require.Equal(t, job.Attempt, deletedJob.Attempt)
			require.Equal(t, job.AttemptedAt, deletedJob.AttemptedAt)
			require.Equal(t, job.CreatedAt, deletedJob.CreatedAt)
			require.Equal(t, job.EncodedArgs, deletedJob.EncodedArgs)
			require.Equal(t, "message1", deletedJob.Errors[0].Error)
			require.Equal(t, "message2", deletedJob.Errors[1].Error)
			require.Equal(t, job.FinalizedAt, deletedJob.FinalizedAt)
			require.Equal(t, job.Kind, deletedJob.Kind)
			require.Equal(t, job.MaxAttempts, deletedJob.MaxAttempts)
			require.Equal(t, job.Metadata, deletedJob.Metadata)
			require.Equal(t, job.Priority, deletedJob.Priority)
			require.Equal(t, job.Queue, deletedJob.Queue)
			require.Equal(t, job.ScheduledAt, deletedJob.ScheduledAt)
			require.Equal(t, job.State, deletedJob.State)
			require.Equal(t, job.Tags, deletedJob.Tags)
			require.Equal(t, []byte("unique-key"), deletedJob.UniqueKey)
			require.Equal(t, rivertype.JobStates(), deletedJob.UniqueStates)

			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)

			// Non-matching job should remain
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: otherJob.ID})
			require.NoError(t, err)
		})

		t.Run("IgnoresRunningJobs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: new(rivertype.JobStateRunning)})

			deletedJobs, err := exec.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
				Max:           100,
				NamedArgs:     map[string]any{"job_id": job.ID},
				OrderByClause: "id",
				WhereClause:   "id = @job_id",
			})
			require.NoError(t, err)
			require.Empty(t, deletedJobs)

			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
		})

		t.Run("HandlesRequiredArgumentTypes", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			{
				var (
					job1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: new("test_kind1")})
					job2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: new("test_kind2")})
				)

				deletedJobs, err := exec.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
					Max:           100,
					NamedArgs:     map[string]any{"kind": job1.Kind},
					OrderByClause: "id",
					WhereClause:   "kind = @kind",
				})
				require.NoError(t, err)
				require.Len(t, deletedJobs, 1)

				// Non-matching job should remain
				_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
				require.NoError(t, err)
			}

			{
				var (
					job1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: new("test_kind3")})
					job2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: new("test_kind4")})
				)

				deletedJobs, err := exec.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
					Max:           100,
					NamedArgs:     map[string]any{"list_arg_00": job1.Kind, "list_arg_01": job2.Kind},
					OrderByClause: "id",
					WhereClause:   "kind IN (@list_arg_00, @list_arg_01)",
				})
				require.NoError(t, err)
				require.Len(t, deletedJobs, 2)
			}
		})

		t.Run("SortedResults", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var (
				job1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job3 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job4 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job5 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
			)

			deletedJobs, err := exec.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
				Max: 100,
				// NamedArgs:     map[string]any{"kind": job1.Kind},
				OrderByClause: "id",
				WhereClause:   "true",
			})
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID, job2.ID, job3.ID, job4.ID, job5.ID}, sliceutil.Map(deletedJobs, func(j *rivertype.JobRow) int64 { return j.ID }))
		})

		t.Run("SortedResultsDescending", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)
			if bundle.driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
				t.Skip("SQLite always returns JobDeleteMany results ordered by ID")
			}

			var (
				job1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job3 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job4 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job5 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
			)

			deletedJobs, err := exec.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
				Max:           100,
				OrderByClause: "id DESC",
				WhereClause:   "true",
			})
			require.NoError(t, err)
			require.Equal(t, []int64{job5.ID, job4.ID, job3.ID, job2.ID, job1.ID}, sliceutil.Map(deletedJobs, func(j *rivertype.JobRow) int64 { return j.ID }))
		})
	})
}
