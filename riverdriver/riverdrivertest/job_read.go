package riverdrivertest

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

func exerciseJobRead[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
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

	t.Run("JobCountByAllStates", func(t *testing.T) {
		t.Parallel()

		t.Run("CountsJobsByState", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCancelled)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

			countsByState, err := exec.JobCountByAllStates(ctx, &riverdriver.JobCountByAllStatesParams{
				Schema: "",
			})
			require.NoError(t, err)

			for _, state := range rivertype.JobStates() {
				require.Contains(t, countsByState, state)
				switch state { //nolint:exhaustive
				case rivertype.JobStateAvailable:
					require.Equal(t, 2, countsByState[state])
				case rivertype.JobStateCancelled:
					require.Equal(t, 1, countsByState[state])
				case rivertype.JobStateCompleted:
					require.Equal(t, 1, countsByState[state])
				case rivertype.JobStateDiscarded:
					require.Equal(t, 1, countsByState[state])
				default:
					require.Equal(t, 0, countsByState[state])
				}
			}
		})

		t.Run("AlternateSchema", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_, err := exec.JobCountByAllStates(ctx, &riverdriver.JobCountByAllStatesParams{
				Schema: "custom_schema",
			})
			requireMissingRelation(t, err, "custom_schema", "river_job")
		})
	})

	t.Run("JobCountByQueueAndState", func(t *testing.T) {
		t.Parallel()

		t.Run("CountsJobsInAvailableAndRunningForEachOfTheSpecifiedQueues", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateRunning)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateRunning)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateRunning)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateRunning)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue3"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue3"), State: ptrutil.Ptr(rivertype.JobStateRunning)})

			countsByQueue, err := exec.JobCountByQueueAndState(ctx, &riverdriver.JobCountByQueueAndStateParams{
				QueueNames: []string{"queue1", "queue2"},
				Schema:     "",
			})
			require.NoError(t, err)

			require.Len(t, countsByQueue, 2)

			require.Equal(t, "queue1", countsByQueue[0].Queue)
			require.Equal(t, int64(2), countsByQueue[0].CountAvailable)
			require.Equal(t, int64(3), countsByQueue[0].CountRunning)
			require.Equal(t, "queue2", countsByQueue[1].Queue)
			require.Equal(t, int64(1), countsByQueue[1].CountAvailable)
			require.Equal(t, int64(1), countsByQueue[1].CountRunning)
		})

		t.Run("IncludesRequestedQueuesThatHaveNoJobs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateRunning)})

			countsByQueue, err := exec.JobCountByQueueAndState(ctx, &riverdriver.JobCountByQueueAndStateParams{
				QueueNames: []string{"queue1", "queue2"},
				Schema:     "",
			})
			require.NoError(t, err)

			require.Len(t, countsByQueue, 2)

			require.Equal(t, "queue1", countsByQueue[0].Queue)
			require.Equal(t, int64(0), countsByQueue[0].CountAvailable)
			require.Equal(t, int64(0), countsByQueue[0].CountRunning)

			require.Equal(t, "queue2", countsByQueue[1].Queue)
			require.Equal(t, int64(1), countsByQueue[1].CountAvailable)
			require.Equal(t, int64(1), countsByQueue[1].CountRunning)
		})

		t.Run("InputQueueNamesAreDeduplicated", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateRunning)})

			countsByQueue, err := exec.JobCountByQueueAndState(ctx, &riverdriver.JobCountByQueueAndStateParams{
				QueueNames: []string{"queue2", "queue1", "queue1"},
				Schema:     "",
			})
			require.NoError(t, err)

			require.Len(t, countsByQueue, 2)

			require.Equal(t, "queue1", countsByQueue[0].Queue)
			require.Equal(t, int64(0), countsByQueue[0].CountAvailable)
			require.Equal(t, int64(0), countsByQueue[0].CountRunning)

			require.Equal(t, "queue2", countsByQueue[1].Queue)
			require.Equal(t, int64(1), countsByQueue[1].CountAvailable)
			require.Equal(t, int64(1), countsByQueue[1].CountRunning)
		})
	})

	t.Run("JobCountByState", func(t *testing.T) {
		t.Parallel()

		t.Run("CountsJobsByState", func(t *testing.T) {
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

			numJobs, err := exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{
				State: rivertype.JobStateAvailable,
			})
			require.NoError(t, err)
			require.Equal(t, 2, numJobs)
		})

		t.Run("AlternateSchema", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_, err := exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{
				Schema: "custom_schema",
				State:  rivertype.JobStateAvailable,
			})
			requireMissingRelation(t, err, "custom_schema", "river_job")
		})
	})

	t.Run("JobGetAvailable", func(t *testing.T) {
		t.Parallel()

		const (
			maxAttemptedBy = 10
			maxToLock      = 100
		)

		t.Run("Success", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       testClientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 1)

			jobRow := jobRows[0]
			require.Equal(t, []string{testClientID}, jobRow.AttemptedBy)
		})

		t.Run("ConstrainedToLimit", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			// Two rows inserted but only one found because of the added limit.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       testClientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      1,
				Queue:          rivercommon.QueueDefault,
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
				ClientID:       testClientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Empty(t, jobRows)
		})

		t.Run("ConstrainedToScheduledAtBeforeNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt: ptrutil.Ptr(now.Add(1 * time.Minute)),
			})

			// Job is scheduled a while from now so it's not found.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       testClientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Now:            &now,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Empty(t, jobRows)
		})

		t.Run("ConstrainedToScheduledAtBeforeCustomNowTime", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().Add(1 * time.Minute)
			// Job 1 is scheduled after now so it's not found:
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt: ptrutil.Ptr(now.Add(1 * time.Minute)),
			})
			// Job 2 is scheduled just before now so it's found:
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Microsecond)),
			})

			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       testClientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Now:            ptrutil.Ptr(now),
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 1)
			require.Equal(t, job2.ID, jobRows[0].ID)
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
				ClientID:       testClientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      2,
				Queue:          rivercommon.QueueDefault,
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
				ClientID:       testClientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      1,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.NoError(t, err)
			require.Len(t, jobRows, 1, "expected to fetch exactly 1 job")
			require.Equal(t, 3, jobRows[0].Priority, "expected final job to have priority 3")
		})

		t.Run("AttemptedByAtMaxTruncated", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			attemptedBy := make([]string, maxAttemptedBy)
			for i := range maxAttemptedBy {
				attemptedBy[i] = "attempt_" + strconv.Itoa(i)
			}

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				AttemptedBy: attemptedBy,
			})

			// Job is in a non-default queue so it's not found.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       testClientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 1)

			jobRow := jobRows[0]
			require.Equal(t, append(
				attemptedBy[1:],
				testClientID,
			), jobRow.AttemptedBy)
			require.Len(t, jobRow.AttemptedBy, maxAttemptedBy)
		})

		// Almost identical to the above, but tests that there are more existing
		// `attempted_by` elements than the maximum allowed. There's a fine bug
		// around use of > versus >= in the query's conditional, so make sure to
		// capture both cases to make sure they work.
		t.Run("AttemptedByOverMaxTruncated", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			attemptedBy := make([]string, maxAttemptedBy+1)
			for i := range maxAttemptedBy + 1 {
				attemptedBy[i] = "attempt_" + strconv.Itoa(i)
			}

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				AttemptedBy: attemptedBy,
			})

			// Job is in a non-default queue so it's not found.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       testClientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 1)

			jobRow := jobRows[0]
			require.Equal(t, append(
				attemptedBy[2:], // start at 2 because there were 2 extra elements
				testClientID,
			), jobRow.AttemptedBy)
			require.Len(t, jobRow.AttemptedBy, maxAttemptedBy)
		})
	})

	t.Run("JobGetByID", func(t *testing.T) {
		t.Parallel()

		t.Run("FetchesAnExistingJob", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{CreatedAt: &now, ScheduledAt: &now})

			fetchedJob, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			require.Equal(t, job.ID, fetchedJob.ID)
			require.Equal(t, rivertype.JobStateAvailable, fetchedJob.State)
			require.WithinDuration(t, now, fetchedJob.CreatedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now, fetchedJob.ScheduledAt, bundle.driver.TimePrecision())
		})

		t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: 0})
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

		jobs, err := exec.JobGetByIDMany(ctx, &riverdriver.JobGetByIDManyParams{
			ID: []int64{job1.ID, job2.ID},
		})
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID},
			sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID }))
	})

	t.Run("JobGetByKindMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind1")})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind2")})

		// Not returned.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind3")})

		jobs, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind: []string{job1.Kind, job2.Kind},
		})
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID},
			sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID }))
	})

	t.Run("JobGetStuck", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		var (
			horizon       = time.Now().UTC()
			beforeHorizon = horizon.Add(-1 * time.Minute)
			afterHorizon  = horizon.Add(1 * time.Minute)
		)

		stuckJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})
		stuckJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})

		t.Logf("horizon   = %s", horizon)
		t.Logf("stuckJob1 = %s", stuckJob1.AttemptedAt)
		t.Logf("stuckJob2 = %s", stuckJob2.AttemptedAt)

		t.Logf("stuckJob1 full = %s", spew.Sdump(stuckJob1))

		// Not returned because we put a maximum of two.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})

		// Not stuck because not in running state.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})

		// Not stuck because after queried horizon.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})

		// Max two stuck
		stuckJobs, err := exec.JobGetStuck(ctx, &riverdriver.JobGetStuckParams{
			Max:          2,
			StuckHorizon: horizon,
		})
		require.NoError(t, err)
		require.Equal(t, []int64{stuckJob1.ID, stuckJob2.ID},
			sliceutil.Map(stuckJobs, func(j *rivertype.JobRow) int64 { return j.ID }))
	})

	t.Run("JobKindList", func(t *testing.T) {
		t.Parallel()

		t.Run("ListsJobKindsInOrderWithMaxLimit", func(t *testing.T) { //nolint:dupl
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_zzz")})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_aaa")})
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_bbb")})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("different_prefix_job")})

			jobKinds, err := exec.JobKindList(ctx, &riverdriver.JobKindListParams{
				After:   "job2",
				Exclude: nil,
				Match:   "job",
				Max:     2,
				Schema:  "",
			})
			require.NoError(t, err)
			require.Equal(t, []string{job2.Kind, job3.Kind}, jobKinds) // sorted by name
		})

		t.Run("ExcludesJobKindsInExcludeList", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_zzz")})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_aaa")})
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_bbb")})

			jobKinds, err := exec.JobKindList(ctx, &riverdriver.JobKindListParams{
				After:   "job2",
				Exclude: []string{job2.Kind},
				Max:     2,
				Match:   "job",
				Schema:  "",
			})
			require.NoError(t, err)
			require.Equal(t, []string{job3.Kind, job1.Kind}, jobKinds)
		})

		t.Run("ListsJobKindsWithSubstringMatch", func(t *testing.T) { //nolint:dupl
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("mid_job_kind")})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("prefix_job")})
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("suffix_job")})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("nojobhere")})

			jobKinds, err := exec.JobKindList(ctx, &riverdriver.JobKindListParams{
				After:   "",
				Exclude: nil,
				Match:   "fix",
				Max:     3,
				Schema:  "",
			})
			require.NoError(t, err)
			require.Equal(t, []string{job2.Kind, job3.Kind}, jobKinds)
		})
	})

	t.Run("JobList", func(t *testing.T) {
		t.Parallel()

		t.Run("ListsJobs", func(t *testing.T) {
			t.Parallel()

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

			// Does not match predicate (makes sure where clause is working).
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			fetchedJobs, err := exec.JobList(ctx, &riverdriver.JobListParams{
				Max:           100,
				NamedArgs:     map[string]any{"job_id_123": job.ID},
				OrderByClause: "id",
				WhereClause:   "id = @job_id_123",
			})
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
			t.Parallel()

			exec, _ := setup(ctx, t)

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind1")})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind2")})

			{
				fetchedJobs, err := exec.JobList(ctx, &riverdriver.JobListParams{
					Max:           100,
					NamedArgs:     map[string]any{"kind": job1.Kind},
					OrderByClause: "id",
					WhereClause:   "kind = @kind",
				})
				require.NoError(t, err)
				require.Len(t, fetchedJobs, 1)
			}

			{
				fetchedJobs, err := exec.JobList(ctx, &riverdriver.JobListParams{
					Max:           100,
					NamedArgs:     map[string]any{"list_arg_00": job1.Kind, "list_arg_01": job2.Kind},
					OrderByClause: "id",
					WhereClause:   "kind IN (@list_arg_00, @list_arg_01)",
				})
				require.NoError(t, err)
				require.Len(t, fetchedJobs, 2)
			}
		})
	})
}
