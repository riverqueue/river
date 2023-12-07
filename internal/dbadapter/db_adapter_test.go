package dbadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/dbutil"
)

func Test_StandardAdapter_Insert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		baselineTime time.Time // baseline time frozen at now when setup is called
		ex           dbutil.Executor
	}

	setup := func(t *testing.T, ex dbutil.Executor) (*StandardAdapter, *testBundle) {
		t.Helper()

		bundle := &testBundle{
			baselineTime: time.Now(),
			ex:           ex,
		}

		adapter := NewStandardAdapter(riverinternaltest.BaseServiceArchetype(t), testAdapterConfig(bundle.ex))
		adapter.TimeNowUTC = func() time.Time { return bundle.baselineTime }

		return adapter, bundle
	}

	setupTx := func(t *testing.T) (*StandardAdapter, *testBundle) {
		t.Helper()
		return setup(t, riverinternaltest.TestTx(ctx, t))
	}

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		adapter, _ := setupTx(t)

		insertParams := makeFakeJobInsertParams(0)
		_, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
	})

	t.Run("InsertAndFetch", func(t *testing.T) {
		t.Parallel()

		adapter, _ := setupTx(t)

		const maxJobsToFetch = 8

		res, err := adapter.JobInsert(ctx, makeFakeJobInsertParams(0))
		require.NoError(t, err)
		require.NotEqual(t, 0, res.Job.ID, "expected job ID to be set, got %d", res.Job.ID)
		require.WithinDuration(t, time.Now(), res.Job.ScheduledAt, 1*time.Second)

		jobs, err := adapter.JobGetAvailable(ctx, rivercommon.QueueDefault, maxJobsToFetch)
		require.NoError(t, err)
		require.Len(t, jobs, 1,
			"inserted 1 job but fetched %d jobs:\n%+v", len(jobs), jobs)
		require.Equal(t, dbsqlc.JobStateRunning, jobs[0].State,
			"expected selected job to be in running state, got %q", jobs[0].State)

		for i := 1; i < 10; i++ {
			_, err := adapter.JobInsert(ctx, makeFakeJobInsertParams(i))
			require.NoError(t, err)
		}

		jobs, err = adapter.JobGetAvailable(ctx, rivercommon.QueueDefault, maxJobsToFetch)
		require.NoError(t, err)
		require.Len(t, jobs, maxJobsToFetch,
			"inserted 9 more jobs and expected to fetch max of %d jobs but fetched %d jobs:\n%+v", maxJobsToFetch, len(jobs), jobs)
		for _, j := range jobs {
			require.Equal(t, dbsqlc.JobStateRunning, j.State,
				"expected selected job to be in running state, got %q", j.State)
		}

		jobs, err = adapter.JobGetAvailable(ctx, rivercommon.QueueDefault, maxJobsToFetch)
		require.NoError(t, err)
		require.Len(t, jobs, 1,
			"expected to fetch 1 remaining job but fetched %d jobs:\n%+v", len(jobs), jobs)
	})

	t.Run("UniqueJobByArgs", func(t *testing.T) {
		t.Parallel()

		adapter, _ := setupTx(t)

		insertParams := makeFakeJobInsertParams(0)
		insertParams.Unique = true
		insertParams.UniqueByArgs = true

		res0, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		insertParams.EncodedArgs = []byte(`{"key":"different"}`)

		// Same operation again, except that because we've modified the unique
		// dimension, another job is allowed to be queued, so the new ID is
		// not the same.
		res2, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.NotEqual(t, res0.Job.ID, res2.Job.ID)
		require.False(t, res2.UniqueSkippedAsDuplicate)
	})

	t.Run("UniqueJobByPeriod", func(t *testing.T) {
		t.Parallel()

		adapter, bundle := setupTx(t)

		insertParams := makeFakeJobInsertParams(0)
		insertParams.Unique = true
		insertParams.UniqueByPeriod = 15 * time.Minute

		res0, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		adapter.TimeNowUTC = func() time.Time { return bundle.baselineTime.Add(insertParams.UniqueByPeriod).Add(1 * time.Second) }

		// Same operation again, except that because we've advanced time passed
		// the period within unique bounds, another job is allowed to be queued,
		// so the new ID is not the same.
		res2, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.NotEqual(t, res0.Job.ID, res2.Job.ID)
		require.False(t, res2.UniqueSkippedAsDuplicate)
	})

	t.Run("UniqueJobByQueue", func(t *testing.T) {
		t.Parallel()

		adapter, _ := setupTx(t)

		insertParams := makeFakeJobInsertParams(0)
		insertParams.Unique = true
		insertParams.UniqueByQueue = true

		res0, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		insertParams.Queue = "alternate_queue"

		// Same operation again, except that because we've modified the unique
		// dimension, another job is allowed to be queued, so the new ID is
		// not the same.
		res2, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.NotEqual(t, res0.Job.ID, res2.Job.ID)
		require.False(t, res2.UniqueSkippedAsDuplicate)
	})

	t.Run("UniqueJobByState", func(t *testing.T) {
		t.Parallel()

		adapter, bundle := setupTx(t)

		insertParams := makeFakeJobInsertParams(0)
		insertParams.Unique = true
		insertParams.UniqueByState = []dbsqlc.JobState{dbsqlc.JobStateAvailable, dbsqlc.JobStateRunning}

		res0, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		// A new job is allowed if we're inserting the job with a state that's
		// not included in the unique state set.
		{
			insertParams := *insertParams // dup
			insertParams.State = dbsqlc.JobStateCompleted

			res2, err := adapter.JobInsert(ctx, &insertParams)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)
		}

		// A new job is also allowed if the state of the originally inserted job
		// changes to one that's not included in the unique state set.
		{
			_, err := adapter.queries.JobUpdate(ctx, bundle.ex, dbsqlc.JobUpdateParams{
				ID:            res0.Job.ID,
				StateDoUpdate: true,
				State:         dbsqlc.JobStateCompleted,
			})
			require.NoError(t, err)

			res2, err := adapter.JobInsert(ctx, insertParams)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)
		}
	})

	// Unlike other unique options, state gets a default set when it's not
	// supplied. This test case checks that the default is working as expected.
	t.Run("UniqueJobByDefaultState", func(t *testing.T) {
		t.Parallel()

		adapter, bundle := setupTx(t)

		insertParams := makeFakeJobInsertParams(0)
		insertParams.Unique = true
		insertParams.UniqueByQueue = true

		res0, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		// Test all the other default unique states (see `defaultUniqueStates`)
		// to make sure that in each case an inserted job still counts as a
		// duplicate. The only state we don't test is `available` because that's
		// already been done above.
		for _, defaultState := range []dbsqlc.JobState{
			dbsqlc.JobStateCompleted,
			dbsqlc.JobStateRunning,
			dbsqlc.JobStateRetryable,
			dbsqlc.JobStateScheduled,
		} {
			_, err = adapter.queries.JobUpdate(ctx, bundle.ex, dbsqlc.JobUpdateParams{
				ID:            res0.Job.ID,
				StateDoUpdate: true,
				State:         defaultState,
			})
			require.NoError(t, err)

			// Still counts as a duplicate.
			res1, err := adapter.JobInsert(ctx, insertParams)
			require.NoError(t, err)
			require.Equal(t, res0.Job.ID, res1.Job.ID)
			require.True(t, res1.UniqueSkippedAsDuplicate)
		}

		_, err = adapter.queries.JobUpdate(ctx, bundle.ex, dbsqlc.JobUpdateParams{
			ID:            res0.Job.ID,
			StateDoUpdate: true,
			State:         dbsqlc.JobStateDiscarded,
		})
		require.NoError(t, err)

		// Uniqueness includes a default set of states, so by moving the
		// original job to "discarded", we're now allowed to insert a new job
		// again, despite not having explicitly set the `ByState` option.
		res2, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.NotEqual(t, res0.Job.ID, res2.Job.ID)
		require.False(t, res2.UniqueSkippedAsDuplicate)
	})

	t.Run("UniqueJobAllOptions", func(t *testing.T) {
		t.Parallel()

		adapter, bundle := setupTx(t)

		insertParams := makeFakeJobInsertParams(0)
		insertParams.Unique = true
		insertParams.UniqueByArgs = true
		insertParams.UniqueByPeriod = 15 * time.Minute
		insertParams.UniqueByQueue = true
		insertParams.UniqueByState = []dbsqlc.JobState{dbsqlc.JobStateAvailable, dbsqlc.JobStateRunning}

		// Gut check to make sure all the unique properties were correctly set.
		require.True(t, insertParams.Unique)
		require.True(t, insertParams.UniqueByArgs)
		require.NotZero(t, insertParams.UniqueByPeriod)
		require.True(t, insertParams.UniqueByQueue)
		require.Equal(t, []dbsqlc.JobState{dbsqlc.JobStateAvailable, dbsqlc.JobStateRunning}, insertParams.UniqueByState)

		res0, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		// With args modified
		{
			insertParams := *insertParams // dup
			insertParams.EncodedArgs = []byte(`{"key":"different"}`)

			// New job because a unique dimension has changed.
			res2, err := adapter.JobInsert(ctx, &insertParams)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)
		}

		// With period modified
		{
			insertParams := *insertParams // dup
			adapter.TimeNowUTC = func() time.Time { return bundle.baselineTime.Add(insertParams.UniqueByPeriod).Add(1 * time.Second) }

			// New job because a unique dimension has changed.
			res2, err := adapter.JobInsert(ctx, &insertParams)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)

			// Make sure to change timeNow back
			adapter.TimeNowUTC = func() time.Time { return bundle.baselineTime }
		}

		// With queue modified
		{
			insertParams := *insertParams // dup
			insertParams.Queue = "alternate_queue"

			// New job because a unique dimension has changed.
			res2, err := adapter.JobInsert(ctx, &insertParams)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)
		}

		// With state modified
		{
			insertParams := *insertParams // dup
			insertParams.State = dbsqlc.JobStateCompleted

			// New job because a unique dimension has changed.
			res2, err := adapter.JobInsert(ctx, &insertParams)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)
		}
	})

	t.Run("UniqueJobContention", func(t *testing.T) {
		t.Parallel()

		adapter, bundle := setup(t, riverinternaltest.TestDB(ctx, t))

		insertParams := makeFakeJobInsertParams(0)
		insertParams.Unique = true
		insertParams.UniqueByPeriod = 15 * time.Minute

		var (
			numContendingJobs = runtime.NumCPU() // max allowed test manager connections
			insertedJobs      = make([]*dbsqlc.RiverJob, numContendingJobs)
			insertedJobsMu    sync.Mutex
			wg                sync.WaitGroup
		)

		for i := 0; i < numContendingJobs; i++ {
			jobNum := i
			wg.Add(1)

			go func() {
				_, err := dbutil.WithTxV(ctx, bundle.ex, func(ctx context.Context, tx pgx.Tx) (struct{}, error) {
					res, err := adapter.JobInsertTx(ctx, tx, insertParams)
					require.NoError(t, err)

					insertedJobsMu.Lock()
					insertedJobs[jobNum] = res.Job
					insertedJobsMu.Unlock()

					return struct{}{}, nil
				})
				require.NoError(t, err)

				wg.Done()
			}()
		}

		wg.Wait()

		firstJobID := insertedJobs[0].ID
		for i := 1; i < numContendingJobs; i++ {
			require.Equal(t, firstJobID, insertedJobs[i].ID)
		}
	})
}

func Test_Adapter_JobInsertMany(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := riverinternaltest.TestTx(ctx, t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adapter := NewStandardAdapter(riverinternaltest.BaseServiceArchetype(t), testAdapterConfig(tx))

	insertParams := make([]*JobInsertParams, 10)
	for i := 0; i < len(insertParams); i++ {
		insertParams[i] = makeFakeJobInsertParams(i)
	}

	count, err := adapter.JobInsertMany(ctx, insertParams)
	require.NoError(t, err)
	require.Len(t, insertParams, int(count))

	jobsAfter, err := adapter.JobGetAvailable(ctx, rivercommon.QueueDefault, int32(len(insertParams)))
	require.NoError(t, err)
	require.Len(t, jobsAfter, len(insertParams))
}

func Test_StandardAdapter_FetchIsPrioritized(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := riverinternaltest.TestTx(ctx, t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adapter := NewStandardAdapter(riverinternaltest.BaseServiceArchetype(t), testAdapterConfig(tx))

	for i := 3; i > 0; i-- {
		// Insert jobs with decreasing priority numbers (3, 2, 1) which means increasing priority.
		insertParams := makeFakeJobInsertParams(i)
		insertParams.Priority = i
		_, err := adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)
	}

	// We should fetch the 2 highest priority jobs first in order (priority 1, then 2):
	jobs, err := adapter.JobGetAvailable(ctx, rivercommon.QueueDefault, 2)
	require.NoError(t, err)
	require.Len(t, jobs, 2, "expected to fetch exactly 2 jobs")

	// Because the jobs are ordered within the fetch query's CTE but *not* within
	// the final query, the final result list may not actually be sorted. This is
	// fine, because we've already ensured that we've fetched the jobs we wanted
	// to fetch via that ORDER BY. For testing we'll need to sort the list after
	// fetch to easily assert that the expected jobs are in it.
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].Priority < jobs[j].Priority })

	require.Equal(t, int16(1), jobs[0].Priority, "expected first job to have priority 1")
	require.Equal(t, int16(2), jobs[1].Priority, "expected second job to have priority 2")

	// Should fetch the one remaining job on the next attempt:
	jobs, err = adapter.JobGetAvailable(ctx, rivercommon.QueueDefault, 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1, "expected to fetch exactly 1 job")
	require.Equal(t, int16(3), jobs[0].Priority, "expected final job to have priority 2")
}

func Test_StandardAdapter_JobSetStateCompleted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		baselineTime time.Time // baseline time frozen at now when setup is called
		ex           dbutil.Executor
	}

	setup := func(t *testing.T, ex dbutil.Executor) (*StandardAdapter, *testBundle) {
		t.Helper()

		bundle := &testBundle{
			baselineTime: time.Now(),
			ex:           ex,
		}

		adapter := NewStandardAdapter(riverinternaltest.BaseServiceArchetype(t), testAdapterConfig(bundle.ex))
		adapter.TimeNowUTC = func() time.Time { return bundle.baselineTime }

		return adapter, bundle
	}

	setupTx := func(t *testing.T) (*StandardAdapter, *testBundle) {
		t.Helper()
		return setup(t, riverinternaltest.TestTx(ctx, t))
	}

	t.Run("CompletesARunningJob", func(t *testing.T) {
		t.Parallel()

		adapter, bundle := setupTx(t)

		params := makeFakeJobInsertParams(0)
		params.State = dbsqlc.JobStateRunning
		res, err := adapter.JobInsert(ctx, params)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRunning, res.Job.State)

		jAfter, err := adapter.JobSetStateIfRunning(ctx, JobSetStateCompleted(res.Job.ID, bundle.baselineTime))
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateCompleted, jAfter.State)
		require.WithinDuration(t, bundle.baselineTime, *jAfter.FinalizedAt, time.Microsecond)

		j, err := adapter.queries.JobGetByID(ctx, bundle.ex, res.Job.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateCompleted, j.State)
	})

	t.Run("DoesNotCompleteARetryableJob", func(t *testing.T) {
		t.Parallel()

		adapter, bundle := setupTx(t)

		params := makeFakeJobInsertParams(0)
		params.State = dbsqlc.JobStateRetryable
		res, err := adapter.JobInsert(ctx, params)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, res.Job.State)

		jAfter, err := adapter.JobSetStateIfRunning(ctx, JobSetStateCompleted(res.Job.ID, bundle.baselineTime))
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, jAfter.State)
		require.Nil(t, jAfter.FinalizedAt)

		j, err := adapter.queries.JobGetByID(ctx, bundle.ex, res.Job.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, j.State)
	})
}

func Test_StandardAdapter_JobSetStateErrored(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		baselineTime time.Time // baseline time frozen at now when setup is called
		errPayload   []byte
		ex           dbutil.Executor
	}

	setup := func(t *testing.T, executor dbutil.Executor) (*StandardAdapter, *testBundle) {
		t.Helper()

		tNow := time.Now()

		errPayload, err := json.Marshal(dbsqlc.AttemptError{
			Attempt: 1, At: tNow.UTC(), Error: "fake error", Trace: "foo.go:123\nbar.go:456",
		})

		require.NoError(t, err)
		bundle := &testBundle{
			baselineTime: tNow,
			errPayload:   errPayload,
			ex:           executor,
		}

		adapter := NewStandardAdapter(riverinternaltest.BaseServiceArchetype(t), testAdapterConfig(bundle.ex))
		adapter.TimeNowUTC = func() time.Time { return bundle.baselineTime }

		return adapter, bundle
	}

	setupTx := func(t *testing.T) (*StandardAdapter, *testBundle) {
		t.Helper()
		return setup(t, riverinternaltest.TestTx(ctx, t))
	}

	t.Run("SetsARunningJobToRetryable", func(t *testing.T) {
		t.Parallel()

		adapter, bundle := setupTx(t)

		params := makeFakeJobInsertParams(0)
		params.State = dbsqlc.JobStateRunning
		res, err := adapter.JobInsert(ctx, params)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRunning, res.Job.State)

		jAfter, err := adapter.JobSetStateIfRunning(ctx, JobSetStateErrored(res.Job.ID, bundle.baselineTime, bundle.errPayload))
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, jAfter.State)
		require.WithinDuration(t, bundle.baselineTime, jAfter.ScheduledAt, time.Microsecond)

		j, err := adapter.queries.JobGetByID(ctx, bundle.ex, res.Job.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, j.State)

		// validate error payload:
		require.Len(t, jAfter.Errors, 1)
		require.Equal(t, bundle.baselineTime.UTC(), jAfter.Errors[0].At)
		require.Equal(t, uint16(1), jAfter.Errors[0].Attempt)
		require.Equal(t, "fake error", jAfter.Errors[0].Error)
		require.Equal(t, "foo.go:123\nbar.go:456", jAfter.Errors[0].Trace)
	})

	t.Run("DoesNotTouchAlreadyRetryableJob", func(t *testing.T) {
		t.Parallel()

		adapter, bundle := setupTx(t)

		params := makeFakeJobInsertParams(0)
		params.State = dbsqlc.JobStateRetryable
		params.ScheduledAt = bundle.baselineTime.Add(10 * time.Second)
		res, err := adapter.JobInsert(ctx, params)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, res.Job.State)

		jAfter, err := adapter.JobSetStateIfRunning(ctx, JobSetStateErrored(res.Job.ID, bundle.baselineTime, bundle.errPayload))
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, jAfter.State)
		require.WithinDuration(t, params.ScheduledAt, jAfter.ScheduledAt, time.Microsecond)

		j, err := adapter.queries.JobGetByID(ctx, bundle.ex, res.Job.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, j.State)
		require.WithinDuration(t, params.ScheduledAt, jAfter.ScheduledAt, time.Microsecond)
	})
}

func Test_StandardAdapter_LeadershipAttemptElect_CannotElectTwiceInARow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := riverinternaltest.TestTx(ctx, t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adapter := NewStandardAdapter(riverinternaltest.BaseServiceArchetype(t), testAdapterConfig(tx))
	won, err := adapter.LeadershipAttemptElect(ctx, false, rivercommon.QueueDefault, "fakeWorker0", 30*time.Second)
	require.NoError(t, err)
	require.True(t, won)

	won, err = adapter.LeadershipAttemptElect(ctx, false, rivercommon.QueueDefault, "fakeWorker0", 30*time.Second)
	require.NoError(t, err)
	require.False(t, won)
}

func Benchmark_StandardAdapter_Insert(b *testing.B) {
	ctx := context.Background()
	tx := riverinternaltest.TestTx(ctx, b)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adapter := NewStandardAdapter(riverinternaltest.BaseServiceArchetype(b), testAdapterConfig(tx))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := adapter.JobInsert(ctx, makeFakeJobInsertParams(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_StandardAdapter_Insert_Parallelized(b *testing.B) {
	ctx := context.Background()
	dbPool := riverinternaltest.TestDB(ctx, b)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adapter := NewStandardAdapter(riverinternaltest.BaseServiceArchetype(b), testAdapterConfig(dbPool))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := adapter.JobInsert(ctx, makeFakeJobInsertParams(i)); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func Benchmark_StandardAdapter_Fetch_100(b *testing.B) {
	ctx := context.Background()

	dbPool := riverinternaltest.TestDB(ctx, b)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adapter := NewStandardAdapter(riverinternaltest.BaseServiceArchetype(b), testAdapterConfig(dbPool))

	for i := 0; i < b.N*100; i++ {
		insertParams := makeFakeJobInsertParams(i)
		if _, err := adapter.JobInsert(ctx, insertParams); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := adapter.JobGetAvailable(ctx, rivercommon.QueueDefault, 100); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_StandardAdapter_Fetch_100_Parallelized(b *testing.B) {
	ctx := context.Background()
	dbPool := riverinternaltest.TestDB(ctx, b)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adapter := NewStandardAdapter(riverinternaltest.BaseServiceArchetype(b), testAdapterConfig(dbPool))

	for i := 0; i < b.N*100*runtime.NumCPU(); i++ {
		insertParams := makeFakeJobInsertParams(i)
		if _, err := adapter.JobInsert(ctx, insertParams); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := adapter.JobGetAvailable(ctx, rivercommon.QueueDefault, 100); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func testAdapterConfig(ex dbutil.Executor) *StandardAdapterConfig {
	return &StandardAdapterConfig{
		AdvisoryLockPrefix: 0,
		Executor:           ex,
		WorkerName:         "fakeWorker0",
	}
}

func makeFakeJobInsertParams(i int) *JobInsertParams {
	return &JobInsertParams{
		EncodedArgs: []byte(fmt.Sprintf(`{"job_num":%d}`, i)),
		Kind:        "fake_job",
		MaxAttempts: rivercommon.MaxAttemptsDefault,
		Metadata:    []byte("{}"),
		Priority:    rivercommon.PriorityDefault,
		Queue:       rivercommon.QueueDefault,
		State:       dbsqlc.JobStateAvailable,
	}
}
