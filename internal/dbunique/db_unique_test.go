package dbunique

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

const queueAlternate = "alternate_queue"

func makeInsertParams(createdAt *time.Time) *riverdriver.JobInsertFastParams {
	return &riverdriver.JobInsertFastParams{
		CreatedAt:   createdAt,
		EncodedArgs: []byte(`{}`),
		Kind:        "fake_job",
		MaxAttempts: rivercommon.MaxAttemptsDefault,
		Metadata:    []byte(`{}`),
		Priority:    rivercommon.PriorityDefault,
		Queue:       rivercommon.QueueDefault,
		ScheduledAt: nil,
		State:       rivertype.JobStateAvailable,
	}
}

func TestDefaultUniqueStatesSorted(t *testing.T) {
	t.Parallel()

	states := slices.Clone(defaultUniqueStates)
	slices.Sort(states)
	require.Equal(t, states, defaultUniqueStates, "Default unique states should be sorted")
}

func TestUniqueInserter_JobInsert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		baselineTime time.Time // baseline time frozen at now when setup is called
		driver       riverdriver.Driver[pgx.Tx]
		exec         riverdriver.Executor
		tx           pgx.Tx
	}

	setup := func(t *testing.T) (*UniqueInserter, *testBundle) {
		t.Helper()

		var (
			driver = riverpgxv5.New(nil)
			tx     = riverinternaltest.TestTx(ctx, t)
		)

		bundle := &testBundle{
			driver: driver,
			exec:   driver.UnwrapExecutor(tx),
			tx:     tx,
		}

		inserter := baseservice.Init(riversharedtest.BaseServiceArchetype(t), &UniqueInserter{})

		// Tests that use ByPeriod below can be sensitive to intermittency if
		// the tests run at say 14:59:59.998, then it's possible to accidentally
		// cross a period threshold, even if very unlikely. So here, seed mostly
		// the current time, but make sure it's nicened up a little to be
		// roughly in the middle of the hour and well clear of any period
		// boundaries.
		bundle.baselineTime = inserter.Time.StubNowUTC(time.Now().UTC().Truncate(1 * time.Hour).Add(37*time.Minute + 23*time.Second + 123*time.Millisecond))

		return inserter, bundle
	}

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)

		insertParams := makeInsertParams(&bundle.baselineTime)
		res, err := inserter.JobInsert(ctx, bundle.exec, insertParams, nil)
		require.NoError(t, err)

		// Sanity check, following assertion depends on this:
		require.Nil(t, insertParams.ScheduledAt)

		require.Positive(t, res.Job.ID, "expected job ID to be set, got %d", res.Job.ID)
		require.JSONEq(t, string(insertParams.EncodedArgs), string(res.Job.EncodedArgs))
		require.Equal(t, 0, res.Job.Attempt)
		require.Nil(t, res.Job.AttemptedAt)
		require.Empty(t, res.Job.AttemptedBy)
		require.Equal(t, bundle.baselineTime.Truncate(1*time.Microsecond), res.Job.CreatedAt)
		require.Empty(t, res.Job.Errors)
		require.Nil(t, res.Job.FinalizedAt)
		require.Equal(t, insertParams.Kind, res.Job.Kind)
		require.Equal(t, insertParams.MaxAttempts, res.Job.MaxAttempts)
		require.Equal(t, insertParams.Metadata, res.Job.Metadata)
		require.Equal(t, insertParams.Priority, res.Job.Priority)
		require.Equal(t, insertParams.Queue, res.Job.Queue)
		require.Equal(t, rivertype.JobStateAvailable, res.Job.State)
		require.WithinDuration(t, time.Now(), res.Job.ScheduledAt, 2*time.Second)
		require.Empty(t, res.Job.Tags)
	})

	t.Run("InsertAndFetch", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)

		const maxJobsToFetch = 8

		res, err := inserter.JobInsert(ctx, bundle.exec, makeInsertParams(&bundle.baselineTime), nil)
		require.NoError(t, err)
		require.NotEqual(t, 0, res.Job.ID, "expected job ID to be set, got %d", res.Job.ID)
		require.WithinDuration(t, time.Now(), res.Job.ScheduledAt, 1*time.Second)

		jobs, err := bundle.exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
			AttemptedBy: "test-id",
			Max:         maxJobsToFetch,
			Queue:       rivercommon.QueueDefault,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 1,
			"inserted 1 job but fetched %d jobs:\n%+v", len(jobs), jobs)
		require.Equal(t, rivertype.JobStateRunning, jobs[0].State,
			"expected selected job to be in running state, got %q", jobs[0].State)

		for i := 1; i < 10; i++ {
			_, err := inserter.JobInsert(ctx, bundle.exec, makeInsertParams(&bundle.baselineTime), nil)
			require.NoError(t, err)
		}

		jobs, err = bundle.exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
			AttemptedBy: "test-id",
			Max:         maxJobsToFetch,
			Queue:       rivercommon.QueueDefault,
		})
		require.NoError(t, err)
		require.Len(t, jobs, maxJobsToFetch,
			"inserted 9 more jobs and expected to fetch max of %d jobs but fetched %d jobs:\n%+v", maxJobsToFetch, len(jobs), jobs)
		for _, j := range jobs {
			require.Equal(t, rivertype.JobStateRunning, j.State,
				"expected selected job to be in running state, got %q", j.State)
		}

		jobs, err = bundle.exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
			AttemptedBy: "test-id",
			Max:         maxJobsToFetch,
			Queue:       rivercommon.QueueDefault,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 1,
			"expected to fetch 1 remaining job but fetched %d jobs:\n%+v", len(jobs), jobs)
	})

	t.Run("UniqueJobByArgsFastPath", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)

		insertParams := makeInsertParams(&bundle.baselineTime)
		uniqueOpts := &UniqueOpts{
			ByArgs: true,
		}

		res0, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.NotNil(t, res0.Job.UniqueKey)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		insertParams.EncodedArgs = []byte(`{"key":"different"}`)

		// Same operation again, except that because we've modified the unique
		// dimension, another job is allowed to be queued, so the new ID is
		// not the same.
		res2, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.NotEqual(t, res0.Job.ID, res2.Job.ID)
		require.False(t, res2.UniqueSkippedAsDuplicate)
	})

	t.Run("UniqueJobByArgsSlowPath", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)

		insertParams := makeInsertParams(&bundle.baselineTime)
		uniqueOpts := &UniqueOpts{
			ByArgs:  true,
			ByState: []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCancelled}, // use of non-standard states triggers slow path
		}

		res0, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Nil(t, res0.Job.UniqueKey)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		insertParams.EncodedArgs = []byte(`{"key":"different"}`)

		// Same operation again, except that because we've modified the unique
		// dimension, another job is allowed to be queued, so the new ID is
		// not the same.
		res2, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.NotEqual(t, res0.Job.ID, res2.Job.ID)
		require.False(t, res2.UniqueSkippedAsDuplicate)
	})

	t.Run("UniqueJobByPeriodFastPath", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)

		insertParams := makeInsertParams(&bundle.baselineTime)
		uniqueOpts := &UniqueOpts{
			ByPeriod: 15 * time.Minute,
		}

		res0, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.NotNil(t, res0.Job.UniqueKey)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		inserter.Time.StubNowUTC(bundle.baselineTime.Add(uniqueOpts.ByPeriod).Add(1 * time.Second))

		// Same operation again, except that because we've advanced time passed
		// the period within unique bounds, another job is allowed to be queued,
		// so the new ID is not the same.
		res2, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.NotEqual(t, res0.Job.ID, res2.Job.ID)
		require.False(t, res2.UniqueSkippedAsDuplicate)
	})

	t.Run("UniqueJobByPeriodSlowPath", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)

		insertParams := makeInsertParams(&bundle.baselineTime)
		uniqueOpts := &UniqueOpts{
			ByPeriod: 15 * time.Minute,
			ByState:  []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCancelled}, // use of non-standard states triggers slow path
		}

		res0, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.Nil(t, res0.Job.UniqueKey)
		require.NoError(t, err)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		inserter.Time.StubNowUTC(bundle.baselineTime.Add(uniqueOpts.ByPeriod).Add(1 * time.Second))

		// Same operation again, except that because we've advanced time passed
		// the period within unique bounds, another job is allowed to be queued,
		// so the new ID is not the same.
		res2, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.NotEqual(t, res0.Job.ID, res2.Job.ID)
		require.False(t, res2.UniqueSkippedAsDuplicate)
	})

	t.Run("UniqueJobByQueueFastPath", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)

		insertParams := makeInsertParams(&bundle.baselineTime)
		uniqueOpts := &UniqueOpts{
			ByQueue: true,
		}

		res0, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.NotNil(t, res0.Job.UniqueKey)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		insertParams.Queue = queueAlternate

		// Same operation again, except that because we've modified the unique
		// dimension, another job is allowed to be queued, so the new ID is
		// not the same.
		res2, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.NotEqual(t, res0.Job.ID, res2.Job.ID)
		require.False(t, res2.UniqueSkippedAsDuplicate)
	})

	t.Run("UniqueJobByQueueSlowPath", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)

		insertParams := makeInsertParams(&bundle.baselineTime)
		uniqueOpts := &UniqueOpts{
			ByQueue: true,
			ByState: []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCancelled}, // use of non-standard states triggers slow path
		}

		res0, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Nil(t, res0.Job.UniqueKey)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		insertParams.Queue = queueAlternate

		// Same operation again, except that because we've modified the unique
		// dimension, another job is allowed to be queued, so the new ID is
		// not the same.
		res2, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.NotEqual(t, res0.Job.ID, res2.Job.ID)
		require.False(t, res2.UniqueSkippedAsDuplicate)
	})

	// Unlike other unique options, state gets a default set when it's not
	// supplied. This test case checks that the default is working as expected.
	t.Run("UniqueJobByDefaultStateFastPath", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)

		insertParams := makeInsertParams(&bundle.baselineTime)
		uniqueOpts := &UniqueOpts{
			ByQueue: true,
		}

		res0, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.NotNil(t, res0.Job.UniqueKey) // fast path because states are a subset of defaults
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		// Test all the other default unique states (see `defaultUniqueStates`)
		// to make sure that in each case an inserted job still counts as a
		// duplicate. The only state we don't test is `available` because that's
		// already been done above.
		for _, defaultState := range []rivertype.JobState{
			rivertype.JobStateCompleted,
			rivertype.JobStatePending,
			rivertype.JobStateRunning,
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
		} {
			var finalizedAt *time.Time
			if defaultState == rivertype.JobStateCompleted {
				finalizedAt = ptrutil.Ptr(bundle.baselineTime)
			}

			_, err = bundle.exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
				ID:                  res0.Job.ID,
				FinalizedAtDoUpdate: true,
				FinalizedAt:         finalizedAt,
				StateDoUpdate:       true,
				State:               defaultState,
			})
			require.NoError(t, err)

			// Still counts as a duplicate.
			res1, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
			require.NoError(t, err)
			require.Equal(t, res0.Job.ID, res1.Job.ID)
			require.True(t, res1.UniqueSkippedAsDuplicate)
		}

		_, err = bundle.exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
			ID:                  res0.Job.ID,
			FinalizedAtDoUpdate: true,
			FinalizedAt:         ptrutil.Ptr(bundle.baselineTime),
			StateDoUpdate:       true,
			State:               rivertype.JobStateDiscarded,
			UniqueKeyDoUpdate:   true, // `unique_key` is normally NULLed by the client or completer
			UniqueKey:           nil,
		})
		require.NoError(t, err)

		// Uniqueness includes a default set of states, so by moving the
		// original job to "discarded", we're now allowed to insert a new job
		// again, despite not having explicitly set the `ByState` option.
		res2, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.NotEqual(t, res0.Job.ID, res2.Job.ID)
		require.False(t, res2.UniqueSkippedAsDuplicate)
	})

	t.Run("UniqueJobByStateSlowPath", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)

		insertParams := makeInsertParams(&bundle.baselineTime)
		uniqueOpts := &UniqueOpts{
			ByState: []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCancelled},
		}

		res0, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Nil(t, res0.Job.UniqueKey) // slow path because states are *not* a subset of defaults
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		// A new job is allowed if we're inserting the job with a state that's
		// not included in the unique state set.
		{
			insertParams := *insertParams // dup
			insertParams.State = rivertype.JobStateRunning

			res2, err := inserter.JobInsert(ctx, bundle.exec, &insertParams, uniqueOpts)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)
		}

		// A new job is also allowed if the state of the originally inserted job
		// changes to one that's not included in the unique state set.
		{
			_, err := bundle.exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
				ID:                  res0.Job.ID,
				FinalizedAtDoUpdate: true,
				FinalizedAt:         ptrutil.Ptr(bundle.baselineTime),
				StateDoUpdate:       true,
				State:               rivertype.JobStateCompleted,
			})
			require.NoError(t, err)

			res2, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)
		}
	})

	t.Run("UniqueJobAllOptions", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)

		insertParams := makeInsertParams(&bundle.baselineTime)
		uniqueOpts := &UniqueOpts{
			ByArgs:   true,
			ByPeriod: 15 * time.Minute,
			ByQueue:  true,
			ByState:  []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateRunning},
		}

		res0, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.False(t, res0.UniqueSkippedAsDuplicate)

		// Insert a second job with the same args, but expect that the same job
		// ID to come back because we're still within its unique parameters.
		res1, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
		require.NoError(t, err)
		require.Equal(t, res0.Job.ID, res1.Job.ID)
		require.True(t, res1.UniqueSkippedAsDuplicate)

		// With args modified
		{
			insertParams := *insertParams // dup
			insertParams.EncodedArgs = []byte(`{"key":"different"}`)

			// New job because a unique dimension has changed.
			res2, err := inserter.JobInsert(ctx, bundle.exec, &insertParams, uniqueOpts)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)
		}

		// With period modified
		{
			insertParams := *insertParams // dup
			inserter.Time.StubNowUTC(bundle.baselineTime.Add(uniqueOpts.ByPeriod).Add(1 * time.Second))

			// New job because a unique dimension has changed.
			res2, err := inserter.JobInsert(ctx, bundle.exec, &insertParams, uniqueOpts)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)

			// Make sure to change timeNow back
			inserter.Time.StubNowUTC(bundle.baselineTime)
		}

		// With queue modified
		{
			insertParams := *insertParams // dup
			insertParams.Queue = queueAlternate

			// New job because a unique dimension has changed.
			res2, err := inserter.JobInsert(ctx, bundle.exec, &insertParams, uniqueOpts)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)
		}

		// With state modified
		{
			insertParams := *insertParams // dup
			insertParams.State = rivertype.JobStatePending

			// New job because a unique dimension has changed.
			res2, err := inserter.JobInsert(ctx, bundle.exec, &insertParams, uniqueOpts)
			require.NoError(t, err)
			require.NotEqual(t, res0.Job.ID, res2.Job.ID)
			require.False(t, res2.UniqueSkippedAsDuplicate)
		}
	})

	t.Run("UniqueJobContention", func(t *testing.T) {
		t.Parallel()

		inserter, bundle := setup(t)
		require.NoError(t, bundle.tx.Rollback(ctx))
		bundle.driver = riverpgxv5.New(riverinternaltest.TestDB(ctx, t))
		bundle.exec = bundle.driver.GetExecutor()

		insertParams := makeInsertParams(&bundle.baselineTime)
		uniqueOpts := &UniqueOpts{
			ByPeriod: 15 * time.Minute,
		}

		var (
			numContendingJobs = runtime.NumCPU() // max allowed test manager connections
			insertedJobs      = make([]*rivertype.JobRow, numContendingJobs)
			insertedJobsMu    sync.Mutex
			wg                sync.WaitGroup
		)

		for i := 0; i < numContendingJobs; i++ {
			jobNum := i
			wg.Add(1)

			go func() {
				_, err := dbutil.WithTxV(ctx, bundle.exec, func(ctx context.Context, exec riverdriver.ExecutorTx) (struct{}, error) {
					res, err := inserter.JobInsert(ctx, exec, insertParams, uniqueOpts)
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

func BenchmarkUniqueInserter(b *testing.B) {
	ctx := context.Background()

	type testBundle struct {
		driver riverdriver.Driver[pgx.Tx]
		exec   riverdriver.Executor
		tx     pgx.Tx
	}

	setup := func(b *testing.B) (*UniqueInserter, *testBundle) {
		b.Helper()

		var (
			driver = riverpgxv5.New(nil)
			tx     = riverinternaltest.TestTx(ctx, b)
		)

		bundle := &testBundle{
			driver: driver,
			exec:   driver.UnwrapExecutor(tx),
			tx:     tx,
		}

		inserter := baseservice.Init(riversharedtest.BaseServiceArchetype(b), &UniqueInserter{})

		return inserter, bundle
	}

	// Simulates the case where many existing jobs are in the database already.
	// Useful as a benchmark because the advisory lock strategy's look up get
	// slow with many existing jobs.
	generateManyExistingJobs := func(b *testing.B, inserter *UniqueInserter, bundle *testBundle) {
		b.Helper()

		insertParams := makeInsertParams(nil)

		for i := 0; i < 10_000; i++ {
			_, err := inserter.JobInsert(ctx, bundle.exec, insertParams, nil)
			require.NoError(b, err)
		}
	}

	b.Run("FastPathEmptyDatabase", func(b *testing.B) {
		inserter, bundle := setup(b)

		insertParams := makeInsertParams(nil)
		uniqueOpts := &UniqueOpts{ByArgs: true}

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			insertParams.EncodedArgs = []byte(fmt.Sprintf(`{"job_num":%d}`, n%1000))
			_, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
			require.NoError(b, err)
		}
	})

	b.Run("FastPathManyExistingJobs", func(b *testing.B) {
		inserter, bundle := setup(b)

		generateManyExistingJobs(b, inserter, bundle)

		insertParams := makeInsertParams(nil)
		uniqueOpts := &UniqueOpts{ByArgs: true}

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			insertParams.EncodedArgs = []byte(fmt.Sprintf(`{"job_num":%d}`, n%1000))
			_, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
			require.NoError(b, err)
		}
	})

	b.Run("SlowPathEmptyDatabase", func(b *testing.B) {
		inserter, bundle := setup(b)

		insertParams := makeInsertParams(nil)
		uniqueOpts := &UniqueOpts{
			ByArgs:  true,
			ByState: []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCancelled}, // use of non-standard states triggers slow path
		}

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			insertParams.EncodedArgs = []byte(fmt.Sprintf(`{"job_num":%d}`, n%1000))
			_, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
			require.NoError(b, err)
		}
	})

	b.Run("SlowPathManyExistingJobs", func(b *testing.B) {
		inserter, bundle := setup(b)

		generateManyExistingJobs(b, inserter, bundle)

		insertParams := makeInsertParams(nil)
		uniqueOpts := &UniqueOpts{
			ByArgs:  true,
			ByState: []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCancelled}, // use of non-standard states triggers slow path
		}

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			insertParams.EncodedArgs = []byte(fmt.Sprintf(`{"job_num":%d}`, n%1000))
			_, err := inserter.JobInsert(ctx, bundle.exec, insertParams, uniqueOpts)
			require.NoError(b, err)
		}
	})
}
