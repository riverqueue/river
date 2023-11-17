package rivertest

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// Gives us a nice, stable time to test against.
var testTime = time.Date(2023, 10, 30, 10, 45, 23, 123456, time.UTC) //nolint:gochecknoglobals

type Job1Args struct {
	String string `json:"string"`
}

func (Job1Args) Kind() string { return "job1" }

type Job1Worker struct {
	river.WorkerDefaults[Job1Args]
}

func (w *Job1Worker) Work(ctx context.Context, j *river.Job[Job1Args]) error { return nil }

type Job2Args struct {
	Int int `json:"int"`
}

func (Job2Args) Kind() string { return "job2" }

type Job2Worker struct {
	river.WorkerDefaults[Job2Args]
}

func (w *Job2Worker) Work(ctx context.Context, j *river.Job[Job2Args]) error { return nil }

// The tests for this function are quite minimal because it uses the same
// implementation as the `*Tx` variant, so most of the test happens below.
func TestRequireInserted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		mockT  *MockT
	}

	setup := func(t *testing.T) (*river.Client[pgx.Tx], *testBundle) { //nolint:dupl
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)

		workers := river.NewWorkers()
		river.AddWorker(workers, &Job1Worker{})
		river.AddWorker(workers, &Job2Worker{})

		riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
			Queues: map[string]river.QueueConfig{
				river.DefaultQueue: {MaxWorkers: 100},
			},
			Workers: workers,
		})
		require.NoError(t, err)

		err = riverClient.Start(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, riverClient.Stop(ctx)) })

		return riverClient, &testBundle{
			dbPool: dbPool,
			mockT:  NewMockT(t),
		}
	}

	t.Run("VerifiesInsert", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.Insert(ctx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		job := requireInserted(ctx, t, riverpgxv5.New(bundle.dbPool), &Job1Args{}, nil)
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "foo", job.Args.String)
	})
}

func TestRequireInsertedTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		mockT  *MockT
		tx     pgx.Tx
	}

	setup := func(t *testing.T) (*river.Client[pgx.Tx], *testBundle) { //nolint:dupl
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)

		workers := river.NewWorkers()
		river.AddWorker(workers, &Job1Worker{})
		river.AddWorker(workers, &Job2Worker{})

		riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
			Queues: map[string]river.QueueConfig{
				river.DefaultQueue: {MaxWorkers: 100},
			},
			Workers: workers,
		})
		require.NoError(t, err)

		err = riverClient.Start(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, riverClient.Stop(ctx)) })

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return riverClient, &testBundle{
			dbPool: dbPool,
			mockT:  NewMockT(t),
			tx:     tx,
		}
	}

	t.Run("VerifiesInsert", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.Insert(ctx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		job := requireInsertedTx[*riverpgxv5.Driver](ctx, t, bundle.tx, &Job1Args{}, nil)
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "foo", job.Args.String)
	})

	t.Run("VerifiesMultiple", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.Insert(ctx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		_, err = riverClient.Insert(ctx, Job2Args{Int: 123}, nil)
		require.NoError(t, err)

		job1 := requireInsertedTx[*riverpgxv5.Driver](ctx, t, bundle.tx, &Job1Args{}, nil)
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "foo", job1.Args.String)

		job2 := requireInsertedTx[*riverpgxv5.Driver](ctx, t, bundle.tx, &Job2Args{}, nil)
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, 123, job2.Args.Int)
	})

	t.Run("TransactionVisibility", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		// Start a second transaction with different visibility.
		tx, err := bundle.dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		_, err = riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		// Visible in the original transaction.
		job := requireInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job1Args{}, nil)
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "foo", job.Args.String)

		// Not visible in the second transaction.
		_ = requireInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, tx, &Job1Args{}, nil)
		require.True(t, bundle.mockT.Failed)
	})

	t.Run("VerifiesInsertOpts", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		// Verify default insertion options.
		_, err := riverClient.Insert(ctx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		_ = requireInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job1Args{}, &RequireInsertedOpts{
			MaxAttempts: river.DefaultMaxAttempts,
			Priority:    1,
			Queue:       river.DefaultQueue,
		})
		require.False(t, bundle.mockT.Failed)

		// Verify custom insertion options.
		_, err = riverClient.Insert(ctx, Job2Args{Int: 123}, &river.InsertOpts{
			MaxAttempts: 78,
			Priority:    2,
			Queue:       "another-queue",
			ScheduledAt: testTime,
			Tags:        []string{"tag1"},
		})
		require.NoError(t, err)

		_ = requireInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job2Args{}, &RequireInsertedOpts{
			MaxAttempts: 78,
			Priority:    2,
			Queue:       "another-queue",
			ScheduledAt: testTime,
			Tags:        []string{"tag1"},
		})
		require.False(t, bundle.mockT.Failed)
	})

	t.Run("FailsWithoutInsert", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.Insert(ctx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		_ = requireInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job2Args{}, nil)
		require.True(t, bundle.mockT.Failed)
		require.Equal(t,
			failureString("No jobs found with kind: job2")+"\n",
			bundle.mockT.LogOutput())
	})

	t.Run("FailsWithTooManyInserts", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertMany(ctx, []river.InsertManyParams{
			{Args: Job1Args{String: "foo"}},
			{Args: Job1Args{String: "bar"}},
		})
		require.NoError(t, err)

		_ = requireInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job1Args{}, nil)
		require.True(t, bundle.mockT.Failed)
		require.Equal(t,
			failureString("More than one job found with kind: job1 (you might want RequireManyInserted instead)")+"\n",
			bundle.mockT.LogOutput())
	})

	t.Run("FailsOnWrongJobInsert", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		_ = requireInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job1Args{}, nil)
		require.True(t, bundle.mockT.Failed)
		require.Equal(t,
			failureString("No jobs found with kind: job1")+"\n",
			bundle.mockT.LogOutput())
	})

	t.Run("FailsOnInsertOpts", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		// Verify custom insertion options.
		_, err := riverClient.Insert(ctx, Job2Args{Int: 123}, &river.InsertOpts{
			MaxAttempts: 78,
			Priority:    2,
			Queue:       "another-queue",
			ScheduledAt: testTime,
			Tags:        []string{"tag1"},
		})
		require.NoError(t, err)

		// Max attempts
		{
			mockT := NewMockT(t)
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, &RequireInsertedOpts{
				MaxAttempts: 77,
				Priority:    2,
				Queue:       "another-queue",
				ScheduledAt: testTime,
				State:       river.JobStateScheduled,
				Tags:        []string{"tag1"},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' max attempts 78 not equal to expected 77")+"\n",
				mockT.LogOutput())
		}

		// Priority
		{
			mockT := NewMockT(t)
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, &RequireInsertedOpts{
				MaxAttempts: 78,
				Priority:    3,
				Queue:       "another-queue",
				ScheduledAt: testTime,
				State:       river.JobStateScheduled,
				Tags:        []string{"tag1"},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' priority 2 not equal to expected 3")+"\n",
				mockT.LogOutput())
		}

		// Queue
		{
			mockT := NewMockT(t)
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, &RequireInsertedOpts{
				MaxAttempts: 78,
				Priority:    2,
				Queue:       "wrong-queue",
				ScheduledAt: testTime,
				State:       river.JobStateScheduled,
				Tags:        []string{"tag1"},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' queue 'another-queue' not equal to expected 'wrong-queue'")+"\n",
				mockT.LogOutput())
		}

		// Scheduled at
		{
			mockT := NewMockT(t)
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, &RequireInsertedOpts{
				MaxAttempts: 78,
				Priority:    2,
				Queue:       "another-queue",
				ScheduledAt: testTime.Add(3*time.Minute + 23*time.Second + 123*time.Microsecond),
				State:       river.JobStateScheduled,
				Tags:        []string{"tag1"},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' scheduled at 2023-10-30T10:45:23.000123Z not equal to expected 2023-10-30T10:48:46.000246Z")+"\n",
				mockT.LogOutput())
		}

		// State
		{
			mockT := NewMockT(t)
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, &RequireInsertedOpts{
				MaxAttempts: 78,
				Priority:    2,
				Queue:       "another-queue",
				ScheduledAt: testTime,
				State:       river.JobStateCancelled,
				Tags:        []string{"tag1"},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' state 'scheduled' not equal to expected 'cancelled'")+"\n",
				mockT.LogOutput())
		}

		// Tags
		{
			mockT := NewMockT(t)
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, &RequireInsertedOpts{
				MaxAttempts: 78,
				Priority:    2,
				Queue:       "another-queue",
				ScheduledAt: testTime,
				State:       river.JobStateScheduled,
				Tags:        []string{"tag2"},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' tags attempts [tag1] not equal to expected [tag2]")+"\n",
				mockT.LogOutput())
		}
	})
}

// The tests for this function are quite minimal because it uses the same
// implementation as the `*Tx` variant, so most of the test happens below.
func TestRequireManyInserted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		mockT  *MockT
	}

	setup := func(t *testing.T) (*river.Client[pgx.Tx], *testBundle) { //nolint:dupl
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)

		workers := river.NewWorkers()
		river.AddWorker(workers, &Job1Worker{})
		river.AddWorker(workers, &Job2Worker{})

		riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
			Queues: map[string]river.QueueConfig{
				river.DefaultQueue: {MaxWorkers: 100},
			},
			Workers: workers,
		})
		require.NoError(t, err)

		err = riverClient.Start(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, riverClient.Stop(ctx)) })

		return riverClient, &testBundle{
			dbPool: dbPool,
			mockT:  NewMockT(t),
		}
	}

	t.Run("VerifiesInsert", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.Insert(ctx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		jobs := requireManyInserted(ctx, bundle.mockT, riverpgxv5.New(bundle.dbPool), []ExpectedJob{
			{Args: &Job1Args{}},
		})
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "job1", jobs[0].Kind)
	})
}

func TestRequireManyInsertedTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		mockT  *MockT
		tx     pgx.Tx
	}

	setup := func(t *testing.T) (*river.Client[pgx.Tx], *testBundle) { //nolint:dupl
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)

		workers := river.NewWorkers()
		river.AddWorker(workers, &Job1Worker{})
		river.AddWorker(workers, &Job2Worker{})

		riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
			Queues: map[string]river.QueueConfig{
				river.DefaultQueue: {MaxWorkers: 100},
			},
			Workers: workers,
		})
		require.NoError(t, err)

		err = riverClient.Start(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, riverClient.Stop(ctx)) })

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return riverClient, &testBundle{
			dbPool: dbPool,
			mockT:  NewMockT(t),
			tx:     tx,
		}
	}

	t.Run("VerifiesInsert", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.Insert(ctx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		jobs := requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{Args: &Job1Args{}},
		})
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "job1", jobs[0].Kind)
	})

	t.Run("TransactionVisibility", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		// Start a second transaction with different visibility.
		tx, err := bundle.dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		_, err = riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		// Visible in the original transaction.
		jobs := requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{Args: &Job1Args{}},
		})
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "job1", jobs[0].Kind)

		// Not visible in the second transaction.
		_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, tx, []ExpectedJob{
			{Args: &Job1Args{}},
		})
		require.True(t, bundle.mockT.Failed)
	})

	t.Run("VerifiesMultipleDifferentKind", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.Insert(ctx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		_, err = riverClient.Insert(ctx, Job2Args{Int: 123}, nil)
		require.NoError(t, err)

		jobs := requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{Args: &Job1Args{}},
			{Args: &Job2Args{}},
		})
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "job1", jobs[0].Kind)
		require.Equal(t, "job2", jobs[1].Kind)
	})

	t.Run("VerifiesMultipleSameKind", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertMany(ctx, []river.InsertManyParams{
			{Args: Job1Args{String: "foo"}},
			{Args: Job1Args{String: "bar"}},
		})
		require.NoError(t, err)

		jobs := requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{Args: &Job1Args{}},
			{Args: &Job1Args{}},
		})
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "job1", jobs[0].Kind)
		require.Equal(t, "job1", jobs[1].Kind)
	})

	t.Run("VerifiesMultitude", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertMany(ctx, []river.InsertManyParams{
			{Args: Job1Args{String: "foo"}},
			{Args: Job1Args{String: "bar"}},
			{Args: Job2Args{Int: 123}},
			{Args: Job2Args{Int: 456}},
			{Args: Job1Args{String: "baz"}},
		})
		require.NoError(t, err)

		jobs := requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{Args: &Job1Args{}},
			{Args: &Job1Args{}},
			{Args: &Job2Args{}},
			{Args: &Job2Args{}},
			{Args: &Job1Args{}},
		})
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "job1", jobs[0].Kind)
		require.Equal(t, "job1", jobs[1].Kind)
		require.Equal(t, "job2", jobs[2].Kind)
		require.Equal(t, "job2", jobs[3].Kind)
		require.Equal(t, "job1", jobs[4].Kind)
	})

	t.Run("VerifiesInsertOpts", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		// Verify default insertion options.
		_, err := riverClient.Insert(ctx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{
				Args: &Job1Args{},
				Opts: &RequireInsertedOpts{
					MaxAttempts: river.DefaultMaxAttempts,
					Priority:    1,
					Queue:       river.DefaultQueue,
				},
			},
		})
		require.False(t, bundle.mockT.Failed)

		// Verify custom insertion options.
		_, err = riverClient.Insert(ctx, Job2Args{Int: 123}, &river.InsertOpts{
			MaxAttempts: 78,
			Priority:    2,
			Queue:       "another-queue",
			ScheduledAt: testTime,
			Tags:        []string{"tag1"},
		})
		require.NoError(t, err)

		_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{
				Args: &Job2Args{},
				Opts: &RequireInsertedOpts{
					MaxAttempts: 78,
					Priority:    2,
					Queue:       "another-queue",
					ScheduledAt: testTime,
					Tags:        []string{"tag1"},
				},
			},
		})
		require.False(t, bundle.mockT.Failed)
	})

	t.Run("FailsWithoutInsert", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{Args: &Job1Args{}},
		})
		require.True(t, bundle.mockT.Failed)
		require.Equal(t,
			failureString("Inserted jobs didn't match expectation; expected: [job1], actual: []")+"\n",
			bundle.mockT.LogOutput())
	})

	t.Run("FailsWithTooManyInserts", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertMany(ctx, []river.InsertManyParams{
			{Args: Job1Args{String: "foo"}},
			{Args: Job1Args{String: "bar"}},
		})
		require.NoError(t, err)

		_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{Args: &Job1Args{}},
		})
		require.True(t, bundle.mockT.Failed)
		require.Equal(t,
			failureString("Inserted jobs didn't match expectation; expected: [job1], actual: [job1 job1]")+"\n",
			bundle.mockT.LogOutput())
	})

	t.Run("FailsWrongInsertOrder", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertMany(ctx, []river.InsertManyParams{
			{Args: Job2Args{Int: 123}},
			{Args: Job1Args{String: "foo"}},
		})
		require.NoError(t, err)

		_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{Args: &Job1Args{}},
			{Args: &Job2Args{}},
		})
		require.True(t, bundle.mockT.Failed)
		require.Equal(t,
			failureString("Inserted jobs didn't match expectation; expected: [job1 job2], actual: [job2 job1]")+"\n",
			bundle.mockT.LogOutput())
	})

	t.Run("FailsMultitude", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertMany(ctx, []river.InsertManyParams{
			{Args: Job1Args{String: "foo"}},
			{Args: Job1Args{String: "bar"}},
			{Args: Job2Args{Int: 123}},
			{Args: Job2Args{Int: 456}},
			{Args: Job2Args{Int: 789}},
		})
		require.NoError(t, err)

		_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{Args: &Job1Args{}},
			{Args: &Job1Args{}},
			{Args: &Job2Args{}},
			{Args: &Job2Args{}},
			{Args: &Job1Args{}},
		})
		require.True(t, bundle.mockT.Failed)
		require.Equal(t,
			failureString("Inserted jobs didn't match expectation; expected: [job1 job1 job2 job2 job1], actual: [job1 job1 job2 job2 job2]")+"\n", //nolint:dupword
			bundle.mockT.LogOutput())
	})

	t.Run("FailsOnInsertOpts", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		// Verify custom insertion options.
		_, err := riverClient.Insert(ctx, Job2Args{Int: 123}, &river.InsertOpts{
			MaxAttempts: 78,
			Priority:    2,
			Queue:       "another-queue",
			ScheduledAt: testTime,
			Tags:        []string{"tag1"},
		})
		require.NoError(t, err)

		// Max attempts
		{
			mockT := NewMockT(t)
			_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, []ExpectedJob{
				{
					Args: &Job2Args{},
					Opts: &RequireInsertedOpts{
						MaxAttempts: 77,
						Priority:    2,
						Queue:       "another-queue",
						ScheduledAt: testTime,
						State:       river.JobStateScheduled,
						Tags:        []string{"tag1"},
					},
				},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' (expected job slice index 0) max attempts 78 not equal to expected 77")+"\n",
				mockT.LogOutput())
		}

		// Priority
		{
			mockT := NewMockT(t)
			_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, []ExpectedJob{
				{
					Args: &Job2Args{},
					Opts: &RequireInsertedOpts{
						MaxAttempts: 78,
						Priority:    3,
						Queue:       "another-queue",
						ScheduledAt: testTime,
						State:       river.JobStateScheduled,
						Tags:        []string{"tag1"},
					},
				},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' (expected job slice index 0) priority 2 not equal to expected 3")+"\n",
				mockT.LogOutput())
		}

		// Queue
		{
			mockT := NewMockT(t)
			_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, []ExpectedJob{
				{
					Args: &Job2Args{},
					Opts: &RequireInsertedOpts{
						MaxAttempts: 78,
						Priority:    2,
						Queue:       "wrong-queue",
						ScheduledAt: testTime,
						State:       river.JobStateScheduled,
						Tags:        []string{"tag1"},
					},
				},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' (expected job slice index 0) queue 'another-queue' not equal to expected 'wrong-queue'")+"\n",
				mockT.LogOutput())
		}

		// Scheduled at
		{
			mockT := NewMockT(t)
			_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, []ExpectedJob{
				{
					Args: &Job2Args{},
					Opts: &RequireInsertedOpts{
						MaxAttempts: 78,
						Priority:    2,
						Queue:       "another-queue",
						ScheduledAt: testTime.Add(3*time.Minute + 23*time.Second + 123*time.Microsecond),
						State:       river.JobStateScheduled,
						Tags:        []string{"tag1"},
					},
				},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' (expected job slice index 0) scheduled at 2023-10-30T10:45:23.000123Z not equal to expected 2023-10-30T10:48:46.000246Z")+"\n",
				mockT.LogOutput())
		}

		// State
		{
			mockT := NewMockT(t)
			_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, []ExpectedJob{
				{
					Args: &Job2Args{},
					Opts: &RequireInsertedOpts{
						MaxAttempts: 78,
						Priority:    2,
						Queue:       "another-queue",
						State:       river.JobStateCancelled,
						ScheduledAt: testTime,
						Tags:        []string{"tag1"},
					},
				},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' (expected job slice index 0) state 'scheduled' not equal to expected 'cancelled'")+"\n",
				mockT.LogOutput())
		}

		// Tags
		{
			mockT := NewMockT(t)
			_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, []ExpectedJob{
				{
					Args: &Job2Args{},
					Opts: &RequireInsertedOpts{
						MaxAttempts: 78,
						Priority:    2,
						Queue:       "another-queue",
						ScheduledAt: testTime,
						State:       river.JobStateScheduled,
						Tags:        []string{"tag2"},
					},
				},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' (expected job slice index 0) tags attempts [tag1] not equal to expected [tag2]")+"\n",
				mockT.LogOutput())
		}
	})
}

// MockT mocks testingT (or *testing.T). It's used to let us verify our test
// helpers.
type MockT struct {
	Failed    bool
	logOutput bytes.Buffer
	tb        testing.TB
}

func NewMockT(tb testing.TB) *MockT {
	tb.Helper()
	return &MockT{tb: tb}
}

func (t *MockT) Errorf(format string, args ...any) {
	_, _ = format, args
}

func (t *MockT) FailNow() {
	t.Failed = true
}

func (t *MockT) Helper() {}

func (t *MockT) Log(args ...any) {
	t.tb.Log(args...)

	t.logOutput.WriteString(fmt.Sprint(args...))
	t.logOutput.WriteString("\n")
}

func (t *MockT) Logf(format string, args ...any) {
	t.tb.Logf(format, args...)

	t.logOutput.WriteString(fmt.Sprintf(format, args...))
	t.logOutput.WriteString("\n")
}

func (t *MockT) LogOutput() string {
	return t.logOutput.String()
}
