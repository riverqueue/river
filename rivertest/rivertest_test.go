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
	"github.com/riverqueue/river/rivertype"
)

// Gives us a nice, stable time to test against.
var testTime = time.Date(2023, 10, 30, 10, 45, 23, 123456, time.UTC) //nolint:gochecknoglobals

type Job1Args struct {
	String string `json:"string"`
}

func (Job1Args) Kind() string { return "job1" }

type Job2Args struct {
	Int int `json:"int"`
}

func (Job2Args) Kind() string { return "job2" }

// The tests for this function are quite minimal because it uses the same
// implementation as the `*Tx` variant, so most of the test happens below.
func TestRequireInserted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		mockT  *MockT
	}

	setup := func(t *testing.T) (*river.Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)

		riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{})
		require.NoError(t, err)

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
		mockT *MockT
		tx    pgx.Tx
	}

	setup := func(t *testing.T) (*river.Client[pgx.Tx], *testBundle) {
		t.Helper()

		riverClient, err := river.NewClient(riverpgxv5.New(nil), &river.Config{})
		require.NoError(t, err)

		return riverClient, &testBundle{
			mockT: NewMockT(t),
			tx:    riverinternaltest.TestTx(ctx, t),
		}
	}

	t.Run("VerifiesInsert", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		job := requireInsertedTx[*riverpgxv5.Driver](ctx, t, bundle.tx, &Job1Args{}, nil)
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "foo", job.Args.String)
	})

	t.Run("VerifiesMultiple", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		_, err = riverClient.InsertTx(ctx, bundle.tx, Job2Args{Int: 123}, nil)
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
		otherTx := riverinternaltest.TestTx(ctx, t)

		_, err := riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		// Visible in the original transaction.
		job := requireInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job1Args{}, nil)
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "foo", job.Args.String)

		// Not visible in the second transaction.
		_ = requireInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, otherTx, &Job1Args{}, nil)
		require.True(t, bundle.mockT.Failed)
	})

	t.Run("FailsWithoutInsert", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "foo"}, nil)
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

		_, err := riverClient.InsertManyTx(ctx, bundle.tx, []river.InsertManyParams{
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

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertTx(ctx, bundle.tx, Job2Args{Int: 123}, nil)
		require.NoError(t, err)

		_ = requireInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job1Args{}, nil)
		require.True(t, bundle.mockT.Failed)
		require.Equal(t,
			failureString("No jobs found with kind: job1")+"\n",
			bundle.mockT.LogOutput())
	})

	t.Run("InsertOpts", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		// Verify custom insertion options.
		insertRes, err := riverClient.InsertTx(ctx, bundle.tx, Job2Args{Int: 123}, &river.InsertOpts{
			MaxAttempts: 78,
			Priority:    2,
			Queue:       "another_queue",
			ScheduledAt: testTime,
			Tags:        []string{"tag1"},
		})
		require.NoError(t, err)
		job := insertRes.Job

		emptyOpts := func() *RequireInsertedOpts { return &RequireInsertedOpts{} }

		sameOpts := func() *RequireInsertedOpts {
			return &RequireInsertedOpts{
				MaxAttempts: 78,
				Priority:    2,
				Queue:       "another_queue",
				ScheduledAt: testTime,
				State:       rivertype.JobStateScheduled,
				Tags:        []string{"tag1"},
			}
		}

		t.Run("MaxAttempts", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := sameOpts()
			opts.MaxAttempts = 77
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' max attempts 78 not equal to expected 77")+"\n",
				mockT.LogOutput())
		})

		t.Run("Priority", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := sameOpts()
			opts.Priority = 3
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' priority 2 not equal to expected 3")+"\n",
				mockT.LogOutput())
		})

		t.Run("Queue", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := sameOpts()
			opts.Queue = "wrong_queue"
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' queue 'another_queue' not equal to expected 'wrong_queue'")+"\n",
				mockT.LogOutput())
		})

		t.Run("ScheduledAt", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := sameOpts()
			opts.ScheduledAt = testTime.Add(3*time.Minute + 23*time.Second + 123*time.Microsecond)
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' scheduled at 2023-10-30T10:45:23.000123Z not equal to expected 2023-10-30T10:48:46.000246Z")+"\n",
				mockT.LogOutput())
		})

		t.Run("State", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := sameOpts()
			opts.State = rivertype.JobStateCancelled
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' state 'scheduled' not equal to expected 'cancelled'")+"\n",
				mockT.LogOutput())
		})

		t.Run("Tags", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := sameOpts()
			opts.Tags = []string{"tag2"}
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' tags [tag1] not equal to expected [tag2]")+"\n",
				mockT.LogOutput())
		})

		t.Run("MultiplePropertiesSucceed", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := emptyOpts()
			opts.MaxAttempts = job.MaxAttempts
			opts.Priority = job.Priority
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.False(t, mockT.Failed, "Should have succeeded, but failed with: "+mockT.LogOutput())
		})

		t.Run("MultiplePropertiesFails", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := sameOpts()
			opts.MaxAttempts = 77
			opts.Priority = 3
			_ = requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' max attempts 78 not equal to expected 77, priority 2 not equal to expected 3")+"\n",
				mockT.LogOutput())
		})

		t.Run("AllSameSucceeds", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := sameOpts()
			requireInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.False(t, mockT.Failed)
		})
	})
}

// The tests for this function are quite minimal because it uses the same
// implementation as the `*Tx` variant, so most of the test happens below.
func TestRequireNotInserted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		mockT  *MockT
	}

	setup := func(t *testing.T) (*river.Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)

		riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{})
		require.NoError(t, err)

		return riverClient, &testBundle{
			dbPool: dbPool,
			mockT:  NewMockT(t),
		}
	}

	t.Run("VerifiesNotInsert", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.Insert(ctx, Job2Args{Int: 123}, nil)
		require.NoError(t, err)

		requireNotInserted(ctx, t, riverpgxv5.New(bundle.dbPool), &Job1Args{}, nil)
		require.False(t, bundle.mockT.Failed)
	})
}

func TestRequireNotInsertedTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		mockT *MockT
		tx    pgx.Tx
	}

	setup := func(t *testing.T) (*river.Client[pgx.Tx], *testBundle) {
		t.Helper()

		riverClient, err := river.NewClient(riverpgxv5.New(nil), &river.Config{})
		require.NoError(t, err)

		return riverClient, &testBundle{
			mockT: NewMockT(t),
			tx:    riverinternaltest.TestTx(ctx, t),
		}
	}

	t.Run("VerifiesInsert", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertTx(ctx, bundle.tx, Job2Args{Int: 123}, nil)
		require.NoError(t, err)

		requireNotInsertedTx[*riverpgxv5.Driver](ctx, t, bundle.tx, &Job1Args{}, nil)
		require.False(t, bundle.mockT.Failed)
	})

	t.Run("VerifiesMultiple", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		requireNotInsertedTx[*riverpgxv5.Driver](ctx, t, bundle.tx, &Job1Args{}, nil)
		require.False(t, bundle.mockT.Failed)

		requireNotInsertedTx[*riverpgxv5.Driver](ctx, t, bundle.tx, &Job2Args{}, nil)
		require.False(t, bundle.mockT.Failed)
	})

	t.Run("TransactionVisibility", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		// Start a second transaction with different visibility.
		otherTx := riverinternaltest.TestTx(ctx, t)

		_, err := riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		// Not visible in the second transaction.
		requireNotInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, otherTx, &Job1Args{}, nil)
		require.False(t, bundle.mockT.Failed)

		// Visible in the original transaction.
		requireNotInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job1Args{}, nil)
		require.True(t, bundle.mockT.Failed)
	})

	t.Run("SucceedsWithoutInsert", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		requireNotInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job2Args{}, nil)
		require.False(t, bundle.mockT.Failed)
	})

	t.Run("FailsWithTooManyInserts", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertManyTx(ctx, bundle.tx, []river.InsertManyParams{
			{Args: Job1Args{String: "foo"}},
			{Args: Job1Args{String: "bar"}},
		})
		require.NoError(t, err)

		requireNotInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job1Args{}, nil)
		require.True(t, bundle.mockT.Failed)
		require.Equal(t,
			failureString("2 jobs found with kind, but expected to find none: job1")+"\n",
			bundle.mockT.LogOutput())
	})

	t.Run("SucceedsOnWrongJobInsert", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertTx(ctx, bundle.tx, Job2Args{Int: 123}, nil)
		require.NoError(t, err)

		requireNotInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job1Args{}, nil)
		require.False(t, bundle.mockT.Failed)
	})

	t.Run("InsertOpts", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		// Verify custom insertion options.
		insertRes, err := riverClient.InsertTx(ctx, bundle.tx, Job2Args{Int: 123}, &river.InsertOpts{
			MaxAttempts: 78,
			Priority:    2,
			Queue:       "another_queue",
			ScheduledAt: testTime,
			Tags:        []string{"tag1"},
		})
		require.NoError(t, err)
		job := insertRes.Job

		emptyOpts := func() *RequireInsertedOpts { return &RequireInsertedOpts{} }

		sameOpts := func() *RequireInsertedOpts {
			return &RequireInsertedOpts{
				MaxAttempts: 78,
				Priority:    2,
				Queue:       "another_queue",
				ScheduledAt: testTime,
				State:       rivertype.JobStateScheduled,
				Tags:        []string{"tag1"},
			}
		}

		t.Run("MaxAttempts", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := emptyOpts()
			opts.MaxAttempts = job.MaxAttempts
			requireNotInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' max attempts equal to excluded %d", job.MaxAttempts)+"\n",
				mockT.LogOutput())
		})

		t.Run("Priority", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := emptyOpts()
			opts.Priority = job.Priority
			requireNotInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' priority equal to excluded %d", job.Priority)+"\n",
				mockT.LogOutput())
		})

		t.Run("Queue", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := emptyOpts()
			opts.Queue = job.Queue
			requireNotInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' queue equal to excluded '%s'", job.Queue)+"\n",
				mockT.LogOutput())
		})

		t.Run("ScheduledAt", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := emptyOpts()
			opts.ScheduledAt = job.ScheduledAt
			requireNotInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' scheduled at equal to excluded %s", opts.ScheduledAt.Format(rfc3339Micro))+"\n",
				mockT.LogOutput())
		})

		t.Run("State", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := emptyOpts()
			opts.State = job.State
			requireNotInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' state equal to excluded '%s'", job.State)+"\n",
				mockT.LogOutput())
		})

		t.Run("Tags", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := emptyOpts()
			opts.Tags = job.Tags
			requireNotInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' tags equal to excluded %+v", job.Tags)+"\n",
				mockT.LogOutput())
		})

		t.Run("MultiplePropertiesSucceed", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := emptyOpts()
			opts.MaxAttempts = job.MaxAttempts // one property matches job, but the other does not
			opts.Priority = 3
			requireNotInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.False(t, mockT.Failed, "Should have succeeded, but failed with: "+mockT.LogOutput())
		})

		t.Run("MultiplePropertiesFail", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := emptyOpts()
			opts.MaxAttempts = job.MaxAttempts
			opts.Priority = job.Priority
			requireNotInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' max attempts equal to excluded %d, priority equal to excluded %d", job.MaxAttempts, job.Priority)+"\n",
				mockT.LogOutput())
		})

		t.Run("AllSameFails", func(t *testing.T) {
			mockT := NewMockT(t)
			opts := sameOpts()
			requireNotInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' max attempts equal to excluded %d, priority equal to excluded %d, queue equal to excluded '%s', scheduled at equal to excluded %s, state equal to excluded '%s', tags equal to excluded %+v", job.MaxAttempts, job.Priority, job.Queue, job.ScheduledAt.Format(rfc3339Micro), job.State, job.Tags)+"\n",
				mockT.LogOutput())
		})

		t.Run("FailsWithTooManyInserts", func(t *testing.T) {
			_, err := riverClient.InsertTx(ctx, bundle.tx, Job2Args{Int: 123}, &river.InsertOpts{
				Priority: 3,
			})
			require.NoError(t, err)

			mockT := NewMockT(t)
			opts := emptyOpts()
			opts.Priority = 3
			requireNotInsertedTx[*riverpgxv5.Driver](ctx, mockT, bundle.tx, &Job2Args{}, opts)
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' priority equal to excluded %d", 3)+"\n",
				mockT.LogOutput())
		})
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

	setup := func(t *testing.T) (*river.Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)

		riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{})
		require.NoError(t, err)

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
		mockT *MockT
		tx    pgx.Tx
	}

	setup := func(t *testing.T) (*river.Client[pgx.Tx], *testBundle) {
		t.Helper()

		riverClient, err := river.NewClient(riverpgxv5.New(nil), &river.Config{})
		require.NoError(t, err)

		return riverClient, &testBundle{
			mockT: NewMockT(t),
			tx:    riverinternaltest.TestTx(ctx, t),
		}
	}

	t.Run("VerifiesInsert", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "foo"}, nil)
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
		otherTx := riverinternaltest.TestTx(ctx, t)

		_, err := riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		// Visible in the original transaction.
		jobs := requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{Args: &Job1Args{}},
		})
		require.False(t, bundle.mockT.Failed)
		require.Equal(t, "job1", jobs[0].Kind)

		// Not visible in the second transaction.
		_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, otherTx, []ExpectedJob{
			{Args: &Job1Args{}},
		})
		require.True(t, bundle.mockT.Failed)
	})

	t.Run("VerifiesMultipleDifferentKind", func(t *testing.T) {
		t.Parallel()

		riverClient, bundle := setup(t)

		_, err := riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		_, err = riverClient.InsertTx(ctx, bundle.tx, Job2Args{Int: 123}, nil)
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

		_, err := riverClient.InsertManyTx(ctx, bundle.tx, []river.InsertManyParams{
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

		_, err := riverClient.InsertManyTx(ctx, bundle.tx, []river.InsertManyParams{
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
		_, err := riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "foo"}, nil)
		require.NoError(t, err)

		_ = requireManyInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, []ExpectedJob{
			{
				Args: &Job1Args{},
				Opts: &RequireInsertedOpts{
					MaxAttempts: river.MaxAttemptsDefault,
					Priority:    1,
					Queue:       river.QueueDefault,
				},
			},
		})
		require.False(t, bundle.mockT.Failed)

		// Verify custom insertion options.
		_, err = riverClient.InsertTx(ctx, bundle.tx, Job2Args{Int: 123}, &river.InsertOpts{
			MaxAttempts: 78,
			Priority:    2,
			Queue:       "another_queue",
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
					Queue:       "another_queue",
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

		_, err := riverClient.InsertManyTx(ctx, bundle.tx, []river.InsertManyParams{
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

		_, err := riverClient.InsertManyTx(ctx, bundle.tx, []river.InsertManyParams{
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

		_, err := riverClient.InsertManyTx(ctx, bundle.tx, []river.InsertManyParams{
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
		_, err := riverClient.InsertTx(ctx, bundle.tx, Job2Args{Int: 123}, &river.InsertOpts{
			MaxAttempts: 78,
			Priority:    2,
			Queue:       "another_queue",
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
						Queue:       "another_queue",
						ScheduledAt: testTime,
						State:       rivertype.JobStateScheduled,
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
						Queue:       "another_queue",
						ScheduledAt: testTime,
						State:       rivertype.JobStateScheduled,
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
						State:       rivertype.JobStateScheduled,
						Tags:        []string{"tag1"},
					},
				},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' (expected job slice index 0) queue 'another_queue' not equal to expected 'wrong-queue'")+"\n",
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
						Queue:       "another_queue",
						ScheduledAt: testTime.Add(3*time.Minute + 23*time.Second + 123*time.Microsecond),
						State:       rivertype.JobStateScheduled,
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
						Queue:       "another_queue",
						State:       rivertype.JobStateCancelled,
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
						Queue:       "another_queue",
						ScheduledAt: testTime,
						State:       rivertype.JobStateScheduled,
						Tags:        []string{"tag2"},
					},
				},
			})
			require.True(t, mockT.Failed)
			require.Equal(t,
				failureString("Job with kind 'job2' (expected job slice index 0) tags [tag1] not equal to expected [tag2]")+"\n",
				mockT.LogOutput())
		}
	})
}

func TestWorkContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(ctx context.Context, t *testing.T) (context.Context, *testBundle) {
		t.Helper()

		client, err := river.NewClient(riverpgxv5.New(nil), &river.Config{})
		require.NoError(t, err)

		return WorkContext(ctx, client), &testBundle{}
	}

	t.Run("ClientFromContext", func(t *testing.T) {
		t.Parallel()

		ctx, _ := setup(ctx, t)

		client := river.ClientFromContext[pgx.Tx](ctx)
		require.NotNil(t, client)
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
