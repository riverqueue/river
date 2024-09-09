package dblist

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

func TestJobListNoJobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec riverdriver.Executor
	}

	setup := func() *testBundle {
		driver := riverpgxv5.New(nil)

		return &testBundle{
			exec: driver.UnwrapExecutor(riverinternaltest.TestTx(ctx, t)),
		}
	}

	t.Run("Minimal", func(t *testing.T) {
		t.Parallel()

		bundle := setup()

		_, err := JobList(ctx, bundle.exec, &JobListParams{
			States:     []rivertype.JobState{rivertype.JobStateCompleted},
			LimitCount: 1,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderAsc}},
		})
		require.NoError(t, err)
	})

	t.Run("WithConditionsAndSortOrders", func(t *testing.T) {
		t.Parallel()

		bundle := setup()

		_, err := JobList(ctx, bundle.exec, &JobListParams{
			Conditions: "queue = 'test' AND priority = 1 AND args->>'foo' = @foo",
			NamedArgs:  pgx.NamedArgs{"foo": "bar"},
			States:     []rivertype.JobState{rivertype.JobStateCompleted},
			LimitCount: 1,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderAsc}},
		})
		require.NoError(t, err)
	})
}

func TestJobListWithJobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		baselineTime time.Time // baseline time frozen at now when setup is called
		driver       riverdriver.Driver[pgx.Tx]
		exec         riverdriver.Executor
		jobs         []*rivertype.JobRow
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		var (
			driver = riverpgxv5.New(nil)
			tx     = riverinternaltest.TestTx(ctx, t)
			exec   = driver.UnwrapExecutor(tx)
		)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("priority")})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{EncodedArgs: []byte(`{"job_num": 2}`)})
		job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Metadata: []byte(`{"some_key": "some_value"}`)})
		job4 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job5 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("alternate_kind")})

		return &testBundle{
			baselineTime: time.Now(),
			driver:       driver,
			exec:         exec,
			jobs:         []*rivertype.JobRow{job1, job2, job3, job4, job5},
		}
	}

	type testListFunc func(jobs []*rivertype.JobRow, err error)

	execTest := func(ctx context.Context, t *testing.T, bundle *testBundle, params *JobListParams, testFunc testListFunc) {
		t.Helper()
		t.Logf("testing JobList in Executor")
		jobs, err := JobList(ctx, bundle.exec, params)
		testFunc(jobs, err)

		t.Logf("testing JobListTx")
		// use a sub-transaction in case it's rolled back or errors:
		tx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)
		jobs, err = JobList(ctx, tx, params)
		testFunc(jobs, err)
	}

	t.Run("Minimal", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		params := &JobListParams{
			LimitCount: 3,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderDesc}},
			States:     []rivertype.JobState{rivertype.JobStateAvailable},
		}

		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)

			// job 1 is excluded due to pagination limit of 2, while job 4 is excluded
			// due to its state:
			job2 := bundle.jobs[1]
			job3 := bundle.jobs[2]
			job5 := bundle.jobs[4]

			returnedIDs := sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID })
			require.Equal(t, []int64{job5.ID, job3.ID, job2.ID}, returnedIDs)
		})
	})

	t.Run("ComplexConditionsWithNamedArgs", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		params := &JobListParams{
			Conditions: "jsonb_extract_path(args, VARIADIC @paths1::text[]) = @value1::jsonb",
			LimitCount: 2,
			NamedArgs:  map[string]any{"paths1": []string{"job_num"}, "value1": 2},
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderDesc}},
			States:     []rivertype.JobState{rivertype.JobStateAvailable},
		}

		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)

			job2 := bundle.jobs[1]
			returnedIDs := sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID })
			require.Equal(t, []int64{job2.ID}, returnedIDs)
		})
	})

	t.Run("ConditionsWithKinds", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		params := &JobListParams{
			Conditions: "finalized_at IS NULL",
			LimitCount: 2,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderDesc}},
			Kinds:      []string{"alternate_kind"},
			States:     []rivertype.JobState{rivertype.JobStateAvailable},
		}

		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)

			job1 := bundle.jobs[4]
			returnedIDs := sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID })
			require.Equal(t, []int64{job1.ID}, returnedIDs)
		})
	})

	t.Run("ConditionsWithQueues", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		params := &JobListParams{
			Conditions: "finalized_at IS NULL",
			LimitCount: 2,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderDesc}},
			Queues:     []string{"priority"},
			States:     []rivertype.JobState{rivertype.JobStateAvailable},
		}

		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)

			job1 := bundle.jobs[0]
			returnedIDs := sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID })
			require.Equal(t, []int64{job1.ID}, returnedIDs)
		})
	})

	t.Run("WithMetadataAndNoStateFilter", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		params := &JobListParams{
			Conditions: "metadata @> @metadata_filter::jsonb",
			LimitCount: 2,
			NamedArgs:  map[string]any{"metadata_filter": `{"some_key": "some_value"}`},
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderDesc}},
		}

		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)

			job3 := bundle.jobs[2]
			returnedIDs := sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID })
			require.Equal(t, []int64{job3.ID}, returnedIDs)
		})
	})
}
