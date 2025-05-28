package dblist

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
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
		driver *riverpgxv5.Driver
		exec   riverdriver.Executor
	}

	setup := func() *testBundle {
		driver := riverpgxv5.New(nil)

		return &testBundle{
			driver: driver,
			exec:   driver.UnwrapExecutor(riverdbtest.TestTxPgx(ctx, t)),
		}
	}

	t.Run("Minimal", func(t *testing.T) {
		t.Parallel()

		bundle := setup()

		_, err := JobList(ctx, bundle.exec, &JobListParams{
			States:     []rivertype.JobState{rivertype.JobStateCompleted},
			LimitCount: 1,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderAsc}},
		}, bundle.driver.SQLFragmentColumnIn)
		require.NoError(t, err)
	})

	t.Run("WithConditionsAndSortOrders", func(t *testing.T) {
		t.Parallel()

		bundle := setup()

		_, err := JobList(ctx, bundle.exec, &JobListParams{
			States:     []rivertype.JobState{rivertype.JobStateCompleted},
			LimitCount: 1,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderAsc}},
			Where: []WherePredicate{
				{NamedArgs: map[string]any{"foo": "bar"}, SQL: "queue = 'test' AND priority = 1 AND args->>'foo' = @foo"},
			},
		}, bundle.driver.SQLFragmentColumnIn)
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
			tx     = riverdbtest.TestTxPgx(ctx, t)
			exec   = driver.UnwrapExecutor(tx)
		)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("priority"), Priority: ptrutil.Ptr(1)})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{EncodedArgs: []byte(`{"job_num": 2}`), Priority: ptrutil.Ptr(2)})
		job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Metadata: []byte(`{"some_key": "some_value"}`), Priority: ptrutil.Ptr(3)})
		job4 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning), Priority: ptrutil.Ptr(1)})
		job5 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("alternate_kind"), Priority: ptrutil.Ptr(2)})

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
		jobs, err := JobList(ctx, bundle.exec, params, bundle.driver.SQLFragmentColumnIn)
		testFunc(jobs, err)

		t.Logf("testing JobListTx")
		// use a sub-transaction in case it's rolled back or errors:
		tx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)
		jobs, err = JobList(ctx, tx, params, bundle.driver.SQLFragmentColumnIn)
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
			LimitCount: 2,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderDesc}},
			States:     []rivertype.JobState{rivertype.JobStateAvailable},
			Where: []WherePredicate{
				{NamedArgs: map[string]any{"paths1": []string{"job_num"}, "value1": 2}, SQL: "jsonb_extract_path(args, VARIADIC @paths1::text[]) = @value1::jsonb"},
			},
		}

		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)

			job2 := bundle.jobs[1]
			returnedIDs := sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID })
			require.Equal(t, []int64{job2.ID}, returnedIDs)
		})
	})

	t.Run("WhereWithIDs", func(t *testing.T) {
		t.Parallel()
		bundle := setup(t)
		job1, job2, job3 := bundle.jobs[0], bundle.jobs[1], bundle.jobs[2]
		params := &JobListParams{
			IDs:        []int64{job1.ID},
			LimitCount: 10,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderAsc}},
		}
		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID}, sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID }))
		})
		params = &JobListParams{
			IDs:        []int64{job2.ID, job3.ID},
			LimitCount: 10,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderAsc}},
		}
		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)
			require.Equal(t, []int64{job2.ID, job3.ID}, sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID }))
		})
	})

	t.Run("WhereWithIDsAndPriorities", func(t *testing.T) {
		t.Parallel()
		bundle := setup(t)
		job1, job2, job3 := bundle.jobs[0], bundle.jobs[1], bundle.jobs[2]
		params := &JobListParams{
			IDs:        []int64{job1.ID, job2.ID, job3.ID},
			Priorities: []int16{1},
			LimitCount: 10,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderAsc}},
		}
		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)
			// Only job1 is in the IDs list and has priority 1
			expected := []int64{job1.ID}
			actual := sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID })
			require.Equal(t, expected, actual)
		})
	})

	t.Run("WhereWithKinds", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		params := &JobListParams{
			LimitCount: 2,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderDesc}},
			Kinds:      []string{"alternate_kind"},
			States:     []rivertype.JobState{rivertype.JobStateAvailable},
			Where: []WherePredicate{
				{SQL: "finalized_at IS NULL"},
			},
		}

		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)

			job1 := bundle.jobs[4]
			returnedIDs := sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID })
			require.Equal(t, []int64{job1.ID}, returnedIDs)
		})
	})

	t.Run("WhereWithPriorities", func(t *testing.T) {
		t.Parallel()
		bundle := setup(t)
		_, job2, job3, _, job5 := bundle.jobs[0], bundle.jobs[1], bundle.jobs[2], bundle.jobs[3], bundle.jobs[4]
		params := &JobListParams{
			Priorities: []int16{2, 3},
			LimitCount: 10,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderAsc}},
		}
		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)
			expected := []int64{job2.ID, job3.ID, job5.ID}
			actual := sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID })
			require.Equal(t, expected, actual)
		})
	})

	t.Run("WhereWithQueues", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		params := &JobListParams{
			LimitCount: 2,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderDesc}},
			Queues:     []string{"priority"},
			States:     []rivertype.JobState{rivertype.JobStateAvailable},
			Where: []WherePredicate{
				{SQL: "finalized_at IS NULL"},
			},
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
			LimitCount: 2,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderDesc}},
			Where: []WherePredicate{
				{NamedArgs: map[string]any{"metadata_filter": `{"some_key": "some_value"}`}, SQL: "metadata @> @metadata_filter::jsonb"},
			},
		}

		execTest(ctx, t, bundle, params, func(jobs []*rivertype.JobRow, err error) {
			require.NoError(t, err)

			job3 := bundle.jobs[2]
			returnedIDs := sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID })
			require.Equal(t, []int64{job3.ID}, returnedIDs)
		})
	})

	t.Run("NamedArgNotPresentInQueryError", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		params := &JobListParams{
			LimitCount: 2,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderDesc}},
			Where: []WherePredicate{
				{NamedArgs: map[string]any{"not_present": "foo"}, SQL: "1"},
			},
		}

		_, err := JobList(ctx, bundle.exec, params, bundle.driver.SQLFragmentColumnIn)
		require.EqualError(t, err, `expected "1" to contain named arg symbol @not_present`)
	})

	t.Run("DuplicateNamedArgError", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		params := &JobListParams{
			LimitCount: 2,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderDesc}},
			Where: []WherePredicate{
				{NamedArgs: map[string]any{"duplicate": "foo"}, SQL: "duplicate = @duplicate"},
				{NamedArgs: map[string]any{"duplicate": "foo"}, SQL: "duplicate = @duplicate"},
			},
		}

		_, err := JobList(ctx, bundle.exec, params, bundle.driver.SQLFragmentColumnIn)
		require.EqualError(t, err, "named argument @duplicate already registered")
	})
}
