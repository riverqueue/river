package riverdrivertest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
)

func exerciseSQLFragments[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	t.Run("SQLFragmentColumnIn", func(t *testing.T) {
		t.Parallel()

		t.Run("IntegerValues", func(t *testing.T) {
			t.Parallel()

			exec, driver := executorWithTx(ctx, t)

			var (
				job1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				_    = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
			)

			sqlFragment, arg, err := driver.SQLFragmentColumnIn("id", []int64{job1.ID, job2.ID})
			require.NoError(t, err)

			jobs, err := exec.JobList(ctx, &riverdriver.JobListParams{
				Max:           100,
				NamedArgs:     map[string]any{"id": arg},
				OrderByClause: "id",
				WhereClause:   sqlFragment,
			})
			require.NoError(t, err)
			require.Len(t, jobs, 2)
			require.Equal(t, job1.ID, jobs[0].ID)
			require.Equal(t, job2.ID, jobs[1].ID)
		})

		t.Run("StringValues", func(t *testing.T) {
			t.Parallel()

			exec, driver := executorWithTx(ctx, t)

			var (
				job1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind1")})
				job2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind2")})
				_    = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind3")})
			)

			sqlFragment, arg, err := driver.SQLFragmentColumnIn("kind", []string{job1.Kind, job2.Kind})
			require.NoError(t, err)

			jobs, err := exec.JobList(ctx, &riverdriver.JobListParams{
				Max:           100,
				NamedArgs:     map[string]any{"kind": arg},
				OrderByClause: "kind",
				WhereClause:   sqlFragment,
			})
			require.NoError(t, err)
			require.Len(t, jobs, 2)
			require.Equal(t, job1.Kind, jobs[0].Kind)
			require.Equal(t, job2.Kind, jobs[1].Kind)
		})
	})
}
