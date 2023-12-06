package dbsqlc

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
)

func TestJobList(t *testing.T) {
	t.Parallel()

	t.Run("Minimal", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		tx := riverinternaltest.TestTx(ctx, t)
		queries := New()

		_, err := queries.JobList(ctx, tx, JobListParams{
			State:      JobStateCompleted,
			LimitCount: 1,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderAsc}},
		})
		require.NoError(t, err)
	})

	t.Run("WithConditionsAndSortOrders", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		tx := riverinternaltest.TestTx(ctx, t)
		queries := New()

		_, err := queries.JobList(ctx, tx, JobListParams{
			Conditions: "queue = 'test' AND priority = 1 AND args->>'foo' = @foo",
			NamedArgs:  pgx.NamedArgs{"foo": "bar"},
			State:      JobStateCompleted,
			LimitCount: 1,
			OrderBy:    []JobListOrderBy{{Expr: "id", Order: SortOrderAsc}},
		})
		require.NoError(t, err)
	})
}
