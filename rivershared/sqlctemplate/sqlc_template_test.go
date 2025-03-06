package sqlctemplate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplacer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (*Replacer, *testBundle) { //nolint:unparam
		t.Helper()

		return NewReplacer(), &testBundle{}
	}

	t.Run("BasicTemplate", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithTemplates(ctx, map[string]Replacement{
			"schema": {Value: "test_schema."},
		}, nil)

		updatedSQL, args, err := replacer.RunSafely(ctx, `
			-- name: JobCountByState :one
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job
			WHERE state = @state;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			-- name: JobCountByState :one
			SELECT count(*)
			FROM test_schema.river_job
			WHERE state = @state;
		`, updatedSQL)
	})

	t.Run("BeginEndTemplate", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithTemplates(ctx, map[string]Replacement{
			"order_by_clause": {Value: "kind, id"},
			"where_clause":    {Value: "kind = 'no_op'"},
		}, nil)

		updatedSQL, args, err := replacer.RunSafely(ctx, `
			-- name: JobList :many
			SELECT *
			FROM river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ 1 /* TEMPLATE_END */
			ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */
			LIMIT @max::int;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			-- name: JobList :many
			SELECT *
			FROM river_job
			WHERE kind = 'no_op'
			ORDER BY kind, id
			LIMIT @max::int;
		`, updatedSQL)
	})

	t.Run("RepeatedTemplate", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithTemplates(ctx, map[string]Replacement{
			"schema": {Value: "test_schema."},
		}, nil)

		updatedSQL, args, err := replacer.RunSafely(ctx, `
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job r1
				INNER JOIN /* TEMPLATE: schema */river_job r2 ON r1.id = r2.id;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			SELECT count(*)
			FROM test_schema.river_job r1
				INNER JOIN test_schema.river_job r2 ON r1.id = r2.id;
		`, updatedSQL)
	})

	t.Run("AllTemplatesStableCached", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithTemplates(ctx, map[string]Replacement{
			"schema": {Stable: true, Value: "test_schema."},
		}, nil)

		updatedSQL, args, err := replacer.RunSafely(ctx, `
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			SELECT count(*)
			FROM test_schema.river_job;
		`, updatedSQL)

		require.Len(t, replacer.cache, 1)

		// Invoke again to make sure we get back the same result.
		updatedSQL, args, err = replacer.RunSafely(ctx, `
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			SELECT count(*)
			FROM test_schema.river_job;
		`, updatedSQL)
	})

	t.Run("AnyTemplateNotStableNotCached", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithTemplates(ctx, map[string]Replacement{
			"schema":       {Stable: true, Value: "test_schema."},
			"where_clause": {Value: "kind = 'no_op'"},
		}, nil)

		updatedSQL, args, err := replacer.RunSafely(ctx, `
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ 1 /* TEMPLATE_END */;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			SELECT count(*)
			FROM test_schema.river_job
			WHERE kind = 'no_op';
		`, updatedSQL)

		require.Empty(t, replacer.cache)
	})

	t.Run("NamedArgsNoInitialArgs", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithTemplates(ctx, map[string]Replacement{
			"where_clause": {Value: "kind = @kind"},
		}, map[string]any{
			"kind": "no_op",
		})

		updatedSQL, args, err := replacer.RunSafely(ctx, `
			SELECT count(*)
			FROM river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ 1 /* TEMPLATE_END */;
		`, nil)
		require.NoError(t, err)
		require.Equal(t, []any{"no_op"}, args)
		require.Equal(t, `
			SELECT count(*)
			FROM river_job
			WHERE kind = $1;
		`, updatedSQL)
	})

	t.Run("NamedArgsWithInitialArgs", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithTemplates(ctx, map[string]Replacement{
			"where_clause": {Value: "kind = @kind"},
		}, map[string]any{
			"kind": "no_op",
		})

		updatedSQL, args, err := replacer.RunSafely(ctx, `
			SELECT count(*)
			FROM river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ 1 /* TEMPLATE_END */
				AND status = $1;
		`, []any{"succeeded"})
		require.NoError(t, err)
		require.Equal(t, []any{"succeeded", "no_op"}, args)
		require.Equal(t, `
			SELECT count(*)
			FROM river_job
			WHERE kind = $2
				AND status = $1;
		`, updatedSQL)
	})

	t.Run("MultipleWithTemplatesOverrides", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithTemplates(ctx, map[string]Replacement{
			"schema":       {Stable: true, Value: "test_schema."},
			"where_clause": {Value: "kind = @kind"},
		}, map[string]any{
			"kind": "no_op",
		})

		ctx = WithTemplates(ctx, map[string]Replacement{
			"where_clause": {Value: "kind = @kind AND status = @status"},
		}, map[string]any{
			"status": "succeeded",
		})

		updatedSQL, args, err := replacer.RunSafely(ctx, `
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ 1 /* TEMPLATE_END */;
		`, nil)
		require.NoError(t, err)
		require.Equal(t, []any{"no_op", "succeeded"}, args)
		require.Equal(t, `
			SELECT count(*)
			FROM test_schema.river_job
			WHERE kind = $1 AND status = $2;
		`, updatedSQL)
	})
}
