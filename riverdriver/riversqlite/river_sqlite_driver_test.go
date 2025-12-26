package riversqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

// Verify interface compliance.
var _ riverdriver.Driver[*sql.Tx] = New(nil)

func TestDurationAsString(t *testing.T) {
	t.Parallel()

	require.Equal(t, "3.000 seconds", durationAsString(3*time.Second))
	require.Equal(t, "3.255 seconds", durationAsString(3*time.Second+255*time.Millisecond))
}

func TestAddQueuesClauseSQL(t *testing.T) {
	t.Parallel()

	t.Run("IsExcludedFalse", func(t *testing.T) {
		t.Parallel()

		var (
			replacements = make(map[string]sqlctemplate.Replacement)
			namedArgs    = make(map[string]any)
		)

		require.NoError(t, addQueuesClauseSQL(replacements, namedArgs, "is_excluded_clause", "queue", []string{"queue_a", "queue_b"}, false))

		require.Equal(t, map[string]sqlctemplate.Replacement{
			"is_excluded_clause": {
				Value: `
		EXISTS (
			SELECT 1
			FROM json_each(@is_excluded_clause_arg)
			WHERE json_each.value = queue
		)
	`,
			},
		}, replacements)
		require.Equal(t, `["queue_a","queue_b"]`, string(namedArgs["is_excluded_clause_arg"].([]byte))) //nolint:forcetypeassert
	})

	t.Run("IsExcludedTrue", func(t *testing.T) {
		t.Parallel()

		var (
			replacements = make(map[string]sqlctemplate.Replacement)
			namedArgs    = make(map[string]any)
		)

		require.NoError(t, addQueuesClauseSQL(replacements, namedArgs, "is_excluded_clause", "queue", []string{"queue_a", "queue_b"}, true))

		require.Equal(t, map[string]sqlctemplate.Replacement{
			"is_excluded_clause": {
				Value: `
		NOT EXISTS (
			SELECT 1
			FROM json_each(@is_excluded_clause_arg)
			WHERE json_each.value = queue
		)
	`,
			},
		}, replacements)
		require.Equal(t, `["queue_a","queue_b"]`, string(namedArgs["is_excluded_clause_arg"].([]byte))) //nolint:forcetypeassert
	})
}

func TestInterpretError(t *testing.T) {
	t.Parallel()

	require.EqualError(t, interpretError(errors.New("an error")), "an error")
	require.ErrorIs(t, interpretError(sql.ErrNoRows), rivertype.ErrNotFound)
	require.NoError(t, interpretError(nil))
}

func TestTimeString(t *testing.T) {
	t.Parallel()

	require.Equal(t, "2025-04-30 13:26:39.123", timeString(time.Date(2025, 4, 30, 13, 26, 39, 123456789, time.UTC)))
	require.Equal(t, "2025-04-30 13:26:39.124", timeString(time.Date(2025, 4, 30, 13, 26, 39, 123800000, time.UTC))) // test rounding
}

func TestTimeStringNullable(t *testing.T) {
	t.Parallel()

	require.Nil(t, timeStringNullable(nil))
	require.Equal(t, "2025-04-30 13:26:39.123", *timeStringNullable(ptrutil.Ptr(time.Date(2025, 4, 30, 13, 26, 39, 123456789, time.UTC))))
	require.Equal(t, "2025-04-30 13:26:39.124", *timeStringNullable(ptrutil.Ptr(time.Date(2025, 4, 30, 13, 26, 39, 123800000, time.UTC)))) // test rounding
}

func TestSchemaTemplateParam(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		driver *Driver
	}

	setup := func(t *testing.T) (*sqlctemplate.Replacer, *testBundle) {
		t.Helper()

		return &sqlctemplate.Replacer{}, &testBundle{
			driver: New(nil),
		}
	}

	t.Run("NoSchema", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		updatedSQL, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, ""),
			bundle.driver.ArgPlaceholder(),
			"SELECT 1 FROM /* TEMPLATE: schema */river_job",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1 FROM river_job", updatedSQL)
	})

	t.Run("WithSchema", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		updatedSQL, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, "custom_schema"),
			bundle.driver.ArgPlaceholder(),
			"SELECT 1 FROM /* TEMPLATE: schema */river_job",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1 FROM custom_schema.river_job", updatedSQL)
	})
}
