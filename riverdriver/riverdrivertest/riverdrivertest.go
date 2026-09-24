package riverdrivertest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
)

// Exercise fully exercises a driver. The driver's listener is exercised if
// supported.
func Exercise[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string),
	executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx]),
) {
	t.Helper()

	{
		driver, _ := driverWithSchema(ctx, t, nil)
		if driver.SupportsListener() {
			exerciseListener(ctx, t, driverWithSchema)
		} else {
			t.Logf("Driver does not support listener; skipping listener tests")
		}
	}

	exerciseDriverPool(ctx, t, driverWithSchema, executorWithTx)
	exerciseMigration(ctx, t, driverWithSchema, executorWithTx)
	exerciseNotification(ctx, t, executorWithTx)
	exerciseSQLFragments(ctx, t, executorWithTx)
	exerciseExecutorTx(ctx, t, driverWithSchema, executorWithTx)
	exerciseSchemaIntrospection(ctx, t, driverWithSchema, executorWithTx)
	exerciseSchemaName(ctx, t, driverWithSchema)
	exerciseJobInsert(ctx, t, driverWithSchema, executorWithTx)
	exerciseJobRead(ctx, t, executorWithTx)
	exerciseJobUpdate(ctx, t, executorWithTx)
	exerciseJobDelete(ctx, t, executorWithTx)
	exerciseLeader(ctx, t, executorWithTx)
	exerciseQueue(ctx, t, executorWithTx)
}

const testClientID = "test-client-id"

func exerciseDriverPool[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string),
	executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx]),
) {
	t.Helper()

	t.Run("PoolIsSet", func(t *testing.T) {
		t.Parallel()

		t.Run("PoolIsSetOnDriverWithSchema", func(t *testing.T) {
			t.Parallel()

			driver, _ := driverWithSchema(ctx, t, nil)
			require.True(t, driver.PoolIsSet())
		})
	})

	t.Run("PoolSet", func(t *testing.T) {
		t.Parallel()

		t.Run("PoolSetNotImplementedOrAlreadySetError", func(t *testing.T) {
			t.Parallel()

			driver, _ := driverWithSchema(ctx, t, nil)
			err := driver.PoolSet(struct{}{})
			require.Error(t, err)
			if !errors.Is(err, riverdriver.ErrNotImplemented) {
				require.EqualError(t, err, "cannot PoolSet when internal pool is already non-nil")
			}
		})
	})

	t.Run("SupportsListenNotify", func(t *testing.T) {
		t.Parallel()

		_, driver := executorWithTx(ctx, t)

		switch driver.DatabaseName() {
		case riverdriver.DatabaseNamePostgres:
			require.True(t, driver.SupportsListenNotify())
		case riverdriver.DatabaseNameSQLite:
			require.True(t, driver.SupportsListenNotify())
		default:
			require.FailNow(t, "Don't know how to check SupportsListenNotify for: "+driver.DatabaseName())
		}
	})
}

func requireMissingRelation(t *testing.T, err error, schema, missingRelation string) {
	t.Helper()

	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		require.Equal(t, pgerrcode.UndefinedTable, pgErr.Code)
		require.Equal(t, fmt.Sprintf(`relation "%s.%s" does not exist`, schema, missingRelation), pgErr.Message)
	} else {
		// lib/pq: pq: relation %s.%s does not exist
		// SQLite: no such table: %s.%s
		// Turso: turso: error: Invalid argument supplied: no such database: %s
		require.Regexp(t, fmt.Sprintf(`(pq: relation "%s\.%s" does not exist|no such table: %s\.%s|no such database: %s)`, schema, missingRelation, schema, missingRelation, schema), err.Error())
	}
}

// sqliteJobJSONColumns are the columns of a SQLite job row that hold JSON.
//
//nolint:gochecknoglobals
var sqliteJobJSONColumns = []string{"args", "attempted_by", "errors", "metadata", "tags"}

// sqliteMalformedValue is text that isn't valid JSON. Stored in one of a SQLite
// job row's JSON columns, it's rejected with a "malformed JSON" error by any of
// SQLite's JSON functions that touch it.
const sqliteMalformedValue = "not json"

// sqliteJobColumnText returns a column of a SQLite job row cast to text, which
// can be used to check that a value that isn't valid JSON (and so can't be
// read through the driver) was left in place.
func sqliteJobColumnText(ctx context.Context, t *testing.T, exec riverdriver.Executor, jobID int64, column string) string {
	t.Helper()

	var value string
	require.NoError(t, exec.QueryRow(ctx, "SELECT cast("+column+" AS text) FROM river_job WHERE id = ?", jobID).Scan(&value))
	return value
}

// sqliteSetJobColumnMalformed overwrites a column of a SQLite job row with
// sqliteMalformedValue as text, simulating a JSON column changed out of band to
// a value that isn't valid JSON. Postgres' column types don't allow the
// equivalent.
func sqliteSetJobColumnMalformed(ctx context.Context, t *testing.T, exec riverdriver.Executor, jobID int64, column string) {
	t.Helper()

	require.NoError(t, exec.Exec(ctx, "UPDATE river_job SET "+column+" = ? WHERE id = ?", sqliteMalformedValue, jobID))
}

// sqliteSetJobJSONColumn overwrites a JSON column of a SQLite job row with the
// given JSON, simulating a row changed out of band into a shape that River
// can't decode. Postgres' column types don't allow the equivalent.
func sqliteSetJobJSONColumn(ctx context.Context, t *testing.T, exec riverdriver.Executor, jobID int64, column, jsonValue string) {
	t.Helper()

	require.NoError(t, exec.Exec(ctx, "UPDATE river_job SET "+column+" = jsonb(?) WHERE id = ?", jsonValue, jobID))
}
