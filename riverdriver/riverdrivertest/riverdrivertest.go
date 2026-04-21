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

const (
	databaseNameMySQL    = "mysql"
	databaseNamePostgres = "postgres"
	databaseNameSQLite   = "sqlite"
	testClientID         = "test-client-id"
)

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

	t.Run("SafeIdentifier", func(t *testing.T) {
		t.Parallel()

		_, driver := executorWithTx(ctx, t)

		switch driver.DatabaseName() {
		case databaseNamePostgres, databaseNameSQLite:
			require.Equal(t, `"my_schema"`, driver.SafeIdentifier("my_schema"))
			require.Equal(t, `"has space"`, driver.SafeIdentifier("has space"))
			require.Equal(t, `"has""quote"`, driver.SafeIdentifier(`has"quote`))
		case databaseNameMySQL:
			require.Equal(t, "`my_schema`", driver.SafeIdentifier("my_schema"))
			require.Equal(t, "`has space`", driver.SafeIdentifier("has space"))
			require.Equal(t, "`has``backtick`", driver.SafeIdentifier("has`backtick"))
		default:
			require.FailNow(t, "Don't know how to check SafeIdentifier for: "+driver.DatabaseName())
		}
	})

	t.Run("SupportsListenNotify", func(t *testing.T) {
		t.Parallel()

		_, driver := executorWithTx(ctx, t)

		switch driver.DatabaseName() {
		case databaseNamePostgres:
			require.True(t, driver.SupportsListenNotify())
		case databaseNameMySQL, databaseNameSQLite:
			require.False(t, driver.SupportsListenNotify())
		default:
			require.FailNow(t, "Don't know how to check SupportsListenNotify for: "+driver.DatabaseName())
		}
	})
}

func requireMissingRelation(t *testing.T, err error, schema, missingRelation string) {
	t.Helper()

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		require.Equal(t, pgerrcode.UndefinedTable, pgErr.Code)
		require.Equal(t, fmt.Sprintf(`relation "%s.%s" does not exist`, schema, missingRelation), pgErr.Message)
	} else {
		// lib/pq: pq: relation %s.%s does not exist
		// SQLite: no such table: %s.%s
		// MySQL: Unknown database '%s'
		require.Regexp(t, fmt.Sprintf(`(pq: relation "%s\.%s" does not exist|no such table: %s\.%s|Unknown database '%s')`, schema, missingRelation, schema, missingRelation, schema), err.Error())
	}
}
