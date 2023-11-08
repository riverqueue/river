package dbmigrate

import (
	"context"
	"slices"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/internal/util/sliceutil"
)

//nolint:gochecknoglobals
var (
	// We base our test migrations on the actual line of migrations, so get
	// their maximum version number which we'll use to define test version
	// numbers so that the tests don't break anytime we add a new one.
	riverMigrationsMaxVersion = riverMigrations[len(riverMigrations)-1].Version

	testVersions = validateAndInit(append(riverMigrations, []*migrationBundle{
		{
			Version: riverMigrationsMaxVersion + 1,
			Up:      "CREATE TABLE test_table(id bigserial PRIMARY KEY);",
			Down:    "DROP TABLE test_table;",
		},
		{
			Version: riverMigrationsMaxVersion + 2,
			Up:      "ALTER TABLE test_table ADD COLUMN name varchar(200); CREATE INDEX idx_test_table_name ON test_table(name);",
			Down:    "DROP INDEX idx_test_table_name; ALTER TABLE test_table DROP COLUMN name;",
		},
	}...))
)

func TestMigrator(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		queries = dbsqlc.New()
	)

	type testBundle struct {
		tx pgx.Tx
	}

	setup := func(t *testing.T) (*Migrator, *testBundle) {
		t.Helper()

		// The test suite largely works fine with test transactions, bue due to
		// the invasive nature of changing schemas, it's quite easy to have test
		// transactions deadlock with each other as they run in parallel. Here
		// we use test DBs instead of test transactions, but this could be
		// changed to test transactions as long as test cases were made to run
		// non-parallel.
		testDB := riverinternaltest.TestDB(ctx, t)

		// Despite being in an isolated database, we still start a transaction
		// because we don't want schema changes we make to persist.
		tx, err := testDB.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback(ctx) })

		bundle := &testBundle{
			tx: tx,
		}

		migrator := NewMigrator(riverinternaltest.BaseServiceArchetype(t))
		migrator.migrations = testVersions

		return migrator, bundle
	}

	t.Run("Down", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// Run an initial time
		{
			res, err := migrator.Down(ctx, bundle.tx, &MigrateOptions{})
			require.NoError(t, err)
			require.Equal(t, seqToOne(2), res.Versions)

			err = dbExecError(ctx, bundle.tx, "SELECT * FROM river_migration")
			require.Error(t, err)
		}

		// Run once more to verify idempotency
		{
			res, err := migrator.Down(ctx, bundle.tx, &MigrateOptions{})
			require.NoError(t, err)
			require.Equal(t, []int64{}, res.Versions)

			err = dbExecError(ctx, bundle.tx, "SELECT * FROM river_migration")
			require.Error(t, err)
		}
	})

	t.Run("DownAfterUp", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.Up(ctx, bundle.tx, &MigrateOptions{})
		require.NoError(t, err)

		res, err := migrator.Down(ctx, bundle.tx, &MigrateOptions{})
		require.NoError(t, err)
		require.Equal(t, seqToOne(riverMigrationsMaxVersion+2), res.Versions)

		err = dbExecError(ctx, bundle.tx, "SELECT * FROM river_migration")
		require.Error(t, err)
	})

	t.Run("DownWithMaxSteps", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.Up(ctx, bundle.tx, &MigrateOptions{})
		require.NoError(t, err)

		res, err := migrator.Down(ctx, bundle.tx, &MigrateOptions{MaxSteps: ptrutil.Ptr(1)})
		require.NoError(t, err)
		require.Equal(t, []int64{riverMigrationsMaxVersion + 2}, res.Versions)

		migrations, err := queries.RiverMigrationGetAll(ctx, bundle.tx)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(riverMigrationsMaxVersion+1), migrationVersions(migrations))

		err = dbExecError(ctx, bundle.tx, "SELECT name FROM test_table")
		require.Error(t, err)
	})

	t.Run("DownWithMaxStepsZero", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.Up(ctx, bundle.tx, &MigrateOptions{})
		require.NoError(t, err)

		res, err := migrator.Down(ctx, bundle.tx, &MigrateOptions{MaxSteps: ptrutil.Ptr(0)})
		require.NoError(t, err)
		require.Equal(t, []int64{}, res.Versions)
	})

	t.Run("Up", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// Run an initial time
		{
			res, err := migrator.Up(ctx, bundle.tx, &MigrateOptions{})
			require.NoError(t, err)
			require.Equal(t, []int64{riverMigrationsMaxVersion + 1, riverMigrationsMaxVersion + 2}, res.Versions)

			migrations, err := queries.RiverMigrationGetAll(ctx, bundle.tx)
			require.NoError(t, err)
			require.Equal(t, seqOneTo(riverMigrationsMaxVersion+2), migrationVersions(migrations))

			_, err = bundle.tx.Exec(ctx, "SELECT * FROM test_table")
			require.NoError(t, err)
		}

		// Run once more to verify idempotency
		{
			res, err := migrator.Up(ctx, bundle.tx, &MigrateOptions{})
			require.NoError(t, err)
			require.Equal(t, []int64{}, res.Versions)

			migrations, err := queries.RiverMigrationGetAll(ctx, bundle.tx)
			require.NoError(t, err)
			require.Equal(t, seqOneTo(riverMigrationsMaxVersion+2), migrationVersions(migrations))

			_, err = bundle.tx.Exec(ctx, "SELECT * FROM test_table")
			require.NoError(t, err)
		}
	})

	t.Run("UpWithMaxSteps", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		res, err := migrator.Up(ctx, bundle.tx, &MigrateOptions{MaxSteps: ptrutil.Ptr(1)})
		require.NoError(t, err)
		require.Equal(t, []int64{riverMigrationsMaxVersion + 1}, res.Versions)

		migrations, err := queries.RiverMigrationGetAll(ctx, bundle.tx)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(riverMigrationsMaxVersion+1), migrationVersions(migrations))

		// Column `name` is only added in the second test version.
		err = dbExecError(ctx, bundle.tx, "SELECT name FROM test_table")
		require.Error(t, err)

		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.UndefinedColumn, pgErr.Code)
	})

	t.Run("UpWithMaxStepsZero", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		res, err := migrator.Up(ctx, bundle.tx, &MigrateOptions{MaxSteps: ptrutil.Ptr(0)})
		require.NoError(t, err)
		require.Equal(t, []int64{}, res.Versions)
	})
}

// A command returning an error aborts the transaction. This is a shortcut to
// execute a command in a subtransaction so that we can verify an error, but
// continue to use the original transaction.
func dbExecError(ctx context.Context, executor dbutil.Executor, sql string) error {
	return dbutil.WithTx(ctx, executor, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, sql)
		return err
	})
}

func migrationVersions(migrations []*dbsqlc.RiverMigration) []int64 {
	return sliceutil.Map(migrations, func(r *dbsqlc.RiverMigration) int64 { return r.Version })
}

func seqOneTo(max int64) []int64 {
	seq := make([]int64, max)

	for i := 0; i < int(max); i++ {
		seq[i] = int64(i + 1)
	}

	return seq
}

func seqToOne(max int64) []int64 {
	seq := seqOneTo(max)
	slices.Reverse(seq)
	return seq
}
