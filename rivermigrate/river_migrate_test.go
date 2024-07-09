package rivermigrate

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"slices"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
)

const (
	// The name of an actual migration line embedded in our test data below.
	migrationLineAlternate           = "alternate"
	migrationLineAlternateMaxVersion = 6
)

//go:embed migration/*/*.sql
var migrationFS embed.FS

// A test driver with the same migrations as the standard Pgx driver, but which
// includes an alternate line so we can test that those work.
type driverWithAlternateLine struct {
	*riverpgxv5.Driver
}

func (d *driverWithAlternateLine) GetMigrationFS(line string) fs.FS {
	switch line {
	case riverdriver.MigrationLineMain:
		return d.Driver.GetMigrationFS(line)
	case migrationLineAlternate:
		return migrationFS
	}
	panic("migration line does not exist: " + line)
}

func (d *driverWithAlternateLine) GetMigrationLines() []string {
	return append(d.Driver.GetMigrationLines(), migrationLineAlternate)
}

func TestMigrator(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	migrationsBundle := buildTestMigrationsBundle(t)

	type testBundle struct {
		dbPool *pgxpool.Pool
		driver *driverWithAlternateLine
		logger *slog.Logger
		tx     pgx.Tx
	}

	setup := func(t *testing.T) (*Migrator[pgx.Tx], *testBundle) {
		t.Helper()

		// The test suite largely works fine with test transactions, but due to
		// the invasive nature of changing schemas, it's quite easy to have test
		// transactions deadlock with each other as they run in parallel. Here
		// we use test DBs instead of test transactions, but this could be
		// changed to test transactions as long as test cases were made to run
		// non-parallel.
		dbPool := riverinternaltest.TestDB(ctx, t)

		// Despite being in an isolated database, we still start a transaction
		// because we don't want schema changes we make to persist.
		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback(ctx) })

		bundle := &testBundle{
			dbPool: dbPool,
			driver: &driverWithAlternateLine{Driver: riverpgxv5.New(dbPool)},
			logger: riversharedtest.Logger(t),
			tx:     tx,
		}

		migrator := New(bundle.driver, &Config{Logger: bundle.logger})
		migrator.migrations = migrationsBundle.WithTestVersionsMap

		return migrator, bundle
	}

	// Gets a migrator using the driver for `database/sql`.
	setupDatabaseSQLMigrator := func(t *testing.T, bundle *testBundle) (*Migrator[*sql.Tx], *sql.Tx) {
		t.Helper()

		stdPool := stdlib.OpenDBFromPool(bundle.dbPool)
		t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

		tx, err := stdPool.BeginTx(ctx, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tx.Rollback()) })

		driver := riverdatabasesql.New(stdPool)
		migrator := New(driver, &Config{Logger: bundle.logger})
		migrator.migrations = migrationsBundle.WithTestVersionsMap

		return migrator, tx
	}

	t.Run("AllVersions", func(t *testing.T) {
		t.Parallel()

		migrator, _ := setup(t)

		migrations := migrator.AllVersions()
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion), sliceutil.Map(migrations, migrationToInt))
	})

	t.Run("MigrateDownDefault", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// The migration version in which `river_job` comes in.
		const migrateVersionIncludingRiverJob = 2

		// Run two initial times to get to the version before river_job is dropped.
		// Defaults to only running one step when moving in the down direction.
		for i := migrationsBundle.MaxVersion; i > migrateVersionIncludingRiverJob; i-- {
			res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{})
			require.NoError(t, err)
			require.Equal(t, DirectionDown, res.Direction)
			require.Equal(t, []int{i}, sliceutil.Map(res.Versions, migrateVersionToInt))

			err = dbExecError(ctx, bundle.driver.UnwrapExecutor(bundle.tx), "SELECT * FROM river_job")
			require.NoError(t, err)
		}

		// Run once more to go down one more step
		{
			res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{})
			require.NoError(t, err)
			require.Equal(t, DirectionDown, res.Direction)
			require.Equal(t, []int{migrateVersionIncludingRiverJob}, sliceutil.Map(res.Versions, migrateVersionToInt))

			err = dbExecError(ctx, bundle.driver.UnwrapExecutor(bundle.tx), "SELECT * FROM river_job")
			require.Error(t, err)
		}
	})

	t.Run("MigrateDownAfterUp", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.WithTestVersionsMaxVersion}, sliceutil.Map(res.Versions, migrateVersionToInt))
	})

	t.Run("MigrateDownWithMaxSteps", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{MaxSteps: 2})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.WithTestVersionsMaxVersion, migrationsBundle.WithTestVersionsMaxVersion - 1},
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion-2),
			sliceutil.Map(migrations, driverMigrationToInt))

		err = dbExecError(ctx, bundle.driver.UnwrapExecutor(bundle.tx), "SELECT name FROM test_table")
		require.Error(t, err)
	})

	t.Run("MigrateDownWithPool", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// We don't actually migrate anything (max steps = -1) because doing so
		// would mess with the test database, but this still runs most code to
		// check that the function generally works.
		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{MaxSteps: -1})
		require.NoError(t, err)
		require.Equal(t, []int{}, sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("MigrateDownWithDatabaseSQLDriver", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)
		migrator, tx := setupDatabaseSQLMigrator(t, bundle)

		res, err := migrator.MigrateTx(ctx, tx, DirectionDown, &MigrateOpts{MaxSteps: 1})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.MaxVersion}, sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := migrator.driver.UnwrapExecutor(tx).MigrationGetAllAssumingMain(ctx)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion-1),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("MigrateDownWithTargetVersion", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{TargetVersion: 4})
		require.NoError(t, err)
		require.Equal(t, seqDownTo(migrationsBundle.WithTestVersionsMaxVersion, 5),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetAllAssumingMain(ctx)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(4),
			sliceutil.Map(migrations, driverMigrationToInt))

		err = dbExecError(ctx, bundle.driver.UnwrapExecutor(bundle.tx), "SELECT name FROM test_table")
		require.Error(t, err)
	})

	t.Run("MigrateDownWithTargetVersionMinusOne", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{TargetVersion: -1})
		require.NoError(t, err)
		require.Equal(t, seqDownTo(migrationsBundle.WithTestVersionsMaxVersion, 1),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		err = dbExecError(ctx, bundle.driver.UnwrapExecutor(bundle.tx), "SELECT name FROM river_migrate")
		require.Error(t, err)
	})

	t.Run("MigrateDownWithTargetVersionInvalid", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// migration doesn't exist
		{
			_, err := migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{TargetVersion: migrationsBundle.MaxVersion + 77})
			require.EqualError(t, err, fmt.Sprintf("version %d is not a valid River migration version", migrationsBundle.MaxVersion+77))
		}

		// migration exists but not one that's applied
		{
			_, err := migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{TargetVersion: migrationsBundle.MaxVersion + 1})
			require.EqualError(t, err, fmt.Sprintf("version %d is not in target list of valid migrations to apply", migrationsBundle.MaxVersion+1))
		}
	})

	t.Run("MigrateDownDryRun", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{DryRun: true})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.WithTestVersionsMaxVersion}, sliceutil.Map(res.Versions, migrateVersionToInt))

		// Migrate down returned a result above for a migration that was
		// removed, but because we're in a dry run, the database still shows
		// this version.
		migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("GetVersion", func(t *testing.T) {
		t.Parallel()

		migrator, _ := setup(t)

		{
			migrateVersion, err := migrator.GetVersion(migrationsBundle.WithTestVersionsMaxVersion)
			require.NoError(t, err)
			require.Equal(t, migrationsBundle.WithTestVersionsMaxVersion, migrateVersion.Version)
		}

		{
			_, err := migrator.GetVersion(99_999)
			availableVersions := seqOneTo(migrationsBundle.WithTestVersionsMaxVersion)
			require.EqualError(t, err, fmt.Sprintf("migration %d not found (available versions: %v)", 99_999, availableVersions))
		}
	})

	t.Run("MigrateNilOpts", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, nil)
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.MaxVersion + 1, migrationsBundle.MaxVersion + 2}, sliceutil.Map(res.Versions, migrateVersionToInt))
	})

	t.Run("MigrateUpDefault", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// Run an initial time
		{
			res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
			require.NoError(t, err)
			require.Equal(t, DirectionUp, res.Direction)
			require.Equal(t, []int{migrationsBundle.WithTestVersionsMaxVersion - 1, migrationsBundle.WithTestVersionsMaxVersion},
				sliceutil.Map(res.Versions, migrateVersionToInt))

			migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
			require.NoError(t, err)
			require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion),
				sliceutil.Map(migrations, driverMigrationToInt))

			_, err = bundle.tx.Exec(ctx, "SELECT * FROM test_table")
			require.NoError(t, err)
		}

		// Run once more to verify idempotency
		{
			res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
			require.NoError(t, err)
			require.Equal(t, DirectionUp, res.Direction)
			require.Equal(t, []int{}, sliceutil.Map(res.Versions, migrateVersionToInt))

			migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
			require.NoError(t, err)
			require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion),
				sliceutil.Map(migrations, driverMigrationToInt))

			_, err = bundle.tx.Exec(ctx, "SELECT * FROM test_table")
			require.NoError(t, err)
		}
	})

	t.Run("MigrateUpWithMaxSteps", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{MaxSteps: 1})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.WithTestVersionsMaxVersion - 1},
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion-1),
			sliceutil.Map(migrations, driverMigrationToInt))

		// Column `name` is only added in the second test version.
		err = dbExecError(ctx, bundle.driver.UnwrapExecutor(bundle.tx), "SELECT name FROM test_table")
		require.Error(t, err)

		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.UndefinedColumn, pgErr.Code)
	})

	t.Run("MigrateUpWithPool", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// We don't actually migrate anything (max steps = -1) because doing so
		// would mess with the test database, but this still runs most code to
		// check that the function generally works.
		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: -1})
		require.NoError(t, err)
		require.Equal(t, []int{}, sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("MigrateUpWithDatabaseSQLDriver", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)
		migrator, tx := setupDatabaseSQLMigrator(t, bundle)

		res, err := migrator.MigrateTx(ctx, tx, DirectionUp, &MigrateOpts{MaxSteps: 1})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.MaxVersion + 1}, sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := migrator.driver.UnwrapExecutor(tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion+1),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("MigrateUpWithTargetVersion", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{TargetVersion: migrationsBundle.MaxVersion + 2})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.MaxVersion + 1, migrationsBundle.MaxVersion + 2},
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion+2), sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("MigrateUpWithTargetVersionInvalid", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// migration doesn't exist
		{
			_, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{TargetVersion: 77})
			require.EqualError(t, err, "version 77 is not a valid River migration version")
		}

		// migration exists but already applied
		{
			_, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{TargetVersion: 3})
			require.EqualError(t, err, "version 3 is not in target list of valid migrations to apply")
		}
	})

	t.Run("MigrateUpDryRun", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{DryRun: true})
		require.NoError(t, err)
		require.Equal(t, DirectionUp, res.Direction)
		require.Equal(t, []int{migrationsBundle.WithTestVersionsMaxVersion - 1, migrationsBundle.WithTestVersionsMaxVersion},
			sliceutil.Map(res.Versions, migrateVersionToInt))

		// Migrate up returned a result above for migrations that were applied,
		// but because we're in a dry run, the database still shows the test
		// migration versions not applied.
		migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("ValidateSuccess", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// Migrate all the way up.
		_, err := migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.ValidateTx(ctx, bundle.tx)
		require.NoError(t, err)
		require.Equal(t, &ValidateResult{OK: true}, res)
	})

	t.Run("ValidateUnappliedMigrations", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		res, err := migrator.ValidateTx(ctx, bundle.tx)
		require.NoError(t, err)
		require.Equal(t, &ValidateResult{
			Messages: []string{fmt.Sprintf("Unapplied migrations: [%d %d]", migrationsBundle.MaxVersion+1, migrationsBundle.MaxVersion+2)},
		}, res)
	})

	t.Run("MigrateDownToZeroAndBackUp", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		requireMigrationTableExists := func(expectedExists bool) {
			migrationExists, err := bundle.driver.UnwrapExecutor(bundle.tx).TableExists(ctx, "river_migration")
			require.NoError(t, err)
			require.Equal(t, expectedExists, migrationExists)
		}

		requireMigrationTableExists(true)

		res, err := migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{TargetVersion: -1})
		require.NoError(t, err)
		require.Equal(t, seqDownTo(migrationsBundle.MaxVersion, 1),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		requireMigrationTableExists(false)

		res, err = migrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("AlternateLineUpAndDown", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		// We have to reinitialize the alternateMigrator because the migrations bundle is
		// set in the constructor.
		alternateMigrator := New(bundle.driver, &Config{
			Line:   migrationLineAlternate,
			Logger: bundle.logger,
		})

		res, err := alternateMigrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationLineAlternateMaxVersion),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, migrationLineAlternate)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationLineAlternateMaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))

		res, err = alternateMigrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{TargetVersion: -1})
		require.NoError(t, err)
		require.Equal(t, seqDownTo(migrationLineAlternateMaxVersion, 1),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		// The main migration line should not have been touched.
		migrations, err = bundle.driver.UnwrapExecutor(bundle.tx).MigrationGetByLine(ctx, riverdriver.MigrationLineMain)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("AlternateLineBeforeLineColumn", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// Main line to just before the `line` column was added.
		_, err := migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{TargetVersion: 4})
		require.NoError(t, err)

		alternateMigrator := New(bundle.driver, &Config{
			Line:   migrationLineAlternate,
			Logger: bundle.logger,
		})

		// Alternate line not allowed because `river_job.line` doesn't exist.
		_, err = alternateMigrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
		require.EqualError(t, err, "can't add a non-main migration line until `river_migration.line` is raised; fully migrate the main migration line and try again")

		// Main line to zero.
		_, err = migrator.MigrateTx(ctx, bundle.tx, DirectionDown, &MigrateOpts{TargetVersion: -1})
		require.NoError(t, err)

		// Alternate line not allowed because `river_job` doesn't exist.
		_, err = alternateMigrator.MigrateTx(ctx, bundle.tx, DirectionUp, &MigrateOpts{})
		require.EqualError(t, err, "can't add a non-main migration line until `river_migration` is raised; fully migrate the main migration line and try again")
	})
}

// This test uses a custom set of test-only migration files on the file system
// in `rivermigrate/migrate/*`.
func TestMigrationsFromFS(t *testing.T) {
	t.Parallel()

	t.Run("Main", func(t *testing.T) {
		t.Parallel()

		// This is not the actual main line, but rather one embedded in this
		// package's test data (see `rivermigrate/migrate/*`).
		migrations, err := migrationsFromFS(migrationFS, "main")
		require.NoError(t, err)
		require.Equal(t, []int{1, 2}, sliceutil.Map(migrations, migrationToInt))
	})

	t.Run("Alternate", func(t *testing.T) {
		t.Parallel()

		migrations, err := migrationsFromFS(migrationFS, migrationLineAlternate)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationLineAlternateMaxVersion), sliceutil.Map(migrations, migrationToInt))
	})

	t.Run("DoesNotExist", func(t *testing.T) {
		t.Parallel()

		_, err := migrationsFromFS(migrationFS, "does_not_exist")
		require.EqualError(t, err, `no migrations found for line: "does_not_exist"`)
	})
}

// A bundle of migrations for use in tests. An original set of migrations are
// read from riverpgxv5, then augmented with a couple additional migrations used
// for test purposes.
type testMigrationsBundle struct {
	MaxVersion                 int
	WithTestVersionsMap        map[int]Migration
	WithTestVersionsMaxVersion int
}

func buildTestMigrationsBundle(t *testing.T) *testMigrationsBundle {
	t.Helper()

	// `migration/` subdir is added by migrationsFromFS
	migrationFS := os.DirFS("../riverdriver/riverpgxv5")

	migrations, err := migrationsFromFS(migrationFS, riverdriver.MigrationLineMain)
	require.NoError(t, err)

	// We base our test migrations on the actual line of migrations, so get
	// their maximum version number which we'll use to define test version
	// numbers so that the tests don't break anytime we add a new one.
	migrationsMaxVersion := migrations[len(migrations)-1].Version

	testVersions := []Migration{
		{
			Version: migrationsMaxVersion + 1,
			SQLUp:   "CREATE TABLE test_table(id bigserial PRIMARY KEY);",
			SQLDown: "DROP TABLE test_table;",
		},
		{
			Version: migrationsMaxVersion + 2,
			SQLUp:   "ALTER TABLE test_table ADD COLUMN name varchar(200); CREATE INDEX idx_test_table_name ON test_table(name);",
			SQLDown: "DROP INDEX idx_test_table_name; ALTER TABLE test_table DROP COLUMN name;",
		},
	}

	return &testMigrationsBundle{
		MaxVersion:                 migrationsMaxVersion,
		WithTestVersionsMap:        validateAndInit(append(migrations, testVersions...)),
		WithTestVersionsMaxVersion: migrationsMaxVersion + len(testVersions),
	}
}

// A command returning an error aborts the transaction. This is a shortcut to
// execute a command in a subtransaction so that we can verify an error, but
// continue to use the original transaction.
func dbExecError(ctx context.Context, exec riverdriver.Executor, sql string) error {
	return dbutil.WithTx(ctx, exec, func(ctx context.Context, exec riverdriver.ExecutorTx) error {
		_, err := exec.Exec(ctx, sql)
		return err
	})
}

func driverMigrationToInt(r *riverdriver.Migration) int { return r.Version }
func migrationToInt(migration Migration) int            { return migration.Version }

// Produces a sequence down to one. Max is included.
func seqOneTo(max int) []int {
	seq := make([]int, 0, max)

	for i := 1; i <= max; i++ {
		seq = append(seq, i)
	}

	return seq
}

func seqDownTo(max, min int) []int {
	seq := make([]int, 0, max-min+1)

	for i := min; i <= max; i++ {
		seq = append(seq, i)
	}

	slices.Reverse(seq)
	return seq
}
