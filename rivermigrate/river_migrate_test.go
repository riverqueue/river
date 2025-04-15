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

	"github.com/davecgh/go-spew/spew"
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
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
)

const (
	// The name of an actual migration line embedded in our test data below.
	migrationLineAlternate                = "alternate"
	migrationLineAlternateMaxVersion      = 6
	migrationLineCommitRequired           = "commit_required"
	migrationLineCommitRequiredMaxVersion = 3
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
	case migrationLineAlternate + "2":
		panic(line + " is only meant for testing line suggestions")
	case migrationLineCommitRequired:
		return migrationFS
	}
	panic("migration line does not exist: " + line)
}

func (d *driverWithAlternateLine) GetMigrationLines() []string {
	return append(d.Driver.GetMigrationLines(), migrationLineAlternate, migrationLineAlternate+"2", migrationLineCommitRequired)
}

func TestMigrator(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	migrationsBundle := buildTestMigrationsBundle(t)

	type testBundle struct {
		dbPool *pgxpool.Pool
		driver *driverWithAlternateLine
		logger *slog.Logger
		schema string
	}

	setup := func(t *testing.T) (*Migrator[pgx.Tx], *testBundle) {
		t.Helper()

		// Not all migrations can be executed together in a single transaction.
		// Examples include `CREATE INDEX CONCURRENTLY`, or adding an enum value
		// that's used by a later migration. As such, the migrator and its tests
		// must use a full database with commits between each migration.
		//
		// To make this easier to clean up afterward, we create a new, clean schema
		// for each test run and then drop it afterward.
		dbPool := riverinternaltest.TestDB(ctx, t)
		schema := "river_migrate_test_" + randutil.Hex(8)
		_, err := dbPool.Exec(ctx, "CREATE SCHEMA "+schema)
		require.NoError(t, err)

		t.Cleanup(func() {
			_, err := dbPool.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schema))
			require.NoError(t, err)
		})

		bundle := &testBundle{
			dbPool: dbPool,
			driver: &driverWithAlternateLine{Driver: riverpgxv5.New(dbPool)},
			logger: riversharedtest.Logger(t),
			schema: schema,
		}

		migrator, err := New(bundle.driver, &Config{
			Logger: bundle.logger,
			schema: schema,
		})
		require.NoError(t, err)
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
		migrator, err := New(driver, &Config{
			Logger: bundle.logger,
			schema: bundle.schema,
		})
		require.NoError(t, err)
		migrator.migrations = migrationsBundle.WithTestVersionsMap

		return migrator, tx
	}

	t.Run("NewUnknownLine", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		_, err := New(bundle.driver, &Config{Line: "unknown_line"})
		require.EqualError(t, err, "migration line does not exist: unknown_line")

		_, err = New(bundle.driver, &Config{Line: "mai"})
		require.EqualError(t, err, "migration line does not exist: mai (did you mean `main`?)")

		_, err = New(bundle.driver, &Config{Line: "maim"})
		require.EqualError(t, err, "migration line does not exist: maim (did you mean `main`?)")

		_, err = New(bundle.driver, &Config{Line: "maine"})
		require.EqualError(t, err, "migration line does not exist: maine (did you mean `main`?)")

		_, err = New(bundle.driver, &Config{Line: "ma"})
		require.EqualError(t, err, "migration line does not exist: ma (did you mean `main`?)")

		// Too far off.
		_, err = New(bundle.driver, &Config{Line: "m"})
		require.EqualError(t, err, "migration line does not exist: m")

		_, err = New(bundle.driver, &Config{Line: "alternat"})
		require.EqualError(t, err, "migration line does not exist: alternat (did you mean one of `alternate`, `alternate2`?)")
	})

	t.Run("AllVersions", func(t *testing.T) {
		t.Parallel()

		migrator, _ := setup(t)

		migrations := migrator.AllVersions()
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion), sliceutil.Map(migrations, migrationToInt))
	})

	t.Run("ExistingMigrationsDefault", func(t *testing.T) {
		t.Parallel()

		migrator, _ := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{TargetVersion: migrationsBundle.MaxVersion})
		require.NoError(t, err)

		migrations, err := migrator.ExistingVersions(ctx)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion), sliceutil.Map(migrations, migrationToInt))
	})

	t.Run("ExistingMigrationsEmpty", func(t *testing.T) {
		t.Parallel()

		migrator, _ := setup(t)

		migrations, err := migrator.ExistingVersions(ctx)
		require.NoError(t, err)
		require.Equal(t, []int{}, sliceutil.Map(migrations, migrationToInt))
	})

	t.Run("ExistingMigrationsTxDefaultLine", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{TargetVersion: migrationsBundle.MaxVersion})
		require.NoError(t, err)

		tx, err := bundle.dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tx.Rollback(ctx)) })

		migrations, err := migrator.ExistingVersionsTx(ctx, tx)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion), sliceutil.Map(migrations, migrationToInt))
	})

	t.Run("ExistingMigrationsTxEmpty", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		tx, err := bundle.dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tx.Rollback(ctx)) })

		migrations, err := migrator.ExistingVersionsTx(ctx, tx)
		require.NoError(t, err)
		require.Equal(t, []int{}, sliceutil.Map(migrations, migrationToInt))
	})

	t.Run("ExistingMigrationsTxFullyMigrated", func(t *testing.T) {
		t.Parallel()

		migrator, _ := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		migrations, err := migrator.ExistingVersions(ctx)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion), sliceutil.Map(migrations, migrationToInt))
	})

	t.Run("MigrateDownDefault", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// The migration version in which `river_job` comes in.
		const migrateVersionIncludingRiverJob = 2

		// Run two initial times to get to the version before river_job is dropped.
		// Defaults to only running one step when moving in the down direction.
		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: migrateVersionIncludingRiverJob})
		require.NoError(t, err)
		require.Equal(t, DirectionUp, res.Direction)
		require.Equal(t, seqOneTo(migrateVersionIncludingRiverJob), sliceutil.Map(res.Versions, migrateVersionToInt))

		err = dbExecError(ctx, bundle.driver.GetExecutor(), fmt.Sprintf("SELECT * FROM %s.river_job", bundle.schema))
		require.NoError(t, err)

		// Run once more to go down one more step
		{
			res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{})
			require.NoError(t, err)
			require.Equal(t, DirectionDown, res.Direction)
			require.Equal(t, []int{migrateVersionIncludingRiverJob}, sliceutil.Map(res.Versions, migrateVersionToInt))

			version := res.Versions[0]
			require.Equal(t, "initial schema", version.Name)

			err = dbExecError(ctx, bundle.driver.GetExecutor(), fmt.Sprintf("SELECT * FROM %s.river_job", bundle.schema))
			require.Error(t, err)
		}
	})

	t.Run("MigrateDownAfterUp", func(t *testing.T) {
		t.Parallel()

		migrator, _ := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.WithTestVersionsMaxVersion}, sliceutil.Map(res.Versions, migrateVersionToInt))
	})

	t.Run("MigrateDownWithMaxSteps", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{MaxSteps: 2})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.WithTestVersionsMaxVersion, migrationsBundle.WithTestVersionsMaxVersion - 1},
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line:   riverdriver.MigrationLineMain,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion-2),
			sliceutil.Map(migrations, driverMigrationToInt))

		err = dbExecError(ctx, bundle.driver.GetExecutor(), fmt.Sprintf("SELECT name FROM %s.test_table", bundle.schema))
		require.Error(t, err)
	})

	t.Run("MigrateDownWithPool", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: migrationsBundle.MaxVersion})
		require.NoError(t, err)

		// We don't actually migrate anything (max steps = -1) because doing so
		// would mess with the test database, but this still runs most code to
		// check that the function generally works.
		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{MaxSteps: -1})
		require.NoError(t, err)
		require.Equal(t, []int{}, sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line:   riverdriver.MigrationLineMain,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("MigrateDownWithDatabaseSQLDriver", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)
		migrator, tx := setupDatabaseSQLMigrator(t, bundle)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: migrationsBundle.MaxVersion})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{MaxSteps: 1})
		require.NoError(t, err)
		spew.Dump(res.Versions)
		require.Equal(t, []int{migrationsBundle.MaxVersion}, sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := migrator.driver.UnwrapExecutor(tx).MigrationGetAllAssumingMain(ctx, &riverdriver.MigrationGetAllAssumingMainParams{
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion-1),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("MigrateDownWithTargetVersion", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{TargetVersion: 4})
		require.NoError(t, err)
		require.Equal(t, seqDownTo(migrationsBundle.WithTestVersionsMaxVersion, 5),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.GetExecutor().MigrationGetAllAssumingMain(ctx, &riverdriver.MigrationGetAllAssumingMainParams{
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(4),
			sliceutil.Map(migrations, driverMigrationToInt))

		err = dbExecError(ctx, bundle.driver.GetExecutor(), fmt.Sprintf("SELECT name FROM %s.test_table", bundle.schema))
		require.Error(t, err)
	})

	t.Run("MigrateDownWithTargetVersionMinusOne", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{TargetVersion: -1})
		require.NoError(t, err)
		require.Equal(t, seqDownTo(migrationsBundle.WithTestVersionsMaxVersion, 1),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		err = dbExecError(ctx, bundle.driver.GetExecutor(), fmt.Sprintf("SELECT name FROM %s.river_migrate", bundle.schema))
		require.Error(t, err)
	})

	t.Run("MigrateDownWithTargetVersionInvalid", func(t *testing.T) {
		t.Parallel()

		migrator, _ := setup(t)

		// migration doesn't exist
		{
			_, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{TargetVersion: migrationsBundle.MaxVersion + 77})
			require.EqualError(t, err, fmt.Sprintf("version %d is not a valid River migration version", migrationsBundle.MaxVersion+77))
		}

		// migration exists but not one that's applied
		{
			_, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{TargetVersion: migrationsBundle.MaxVersion + 1})
			require.EqualError(t, err, fmt.Sprintf("version %d is not in target list of valid migrations to apply", migrationsBundle.MaxVersion+1))
		}
	})

	t.Run("MigrateDownDryRun", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{DryRun: true})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.WithTestVersionsMaxVersion}, sliceutil.Map(res.Versions, migrateVersionToInt))

		// Migrate down returned a result above for a migration that was
		// removed, but because we're in a dry run, the database still shows
		// this version.
		migrations, err := bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line:   riverdriver.MigrationLineMain,
			Schema: bundle.schema,
		})
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

		migrator, _ := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: migrationsBundle.MaxVersion})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionUp, nil)
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.MaxVersion + 1, migrationsBundle.MaxVersion + 2}, sliceutil.Map(res.Versions, migrateVersionToInt))
	})

	t.Run("MigrateUpDefault", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// Run an initial time
		{
			res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
			require.NoError(t, err)
			require.Equal(t, DirectionUp, res.Direction)
			require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion),
				sliceutil.Map(res.Versions, migrateVersionToInt))

			migrations, err := bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
				Line:   riverdriver.MigrationLineMain,
				Schema: bundle.schema,
			})
			require.NoError(t, err)
			require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion),
				sliceutil.Map(migrations, driverMigrationToInt))

			_, err = bundle.dbPool.Exec(ctx, fmt.Sprintf("SELECT * FROM %s.test_table", bundle.schema))
			require.NoError(t, err)
		}

		// Run once more to verify idempotency
		{
			res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
			require.NoError(t, err)
			require.Equal(t, DirectionUp, res.Direction)
			require.Equal(t, []int{}, sliceutil.Map(res.Versions, migrateVersionToInt))

			migrations, err := bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
				Line:   riverdriver.MigrationLineMain,
				Schema: bundle.schema,
			})
			require.NoError(t, err)
			require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion),
				sliceutil.Map(migrations, driverMigrationToInt))

			_, err = bundle.dbPool.Exec(ctx, fmt.Sprintf("SELECT * FROM %s.test_table", bundle.schema))
			require.NoError(t, err)
		}
	})

	t.Run("MigrateUpWithMaxSteps", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: migrationsBundle.MaxVersion})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: 1})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.WithTestVersionsMaxVersion - 1},
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line:   riverdriver.MigrationLineMain,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion-1),
			sliceutil.Map(migrations, driverMigrationToInt))

		// Column `name` is only added in the second test version.
		err = dbExecError(ctx, bundle.driver.GetExecutor(), fmt.Sprintf("SELECT name FROM %s.test_table", bundle.schema))
		require.Error(t, err)

		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.UndefinedColumn, pgErr.Code)
	})

	t.Run("MigrateUpWithPool", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: migrationsBundle.MaxVersion})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion), sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line:   riverdriver.MigrationLineMain,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("MigrateUpWithDatabaseSQLDriver", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)
		migrator, tx := setupDatabaseSQLMigrator(t, bundle)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: migrationsBundle.MaxVersion})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: 1})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsBundle.MaxVersion + 1}, sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := migrator.driver.UnwrapExecutor(tx).MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line:   riverdriver.MigrationLineMain,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion+1),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("MigrateUpWithTargetVersion", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{TargetVersion: migrationsBundle.MaxVersion + 2})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line:   riverdriver.MigrationLineMain,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion+2), sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("MigrateUpWithTargetVersionInvalid", func(t *testing.T) {
		t.Parallel()

		migrator, _ := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		// migration doesn't exist
		{
			_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{TargetVersion: 77})
			require.EqualError(t, err, "version 77 is not a valid River migration version")
		}

		// migration exists but already applied
		{
			_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{TargetVersion: 3})
			require.EqualError(t, err, "version 3 is not in target list of valid migrations to apply")
		}
	})

	t.Run("MigrateUpDryRun", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: migrationsBundle.MaxVersion})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{DryRun: true})
		require.NoError(t, err)
		require.Equal(t, DirectionUp, res.Direction)
		require.Equal(t, []int{migrationsBundle.WithTestVersionsMaxVersion - 1, migrationsBundle.WithTestVersionsMaxVersion},
			sliceutil.Map(res.Versions, migrateVersionToInt))

		// Migrate up returned a result above for migrations that were applied,
		// but because we're in a dry run, the database still shows the test
		// migration versions not applied.
		migrations, err := bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line:   riverdriver.MigrationLineMain,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("ValidateSuccess", func(t *testing.T) {
		t.Parallel()

		migrator, _ := setup(t)

		// Migrate all the way up.
		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.Validate(ctx)
		require.NoError(t, err)
		require.Equal(t, &ValidateResult{OK: true}, res)
	})

	t.Run("ValidateUnappliedMigrations", func(t *testing.T) {
		t.Parallel()

		migrator, _ := setup(t)

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: migrationsBundle.MaxVersion})
		require.NoError(t, err)

		res, err := migrator.Validate(ctx)
		require.NoError(t, err)
		require.Equal(t, &ValidateResult{
			Messages: []string{fmt.Sprintf("Unapplied migrations: [%d %d]", migrationsBundle.MaxVersion+1, migrationsBundle.MaxVersion+2)},
		}, res)
	})

	t.Run("MigrateUpThenDownToZeroAndBackUp", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		requireMigrationTableExists := func(expectedExists bool) {
			migrationExists, err := bundle.driver.GetExecutor().TableExists(ctx, &riverdriver.TableExistsParams{
				Table:  "river_migration",
				Schema: bundle.schema,
			})
			require.NoError(t, err)
			require.Equal(t, expectedExists, migrationExists)
		}

		// We start off with a clean schema so it has no tables:
		requireMigrationTableExists(false)

		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{TargetVersion: migrationsBundle.MaxVersion})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		requireMigrationTableExists(true)

		res, err = migrator.Migrate(ctx, DirectionDown, &MigrateOpts{TargetVersion: -1})
		require.NoError(t, err)
		require.Equal(t, seqDownTo(migrationsBundle.MaxVersion, 1),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		requireMigrationTableExists(false)

		res, err = migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line:   riverdriver.MigrationLineMain,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.WithTestVersionsMaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("AlternateLineUpAndDown", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// Run the main migration line all the way up.
		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: migrationsBundle.MaxVersion})
		require.NoError(t, err)

		// We have to reinitialize the alternateMigrator because the migrations bundle is
		// set in the constructor.
		alternateMigrator, err := New(bundle.driver, &Config{
			Line:   migrationLineAlternate,
			Logger: bundle.logger,
			schema: bundle.schema,
		})
		require.NoError(t, err)

		res, err := alternateMigrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationLineAlternateMaxVersion),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		migrations, err := bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line:   migrationLineAlternate,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationLineAlternateMaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))

		res, err = alternateMigrator.Migrate(ctx, DirectionDown, &MigrateOpts{TargetVersion: -1})
		require.NoError(t, err)
		require.Equal(t, seqDownTo(migrationLineAlternateMaxVersion, 1),
			sliceutil.Map(res.Versions, migrateVersionToInt))

		// The main migration line should not have been touched.
		migrations, err = bundle.driver.GetExecutor().MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line:   riverdriver.MigrationLineMain,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsBundle.MaxVersion),
			sliceutil.Map(migrations, driverMigrationToInt))
	})

	t.Run("AlternateLineBeforeLineColumn", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// Main line to just before the `line` column was added.
		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{TargetVersion: 4})
		require.NoError(t, err)

		alternateMigrator, err := New(bundle.driver, &Config{
			Line:   migrationLineAlternate,
			Logger: bundle.logger,
			schema: bundle.schema,
		})
		require.NoError(t, err)

		// Alternate line not allowed because `river_job.line` doesn't exist.
		_, err = alternateMigrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.EqualError(t, err, "can't add a non-main migration line until `river_migration.line` is raised; fully migrate the main migration line and try again")

		// Main line to zero.
		_, err = migrator.Migrate(ctx, DirectionDown, &MigrateOpts{TargetVersion: -1})
		require.NoError(t, err)

		// Alternate line not allowed because `river_job` doesn't exist.
		_, err = alternateMigrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.EqualError(t, err, "can't add a non-main migration line until `river_migration` is raised; fully migrate the main migration line and try again")
	})

	// Demonstrates that even when not using River's internal migration system,
	// version 005 is still able to run.
	//
	// This is special because it's the first time the table's changed since
	// version 001.
	t.Run("Version005ToleratesRiverMigrateNotPresent", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// Migrate down to version 004.
		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: 4})
		require.NoError(t, err)
		require.Equal(t, DirectionUp, res.Direction)
		require.Equal(t, seqOneTo(4), sliceutil.Map(res.Versions, migrateVersionToInt))

		// Drop `river_migration` table as if version 001 had never originally run.
		_, err = bundle.dbPool.Exec(ctx, fmt.Sprintf("DROP TABLE %s.river_migration", bundle.schema))
		require.NoError(t, err)

		{
			schemaReplacement := map[string]sqlctemplate.Replacement{
				"schema": {Value: bundle.schema + "."},
			}

			ctx := sqlctemplate.WithReplacements(ctx, schemaReplacement, nil)

			// Run version 005 to make sure it can tolerate the absence of
			// `river_migration`. Note that we have to run the version's SQL
			// directly because using the migrator will try to interact with
			// `river_migration`, which is no longer present.
			sql, _ := migrator.replacer.Run(ctx, migrationsBundle.WithTestVersionsMap[5].SQLUp, nil)
			_, err = bundle.dbPool.Exec(ctx, sql)
			require.NoError(t, err)

			// And the version 005 down migration to verify the same.
			sql, _ = migrator.replacer.Run(ctx, migrationsBundle.WithTestVersionsMap[5].SQLDown, nil)
			_, err = bundle.dbPool.Exec(ctx, sql)
			require.NoError(t, err)
		}
	})

	t.Run("MigrationsWithCommitRequired", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		t.Cleanup(func() {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)

			// Clean up the types we created.
			_, err = tx.Exec(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s.foobar_in_bitmask", bundle.schema))
			require.NoError(t, err)

			_, err = tx.Exec(ctx, fmt.Sprintf("DROP TYPE IF EXISTS %s.foobar", bundle.schema))
			require.NoError(t, err)

			_, err = tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s.river_migration WHERE line = $1", bundle.schema), migrationLineCommitRequired)
			require.NoError(t, err)

			require.NoError(t, tx.Commit(ctx))
		})

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		// We have to reinitialize the commitRequiredMigrator because the migrations
		// bundle is set in the constructor.
		commitRequiredMigrator, err := New(bundle.driver, &Config{
			Line:   migrationLineCommitRequired,
			Logger: bundle.logger,
			schema: bundle.schema,
		})
		require.NoError(t, err)

		res, err := commitRequiredMigrator.Migrate(ctx, DirectionUp, nil)
		require.NoError(t, err)
		require.Equal(t, DirectionUp, res.Direction)
		require.Equal(t, []int{1, 2, 3}, sliceutil.Map(res.Versions, migrateVersionToInt))
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

		migration := migrations[0]
		require.Equal(t, "first", migration.Name)
		require.Equal(t, "SELECT 1;\n", migration.SQLDown)
		require.Equal(t, "SELECT 1;\n", migration.SQLUp)
	})

	t.Run("Alternate", func(t *testing.T) {
		t.Parallel()

		migrations, err := migrationsFromFS(migrationFS, migrationLineAlternate)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationLineAlternateMaxVersion), sliceutil.Map(migrations, migrationToInt))

		migration := migrations[0]
		require.Equal(t, "premier", migration.Name)
		require.Equal(t, "SELECT 1;\n", migration.SQLDown)
		require.Equal(t, "SELECT 1;\n", migration.SQLUp)
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
			SQLUp:   "CREATE TABLE /* TEMPLATE: schema */test_table(id bigserial PRIMARY KEY);",
			SQLDown: "DROP TABLE /* TEMPLATE: schema */test_table;",
		},
		{
			Version: migrationsMaxVersion + 2,
			SQLUp:   "ALTER TABLE /* TEMPLATE: schema */test_table ADD COLUMN name varchar(200); CREATE INDEX idx_test_table_name ON /* TEMPLATE: schema */test_table(name);",
			SQLDown: "DROP INDEX /* TEMPLATE: schema */idx_test_table_name; ALTER TABLE /* TEMPLATE: schema */test_table DROP COLUMN name;",
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

// Produces a sequence down to one. UpperLimit is included.
func seqOneTo(upperLimit int) []int {
	seq := make([]int, 0, upperLimit)

	for i := 1; i <= upperLimit; i++ {
		seq = append(seq, i)
	}

	return seq
}

func seqDownTo(upperLimit, lowerLimit int) []int {
	seq := make([]int, 0, upperLimit-lowerLimit+1)

	for i := lowerLimit; i <= upperLimit; i++ {
		seq = append(seq, i)
	}

	slices.Reverse(seq)
	return seq
}
