package riverdrivertest

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
)

func exerciseMigration[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string),
	executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx]),
) {
	t.Helper()

	t.Run("GetMigrationFS", func(t *testing.T) {
		t.Parallel()

		driver, _ := driverWithSchema(ctx, t, nil)

		for _, line := range driver.GetMigrationLines() {
			migrationFS := driver.GetMigrationFS(line)

			// Directory for the advertised migration line should exist.
			_, err := migrationFS.Open("migration/" + line)
			require.NoError(t, err)
		}
	})

	t.Run("GetMigrationTruncateTables", func(t *testing.T) {
		t.Parallel()

		t.Run("AllLinesNonEmpty", func(t *testing.T) {
			t.Parallel()

			driver, _ := driverWithSchema(ctx, t, nil)

			for _, line := range driver.GetMigrationLines() {
				truncateTables := driver.GetMigrationTruncateTables(line, 0)

				// Technically a migration line's truncate tables might be empty,
				// but this never happens in any of our migration lines, so check
				// non-empty until it becomes an actual problem.
				require.NotEmpty(t, truncateTables)
			}
		})

		t.Run("MainLine", func(t *testing.T) {
			t.Parallel()

			driver, _ := driverWithSchema(ctx, t, nil)

			require.Empty(t, driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 1))
			require.Equal(t, []string{"river_job", "river_leader"},
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 2))
			require.Equal(t, []string{"river_job", "river_leader"},
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 3))
			require.Equal(t, []string{"river_job", "river_leader", "river_queue"},
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 4))
			require.Equal(t, []string{"river_job", "river_leader", "river_queue", "river_client", "river_client_queue"},
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 5))
			require.Equal(t, []string{"river_job", "river_leader", "river_queue", "river_client", "river_client_queue"},
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 6))
			require.Equal(t, []string{"river_job", "river_leader", "river_queue", "river_client", "river_client_queue"},
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 0))
		})
	})

	t.Run("GetMigrationLines", func(t *testing.T) {
		t.Parallel()

		driver, _ := driverWithSchema(ctx, t, nil)

		// Should contain at minimum a main migration line.
		require.Contains(t, driver.GetMigrationLines(), riverdriver.MigrationLineMain)
	})

	// This doesn't map to a particular function, but make sure the driver is
	// capable of migration all the way up, then all the way back down.
	t.Run("MigrateUpAndDown", func(t *testing.T) {
		t.Parallel()

		driver, _ := driverWithSchema(ctx, t, nil)

		for _, line := range driver.GetMigrationLines() {
			t.Run(strings.ToUpper(line[0:1])+line[1:], func(t *testing.T) {
				driver, schema := driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
					Lines: []string{},
				})

				migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
					Line:   line,
					Logger: riversharedtest.Logger(t),
					Schema: schema,
				})
				require.NoError(t, err)

				{
					t.Log("Migrating up (round 1)")
					_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
					require.NoError(t, err)

					t.Log("Migrating down (round 1)")
					_, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
						TargetVersion: -1,
					})
					require.NoError(t, err)
				}

				// Do the process a second time to make sure all migrations
				// really were idempotent and didn't leave artifacts.
				{
					t.Log("Migrating up (round 2)")
					_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
					require.NoError(t, err)

					t.Log("Migrating down (round 2)")
					_, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
						TargetVersion: -1,
					})
					require.NoError(t, err)
				}

				// Last check to make sure we really went down to zero.
				exists, err := driver.GetExecutor().TableExists(ctx, &riverdriver.TableExistsParams{
					Schema: schema,
					Table:  "river_migration",
				})
				require.NoError(t, err)
				require.False(t, exists)
			})
		}
	})

	type testBundle struct {
		driver riverdriver.Driver[TTx]
	}

	setup := func(ctx context.Context, t *testing.T) (riverdriver.Executor, *testBundle) {
		t.Helper()

		execTx, driver := executorWithTx(ctx, t)

		return execTx, &testBundle{
			driver: driver,
		}
	}

	t.Run("MigrationDeleteAssumingMainMany", func(t *testing.T) {
		t.Parallel()

		// Use dedicated schema instead of test transaction because SQLite
		// doesn't support transaction DDL.
		var (
			driver, schema = driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
				LineTargetVersions: map[string]int{
					riverdriver.MigrationLineMain: 4,
				},
			})
			exec = driver.GetExecutor()
		)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Schema: schema, Table: []string{"river_migration"}}))

		// Doesn't use testfactory because we're using an old schema version.
		var (
			migration1 *riverdriver.Migration
			migration2 *riverdriver.Migration
		)
		{
			migrations, err := exec.MigrationInsertManyAssumingMain(ctx, &riverdriver.MigrationInsertManyAssumingMainParams{
				Schema:   schema,
				Versions: []int{1, 2},
			})
			require.NoError(t, err)
			migration1 = migrations[0]
			migration2 = migrations[1]
		}

		migrations, err := exec.MigrationDeleteAssumingMainMany(ctx, &riverdriver.MigrationDeleteAssumingMainManyParams{
			Schema: schema,
			Versions: []int{
				migration1.Version,
				migration2.Version,
			},
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		slices.SortFunc(migrations, func(a, b *riverdriver.Migration) int { return a.Version - b.Version })
		require.Equal(t, riverdriver.MigrationLineMain, migrations[0].Line)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, riverdriver.MigrationLineMain, migrations[1].Line)
		require.Equal(t, migration2.Version, migrations[1].Version)
	})

	t.Run("MigrationDeleteByLineAndVersionMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Table: []string{"river_migration"}}))

		// not touched
		_ = testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})

		migration1 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: ptrutil.Ptr("alternate")})
		migration2 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: ptrutil.Ptr("alternate")})

		migrations, err := exec.MigrationDeleteByLineAndVersionMany(ctx, &riverdriver.MigrationDeleteByLineAndVersionManyParams{
			Line: "alternate",

			Versions: []int{
				migration1.Version,
				migration2.Version,
			},
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		slices.SortFunc(migrations, func(a, b *riverdriver.Migration) int { return a.Version - b.Version })
		require.Equal(t, "alternate", migrations[0].Line)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, "alternate", migrations[1].Line)
		require.Equal(t, migration2.Version, migrations[1].Version)
	})

	t.Run("MigrationGetAllAssumingMain", func(t *testing.T) {
		t.Parallel()

		// Use dedicated schema instead of test transaction because SQLite
		// doesn't support transaction DDL.
		var (
			driver, schema = driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
				LineTargetVersions: map[string]int{
					riverdriver.MigrationLineMain: 4,
				},
			})
			exec = driver.GetExecutor()
		)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Schema: schema, Table: []string{"river_migration"}}))

		// Doesn't use testfactory because we're using an old schema version.
		var (
			migration1 *riverdriver.Migration
			migration2 *riverdriver.Migration
		)
		{
			migrations, err := exec.MigrationInsertManyAssumingMain(ctx, &riverdriver.MigrationInsertManyAssumingMainParams{
				Schema:   schema,
				Versions: []int{1, 2},
			})
			require.NoError(t, err)
			migration1 = migrations[0]
			migration2 = migrations[1]
		}

		migrations, err := exec.MigrationGetAllAssumingMain(ctx, &riverdriver.MigrationGetAllAssumingMainParams{
			Schema: schema,
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, migration2.Version, migrations[1].Version)

		// Check the full properties of one of the migrations.
		migration1Fetched := migrations[0]
		require.WithinDuration(t, migration1.CreatedAt, migration1Fetched.CreatedAt, driver.TimePrecision())
		require.Equal(t, riverdriver.MigrationLineMain, migration1Fetched.Line)
		require.Equal(t, migration1.Version, migration1Fetched.Version)
	})

	t.Run("MigrationGetByLine", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Table: []string{"river_migration"}}))

		// not returned
		_ = testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})

		migration1 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: ptrutil.Ptr("alternate")})
		migration2 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: ptrutil.Ptr("alternate")})

		migrations, err := exec.MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line: "alternate",
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, migration2.Version, migrations[1].Version)

		// Check the full properties of one of the migrations.
		migration1Fetched := migrations[0]
		require.WithinDuration(t, migration1.CreatedAt, migration1Fetched.CreatedAt, bundle.driver.TimePrecision())
		require.Equal(t, "alternate", migration1Fetched.Line)
		require.Equal(t, migration1.Version, migration1Fetched.Version)
	})

	t.Run("MigrationInsertMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Table: []string{"river_migration"}}))

		migrations, err := exec.MigrationInsertMany(ctx, &riverdriver.MigrationInsertManyParams{
			Line:     "alternate",
			Versions: []int{1, 2},
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, "alternate", migrations[0].Line)
		require.Equal(t, 1, migrations[0].Version)
		require.Equal(t, "alternate", migrations[1].Line)
		require.Equal(t, 2, migrations[1].Version)
	})

	t.Run("MigrationInsertManyAssumingMain", func(t *testing.T) {
		t.Parallel()

		// Use dedicated schema instead of test transaction because SQLite
		// doesn't support transaction DDL.
		var (
			driver, schema = driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
				LineTargetVersions: map[string]int{
					riverdriver.MigrationLineMain: 4,
				},
			})
			exec = driver.GetExecutor()
		)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Schema: schema, Table: []string{"river_migration"}}))

		migrations, err := exec.MigrationInsertManyAssumingMain(ctx, &riverdriver.MigrationInsertManyAssumingMainParams{
			Schema:   schema,
			Versions: []int{1, 2},
		})

		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, riverdriver.MigrationLineMain, migrations[0].Line)
		require.Equal(t, 1, migrations[0].Version)
		require.Equal(t, riverdriver.MigrationLineMain, migrations[1].Line)
		require.Equal(t, 2, migrations[1].Version)
	})
}
