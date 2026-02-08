package riverdrivertest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
)

func exerciseSchemaIntrospection[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string),
	executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx]),
) {
	t.Helper()

	type testBundle struct {
		driver riverdriver.Driver[TTx]
	}

	setup := func(ctx context.Context, t *testing.T) (riverdriver.Executor, *testBundle) {
		t.Helper()

		exec, driver := executorWithTx(ctx, t)

		return exec, &testBundle{
			driver: driver,
		}
	}

	t.Run("ColumnExists", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		exists, err := exec.ColumnExists(ctx, &riverdriver.ColumnExistsParams{
			Column: "line",
			Table:  "river_migration",
		})
		require.NoError(t, err)
		require.True(t, exists)

		exists, err = exec.ColumnExists(ctx, &riverdriver.ColumnExistsParams{
			Column: "does_not_exist",
			Table:  "river_migration",
		})
		require.NoError(t, err)
		require.False(t, exists)

		exists, err = exec.ColumnExists(ctx, &riverdriver.ColumnExistsParams{
			Column: "line",
			Table:  "does_not_exist",
		})
		require.NoError(t, err)
		require.False(t, exists)

		// A different schema on main, but before the `line` column was added to
		// migrations.
		driver2, schemaVersion2 := driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
			LineTargetVersions: map[string]int{
				riverdriver.MigrationLineMain: 2,
			},
		})

		exists, err = driver2.GetExecutor().ColumnExists(ctx, &riverdriver.ColumnExistsParams{
			Column: "line",
			Schema: schemaVersion2,
			Table:  "river_migration",
		})
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("IndexDropIfExists", func(t *testing.T) {
		t.Parallel()

		t.Run("DropsIndex", func(t *testing.T) {
			t.Parallel()

			// Postgres runs the drop with `CONCURRENTLY` so this must use a full
			// schema rather than a transaction block.
			driver, schema := driverWithSchema(ctx, t, nil)

			// Oddly, when creating indexes on SQLite the schema must go before
			// the index name, but on Postgres it should go before the table.
			// The schema is empty for SQLite anyway since we're operating in
			// isolation in a particular database file.
			if driver.DatabaseName() == databaseNameSQLite {
				require.NoError(t, driver.GetExecutor().Exec(ctx, "CREATE INDEX river_job_index_drop_if_exists ON river_job (id)"))
			} else {
				require.NoError(t, driver.GetExecutor().Exec(ctx, fmt.Sprintf("CREATE INDEX river_job_index_drop_if_exists ON %s.river_job (id)", schema)))
			}

			err := driver.GetExecutor().IndexDropIfExists(ctx, &riverdriver.IndexDropIfExistsParams{
				Index:  "river_job_index_drop_if_exists ",
				Schema: schema,
			})
			require.NoError(t, err)
		})

		t.Run("IndexThatDoesNotExistIgnore", func(t *testing.T) {
			t.Parallel()

			// Postgres runs the drop with `CONCURRENTLY` so this must use a full
			// schema rather than a transaction block.
			driver, schema := driverWithSchema(ctx, t, nil)

			err := driver.GetExecutor().IndexDropIfExists(ctx, &riverdriver.IndexDropIfExistsParams{
				Index:  "does_not_exist",
				Schema: schema,
			})
			require.NoError(t, err)
		})
	})

	t.Run("IndexExists", func(t *testing.T) {
		t.Parallel()

		t.Run("ReturnsTrueIfIndexExists", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			exists, err := exec.IndexExists(ctx, &riverdriver.IndexExistsParams{
				Index: "river_job_prioritized_fetching_index",
			})
			require.NoError(t, err)
			require.True(t, exists)
		})

		t.Run("ReturnsFalseIfIndexDoesNotExistInAlternateSchema", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			exists, err := exec.IndexExists(ctx, &riverdriver.IndexExistsParams{
				Index:  "river_job_prioritized_fetching_index",
				Schema: "custom_schema",
			})
			if bundle.driver.DatabaseName() == databaseNameSQLite {
				requireMissingRelation(t, err, "custom_schema", "sqlite_master")
			} else {
				require.NoError(t, err)
				require.False(t, exists)
			}
		})

		t.Run("ReturnsFalseIfIndexDoesNotExist", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			exists, err := exec.IndexExists(ctx, &riverdriver.IndexExistsParams{
				Index: "river_job_prioritized_fetching_index_with_extra_suffix_123",
			})
			require.NoError(t, err)
			require.False(t, exists)
		})
	})

	t.Run("IndexReindex", func(t *testing.T) {
		t.Parallel()

		// Postgres runs the reindex with `CONCURRENTLY` so this must use a full
		// schema rather than a transaction block.
		driver, schema := driverWithSchema(ctx, t, nil)

		err := driver.GetExecutor().IndexReindex(ctx, &riverdriver.IndexReindexParams{
			Index:  "river_job_kind",
			Schema: schema,
		})
		require.NoError(t, err)
	})

	t.Run("IndexesExist", func(t *testing.T) {
		t.Parallel()

		t.Run("ReturnsTrueIfIndexExistsInSchema", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			exists, err := exec.IndexesExist(ctx, &riverdriver.IndexesExistParams{
				IndexNames: []string{"river_job_kind", "river_job_prioritized_fetching_index", "special_index"},
				Schema:     "", // empty schema means current schema
			})
			require.NoError(t, err)

			require.True(t, exists["river_job_kind"])
			require.True(t, exists["river_job_prioritized_fetching_index"])
			require.False(t, exists["special_index"])
		})

		t.Run("ReturnsFalseIfIndexDoesNotExistInAlternateSchema", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			exists, err := exec.IndexesExist(ctx, &riverdriver.IndexesExistParams{
				IndexNames: []string{"river_job_kind", "river_job_prioritized_fetching_index"},
				Schema:     "custom_schema_that_does_not_exist",
			})
			if bundle.driver.DatabaseName() == databaseNameSQLite {
				requireMissingRelation(t, err, "custom_schema_that_does_not_exist", "sqlite_master")
			} else {
				require.NoError(t, err)

				require.False(t, exists["river_job_kind"])
				require.False(t, exists["river_job_prioritized_fetching_index"])
			}
		})
	})

	t.Run("SchemaGetExpired", func(t *testing.T) {
		t.Parallel()

		t.Run("FiltersSchemasNotMatchingPrefix", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			schemas, err := exec.SchemaGetExpired(ctx, &riverdriver.SchemaGetExpiredParams{
				BeforeName: "zzz",
				Prefix:     "this_prefix_will_not_exist_",
			})
			require.NoError(t, err)
			require.Empty(t, schemas)
		})

		t.Run("ListsSchemasBelowMarker", func(t *testing.T) {
			t.Parallel()

			var (
				driver1, schema1 = driverWithSchema(ctx, t, nil)
				driver2, schema2 = driverWithSchema(ctx, t, nil)
			)

			// This isn't super great, but we need to a little more work to get
			// the real schema names for SQLite. SQLite schemas are normally
			// empty because they're actually separate databases and can't be
			// referenced with their fully qualified name. So instead, extract
			// the name of the current database via pragma and use it as schema.
			if driver1.DatabaseName() == databaseNameSQLite {
				getCurrentSchema := func(exec riverdriver.Executor) string {
					var databaseFile string
					require.NoError(t, exec.QueryRow(ctx, "SELECT file FROM pragma_database_list WHERE name = ?1", "main").Scan(&databaseFile))

					t.Logf("database file = %s", databaseFile)

					lastSlashIndex := strings.LastIndex(databaseFile, "/")
					require.NotEqual(t, -1, lastSlashIndex)

					schema, _, ok := strings.Cut(databaseFile[lastSlashIndex+1:], ".sqlite3")
					require.True(t, ok)
					return schema
				}

				schema1 = getCurrentSchema(driver1.GetExecutor())
				schema2 = getCurrentSchema(driver2.GetExecutor())
			}

			// Package name packagePrefix like `river_test_`.
			packagePrefix, _, ok := strings.Cut(schema1, time.Now().Format("_2006_"))
			require.True(t, ok)

			// With added year like `river_test_2006_`.
			packagePrefixWithYear := packagePrefix + "_2006_"

			schemas, err := driver1.GetExecutor().SchemaGetExpired(ctx, &riverdriver.SchemaGetExpiredParams{
				BeforeName: "zzz",
				Prefix:     time.Now().Format(packagePrefixWithYear),
			})
			require.NoError(t, err)

			// Using "zzz" as a cursor we expect to get both the schemas that
			// were created before it. However, because this test case may be
			// running in parallel with many others, there may be many other
			// test schemas that exist concurrently, so we use Contains instead
			// of comparing against a specific list. It'd be nice to use one of
			// the test schemas as a cursor, but this isn't possible because
			// schemas may be reused non-determnistically, so while generally
			// schema1 < schema2, it may be that schema1 > schema2.
			require.Contains(t, schemas, schema1)
			require.Contains(t, schemas, schema2)

			// Fetch for a year ago which returns nothing.
			schemas, err = driver1.GetExecutor().SchemaGetExpired(ctx, &riverdriver.SchemaGetExpiredParams{
				BeforeName: "zzz",
				Prefix:     time.Now().Add(-365 * 24 * time.Hour).Format(packagePrefixWithYear),
			})
			require.NoError(t, err)
			require.Empty(t, schemas)
		})
	})

	t.Run("TableExists", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		exists, err := exec.TableExists(ctx, &riverdriver.TableExistsParams{
			Table: "river_migration",
		})
		require.NoError(t, err)
		require.True(t, exists)

		exists, err = exec.TableExists(ctx, &riverdriver.TableExistsParams{
			Table: "does_not_exist",
		})
		require.NoError(t, err)
		require.False(t, exists)

		driver2, schema2 := driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
			Lines: []string{},
		})

		exists, err = driver2.GetExecutor().TableExists(ctx, &riverdriver.TableExistsParams{
			Schema: schema2,
			Table:  "river_migration",
		})
		require.NoError(t, err)
		require.False(t, exists)
	})
}
