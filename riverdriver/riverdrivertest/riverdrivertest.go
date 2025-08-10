package riverdrivertest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivershared/util/hashutil"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

const (
	databaseNamePostgres = "postgres"
	databaseNameSQLite   = "sqlite"
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

	t.Run("SQLFragmentColumnIn", func(t *testing.T) {
		t.Parallel()

		t.Run("IntegerValues", func(t *testing.T) {
			t.Parallel()

			exec, driver := executorWithTx(ctx, t)

			var (
				job1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				_    = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
			)

			sqlFragment, arg, err := driver.SQLFragmentColumnIn("id", []int64{job1.ID, job2.ID})
			require.NoError(t, err)

			jobs, err := exec.JobList(ctx, &riverdriver.JobListParams{
				Max:           100,
				NamedArgs:     map[string]any{"id": arg},
				OrderByClause: "id",
				WhereClause:   sqlFragment,
			})
			require.NoError(t, err)
			require.Len(t, jobs, 2)
			require.Equal(t, job1.ID, jobs[0].ID)
			require.Equal(t, job2.ID, jobs[1].ID)
		})

		t.Run("StringValues", func(t *testing.T) {
			t.Parallel()

			exec, driver := executorWithTx(ctx, t)

			var (
				job1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind1")})
				job2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind2")})
				_    = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind3")})
			)

			sqlFragment, arg, err := driver.SQLFragmentColumnIn("kind", []string{job1.Kind, job2.Kind})
			require.NoError(t, err)

			jobs, err := exec.JobList(ctx, &riverdriver.JobListParams{
				Max:           100,
				NamedArgs:     map[string]any{"kind": arg},
				OrderByClause: "kind",
				WhereClause:   sqlFragment,
			})
			require.NoError(t, err)
			require.Len(t, jobs, 2)
			require.Equal(t, job1.Kind, jobs[0].Kind)
			require.Equal(t, job2.Kind, jobs[1].Kind)
		})
	})

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
		case databaseNamePostgres:
			require.True(t, driver.SupportsListenNotify())
		case databaseNameSQLite:
			require.False(t, driver.SupportsListenNotify())
		default:
			require.FailNow(t, "Don't know how to check SupportsListenNotify for: "+driver.DatabaseName())
		}
	})

	type testBundle struct {
		driver riverdriver.Driver[TTx]
	}

	setup := func(ctx context.Context, t *testing.T) (riverdriver.Executor, *testBundle) {
		t.Helper()

		tx, driver := executorWithTx(ctx, t)

		return tx, &testBundle{
			driver: driver,
		}
	}

	const clientID = "test-client-id"

	t.Run("Begin", func(t *testing.T) {
		t.Parallel()

		t.Run("BasicVisibility", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			tx, err := exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback(ctx) })

			// Job visible in subtransaction, but not parent.
			{
				job := testfactory.Job(ctx, t, tx, &testfactory.JobOpts{})
				_ = testfactory.Job(ctx, t, tx, &testfactory.JobOpts{})

				_, err := tx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
				require.NoError(t, err)

				require.NoError(t, tx.Rollback(ctx))

				_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
				require.ErrorIs(t, err, rivertype.ErrNotFound)
			}
		})

		t.Run("NestedTransactions", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			tx1, err := exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx1.Rollback(ctx) })

			// Job visible in tx1, but not top level executor.
			{
				job1 := testfactory.Job(ctx, t, tx1, &testfactory.JobOpts{})

				{
					tx2, err := tx1.Begin(ctx)
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx2.Rollback(ctx) })

					// Job visible in tx2, but not top level executor.
					{
						job2 := testfactory.Job(ctx, t, tx2, &testfactory.JobOpts{})

						_, err := tx2.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
						require.NoError(t, err)

						require.NoError(t, tx2.Rollback(ctx))

						_, err = tx1.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
						require.ErrorIs(t, err, rivertype.ErrNotFound)
					}

					_, err = tx1.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
					require.NoError(t, err)
				}

				// Repeat the same subtransaction again.
				{
					tx2, err := tx1.Begin(ctx)
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx2.Rollback(ctx) })

					// Job visible in tx2, but not top level executor.
					{
						job2 := testfactory.Job(ctx, t, tx2, &testfactory.JobOpts{})

						_, err = tx2.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
						require.NoError(t, err)

						require.NoError(t, tx2.Rollback(ctx))

						_, err = tx1.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
						require.ErrorIs(t, err, rivertype.ErrNotFound)
					}

					_, err = tx1.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
					require.NoError(t, err)
				}

				require.NoError(t, tx1.Rollback(ctx))

				_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
				require.ErrorIs(t, err, rivertype.ErrNotFound)
			}
		})

		t.Run("RollbackAfterCommit", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			tx1, err := exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx1.Rollback(ctx) })

			tx2, err := tx1.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx2.Rollback(ctx) })

			job := testfactory.Job(ctx, t, tx2, &testfactory.JobOpts{})

			require.NoError(t, tx2.Commit(ctx))
			_ = tx2.Rollback(ctx) // "tx is closed" error generally returned, but don't require this

			// Despite rollback being called after commit, the job is still
			// visible from the outer transaction.
			_, err = tx1.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
		})
	})

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

	t.Run("Exec", func(t *testing.T) {
		t.Parallel()

		t.Run("NoArgs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			require.NoError(t, exec.Exec(ctx, "SELECT 1 + 2"))
		})

		t.Run("WithArgs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			require.NoError(t, exec.Exec(ctx, "SELECT $1 || $2", "foo", "bar"))
		})
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

	t.Run("JobCancel", func(t *testing.T) {
		t.Parallel()

		for _, startingState := range []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
		} {
			t.Run(fmt.Sprintf("CancelsJobIn%sState", strings.ToUpper(string(startingState[0]))+string(startingState)[1:]), func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				now := time.Now().UTC()
				nowStr := now.Format(time.RFC3339Nano)

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					State:     &startingState,
					UniqueKey: []byte("unique-key"),
				})
				require.Equal(t, startingState, job.State)

				jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
					ID:                job.ID,
					CancelAttemptedAt: now,
					ControlTopic:      string(notifier.NotificationTopicControl),
				})
				require.NoError(t, err)
				require.NotNil(t, jobAfter)

				require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
				require.WithinDuration(t, time.Now(), *jobAfter.FinalizedAt, 2*time.Second)
				require.JSONEq(t, fmt.Sprintf(`{"cancel_attempted_at":%q}`, nowStr), string(jobAfter.Metadata))
			})
		}

		t.Run("RunningJobIsNotImmediatelyCancelled", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()
			nowStr := now.Format(time.RFC3339Nano)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})
			require.Equal(t, rivertype.JobStateRunning, job.State)

			jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
				ID:                job.ID,
				CancelAttemptedAt: now,
				ControlTopic:      string(notifier.NotificationTopicControl),
			})
			require.NoError(t, err)
			require.NotNil(t, jobAfter)
			require.Equal(t, rivertype.JobStateRunning, jobAfter.State)
			require.Nil(t, jobAfter.FinalizedAt)
			require.JSONEq(t, fmt.Sprintf(`{"cancel_attempted_at":%q}`, nowStr), string(jobAfter.Metadata))
			require.Equal(t, "unique-key", string(jobAfter.UniqueKey))
		})

		for _, startingState := range []rivertype.JobState{
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
		} {
			t.Run(fmt.Sprintf("DoesNotAlterFinalizedJobIn%sState", startingState), func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					FinalizedAt: ptrutil.Ptr(time.Now()),
					State:       &startingState,
				})

				jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
					ID:                job.ID,
					CancelAttemptedAt: time.Now(),
					ControlTopic:      string(notifier.NotificationTopicControl),
				})
				require.NoError(t, err)
				require.Equal(t, startingState, jobAfter.State)
				require.WithinDuration(t, *job.FinalizedAt, *jobAfter.FinalizedAt, time.Microsecond)
				require.JSONEq(t, `{}`, string(jobAfter.Metadata))
			})
		}

		t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
				ID:                1234567890,
				CancelAttemptedAt: time.Now(),
				ControlTopic:      string(notifier.NotificationTopicControl),
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, jobAfter)
		})
	})

	t.Run("JobCountByAllStates", func(t *testing.T) {
		t.Parallel()

		t.Run("CountsJobsByState", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCancelled)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

			countsByState, err := exec.JobCountByAllStates(ctx, &riverdriver.JobCountByAllStatesParams{
				Schema: "",
			})
			require.NoError(t, err)

			for _, state := range rivertype.JobStates() {
				require.Contains(t, countsByState, state)
				switch state { //nolint:exhaustive
				case rivertype.JobStateAvailable:
					require.Equal(t, 2, countsByState[state])
				case rivertype.JobStateCancelled:
					require.Equal(t, 1, countsByState[state])
				case rivertype.JobStateCompleted:
					require.Equal(t, 1, countsByState[state])
				case rivertype.JobStateDiscarded:
					require.Equal(t, 1, countsByState[state])
				default:
					require.Equal(t, 0, countsByState[state])
				}
			}
		})

		t.Run("AlternateSchema", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_, err := exec.JobCountByAllStates(ctx, &riverdriver.JobCountByAllStatesParams{
				Schema: "custom_schema",
			})
			requireMissingRelation(t, err, "custom_schema", "river_job")
		})
	})

	t.Run("JobCountByQueueAndState", func(t *testing.T) {
		t.Parallel()

		t.Run("CountsJobsInAvailableAndRunningForEachOfTheSpecifiedQueues", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateRunning)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateRunning)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue1"), State: ptrutil.Ptr(rivertype.JobStateRunning)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue2"), State: ptrutil.Ptr(rivertype.JobStateRunning)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue3"), State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue3"), State: ptrutil.Ptr(rivertype.JobStateRunning)})

			countsByQueue, err := exec.JobCountByQueueAndState(ctx, &riverdriver.JobCountByQueueAndStateParams{
				QueueNames: []string{"queue1", "queue2"},
				Schema:     "",
			})
			require.NoError(t, err)

			require.Len(t, countsByQueue, 2)

			require.Equal(t, "queue1", countsByQueue[0].Queue)
			require.Equal(t, int64(2), countsByQueue[0].CountAvailable)
			require.Equal(t, int64(3), countsByQueue[0].CountRunning)
			require.Equal(t, "queue2", countsByQueue[1].Queue)
			require.Equal(t, int64(1), countsByQueue[1].CountAvailable)
			require.Equal(t, int64(1), countsByQueue[1].CountRunning)
		})
	})

	t.Run("JobCountByState", func(t *testing.T) {
		t.Parallel()

		t.Run("CountsJobsByState", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			// Included because they're the queried state.
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})

			// Excluded because they're not.
			finalizedAt := ptrutil.Ptr(time.Now())
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: finalizedAt, State: ptrutil.Ptr(rivertype.JobStateCancelled)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: finalizedAt, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: finalizedAt, State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

			numJobs, err := exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{
				State: rivertype.JobStateAvailable,
			})
			require.NoError(t, err)
			require.Equal(t, 2, numJobs)
		})

		t.Run("AlternateSchema", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_, err := exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{
				Schema: "custom_schema",
				State:  rivertype.JobStateAvailable,
			})
			requireMissingRelation(t, err, "custom_schema", "river_job")
		})
	})

	t.Run("JobDelete", func(t *testing.T) {
		t.Parallel()

		t.Run("DoesNotDeleteARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State: ptrutil.Ptr(rivertype.JobStateRunning),
			})

			jobAfter, err := exec.JobDelete(ctx, &riverdriver.JobDeleteParams{
				ID: job.ID,
			})
			require.ErrorIs(t, err, rivertype.ErrJobRunning)
			require.Nil(t, jobAfter)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, jobUpdated.State)
		})

		for _, state := range []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
			rivertype.JobStatePending,
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
		} {
			t.Run(fmt.Sprintf("DeletesA_%s_Job", state), func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				now := time.Now().UTC()

				setFinalized := slices.Contains([]rivertype.JobState{
					rivertype.JobStateCancelled,
					rivertype.JobStateCompleted,
					rivertype.JobStateDiscarded,
				}, state)

				var finalizedAt *time.Time
				if setFinalized {
					finalizedAt = &now
				}

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					FinalizedAt: finalizedAt,
					ScheduledAt: ptrutil.Ptr(now.Add(1 * time.Hour)),
					State:       &state,
				})

				jobAfter, err := exec.JobDelete(ctx, &riverdriver.JobDeleteParams{
					ID: job.ID,
				})
				require.NoError(t, err)
				require.NotNil(t, jobAfter)
				require.Equal(t, job.ID, jobAfter.ID)
				require.Equal(t, state, jobAfter.State)

				_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
				require.ErrorIs(t, err, rivertype.ErrNotFound)
			})
		}

		t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			jobAfter, err := exec.JobDelete(ctx, &riverdriver.JobDeleteParams{
				ID: 1234567890,
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, jobAfter)
		})

		t.Run("AlternateSchema", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			_, err := exec.JobDelete(ctx, &riverdriver.JobDeleteParams{
				ID:     job.ID,
				Schema: "custom_schema",
			})
			requireMissingRelation(t, err, "custom_schema", "river_job")
		})
	})

	t.Run("JobDeleteBefore", func(t *testing.T) {
		t.Parallel()

		var (
			horizon       = time.Now()
			beforeHorizon = horizon.Add(-1 * time.Minute)
			afterHorizon  = horizon.Add(1 * time.Minute)
		)

		t.Run("Success", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			deletedJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCancelled)})
			deletedJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
			deletedJob3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

			// Not deleted because not appropriate state.
			notDeletedJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			notDeletedJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

			// Not deleted because after the delete horizon.
			notDeletedJob3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateCancelled)})

			// Max two deleted on the first pass.
			numDeleted, err := exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledDoDelete:           true,
				CancelledFinalizedAtHorizon: horizon,
				CompletedDoDelete:           true,
				CompletedFinalizedAtHorizon: horizon,
				DiscardedDoDelete:           true,
				DiscardedFinalizedAtHorizon: horizon,
				Max:                         2,
			})
			require.NoError(t, err)
			require.Equal(t, 2, numDeleted)

			// And one more pass gets the last one.
			numDeleted, err = exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledDoDelete:           true,
				CancelledFinalizedAtHorizon: horizon,
				CompletedDoDelete:           true,
				CompletedFinalizedAtHorizon: horizon,
				DiscardedDoDelete:           true,
				DiscardedFinalizedAtHorizon: horizon,
				Max:                         2,
			})
			require.NoError(t, err)
			require.Equal(t, 1, numDeleted)

			// All deleted
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: deletedJob1.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: deletedJob2.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: deletedJob3.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)

			// Not deleted
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: notDeletedJob1.ID})
			require.NoError(t, err)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: notDeletedJob2.ID})
			require.NoError(t, err)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: notDeletedJob3.ID})
			require.NoError(t, err)
		})

		t.Run("QueuesExcluded", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var ( //nolint:dupl
				cancelledJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCancelled)})
				completedJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
				discardedJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

				excludedQueue1 = "excluded1"
				excludedQueue2 = "excluded2"

				// Not deleted because in an omitted queue.
				notDeletedJob1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, Queue: &excludedQueue1, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
				notDeletedJob2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, Queue: &excludedQueue2, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
			)

			numDeleted, err := exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledDoDelete:           true,
				CancelledFinalizedAtHorizon: horizon,
				CompletedDoDelete:           true,
				CompletedFinalizedAtHorizon: horizon,
				DiscardedDoDelete:           true,
				DiscardedFinalizedAtHorizon: horizon,
				Max:                         1_000,
				QueuesExcluded:              []string{excludedQueue1, excludedQueue2},
			})
			require.NoError(t, err)
			require.Equal(t, 3, numDeleted)

			// All deleted
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: cancelledJob.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: completedJob.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: discardedJob.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)

			// Not deleted
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: notDeletedJob1.ID})
			require.NoError(t, err)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: notDeletedJob2.ID})
			require.NoError(t, err)
		})

		t.Run("QueuesIncluded", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			// I ran into yet another huge sqlc SQLite bug in that when mixing
			// normal parameters with a `sqlc.slice` the latter must appear at
			// the very end because it'll produce unnamed placeholders (?)
			// instead of positional placeholders (?1) like most parameters. The
			// trick of putting it at the end works, but only if you have
			// exactly one `sqlc.slice` needed. If you need multiple and they
			// need to be interspersed with other parameters (like in the case
			// of `queues_excluded` and `queues_included`), everything stops
			// working real fast. I could have worked around this by breaking
			// the SQLite version of this operation into two sqlc queries, but
			// since we only expect to need `queues_excluded` on SQLite (and not
			// `queues_included` for the foreseeable future), I've just set
			// SQLite to not support `queues_included` for the time being.
			if bundle.driver.DatabaseName() == databaseNameSQLite {
				t.Logf("Skipping JobDeleteBefore with QueuesIncluded test for SQLite")
				return
			}

			var ( //nolint:dupl
				cancelledJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCancelled)})
				completedJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
				discardedJob = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

				includedQueue1 = "included1"
				includedQueue2 = "included2"

				// Not deleted because in an omitted queue.
				deletedJob1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, Queue: &includedQueue1, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
				deletedJob2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, Queue: &includedQueue2, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
			)

			numDeleted, err := exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledDoDelete:           true,
				CancelledFinalizedAtHorizon: horizon,
				CompletedDoDelete:           true,
				CompletedFinalizedAtHorizon: horizon,
				DiscardedDoDelete:           true,
				DiscardedFinalizedAtHorizon: horizon,
				Max:                         1_000,
				QueuesIncluded:              []string{includedQueue1, includedQueue2},
			})
			require.NoError(t, err)
			require.Equal(t, 2, numDeleted)

			// Not deleted
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: cancelledJob.ID})
			require.NoError(t, err)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: completedJob.ID})
			require.NoError(t, err)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: discardedJob.ID})
			require.NoError(t, err)

			// Deleted as part of included queues
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: deletedJob1.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: deletedJob2.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})
	})

	t.Run("JobDeleteMany", func(t *testing.T) {
		t.Parallel()

		t.Run("DeletesJobs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Attempt:      ptrutil.Ptr(3),
				AttemptedAt:  &now,
				CreatedAt:    &now,
				EncodedArgs:  []byte(`{"encoded": "args"}`),
				Errors:       [][]byte{[]byte(`{"error": "message1"}`), []byte(`{"error": "message2"}`)},
				FinalizedAt:  &now,
				Metadata:     []byte(`{"meta": "data"}`),
				ScheduledAt:  &now,
				State:        ptrutil.Ptr(rivertype.JobStateCompleted),
				Tags:         []string{"tag"},
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			// Does not match predicate (makes sure where clause is working).
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			deletedJobs, err := exec.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
				Max:           100,
				NamedArgs:     map[string]any{"job_id_123": job.ID},
				OrderByClause: "id",
				WhereClause:   "id = @job_id_123",
			})
			require.NoError(t, err)
			require.Len(t, deletedJobs, 1)

			deletedJob := deletedJobs[0]
			require.Equal(t, job.Attempt, deletedJob.Attempt)
			require.Equal(t, job.AttemptedAt, deletedJob.AttemptedAt)
			require.Equal(t, job.CreatedAt, deletedJob.CreatedAt)
			require.Equal(t, job.EncodedArgs, deletedJob.EncodedArgs)
			require.Equal(t, "message1", deletedJob.Errors[0].Error)
			require.Equal(t, "message2", deletedJob.Errors[1].Error)
			require.Equal(t, job.FinalizedAt, deletedJob.FinalizedAt)
			require.Equal(t, job.Kind, deletedJob.Kind)
			require.Equal(t, job.MaxAttempts, deletedJob.MaxAttempts)
			require.Equal(t, job.Metadata, deletedJob.Metadata)
			require.Equal(t, job.Priority, deletedJob.Priority)
			require.Equal(t, job.Queue, deletedJob.Queue)
			require.Equal(t, job.ScheduledAt, deletedJob.ScheduledAt)
			require.Equal(t, job.State, deletedJob.State)
			require.Equal(t, job.Tags, deletedJob.Tags)
			require.Equal(t, []byte("unique-key"), deletedJob.UniqueKey)
			require.Equal(t, rivertype.JobStates(), deletedJob.UniqueStates)

			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})

		t.Run("IgnoresRunningJobs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

			deletedJobs, err := exec.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
				Max:           100,
				NamedArgs:     map[string]any{"job_id": job.ID},
				OrderByClause: "id",
				WhereClause:   "id = @job_id",
			})
			require.NoError(t, err)
			require.Empty(t, deletedJobs)

			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
		})

		t.Run("HandlesRequiredArgumentTypes", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			{
				var (
					job1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind1")})
					_    = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind2")})
				)

				deletedJobs, err := exec.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
					Max:           100,
					NamedArgs:     map[string]any{"kind": job1.Kind},
					OrderByClause: "id",
					WhereClause:   "kind = @kind",
				})
				require.NoError(t, err)
				require.Len(t, deletedJobs, 1)
			}

			{
				var (
					job1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind3")})
					job2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind4")})
				)

				deletedJobs, err := exec.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
					Max:           100,
					NamedArgs:     map[string]any{"list_arg_00": job1.Kind, "list_arg_01": job2.Kind},
					OrderByClause: "id",
					WhereClause:   "kind IN (@list_arg_00, @list_arg_01)",
				})
				require.NoError(t, err)
				require.Len(t, deletedJobs, 2)
			}
		})

		t.Run("SortedResults", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var (
				job1 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job2 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job3 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job4 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
				job5 = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
			)

			deletedJobs, err := exec.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
				Max: 100,
				// NamedArgs:     map[string]any{"kind": job1.Kind},
				OrderByClause: "id",
				WhereClause:   "true",
			})
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID, job2.ID, job3.ID, job4.ID, job5.ID}, sliceutil.Map(deletedJobs, func(j *rivertype.JobRow) int64 { return j.ID }))
		})
	})

	t.Run("JobGetAvailable", func(t *testing.T) {
		t.Parallel()

		const (
			maxAttemptedBy = 10
			maxToLock      = 100
		)

		t.Run("Success", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       clientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 1)

			jobRow := jobRows[0]
			require.Equal(t, []string{clientID}, jobRow.AttemptedBy)
		})

		t.Run("ConstrainedToLimit", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			// Two rows inserted but only one found because of the added limit.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       clientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      1,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 1)
		})

		t.Run("ConstrainedToQueue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Queue: ptrutil.Ptr("other-queue"),
			})

			// Job is in a non-default queue so it's not found.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       clientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Empty(t, jobRows)
		})

		t.Run("ConstrainedToScheduledAtBeforeNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt: ptrutil.Ptr(now.Add(1 * time.Minute)),
			})

			// Job is scheduled a while from now so it's not found.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       clientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Now:            &now,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Empty(t, jobRows)
		})

		t.Run("ConstrainedToScheduledAtBeforeCustomNowTime", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().Add(1 * time.Minute)
			// Job 1 is scheduled after now so it's not found:
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt: ptrutil.Ptr(now.Add(1 * time.Minute)),
			})
			// Job 2 is scheduled just before now so it's found:
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Microsecond)),
			})

			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       clientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Now:            ptrutil.Ptr(now),
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 1)
			require.Equal(t, job2.ID, jobRows[0].ID)
		})

		t.Run("Prioritized", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			// Insert jobs with decreasing priority numbers (3, 2, 1) which means increasing priority.
			for i := 3; i > 0; i-- {
				_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					Priority: &i,
				})
			}

			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       clientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      2,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 2, "expected to fetch exactly 2 jobs")

			// Because the jobs are ordered within the fetch query's CTE but *not* within
			// the final query, the final result list may not actually be sorted. This is
			// fine, because we've already ensured that we've fetched the jobs we wanted
			// to fetch via that ORDER BY. For testing we'll need to sort the list after
			// fetch to easily assert that the expected jobs are in it.
			sort.Slice(jobRows, func(i, j int) bool { return jobRows[i].Priority < jobRows[j].Priority })

			require.Equal(t, 1, jobRows[0].Priority, "expected first job to have priority 1")
			require.Equal(t, 2, jobRows[1].Priority, "expected second job to have priority 2")

			// Should fetch the one remaining job on the next attempt:
			jobRows, err = exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       clientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      1,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.NoError(t, err)
			require.Len(t, jobRows, 1, "expected to fetch exactly 1 job")
			require.Equal(t, 3, jobRows[0].Priority, "expected final job to have priority 3")
		})

		t.Run("AttemptedByAtMaxTruncated", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			attemptedBy := make([]string, maxAttemptedBy)
			for i := range maxAttemptedBy {
				attemptedBy[i] = "attempt_" + strconv.Itoa(i)
			}

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				AttemptedBy: attemptedBy,
			})

			// Job is in a non-default queue so it's not found.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       clientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 1)

			jobRow := jobRows[0]
			require.Equal(t, append(
				attemptedBy[1:],
				clientID,
			), jobRow.AttemptedBy)
			require.Len(t, jobRow.AttemptedBy, maxAttemptedBy)
		})

		// Almost identical to the above, but tests that there are more existing
		// `attempted_by` elements than the maximum allowed. There's a fine bug
		// around use of > versus >= in the query's conditional, so make sure to
		// capture both cases to make sure they work.
		t.Run("AttemptedByOverMaxTruncated", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			attemptedBy := make([]string, maxAttemptedBy+1)
			for i := range maxAttemptedBy + 1 {
				attemptedBy[i] = "attempt_" + strconv.Itoa(i)
			}

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				AttemptedBy: attemptedBy,
			})

			// Job is in a non-default queue so it's not found.
			jobRows, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       clientID,
				MaxAttemptedBy: maxAttemptedBy,
				MaxToLock:      maxToLock,
				Queue:          rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, jobRows, 1)

			jobRow := jobRows[0]
			require.Equal(t, append(
				attemptedBy[2:], // start at 2 because there were 2 extra elements
				clientID,
			), jobRow.AttemptedBy)
			require.Len(t, jobRow.AttemptedBy, maxAttemptedBy)
		})
	})

	t.Run("JobGetByID", func(t *testing.T) {
		t.Parallel()

		t.Run("FetchesAnExistingJob", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{CreatedAt: &now, ScheduledAt: &now})

			fetchedJob, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			require.Equal(t, job.ID, fetchedJob.ID)
			require.Equal(t, rivertype.JobStateAvailable, fetchedJob.State)
			require.WithinDuration(t, now, fetchedJob.CreatedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now, fetchedJob.ScheduledAt, bundle.driver.TimePrecision())
		})

		t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: 0})
			require.Error(t, err)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, job)
		})
	})

	t.Run("JobGetByIDMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

		// Not returned.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

		jobs, err := exec.JobGetByIDMany(ctx, &riverdriver.JobGetByIDManyParams{
			ID: []int64{job1.ID, job2.ID},
		})
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID},
			sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID }))
	})

	t.Run("JobGetByKindMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind1")})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind2")})

		// Not returned.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("kind3")})

		jobs, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind: []string{job1.Kind, job2.Kind},
		})
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID},
			sliceutil.Map(jobs, func(j *rivertype.JobRow) int64 { return j.ID }))
	})

	t.Run("JobGetStuck", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		var (
			horizon       = time.Now().UTC()
			beforeHorizon = horizon.Add(-1 * time.Minute)
			afterHorizon  = horizon.Add(1 * time.Minute)
		)

		stuckJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})
		stuckJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})

		t.Logf("horizon   = %s", horizon)
		t.Logf("stuckJob1 = %s", stuckJob1.AttemptedAt)
		t.Logf("stuckJob2 = %s", stuckJob2.AttemptedAt)

		t.Logf("stuckJob1 full = %s", spew.Sdump(stuckJob1))

		// Not returned because we put a maximum of two.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})

		// Not stuck because not in running state.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})

		// Not stuck because after queried horizon.
		_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateRunning)})

		// Max two stuck
		stuckJobs, err := exec.JobGetStuck(ctx, &riverdriver.JobGetStuckParams{
			Max:          2,
			StuckHorizon: horizon,
		})
		require.NoError(t, err)
		require.Equal(t, []int64{stuckJob1.ID, stuckJob2.ID},
			sliceutil.Map(stuckJobs, func(j *rivertype.JobRow) int64 { return j.ID }))
	})

	t.Run("JobInsertFastMany", func(t *testing.T) {
		t.Parallel()

		t.Run("AllArgs", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			var (
				idStart = rand.Int64()
				now     = time.Now().UTC()
			)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := 0; i < len(insertParams); i++ {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					ID:           ptrutil.Ptr(idStart + int64(i)),
					CreatedAt:    ptrutil.Ptr(now.Add(time.Duration(i) * 5 * time.Second)),
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  rivercommon.MaxAttemptsDefault,
					Metadata:     []byte(`{"meta": "data"}`),
					Priority:     rivercommon.PriorityDefault,
					Queue:        rivercommon.QueueDefault,
					ScheduledAt:  ptrutil.Ptr(now.Add(time.Duration(i) * time.Minute)),
					State:        rivertype.JobStateAvailable,
					Tags:         []string{"tag"},
					UniqueKey:    []byte("unique-key-fast-many-" + strconv.Itoa(i)),
					UniqueStates: 0xff,
				}
			}

			resultRows, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: insertParams,
			})
			require.NoError(t, err)
			require.Len(t, resultRows, len(insertParams))

			for i, result := range resultRows {
				require.False(t, result.UniqueSkippedAsDuplicate)
				job := result.Job

				// SQLite needs to set a special metadata key to be able to
				// check for duplicates. Remove this for purposes of comparing
				// inserted metadata.
				job.Metadata, err = sjson.DeleteBytes(job.Metadata, rivercommon.MetadataKeyUniqueNonce)
				require.NoError(t, err)

				require.Equal(t, idStart+int64(i), job.ID)
				require.Equal(t, 0, job.Attempt)
				require.Nil(t, job.AttemptedAt)
				require.Empty(t, job.AttemptedBy)
				require.WithinDuration(t, now.Add(time.Duration(i)*5*time.Second), job.CreatedAt, time.Millisecond)
				require.JSONEq(t, `{"encoded": "args"}`, string(job.EncodedArgs))
				require.Empty(t, job.Errors)
				require.Nil(t, job.FinalizedAt)
				require.Equal(t, "test_kind", job.Kind)
				require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)
				require.JSONEq(t, `{"meta": "data"}`, string(job.Metadata))
				require.Equal(t, rivercommon.PriorityDefault, job.Priority)
				require.Equal(t, rivercommon.QueueDefault, job.Queue)
				require.WithinDuration(t, now.Add(time.Duration(i)*time.Minute), job.ScheduledAt, bundle.driver.TimePrecision())
				require.Equal(t, rivertype.JobStateAvailable, job.State)
				require.Equal(t, []string{"tag"}, job.Tags)
				require.Equal(t, []byte("unique-key-fast-many-"+strconv.Itoa(i)), job.UniqueKey)
				require.Equal(t, rivertype.JobStates(), job.UniqueStates)
			}
		})

		t.Run("MissingValuesDefaultAsExpected", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := 0; i < len(insertParams); i++ {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  rivercommon.MaxAttemptsDefault,
					Metadata:     []byte(`{"meta": "data"}`),
					Priority:     rivercommon.PriorityDefault,
					Queue:        rivercommon.QueueDefault,
					ScheduledAt:  nil, // explicit nil
					State:        rivertype.JobStateAvailable,
					Tags:         []string{"tag"},
					UniqueKey:    nil,  // explicit nil
					UniqueStates: 0x00, // explicit 0
				}
			}

			results, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: insertParams,
			})
			require.NoError(t, err)
			require.Len(t, results, len(insertParams))

			// Especially in SQLite where both the database and the drivers
			// suck, it's really easy to accidentally insert an empty value
			// instead of a real null so double check that we got real nulls.
			var (
				attemptedAtIsNull  bool
				attemptedByIsNull  bool
				uniqueKeyIsNull    bool
				uniqueStatesIsNull bool
			)
			require.NoError(t, exec.QueryRow(ctx, "SELECT attempted_at IS NULL, attempted_by IS NULL, unique_key IS NULL, unique_states IS NULL FROM river_job").Scan(
				&attemptedAtIsNull,
				&attemptedByIsNull,
				&uniqueKeyIsNull,
				&uniqueStatesIsNull,
			))
			require.True(t, attemptedAtIsNull)
			require.True(t, attemptedByIsNull)
			require.True(t, uniqueKeyIsNull)
			require.True(t, uniqueStatesIsNull)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind: []string{"test_kind"},
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for _, job := range jobsAfter {
				require.NotZero(t, job.ID)
				require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
				require.WithinDuration(t, time.Now().UTC(), job.ScheduledAt, 2*time.Second)

				// UniqueKey and UniqueStates are not set in the insert params, so they should
				// be nil and an empty slice respectively.
				require.Nil(t, job.UniqueKey)
				var emptyJobStates []rivertype.JobState
				require.Equal(t, emptyJobStates, job.UniqueStates)
			}
		})

		t.Run("UniqueConflict", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			uniqueKey := "unique-key-fast-conflict"

			results1, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{
					{
						EncodedArgs:  []byte(`{"encoded": "args"}`),
						Kind:         "test_kind",
						MaxAttempts:  rivercommon.MaxAttemptsDefault,
						Metadata:     []byte(`{"meta": "data"}`),
						Priority:     rivercommon.PriorityDefault,
						Queue:        rivercommon.QueueDefault,
						State:        rivertype.JobStateAvailable,
						Tags:         []string{"tag"},
						UniqueKey:    []byte(uniqueKey),
						UniqueStates: 0xff,
					},
				},
			})
			require.NoError(t, err)
			require.Len(t, results1, 1)
			require.False(t, results1[0].UniqueSkippedAsDuplicate)

			results2, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{
					{
						EncodedArgs:  []byte(`{"encoded": "args"}`),
						Kind:         "test_kind",
						MaxAttempts:  rivercommon.MaxAttemptsDefault,
						Metadata:     []byte(`{"meta": "data"}`),
						Priority:     rivercommon.PriorityDefault,
						Queue:        rivercommon.QueueDefault,
						State:        rivertype.JobStateAvailable,
						Tags:         []string{"tag"},
						UniqueKey:    []byte(uniqueKey),
						UniqueStates: 0xff,
					},
				},
			})
			require.NoError(t, err)
			require.Len(t, results2, 1)
			require.True(t, results2[0].UniqueSkippedAsDuplicate)

			require.Equal(t, results1[0].Job.ID, results2[0].Job.ID)
		})

		t.Run("BinaryNonUTF8UniqueKey", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			uniqueKey := []byte{0x00, 0x01, 0x02}
			results, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{
					{
						EncodedArgs:  []byte(`{"encoded": "args"}`),
						Kind:         "test_kind",
						MaxAttempts:  rivercommon.MaxAttemptsDefault,
						Metadata:     []byte(`{"meta": "data"}`),
						Priority:     rivercommon.PriorityDefault,
						Queue:        rivercommon.QueueDefault,
						ScheduledAt:  nil, // explicit nil
						State:        rivertype.JobStateAvailable,
						Tags:         []string{"tag"},
						UniqueKey:    uniqueKey,
						UniqueStates: 0xff,
					},
				},
			})
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, uniqueKey, results[0].Job.UniqueKey)

			jobs, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind: []string{"test_kind"},
			})
			require.NoError(t, err)
			require.Equal(t, uniqueKey, jobs[0].UniqueKey)
		})
	})

	t.Run("JobInsertFastManyNoReturning", func(t *testing.T) {
		t.Parallel()

		t.Run("AllArgs", func(t *testing.T) {
			exec, bundle := setup(ctx, t)

			// This test needs to use a time from before the transaction begins, otherwise
			// the newly-scheduled jobs won't yet show as available because their
			// scheduled_at (which gets a default value from time.Now() in code) will be
			// after the start of the transaction.
			now := time.Now().UTC().Add(-1 * time.Minute)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := 0; i < len(insertParams); i++ {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					CreatedAt:    ptrutil.Ptr(now.Add(time.Duration(i) * 5 * time.Second)),
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  rivercommon.MaxAttemptsDefault,
					Metadata:     []byte(`{"meta": "data"}`),
					Priority:     rivercommon.PriorityDefault,
					Queue:        rivercommon.QueueDefault,
					ScheduledAt:  &now,
					State:        rivertype.JobStateAvailable,
					Tags:         []string{"tag"},
					UniqueKey:    []byte("unique-key-no-returning-" + strconv.Itoa(i)),
					UniqueStates: 0xff,
				}
			}

			count, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs:   insertParams,
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, insertParams, count)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind:   []string{"test_kind"},
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for i, job := range jobsAfter {
				require.Equal(t, 0, job.Attempt)
				require.Nil(t, job.AttemptedAt)
				require.WithinDuration(t, now.Add(time.Duration(i)*5*time.Second), job.CreatedAt, time.Millisecond)
				require.JSONEq(t, `{"encoded": "args"}`, string(job.EncodedArgs))
				require.Empty(t, job.Errors)
				require.Nil(t, job.FinalizedAt)
				require.Equal(t, "test_kind", job.Kind)
				require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)
				require.JSONEq(t, `{"meta": "data"}`, string(job.Metadata))
				require.Equal(t, rivercommon.PriorityDefault, job.Priority)
				require.Equal(t, rivercommon.QueueDefault, job.Queue)
				require.WithinDuration(t, now, job.ScheduledAt, bundle.driver.TimePrecision())
				require.Equal(t, rivertype.JobStateAvailable, job.State)
				require.Equal(t, []string{"tag"}, job.Tags)
				require.Equal(t, []byte("unique-key-no-returning-"+strconv.Itoa(i)), job.UniqueKey)
			}
		})

		t.Run("MissingValuesDefaultAsExpected", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := 0; i < len(insertParams); i++ {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  rivercommon.MaxAttemptsDefault,
					Metadata:     []byte(`{"meta": "data"}`),
					Priority:     rivercommon.PriorityDefault,
					Queue:        rivercommon.QueueDefault,
					ScheduledAt:  nil, // explicit nil
					State:        rivertype.JobStateAvailable,
					Tags:         []string{"tag"},
					UniqueKey:    nil,  // explicit nil
					UniqueStates: 0x00, // explicit 0
				}
			}

			rowsAffected, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: insertParams,
			})
			require.NoError(t, err)
			require.Equal(t, len(insertParams), rowsAffected)

			// Especially in SQLite where both the database and the drivers
			// suck, it's really easy to accidentally insert an empty value
			// instead of a real null so double check that we got real nulls.
			var (
				attemptedAtIsNull  bool
				attemptedByIsNull  bool
				uniqueKeyIsNull    bool
				uniqueStatesIsNull bool
			)
			require.NoError(t, exec.QueryRow(ctx, "SELECT attempted_at IS NULL, attempted_by IS NULL, unique_key IS NULL, unique_states IS NULL FROM river_job").Scan(
				&attemptedAtIsNull,
				&attemptedByIsNull,
				&uniqueKeyIsNull,
				&uniqueStatesIsNull,
			))
			require.True(t, attemptedAtIsNull)
			require.True(t, attemptedByIsNull)
			require.True(t, uniqueKeyIsNull)
			require.True(t, uniqueStatesIsNull)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind: []string{"test_kind"},
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for _, job := range jobsAfter {
				require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
				require.WithinDuration(t, time.Now().UTC(), job.ScheduledAt, 2*time.Second)

				// UniqueKey and UniqueStates are not set in the insert params, so they should
				// be nil and an empty slice respectively.
				require.Nil(t, job.UniqueKey)
				var emptyJobStates []rivertype.JobState
				require.Equal(t, emptyJobStates, job.UniqueStates)
			}
		})

		t.Run("UniqueConflict", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			uniqueKey := "unique-key-fast-conflict"

			rowsAffected1, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{
					{
						EncodedArgs:  []byte(`{"encoded": "args"}`),
						Kind:         "test_kind",
						MaxAttempts:  rivercommon.MaxAttemptsDefault,
						Metadata:     []byte(`{"meta": "data"}`),
						Priority:     rivercommon.PriorityDefault,
						Queue:        rivercommon.QueueDefault,
						State:        rivertype.JobStateAvailable,
						Tags:         []string{"tag"},
						UniqueKey:    []byte(uniqueKey),
						UniqueStates: 0xff,
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, rowsAffected1)

			rowsAffected2, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{
					{
						EncodedArgs:  []byte(`{"encoded": "args"}`),
						Kind:         "test_kind",
						MaxAttempts:  rivercommon.MaxAttemptsDefault,
						Metadata:     []byte(`{"meta": "data"}`),
						Priority:     rivercommon.PriorityDefault,
						Queue:        rivercommon.QueueDefault,
						State:        rivertype.JobStateAvailable,
						Tags:         []string{"tag"},
						UniqueKey:    []byte(uniqueKey),
						UniqueStates: 0xff,
					},
				},
			})
			if err != nil {
				// PgxV5 uses copy/from which means that it can't support `ON
				// CONFLICT` and therefore returns an error here. Callers that
				// want to bulk insert unique jobs should use the returning
				// variant instead.
				if reflect.TypeOf(bundle.driver).Elem().PkgPath() == "github.com/riverqueue/river/riverdriver/riverpgxv5" {
					var pgErr *pgconn.PgError
					require.ErrorAs(t, err, &pgErr)
					require.Equal(t, pgerrcode.UniqueViolation, pgErr.Code)
					require.Equal(t, "river_job_unique_idx", pgErr.ConstraintName)
					return
				}
			}
			require.NoError(t, err)
			require.Zero(t, rowsAffected2)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind: []string{"test_kind"},
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, 1)
		})

		t.Run("MissingCreatedAtDefaultsToNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := 0; i < len(insertParams); i++ {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					CreatedAt:   nil, // explicit nil
					EncodedArgs: []byte(`{"encoded": "args"}`),
					Kind:        "test_kind",
					MaxAttempts: rivercommon.MaxAttemptsDefault,
					Metadata:    []byte(`{"meta": "data"}`),
					Priority:    rivercommon.PriorityDefault,
					Queue:       rivercommon.QueueDefault,
					ScheduledAt: ptrutil.Ptr(time.Now().UTC()),
					State:       rivertype.JobStateAvailable,
					Tags:        []string{"tag"},
				}
			}

			count, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs:   insertParams,
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, insertParams, count)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind:   []string{"test_kind"},
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for _, job := range jobsAfter {
				require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
			}
		})

		t.Run("MissingScheduledAtDefaultsToNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := 0; i < len(insertParams); i++ {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					EncodedArgs: []byte(`{"encoded": "args"}`),
					Kind:        "test_kind",
					MaxAttempts: rivercommon.MaxAttemptsDefault,
					Metadata:    []byte(`{"meta": "data"}`),
					Priority:    rivercommon.PriorityDefault,
					Queue:       rivercommon.QueueDefault,
					ScheduledAt: nil, // explicit nil
					State:       rivertype.JobStateAvailable,
					Tags:        []string{"tag"},
				}
			}

			count, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs:   insertParams,
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, insertParams, count)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind:   []string{"test_kind"},
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for _, job := range jobsAfter {
				require.WithinDuration(t, time.Now().UTC(), job.ScheduledAt, 2*time.Second)
			}
		})

		t.Run("AlternateSchema", func(t *testing.T) {
			t.Parallel()

			var (
				driver, schema = driverWithSchema(ctx, t, nil)
				exec           = driver.GetExecutor()
			)

			// This test needs to use a time from before the transaction begins, otherwise
			// the newly-scheduled jobs won't yet show as available because their
			// scheduled_at (which gets a default value from time.Now() in code) will be
			// after the start of the transaction.
			now := time.Now().UTC().Add(-1 * time.Minute)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := 0; i < len(insertParams); i++ {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					CreatedAt:    ptrutil.Ptr(now.Add(time.Duration(i) * 5 * time.Second)),
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  rivercommon.MaxAttemptsDefault,
					Metadata:     []byte(`{"meta": "data"}`),
					Priority:     rivercommon.PriorityDefault,
					Queue:        rivercommon.QueueDefault,
					ScheduledAt:  &now,
					State:        rivertype.JobStateAvailable,
					Tags:         []string{"tag"},
					UniqueKey:    []byte("unique-key-no-returning-" + strconv.Itoa(i)),
					UniqueStates: 0xff,
				}
			}

			count, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs:   insertParams,
				Schema: schema,
			})
			require.NoError(t, err)
			require.Len(t, insertParams, count)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind:   []string{"test_kind"},
				Schema: schema,
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
		})
	})

	t.Run("JobInsertFull", func(t *testing.T) {
		t.Parallel()

		t.Run("MinimalArgsWithDefaults", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
				EncodedArgs: []byte(`{"encoded": "args"}`),
				Kind:        "test_kind",
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				State:       rivertype.JobStateAvailable,
			})
			require.NoError(t, err)
			require.Equal(t, 0, job.Attempt)
			require.Nil(t, job.AttemptedAt)
			require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
			require.JSONEq(t, `{"encoded": "args"}`, string(job.EncodedArgs))
			require.Empty(t, job.Errors)
			require.Nil(t, job.FinalizedAt)
			require.Equal(t, "test_kind", job.Kind)
			require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)
			require.Equal(t, rivercommon.QueueDefault, job.Queue)
			require.Equal(t, rivertype.JobStateAvailable, job.State)

			// Especially in SQLite where both the database and the drivers
			// suck, it's really easy to accidentally insert an empty value
			// instead of a real null so double check that we got real nulls.
			var (
				attemptedAtIsNull  bool
				attemptedByIsNull  bool
				errorsIsNull       bool
				finalizedAtIsNull  bool
				uniqueKeyIsNull    bool
				uniqueStatesIsNull bool
			)
			require.NoError(t, exec.QueryRow(ctx, "SELECT attempted_at IS NULL, attempted_by IS NULL, errors IS NULL, finalized_at IS NULL, unique_key IS NULL, unique_states IS NULL FROM river_job").Scan(&attemptedAtIsNull, &attemptedByIsNull, &errorsIsNull, &finalizedAtIsNull, &uniqueKeyIsNull, &uniqueStatesIsNull))
			require.True(t, attemptedAtIsNull)
			require.True(t, attemptedByIsNull)
			require.True(t, errorsIsNull)
			require.True(t, finalizedAtIsNull)
			require.True(t, uniqueKeyIsNull)
			require.True(t, uniqueStatesIsNull)
		})

		t.Run("AllArgs", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
				Attempt:     3,
				AttemptedAt: &now,
				AttemptedBy: []string{"worker1", "worker2"},
				CreatedAt:   &now,
				EncodedArgs: []byte(`{"encoded": "args"}`),
				Errors:      [][]byte{[]byte(`{"error": "message"}`)},
				FinalizedAt: &now,
				Kind:        "test_kind",
				MaxAttempts: 6,
				Metadata:    []byte(`{"meta": "data"}`),
				Priority:    2,
				Queue:       "queue_name",
				ScheduledAt: &now,
				State:       rivertype.JobStateCompleted,
				Tags:        []string{"tag"},
				UniqueKey:   []byte("unique-key"),
			})
			require.NoError(t, err)
			require.Equal(t, 3, job.Attempt)
			require.WithinDuration(t, now, *job.AttemptedAt, bundle.driver.TimePrecision())
			require.Equal(t, []string{"worker1", "worker2"}, job.AttemptedBy)
			require.WithinDuration(t, now, job.CreatedAt, bundle.driver.TimePrecision())
			require.JSONEq(t, `{"encoded": "args"}`, string(job.EncodedArgs))
			require.Equal(t, "message", job.Errors[0].Error)
			require.WithinDuration(t, now, *job.FinalizedAt, bundle.driver.TimePrecision())
			require.Equal(t, "test_kind", job.Kind)
			require.Equal(t, 6, job.MaxAttempts)
			require.JSONEq(t, `{"meta": "data"}`, string(job.Metadata))
			require.Equal(t, 2, job.Priority)
			require.Equal(t, "queue_name", job.Queue)
			require.WithinDuration(t, now, job.ScheduledAt, bundle.driver.TimePrecision())
			require.Equal(t, rivertype.JobStateCompleted, job.State)
			require.Equal(t, []string{"tag"}, job.Tags)
			require.Equal(t, []byte("unique-key"), job.UniqueKey)
		})

		t.Run("JobFinalizedAtConstraint", func(t *testing.T) {
			t.Parallel()

			capitalizeJobState := func(state rivertype.JobState) string {
				return cases.Title(language.English, cases.NoLower).String(string(state))
			}

			for _, state := range []rivertype.JobState{
				rivertype.JobStateCancelled,
				rivertype.JobStateCompleted,
				rivertype.JobStateDiscarded,
			} {
				t.Run(fmt.Sprintf("CannotSetState%sWithoutFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)

					// Create a job with the target state but without a finalized_at,
					// expect an error:
					params := testfactory.Job_Build(t, &testfactory.JobOpts{
						State: &state,
					})
					params.FinalizedAt = nil
					_, err := exec.JobInsertFull(ctx, params)
					require.Error(t, err)
					// two separate error messages here for Postgres and SQLite
					require.Regexp(t, `(CHECK constraint failed: finalized_or_finalized_at_null|violates check constraint "finalized_or_finalized_at_null")`, err.Error())
				})

				t.Run(fmt.Sprintf("CanSetState%sWithFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)

					// Create a job with the target state but with a finalized_at, expect
					// no error:
					_, err := exec.JobInsertFull(ctx, testfactory.Job_Build(t, &testfactory.JobOpts{
						FinalizedAt: ptrutil.Ptr(time.Now()),
						State:       &state,
					}))
					require.NoError(t, err)
				})
			}

			for _, state := range []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			} {
				t.Run(fmt.Sprintf("CanSetState%sWithoutFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)

					// Create a job with the target state but without a finalized_at,
					// expect no error:
					_, err := exec.JobInsertFull(ctx, testfactory.Job_Build(t, &testfactory.JobOpts{
						State: &state,
					}))
					require.NoError(t, err)
				})

				t.Run(fmt.Sprintf("CannotSetState%sWithFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)

					// Create a job with the target state but with a finalized_at, expect
					// an error:
					_, err := exec.JobInsertFull(ctx, testfactory.Job_Build(t, &testfactory.JobOpts{
						FinalizedAt: ptrutil.Ptr(time.Now()),
						State:       &state,
					}))
					require.Error(t, err)
					// two separate error messages here for Postgres and SQLite
					require.Regexp(t, `(CHECK constraint failed: finalized_or_finalized_at_null|violates check constraint "finalized_or_finalized_at_null")`, err.Error())
				})
			}
		})
	})

	t.Run("JobInsertFullMany", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		jobParams1 := testfactory.Job_Build(t, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateCompleted),
		})
		jobParams2 := testfactory.Job_Build(t, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		results, err := exec.JobInsertFullMany(ctx, &riverdriver.JobInsertFullManyParams{
			Jobs: []*riverdriver.JobInsertFullParams{jobParams1, jobParams2},
		})
		require.NoError(t, err)

		require.Len(t, results, 2)
		now := time.Now().UTC()

		assertJobEqualsInput := func(t *testing.T, job *rivertype.JobRow, input *riverdriver.JobInsertFullParams) {
			t.Helper()

			require.Equal(t, input.Attempt, job.Attempt)
			if input.AttemptedAt == nil {
				require.Nil(t, job.AttemptedAt)
			} else {
				t.Logf("job: %+v", job)
				t.Logf("input: %+v", input)
				require.WithinDuration(t, *input.AttemptedAt, *job.AttemptedAt, bundle.driver.TimePrecision())
			}
			require.Equal(t, input.AttemptedBy, job.AttemptedBy)
			require.WithinDuration(t, now, job.CreatedAt, 5*time.Second)
			require.Equal(t, input.EncodedArgs, job.EncodedArgs)
			require.Empty(t, job.Errors)
			if input.FinalizedAt == nil || input.FinalizedAt.IsZero() {
				require.Nil(t, job.FinalizedAt)
			} else {
				require.WithinDuration(t, input.FinalizedAt.UTC(), job.FinalizedAt.UTC(), bundle.driver.TimePrecision())
			}
			require.Equal(t, input.Kind, job.Kind)
			require.Equal(t, input.MaxAttempts, job.MaxAttempts)
			require.Equal(t, input.Metadata, job.Metadata)
			require.Equal(t, input.Priority, job.Priority)
			require.Equal(t, input.Queue, job.Queue)
			if input.ScheduledAt == nil {
				require.WithinDuration(t, now, job.ScheduledAt, 5*time.Second)
			} else {
				require.WithinDuration(t, input.ScheduledAt.UTC(), job.ScheduledAt, bundle.driver.TimePrecision())
			}
			require.Equal(t, input.State, job.State)
			require.Equal(t, input.Tags, job.Tags)
			require.Equal(t, input.UniqueKey, job.UniqueKey)
			require.Empty(t, job.UniqueStates)
		}

		assertJobEqualsInput(t, results[0], jobParams1)
		assertJobEqualsInput(t, results[1], jobParams2)
	})

	t.Run("JobKindListByPrefix", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		t.Run("ListsJobKindsInOrderWithMaxLimit", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_zzz")})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_aaa")})
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_bbb")})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("different_prefix_job")})

			jobKinds, err := exec.JobKindListByPrefix(ctx, &riverdriver.JobKindListByPrefixParams{
				After:   "job2",
				Exclude: nil,
				Max:     2,
				Prefix:  "job",
				Schema:  "",
			})
			require.NoError(t, err)
			require.Equal(t, []string{job2.Kind, job3.Kind}, jobKinds) // sorted by name
		})

		t.Run("ExcludesJobKindsInExcludeList", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_zzz")})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_aaa")})
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("job_bbb")})

			jobKinds, err := exec.JobKindListByPrefix(ctx, &riverdriver.JobKindListByPrefixParams{
				After:   "job2",
				Exclude: []string{job2.Kind},
				Max:     2,
				Prefix:  "job",
				Schema:  "",
			})
			require.NoError(t, err)
			require.Equal(t, []string{job3.Kind, job1.Kind}, jobKinds)
		})
	})

	t.Run("JobList", func(t *testing.T) {
		t.Parallel()

		t.Run("ListsJobs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Attempt:      ptrutil.Ptr(3),
				AttemptedAt:  &now,
				CreatedAt:    &now,
				EncodedArgs:  []byte(`{"encoded": "args"}`),
				Errors:       [][]byte{[]byte(`{"error": "message1"}`), []byte(`{"error": "message2"}`)},
				FinalizedAt:  &now,
				Metadata:     []byte(`{"meta": "data"}`),
				ScheduledAt:  &now,
				State:        ptrutil.Ptr(rivertype.JobStateCompleted),
				Tags:         []string{"tag"},
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			// Does not match predicate (makes sure where clause is working).
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			fetchedJobs, err := exec.JobList(ctx, &riverdriver.JobListParams{
				Max:           100,
				NamedArgs:     map[string]any{"job_id_123": job.ID},
				OrderByClause: "id",
				WhereClause:   "id = @job_id_123",
			})
			require.NoError(t, err)
			require.Len(t, fetchedJobs, 1)

			fetchedJob := fetchedJobs[0]
			require.Equal(t, job.Attempt, fetchedJob.Attempt)
			require.Equal(t, job.AttemptedAt, fetchedJob.AttemptedAt)
			require.Equal(t, job.CreatedAt, fetchedJob.CreatedAt)
			require.Equal(t, job.EncodedArgs, fetchedJob.EncodedArgs)
			require.Equal(t, "message1", fetchedJob.Errors[0].Error)
			require.Equal(t, "message2", fetchedJob.Errors[1].Error)
			require.Equal(t, job.FinalizedAt, fetchedJob.FinalizedAt)
			require.Equal(t, job.Kind, fetchedJob.Kind)
			require.Equal(t, job.MaxAttempts, fetchedJob.MaxAttempts)
			require.Equal(t, job.Metadata, fetchedJob.Metadata)
			require.Equal(t, job.Priority, fetchedJob.Priority)
			require.Equal(t, job.Queue, fetchedJob.Queue)
			require.Equal(t, job.ScheduledAt, fetchedJob.ScheduledAt)
			require.Equal(t, job.State, fetchedJob.State)
			require.Equal(t, job.Tags, fetchedJob.Tags)
			require.Equal(t, []byte("unique-key"), fetchedJob.UniqueKey)
			require.Equal(t, rivertype.JobStates(), fetchedJob.UniqueStates)
		})

		t.Run("HandlesRequiredArgumentTypes", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind1")})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind2")})

			{
				fetchedJobs, err := exec.JobList(ctx, &riverdriver.JobListParams{
					Max:           100,
					NamedArgs:     map[string]any{"kind": job1.Kind},
					OrderByClause: "id",
					WhereClause:   "kind = @kind",
				})
				require.NoError(t, err)
				require.Len(t, fetchedJobs, 1)
			}

			{
				fetchedJobs, err := exec.JobList(ctx, &riverdriver.JobListParams{
					Max:           100,
					NamedArgs:     map[string]any{"list_arg_00": job1.Kind, "list_arg_01": job2.Kind},
					OrderByClause: "id",
					WhereClause:   "kind IN (@list_arg_00, @list_arg_01)",
				})
				require.NoError(t, err)
				require.Len(t, fetchedJobs, 2)
			}
		})
	})

	t.Run("JobRescueMany", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		now := time.Now().UTC()

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		_, err := exec.JobRescueMany(ctx, &riverdriver.JobRescueManyParams{
			ID: []int64{
				job1.ID,
				job2.ID,
			},
			Error: [][]byte{
				[]byte(`{"error": "message1"}`),
				[]byte(`{"error": "message2"}`),
			},
			FinalizedAt: []*time.Time{
				nil,
				&now,
			},
			ScheduledAt: []time.Time{
				now,
				now,
			},

			State: []string{
				string(rivertype.JobStateAvailable),
				string(rivertype.JobStateDiscarded),
			},
		})
		require.NoError(t, err)

		updatedJob1, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
		require.NoError(t, err)
		require.Equal(t, "message1", updatedJob1.Errors[0].Error)
		require.Nil(t, updatedJob1.FinalizedAt)
		require.WithinDuration(t, now, updatedJob1.ScheduledAt, bundle.driver.TimePrecision())
		require.Equal(t, rivertype.JobStateAvailable, updatedJob1.State)

		updatedJob2, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
		require.NoError(t, err)
		require.Equal(t, "message2", updatedJob2.Errors[0].Error)
		require.WithinDuration(t, now, *updatedJob2.FinalizedAt, bundle.driver.TimePrecision())
		require.WithinDuration(t, now, updatedJob2.ScheduledAt, bundle.driver.TimePrecision())
		require.Equal(t, rivertype.JobStateDiscarded, updatedJob2.State)
	})

	t.Run("JobRetry", func(t *testing.T) {
		t.Parallel()

		t.Run("DoesNotUpdateARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State: ptrutil.Ptr(rivertype.JobStateRunning),
			})

			jobAfter, err := exec.JobRetry(ctx, &riverdriver.JobRetryParams{
				ID: job.ID,
			})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, jobAfter.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, jobUpdated.State)
		})

		for _, state := range []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
			rivertype.JobStatePending,
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
		} {
			t.Run(fmt.Sprintf("UpdatesA_%s_JobToBeScheduledImmediately", state), func(t *testing.T) {
				t.Parallel()

				exec, bundle := setup(ctx, t)

				now := time.Now().UTC()

				setFinalized := slices.Contains([]rivertype.JobState{
					rivertype.JobStateCancelled,
					rivertype.JobStateCompleted,
					rivertype.JobStateDiscarded,
				}, state)

				var finalizedAt *time.Time
				if setFinalized {
					finalizedAt = &now
				}

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					FinalizedAt: finalizedAt,
					ScheduledAt: ptrutil.Ptr(now.Add(1 * time.Hour)),
					State:       &state,
				})

				jobAfter, err := exec.JobRetry(ctx, &riverdriver.JobRetryParams{
					ID:  job.ID,
					Now: &now,
				})
				require.NoError(t, err)
				require.Equal(t, rivertype.JobStateAvailable, jobAfter.State)
				require.WithinDuration(t, now, jobAfter.ScheduledAt, bundle.driver.TimePrecision())

				jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
				require.NoError(t, err)
				require.Equal(t, rivertype.JobStateAvailable, jobUpdated.State)
				require.Nil(t, jobUpdated.FinalizedAt)
			})
		}

		t.Run("AltersScheduledAtForAlreadyCompletedJob", func(t *testing.T) {
			// A job which has already completed will have a ScheduledAt that could be
			// long in the past. Now that we're re-scheduling it, we should update that
			// to the current time to slot it in alongside other recently-scheduled jobs
			// and not skip the line; also, its wait duration can't be calculated
			// accurately if we don't reset the scheduled_at.
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				FinalizedAt: &now,
				ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
				State:       ptrutil.Ptr(rivertype.JobStateCompleted),
			})

			jobAfter, err := exec.JobRetry(ctx, &riverdriver.JobRetryParams{
				ID:  job.ID,
				Now: &now,
			})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, jobAfter.State)
			require.WithinDuration(t, now, jobAfter.ScheduledAt, 5*time.Second)
		})

		t.Run("DoesNotAlterScheduledAtIfInThePastAndJobAlreadyAvailable", func(t *testing.T) {
			// We don't want to update ScheduledAt if the job was already available
			// because doing so can make it lose its place in line.
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
			})

			jobAfter, err := exec.JobRetry(ctx, &riverdriver.JobRetryParams{
				ID:  job.ID,
				Now: &now,
			})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, jobAfter.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, jobUpdated.State)
		})

		t.Run("ReturnsErrNotFoundIfJobNotFound", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_, err := exec.JobRetry(ctx, &riverdriver.JobRetryParams{
				ID: 0,
			})
			require.Error(t, err)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})
	})

	t.Run("JobSchedule", func(t *testing.T) {
		t.Parallel()

		t.Run("BasicScheduling", func(t *testing.T) {
			exec, _ := setup(ctx, t)

			var (
				horizon       = time.Now()
				beforeHorizon = horizon.Add(-1 * time.Minute)
				afterHorizon  = horizon.Add(1 * time.Minute)
			)

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRetryable)})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateScheduled)})
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateScheduled)})

			// States that aren't scheduled.
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

			// Right state, but after horizon.
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateRetryable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateScheduled)})

			// First two scheduled because of limit.
			result, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 2,
				Now: &horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 2)
			require.Equal(t, job1.ID, result[0].Job.ID)
			require.False(t, result[0].ConflictDiscarded)
			require.Equal(t, job2.ID, result[1].Job.ID)
			require.False(t, result[1].ConflictDiscarded)

			// And then job3 scheduled.
			result, err = exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 2,
				Now: &horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.Equal(t, job3.ID, result[0].Job.ID)
			require.False(t, result[0].ConflictDiscarded)

			updatedJob1, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob1.State)

			updatedJob2, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob2.State)

			updatedJob3, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job3.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob3.State)
		})

		t.Run("HandlesUniqueConflicts", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var (
				horizon       = time.Now()
				beforeHorizon = horizon.Add(-1 * time.Minute)
			)

			defaultUniqueStates := []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			}
			// The default unique state list, minus retryable to allow for these conflicts:
			nonRetryableUniqueStates := []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			}

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-2"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			// job3 has no conflict (it's the only one with this key), so it should be
			// scheduled.
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-3"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(defaultUniqueStates),
			})

			// This one is a conflict with job1 because it's already running and has
			// the same unique properties:
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			// This one is *not* a conflict with job2 because it's completed, which
			// isn't in the unique states:
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateCompleted),
				UniqueKey:    []byte("unique-key-2"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})

			result, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 100,
				Now: &horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 3)

			updatedJob1, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, updatedJob1.State)
			require.Equal(t, "scheduler_discarded", gjson.GetBytes(updatedJob1.Metadata, "unique_key_conflict").String())

			updatedJob2, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob2.State)
			require.False(t, gjson.GetBytes(updatedJob2.Metadata, "unique_key_conflict").Exists())

			updatedJob3, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job3.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob3.State)
			require.False(t, gjson.GetBytes(updatedJob3.Metadata, "unique_key_conflict").Exists())
		})

		t.Run("SchedulingTwoRetryableJobsThatWillConflictWithEachOther", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var (
				horizon       = time.Now()
				beforeHorizon = horizon.Add(-1 * time.Minute)
			)

			// The default unique state list, minus retryable to allow for these conflicts:
			nonRetryableUniqueStates := []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			}

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})

			result, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 100,
				Now: &horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 2)

			updatedJob1, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob1.State)
			require.False(t, gjson.GetBytes(updatedJob1.Metadata, "unique_key_conflict").Exists())

			updatedJob2, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, updatedJob2.State)
			require.Equal(t, "scheduler_discarded", gjson.GetBytes(updatedJob2.Metadata, "unique_key_conflict").String())
		})
	})

	makeErrPayload := func(t *testing.T, now time.Time) []byte {
		t.Helper()

		errPayload, err := json.Marshal(rivertype.AttemptError{
			Attempt: 1, At: now, Error: "fake error", Trace: "foo.go:123\nbar.go:456",
		})
		require.NoError(t, err)
		return errPayload
	}

	setStateManyParams := func(params ...*riverdriver.JobSetStateIfRunningParams) *riverdriver.JobSetStateIfRunningManyParams {
		batchParams := &riverdriver.JobSetStateIfRunningManyParams{}
		for _, param := range params {
			var (
				attempt     *int
				errData     []byte
				finalizedAt *time.Time
				scheduledAt *time.Time
			)
			if param.Attempt != nil {
				attempt = param.Attempt
			}
			if param.ErrData != nil {
				errData = param.ErrData
			}
			if param.FinalizedAt != nil {
				finalizedAt = param.FinalizedAt
			}
			if param.ScheduledAt != nil {
				scheduledAt = param.ScheduledAt
			}

			batchParams.ID = append(batchParams.ID, param.ID)
			batchParams.Attempt = append(batchParams.Attempt, attempt)
			batchParams.ErrData = append(batchParams.ErrData, errData)
			batchParams.FinalizedAt = append(batchParams.FinalizedAt, finalizedAt)
			batchParams.MetadataDoMerge = append(batchParams.MetadataDoMerge, param.MetadataDoMerge)
			batchParams.MetadataUpdates = append(batchParams.MetadataUpdates, param.MetadataUpdates)
			batchParams.ScheduledAt = append(batchParams.ScheduledAt, scheduledAt)
			batchParams.State = append(batchParams.State, param.State)
		}

		return batchParams
	}

	t.Run("JobSetStateIfRunningMany_JobSetStateCompleted", func(t *testing.T) {
		t.Parallel()

		t.Run("CompletesARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCompleted(job.ID, now, nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateCompleted, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})

		t.Run("DoesNotCompleteARetryableJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCompleted(job.ID, now, nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.Nil(t, jobAfter.FinalizedAt)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})

		t.Run("StoresMetadataUpdates", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Metadata:  []byte(`{"foo":"baz", "something":"else"}`),
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCompleted(job.ID, now, []byte(`{"a":"b", "foo":"bar"}`))))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateCompleted, jobAfter.State)
			require.JSONEq(t, `{"a":"b", "foo":"bar", "something":"else"}`, string(jobAfter.Metadata))
		})

		t.Run("UnknownJobIgnored", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			// The operation doesn't return anything like a "not found" in case
			// of an unknown job so that it doesn't fail in case a job is
			// deleted in the interim as a completer is trying to finalize it.
			_, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCompleted(0, time.Now().UTC(), nil)))
			require.NoError(t, err)
		})
	})

	t.Run("JobSetStateIfRunningMany_JobSetStateErrored", func(t *testing.T) {
		t.Parallel()

		t.Run("SetsARunningJobToRetryable", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now), nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.WithinDuration(t, now, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))

			// validate error payload:
			require.Len(t, jobAfter.Errors, 1)
			require.Equal(t, now, jobAfter.Errors[0].At)
			require.Equal(t, 1, jobAfter.Errors[0].Attempt)
			require.Equal(t, "fake error", jobAfter.Errors[0].Error)
			require.Equal(t, "foo.go:123\nbar.go:456", jobAfter.Errors[0].Trace)
		})

		t.Run("DoesNotTouchAlreadyRetryableJobWithNoMetadataUpdates", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:       ptrutil.Ptr(rivertype.JobStateRetryable),
				ScheduledAt: ptrutil.Ptr(now.Add(10 * time.Second)),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now), nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)
		})

		t.Run("UpdatesOnlyMetadataForAlreadyRetryableJobs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Metadata:    []byte(`{"baz":"qux", "foo":"bar"}`),
				State:       ptrutil.Ptr(rivertype.JobStateRetryable),
				ScheduledAt: ptrutil.Ptr(now.Add(10 * time.Second)),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(
				riverdriver.JobSetStateErrorRetryable(job1.ID, now, makeErrPayload(t, now), []byte(`{"foo":"1", "output":{"a":"b"}}`)),
			))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.JSONEq(t, `{"baz":"qux", "foo":"1", "output":{"a":"b"}}`, string(jobAfter.Metadata))
			require.Empty(t, jobAfter.Errors)
			require.Equal(t, job1.ScheduledAt, jobAfter.ScheduledAt)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.JSONEq(t, `{"baz":"qux", "foo":"1", "output":{"a":"b"}}`, string(jobUpdated.Metadata))
			require.Empty(t, jobUpdated.Errors)
			require.Equal(t, job1.ScheduledAt, jobUpdated.ScheduledAt)
		})

		t.Run("SetsAJobWithCancelAttemptedAtToCancelled", func(t *testing.T) {
			// If a job has cancel_attempted_at in its metadata, it means that the user
			// tried to cancel the job with the Cancel API but that the job
			// finished/errored before the producer received the cancel notification.
			//
			// In this case, we want to move the job to cancelled instead of retryable
			// so that the job is not retried.
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Metadata:    []byte(fmt.Sprintf(`{"cancel_attempted_at":"%s"}`, time.Now().UTC().Format(time.RFC3339))),
				State:       ptrutil.Ptr(rivertype.JobStateRunning),
				ScheduledAt: ptrutil.Ptr(now.Add(-10 * time.Second)),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now), nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
			require.NotNil(t, jobAfter.FinalizedAt)
			// Loose assertion against FinalizedAt just to make sure it was set (it uses
			// the database's now() instead of a passed-in time):
			require.WithinDuration(t, time.Now().UTC(), *jobAfter.FinalizedAt, 2*time.Second)
			// ScheduledAt should not be touched:
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)

			// Errors should still be appended to:
			require.Len(t, jobAfter.Errors, 1)
			require.Contains(t, jobAfter.Errors[0].Error, "fake error")

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCancelled, jobUpdated.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)
		})
	})

	t.Run("JobSetStateIfRunningMany_JobSetStateCancelled", func(t *testing.T) {
		t.Parallel()

		t.Run("CancelsARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:        ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCancelled(job.ID, now, makeErrPayload(t, now), nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCancelled, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})
	})

	t.Run("JobSetStateIfRunningMany_JobSetStateDiscarded", func(t *testing.T) {
		t.Parallel()

		t.Run("DiscardsARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:        ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateDiscarded(job.ID, now, makeErrPayload(t, now), nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateDiscarded, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)
			require.Equal(t, "unique-key", string(jobAfter.UniqueKey))
			require.Equal(t, rivertype.JobStates(), jobAfter.UniqueStates)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, jobUpdated.State)
		})
	})

	t.Run("JobSetStateIfRunningMany_JobSetStateSnoozed", func(t *testing.T) {
		t.Parallel()

		t.Run("SnoozesARunningJob_WithNoPreexistingMetadata", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()
			snoozeUntil := now.Add(1 * time.Minute)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Attempt:   ptrutil.Ptr(5),
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateSnoozed(job.ID, snoozeUntil, 4, []byte(`{"snoozes": 1}`))))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, 4, jobAfter.Attempt)
			require.Equal(t, job.MaxAttempts, jobAfter.MaxAttempts)
			require.JSONEq(t, `{"snoozes": 1}`, string(jobAfter.Metadata))
			require.Equal(t, rivertype.JobStateScheduled, jobAfter.State)
			require.WithinDuration(t, snoozeUntil, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, 4, jobUpdated.Attempt)
			require.Equal(t, job.MaxAttempts, jobUpdated.MaxAttempts)
			require.JSONEq(t, `{"snoozes": 1}`, string(jobUpdated.Metadata))
			require.Equal(t, rivertype.JobStateScheduled, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})

		t.Run("SnoozesARunningJob_WithPreexistingMetadata", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()
			snoozeUntil := now.Add(1 * time.Minute)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Attempt:   ptrutil.Ptr(5),
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
				Metadata:  []byte(`{"foo": "bar", "snoozes": 5}`),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateSnoozed(job.ID, snoozeUntil, 4, []byte(`{"snoozes": 6}`))))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, 4, jobAfter.Attempt)
			require.Equal(t, job.MaxAttempts, jobAfter.MaxAttempts)
			require.JSONEq(t, `{"foo": "bar", "snoozes": 6}`, string(jobAfter.Metadata))
			require.Equal(t, rivertype.JobStateScheduled, jobAfter.State)
			require.WithinDuration(t, snoozeUntil, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, 4, jobUpdated.Attempt)
			require.Equal(t, job.MaxAttempts, jobUpdated.MaxAttempts)
			require.JSONEq(t, `{"foo": "bar", "snoozes": 6}`, string(jobUpdated.Metadata))
			require.Equal(t, rivertype.JobStateScheduled, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})
	})

	t.Run("JobSetStateIfRunningMany_MultipleJobsAtOnce", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		now := time.Now().UTC()
		future := now.Add(10 * time.Second)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(
			riverdriver.JobSetStateCompleted(job1.ID, now, []byte(`{"a":"b"}`)),
			riverdriver.JobSetStateErrorRetryable(job2.ID, future, makeErrPayload(t, now), nil),
			riverdriver.JobSetStateCancelled(job3.ID, now, makeErrPayload(t, now), nil),
		))
		require.NoError(t, err)
		completedJob := jobsAfter[0]
		require.Equal(t, rivertype.JobStateCompleted, completedJob.State)
		require.WithinDuration(t, now, *completedJob.FinalizedAt, time.Microsecond)
		require.JSONEq(t, `{"a":"b"}`, string(completedJob.Metadata))

		retryableJob := jobsAfter[1]
		require.Equal(t, rivertype.JobStateRetryable, retryableJob.State)
		require.WithinDuration(t, future, retryableJob.ScheduledAt, time.Microsecond)
		// validate error payload:
		require.Len(t, retryableJob.Errors, 1)
		require.Equal(t, now, retryableJob.Errors[0].At)
		require.Equal(t, 1, retryableJob.Errors[0].Attempt)
		require.Equal(t, "fake error", retryableJob.Errors[0].Error)
		require.Equal(t, "foo.go:123\nbar.go:456", retryableJob.Errors[0].Trace)

		cancelledJob := jobsAfter[2]
		require.Equal(t, rivertype.JobStateCancelled, cancelledJob.State)
		require.WithinDuration(t, now, *cancelledJob.FinalizedAt, time.Microsecond)
	})

	t.Run("JobUpdate", func(t *testing.T) {
		t.Parallel()

		t.Run("AllArgs", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			now := time.Now().UTC()

			updatedJob, err := exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
				ID:                  job.ID,
				AttemptDoUpdate:     true,
				Attempt:             7,
				AttemptedAtDoUpdate: true,
				AttemptedAt:         &now,
				AttemptedByDoUpdate: true,
				AttemptedBy:         []string{"worker1"},
				ErrorsDoUpdate:      true,
				Errors:              [][]byte{[]byte(`{"error":"message"}`)},
				FinalizedAtDoUpdate: true,
				FinalizedAt:         &now,
				StateDoUpdate:       true,
				State:               rivertype.JobStateDiscarded,
			})
			require.NoError(t, err)
			require.Equal(t, 7, updatedJob.Attempt)
			require.WithinDuration(t, now, *updatedJob.AttemptedAt, bundle.driver.TimePrecision())
			require.Equal(t, []string{"worker1"}, updatedJob.AttemptedBy)
			require.Equal(t, "message", updatedJob.Errors[0].Error)
			require.WithinDuration(t, now, *updatedJob.FinalizedAt, bundle.driver.TimePrecision())
			require.Equal(t, rivertype.JobStateDiscarded, updatedJob.State)
		})

		t.Run("NoArgs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			updatedJob, err := exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
				ID: job.ID,
			})
			require.NoError(t, err)
			require.Equal(t, job.Attempt, updatedJob.Attempt)
			require.Nil(t, updatedJob.AttemptedAt)
			require.Empty(t, updatedJob.AttemptedBy)
			require.Empty(t, updatedJob.Errors)
			require.Nil(t, updatedJob.FinalizedAt)
			require.Equal(t, job.State, updatedJob.State)
		})
	})

	const leaderTTL = 10 * time.Second

	// For use in test cases whera non-clock "now" is _not_ injected. This can
	// normally be very tight, but we see huge variance especially in GitHub
	// Actions, and given it's really not necessary to assert that this is
	// anything except within reasonable recent history, it's okay if it's big.
	const veryGenerousTimeCompareTolerance = 5 * time.Minute

	t.Run("LeaderAttemptElect", func(t *testing.T) {
		t.Parallel()

		t.Run("ElectsLeader", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			elected, err := exec.LeaderAttemptElect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: clientID,
				Now:      &now,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.True(t, elected) // won election

			leader, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.WithinDuration(t, now, leader.ElectedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now.Add(leaderTTL), leader.ExpiresAt, bundle.driver.TimePrecision())
		})

		t.Run("CannotElectTwiceInARow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(clientID),
			})

			elected, err := exec.LeaderAttemptElect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: "different-client-id",
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.False(t, elected) // lost election

			// The time should not have changed because we specified that we were not
			// already elected, and the elect query is a no-op if there's already a
			// updatedLeader:
			updatedLeader, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.Equal(t, leader.ExpiresAt, updatedLeader.ExpiresAt)
		})

		t.Run("WithoutNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			elected, err := exec.LeaderAttemptElect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: clientID,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.True(t, elected) // won election

			leader, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.WithinDuration(t, time.Now(), leader.ElectedAt, veryGenerousTimeCompareTolerance)
			require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, veryGenerousTimeCompareTolerance)
		})
	})

	t.Run("LeaderAttemptReelect", func(t *testing.T) {
		t.Parallel()

		t.Run("ElectsLeader", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			elected, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: clientID,
				Now:      &now,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.True(t, elected) // won election

			leader, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.WithinDuration(t, now, leader.ElectedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now.Add(leaderTTL), leader.ExpiresAt, bundle.driver.TimePrecision())
		})

		t.Run("ReelectsSameLeader", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(clientID),
			})

			// Re-elect the same leader. Use a larger TTL to see if time is updated,
			// because we are in a test transaction and the time is frozen at the start of
			// the transaction.
			elected, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: clientID,
				TTL:      30 * time.Second,
			})
			require.NoError(t, err)
			require.True(t, elected) // won re-election

			// expires_at should be incremented because this is the same leader that won
			// previously and we specified that we're already elected:
			updatedLeader, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.Greater(t, updatedLeader.ExpiresAt, leader.ExpiresAt)
		})

		t.Run("DoesNotReelectDifferentLeader", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(clientID),
			})

			elected, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: "different-client",
				TTL:      30 * time.Second,
			})
			require.NoError(t, err)
			require.False(t, elected) // did not win re-election

			// expires_at should be incremented because this is the same leader that won
			// previously and we specified that we're already elected:
			updatedLeader, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.Equal(t, leader.LeaderID, updatedLeader.LeaderID)
		})

		t.Run("WithoutNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			elected, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderElectParams{
				LeaderID: clientID,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.True(t, elected) // won election

			leader, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
			require.NoError(t, err)
			require.WithinDuration(t, time.Now(), leader.ElectedAt, veryGenerousTimeCompareTolerance)
			require.WithinDuration(t, time.Now().Add(leaderTTL), leader.ExpiresAt, veryGenerousTimeCompareTolerance)
		})
	})

	t.Run("LeaderDeleteExpired", func(t *testing.T) {
		t.Parallel()

		t.Run("DeletesExpired", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			{
				numDeleted, err := exec.LeaderDeleteExpired(ctx, &riverdriver.LeaderDeleteExpiredParams{})
				require.NoError(t, err)
				require.Zero(t, numDeleted)
			}

			_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				ElectedAt: ptrutil.Ptr(now.Add(-2 * time.Hour)),
				ExpiresAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
				LeaderID:  ptrutil.Ptr(clientID),
			})

			{
				numDeleted, err := exec.LeaderDeleteExpired(ctx, &riverdriver.LeaderDeleteExpiredParams{})
				require.NoError(t, err)
				require.Equal(t, 1, numDeleted)
			}
		})

		t.Run("WithInjectedNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			// Elected in the future.
			_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				ElectedAt: ptrutil.Ptr(now.Add(1 * time.Hour)),
				ExpiresAt: ptrutil.Ptr(now.Add(2 * time.Hour)),
				LeaderID:  ptrutil.Ptr(clientID),
			})

			numDeleted, err := exec.LeaderDeleteExpired(ctx, &riverdriver.LeaderDeleteExpiredParams{
				Now: ptrutil.Ptr(now.Add(2*time.Hour + 1*time.Second)),
			})
			require.NoError(t, err)
			require.Equal(t, 1, numDeleted)
		})
	})

	t.Run("LeaderInsert", func(t *testing.T) {
		t.Parallel()

		t.Run("InsertsLeader", func(t *testing.T) {
			exec, bundle := setup(ctx, t)

			var (
				now       = time.Now().UTC()
				electedAt = now.Add(1 * time.Second)
				expiresAt = now.Add(4*time.Hour + 3*time.Minute + 2*time.Second)
			)

			leader, err := exec.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
				ElectedAt: &electedAt,
				ExpiresAt: &expiresAt,
				LeaderID:  clientID,
				TTL:       leaderTTL,
			})
			require.NoError(t, err)
			require.WithinDuration(t, electedAt, leader.ElectedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, expiresAt, leader.ExpiresAt, bundle.driver.TimePrecision())
			require.Equal(t, clientID, leader.LeaderID)
		})

		t.Run("WithNow", func(t *testing.T) {
			exec, bundle := setup(ctx, t)

			now := time.Now().UTC().Add(-1 * time.Minute) // subtract a minute to make sure it'not coincidentally working using wall time

			leader, err := exec.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
				LeaderID: clientID,
				Now:      &now,
				TTL:      leaderTTL,
			})
			require.NoError(t, err)
			require.WithinDuration(t, now, leader.ElectedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now.Add(leaderTTL), leader.ExpiresAt, bundle.driver.TimePrecision())
			require.Equal(t, clientID, leader.LeaderID)
		})
	})

	t.Run("LeaderGetElectedLeader", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		now := time.Now().UTC()

		_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
			LeaderID: ptrutil.Ptr(clientID),
			Now:      &now,
		})

		leader, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
		require.NoError(t, err)
		require.WithinDuration(t, now, leader.ElectedAt, bundle.driver.TimePrecision())
		require.WithinDuration(t, now.Add(leaderTTL), leader.ExpiresAt, bundle.driver.TimePrecision())
		require.Equal(t, clientID, leader.LeaderID)
	})

	t.Run("LeaderResign", func(t *testing.T) {
		t.Parallel()

		t.Run("Success", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			{
				resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
					LeaderID:        clientID,
					LeadershipTopic: string(notifier.NotificationTopicLeadership),
				})
				require.NoError(t, err)
				require.False(t, resigned)
			}

			_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr(clientID),
			})

			{
				resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
					LeaderID:        clientID,
					LeadershipTopic: string(notifier.NotificationTopicLeadership),
				})
				require.NoError(t, err)
				require.True(t, resigned)
			}
		})

		t.Run("DoesNotResignWithoutLeadership", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
				LeaderID: ptrutil.Ptr("other-client-id"),
			})

			resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
				LeaderID:        clientID,
				LeadershipTopic: string(notifier.NotificationTopicLeadership),
			})
			require.NoError(t, err)
			require.False(t, resigned)
		})
	})

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

	t.Run("PGAdvisoryXactLock", func(t *testing.T) {
		t.Parallel()

		{
			driver, _ := driverWithSchema(ctx, t, nil)
			if driver.DatabaseName() == databaseNameSQLite {
				t.Logf("Skipping PGAdvisoryXactLock test for SQLite")
				return
			}
		}

		exec, _ := setup(ctx, t)

		// It's possible for multiple versions of this test to be running at the
		// same time (from different drivers), so make sure the lock we're
		// acquiring per test is unique by using the complete test name. Also
		// add randomness in case a test is run multiple times with `-count`.
		lockHash := hashutil.NewAdvisoryLockHash(0)
		lockHash.Write([]byte(t.Name()))
		lockHash.Write([]byte(randutil.Hex(10)))
		key := lockHash.Key()

		// Tries to acquire the given lock from another test transaction and
		// returns true if the lock was acquired.
		tryAcquireLock := func(exec riverdriver.Executor) bool {
			var lockAcquired bool
			require.NoError(t, exec.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&lockAcquired))
			return lockAcquired
		}

		// Start a transaction to acquire the lock so we can later release the
		// lock by rolling back.
		execTx, err := exec.Begin(ctx)
		require.NoError(t, err)

		// Acquire the advisory lock on the main test transaction.
		_, err = execTx.PGAdvisoryXactLock(ctx, key)
		require.NoError(t, err)

		// Start another test transaction unrelated to the first.
		otherExec, _ := executorWithTx(ctx, t)

		// The other test transaction is unable to acquire the lock because the
		// first test transaction holds it.
		require.False(t, tryAcquireLock(otherExec))

		// Roll back the first test transaction to release the lock.
		require.NoError(t, execTx.Rollback(ctx))

		// The other test transaction can now acquire the lock.
		require.True(t, tryAcquireLock(otherExec))
	})

	t.Run("QueueCreateOrSetUpdatedAt", func(t *testing.T) {
		t.Parallel()

		mustUnmarshalJSON := func(t *testing.T, data []byte) map[string]any {
			t.Helper()

			var dataMap map[string]any
			require.NoError(t, json.Unmarshal(data, &dataMap))
			return dataMap
		}

		t.Run("InsertsANewQueueWithDefaultUpdatedAt", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			metadata := []byte(`{"foo": "bar"}`)
			now := time.Now().UTC()
			queue, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Metadata: metadata,
				Name:     "new-queue",
				Now:      &now,
			})
			require.NoError(t, err)
			require.WithinDuration(t, now, queue.CreatedAt, bundle.driver.TimePrecision())
			require.JSONEq(t, string(metadata), string(queue.Metadata))
			require.Equal(t, "new-queue", queue.Name)
			require.Nil(t, queue.PausedAt)
			require.WithinDuration(t, now, queue.UpdatedAt, bundle.driver.TimePrecision())
		})

		t.Run("InsertsANewQueueWithCustomPausedAt", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC().Add(-5 * time.Minute)
			queue, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Name:     "new-queue",
				PausedAt: ptrutil.Ptr(now),
			})
			require.NoError(t, err)
			require.Equal(t, "new-queue", queue.Name)
			require.WithinDuration(t, now, *queue.PausedAt, bundle.driver.TimePrecision())
		})

		t.Run("UpdatesTheUpdatedAtOfExistingQueue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			metadata := []byte(`{"foo": "bar"}`)
			tBefore := time.Now().UTC()
			queueBefore, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Metadata:  metadata,
				Name:      "updatable-queue",
				UpdatedAt: &tBefore,
			})
			require.NoError(t, err)
			require.WithinDuration(t, tBefore, queueBefore.UpdatedAt, time.Millisecond)

			tAfter := tBefore.Add(2 * time.Second)
			queueAfter, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Metadata:  []byte(`{"other": "metadata"}`),
				Name:      "updatable-queue",
				UpdatedAt: &tAfter,
			})
			require.NoError(t, err)

			// unchanged:
			require.Equal(t, queueBefore.CreatedAt, queueAfter.CreatedAt)
			require.Equal(t, mustUnmarshalJSON(t, metadata), mustUnmarshalJSON(t, queueAfter.Metadata))
			require.Equal(t, "updatable-queue", queueAfter.Name)
			require.Nil(t, queueAfter.PausedAt)

			// Timestamp is bumped:
			require.WithinDuration(t, tAfter, queueAfter.UpdatedAt, time.Millisecond)
		})
	})

	t.Run("QueueDeleteExpired", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		now := time.Now()
		_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now)})
		queue2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-25 * time.Hour))})
		queue3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-26 * time.Hour))})
		queue4 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-48 * time.Hour))})
		_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-23 * time.Hour))})

		horizon := now.Add(-24 * time.Hour)
		deletedQueueNames, err := exec.QueueDeleteExpired(ctx, &riverdriver.QueueDeleteExpiredParams{Max: 2, UpdatedAtHorizon: horizon})
		require.NoError(t, err)

		// queue2 and queue3 should be deleted, with queue4 being skipped due to max of 2:
		require.Equal(t, []string{queue2.Name, queue3.Name}, deletedQueueNames)

		// Try again, make sure queue4 gets deleted this time:
		deletedQueueNames, err = exec.QueueDeleteExpired(ctx, &riverdriver.QueueDeleteExpiredParams{Max: 2, UpdatedAtHorizon: horizon})
		require.NoError(t, err)

		require.Equal(t, []string{queue4.Name}, deletedQueueNames)
	})

	t.Run("QueueGet", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Metadata: []byte(`{"foo": "bar"}`)})

		queueFetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name: queue.Name,
		})
		require.NoError(t, err)

		require.WithinDuration(t, queue.CreatedAt, queueFetched.CreatedAt, bundle.driver.TimePrecision())
		require.Equal(t, queue.Metadata, queueFetched.Metadata)
		require.Equal(t, queue.Name, queueFetched.Name)
		require.Nil(t, queueFetched.PausedAt)
		require.WithinDuration(t, queue.UpdatedAt, queueFetched.UpdatedAt, bundle.driver.TimePrecision())

		queueFetched, err = exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name: "nonexistent-queue",
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		require.Nil(t, queueFetched)
	})

	t.Run("QueueList", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		requireQueuesEqual := func(t *testing.T, target, actual *rivertype.Queue) {
			t.Helper()
			require.WithinDuration(t, target.CreatedAt, actual.CreatedAt, bundle.driver.TimePrecision())
			require.Equal(t, target.Metadata, actual.Metadata)
			require.Equal(t, target.Name, actual.Name)
			if target.PausedAt == nil {
				require.Nil(t, actual.PausedAt)
			} else {
				require.NotNil(t, actual.PausedAt)
				require.WithinDuration(t, *target.PausedAt, *actual.PausedAt, bundle.driver.TimePrecision())
			}
		}

		queues, err := exec.QueueList(ctx, &riverdriver.QueueListParams{
			Max: 10,
		})
		require.NoError(t, err)
		require.Empty(t, queues)

		// Make queue1, already paused:
		queue1 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Metadata: []byte(`{"foo": "bar"}`), PausedAt: ptrutil.Ptr(time.Now())})
		require.NoError(t, err)

		queue2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{})
		queue3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{})

		queues, err = exec.QueueList(ctx, &riverdriver.QueueListParams{
			Max: 2,
		})
		require.NoError(t, err)

		require.Len(t, queues, 2)
		requireQueuesEqual(t, queue1, queues[0])
		requireQueuesEqual(t, queue2, queues[1])

		queues, err = exec.QueueList(ctx, &riverdriver.QueueListParams{
			Max: 3,
		})
		require.NoError(t, err)

		require.Len(t, queues, 3)
		requireQueuesEqual(t, queue3, queues[2])
	})

	t.Run("QueueNameListByPrefix", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		t.Run("ListsQueuesInOrderWithMaxLimit", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_zzz")})
			queue2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_aaa")})
			queue3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_bbb")})
			_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("different_prefix_queue")})

			queueNames, err := exec.QueueNameListByPrefix(ctx, &riverdriver.QueueNameListByPrefixParams{
				After:   "queue2",
				Exclude: nil,
				Max:     2,
				Prefix:  "queue",
				Schema:  "",
			})
			require.NoError(t, err)
			require.Equal(t, []string{queue2.Name, queue3.Name}, queueNames) // sorted by name
		})

		t.Run("ExcludesQueueNamesInExcludeList", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue1 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_zzz")})
			queue2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_aaa")})
			queue3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_bbb")})

			queueNames, err := exec.QueueNameListByPrefix(ctx, &riverdriver.QueueNameListByPrefixParams{
				After:   "queue2",
				Exclude: []string{queue2.Name},
				Max:     2,
				Prefix:  "queue",
				Schema:  "",
			})
			require.NoError(t, err)
			require.Equal(t, []string{queue3.Name, queue1.Name}, queueNames)
		})
	})

	t.Run("QueuePause", func(t *testing.T) {
		t.Parallel()

		t.Run("ExistingPausedQueue", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC().Add(-5 * time.Minute)

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{
				PausedAt:  &now,
				UpdatedAt: &now,
			})

			require.NoError(t, exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: queue.Name,
			}))
			queueFetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue.Name,
			})
			require.NoError(t, err)
			require.NotNil(t, queueFetched.PausedAt)
			require.WithinDuration(t, *queue.PausedAt, *queueFetched.PausedAt, bundle.driver.TimePrecision()) // paused_at stays unchanged
			require.WithinDuration(t, queue.UpdatedAt, queueFetched.UpdatedAt, bundle.driver.TimePrecision()) // updated_at stays unchanged
		})

		t.Run("ExistingUnpausedQueue", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{
				UpdatedAt: ptrutil.Ptr(now.Add(-5 * time.Minute)),
			})
			require.Nil(t, queue.PausedAt)

			require.NoError(t, exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: queue.Name,
				Now:  &now,
			}))

			queueFetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue.Name,
			})
			require.NoError(t, err)
			require.WithinDuration(t, now, *queueFetched.PausedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now, queueFetched.UpdatedAt, bundle.driver.TimePrecision())
		})

		t.Run("NonExistentQueue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			err := exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: "queue1",
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})

		t.Run("AllQueuesExistingQueues", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue1 := testfactory.Queue(ctx, t, exec, nil)
			require.Nil(t, queue1.PausedAt)
			queue2 := testfactory.Queue(ctx, t, exec, nil)
			require.Nil(t, queue2.PausedAt)

			require.NoError(t, exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: rivercommon.AllQueuesString,
			}))

			now := time.Now()

			queue1Fetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue1.Name,
			})
			require.NoError(t, err)
			require.NotNil(t, queue1Fetched.PausedAt)
			require.WithinDuration(t, now, *(queue1Fetched.PausedAt), 500*time.Millisecond)

			queue2Fetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue2.Name,
			})
			require.NoError(t, err)
			require.NotNil(t, queue2Fetched.PausedAt)
			require.WithinDuration(t, now, *(queue2Fetched.PausedAt), 500*time.Millisecond)
		})

		t.Run("AllQueuesNoQueues", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			require.NoError(t, exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: rivercommon.AllQueuesString,
			}))
		})
	})

	t.Run("QueueResume", func(t *testing.T) {
		t.Parallel()

		t.Run("ExistingPausedQueue", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{
				PausedAt:  ptrutil.Ptr(now.Add(-5 * time.Minute)),
				UpdatedAt: ptrutil.Ptr(now.Add(-5 * time.Minute)),
			})

			require.NoError(t, exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
				Name: queue.Name,
				Now:  &now,
			}))

			queueFetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue.Name,
			})
			require.NoError(t, err)
			require.Nil(t, queueFetched.PausedAt)
			require.WithinDuration(t, now, queueFetched.UpdatedAt, bundle.driver.TimePrecision())
		})

		t.Run("ExistingUnpausedQueue", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{
				UpdatedAt: ptrutil.Ptr(now.Add(-5 * time.Minute)),
			})

			require.NoError(t, exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
				Name: queue.Name,
			}))

			queueFetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue.Name,
			})
			require.NoError(t, err)
			require.Nil(t, queueFetched.PausedAt)
			require.WithinDuration(t, queue.UpdatedAt, queueFetched.UpdatedAt, bundle.driver.TimePrecision()) // updated_at stays unchanged
		})

		t.Run("NonExistentQueue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			err := exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
				Name: "queue1",
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})

		t.Run("AllQueuesExistingQueues", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue1 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{})
			require.Nil(t, queue1.PausedAt)
			queue2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{})
			require.Nil(t, queue2.PausedAt)

			require.NoError(t, exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: rivercommon.AllQueuesString,
			}))
			require.NoError(t, exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
				Name: rivercommon.AllQueuesString,
			}))

			queue1Fetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue1.Name,
			})
			require.NoError(t, err)
			require.Nil(t, queue1Fetched.PausedAt)

			queue2Fetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue2.Name,
			})
			require.NoError(t, err)
			require.Nil(t, queue2Fetched.PausedAt)
		})

		t.Run("AllQueuesNoQueues", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			require.NoError(t, exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
				Name: rivercommon.AllQueuesString,
			}))
		})
	})

	t.Run("QueueUpdate", func(t *testing.T) {
		t.Parallel()

		t.Run("UpdatesFieldsIfDoUpdateIsTrue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Metadata: []byte(`{"foo": "bar"}`)})

			updatedQueue, err := exec.QueueUpdate(ctx, &riverdriver.QueueUpdateParams{
				Metadata:         []byte(`{"baz": "qux"}`),
				MetadataDoUpdate: true,
				Name:             queue.Name,
			})
			require.NoError(t, err)
			require.JSONEq(t, `{"baz": "qux"}`, string(updatedQueue.Metadata))
		})

		t.Run("DoesNotUpdateFieldsIfDoUpdateIsFalse", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Metadata: []byte(`{"foo": "bar"}`)})

			var myInt int
			err := exec.QueryRow(ctx, "SELECT 1").Scan(&myInt)
			require.NoError(t, err)
			require.Equal(t, 1, myInt)

			updatedQueue, err := exec.QueueUpdate(ctx, &riverdriver.QueueUpdateParams{
				Metadata:         []byte(`{"baz": "qux"}`),
				MetadataDoUpdate: false,
				Name:             queue.Name,
			})
			require.NoError(t, err)
			require.JSONEq(t, `{"foo": "bar"}`, string(updatedQueue.Metadata))
		})
	})

	t.Run("QueryRow", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		var (
			field1   int
			field2   int
			field3   int
			fieldFoo string
		)

		err := exec.QueryRow(ctx, "SELECT 1, 2, 3, 'foo'").Scan(&field1, &field2, &field3, &fieldFoo)
		require.NoError(t, err)

		require.Equal(t, 1, field1)
		require.Equal(t, 2, field2)
		require.Equal(t, 3, field3)
		require.Equal(t, "foo", fieldFoo)
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
}

type testListenerBundle[TTx any] struct {
	driver riverdriver.Driver[TTx]
	exec   riverdriver.Executor
}

func setupListener[TTx any](ctx context.Context, t *testing.T, driverWithPool func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string)) (riverdriver.Listener, *testListenerBundle[TTx]) {
	t.Helper()

	var (
		driver, schema = driverWithPool(ctx, t, nil)
		listener       = driver.GetListener(&riverdriver.GetListenenerParams{Schema: schema})
	)

	return listener, &testListenerBundle[TTx]{
		driver: driver,
		exec:   driver.GetExecutor(),
	}
}

func exerciseListener[TTx any](ctx context.Context, t *testing.T, driverWithPool func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string)) {
	t.Helper()

	connectListener := func(ctx context.Context, t *testing.T, listener riverdriver.Listener) {
		t.Helper()

		require.NoError(t, listener.Connect(ctx))
		t.Cleanup(func() { require.NoError(t, listener.Close(ctx)) })
	}

	requireNoNotification := func(ctx context.Context, t *testing.T, listener riverdriver.Listener) {
		t.Helper()

		// Ugh, this is a little sketchy, but hard to test in another way.
		ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		notification, err := listener.WaitForNotification(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded, "Expected no notification, but got: %+v", notification)
	}

	waitForNotification := func(ctx context.Context, t *testing.T, listener riverdriver.Listener) *riverdriver.Notification {
		t.Helper()

		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		notification, err := listener.WaitForNotification(ctx)
		require.NoError(t, err)

		return notification
	}

	t.Run("Close_NoOpIfNotConnected", func(t *testing.T) {
		t.Parallel()

		listener, _ := setupListener(ctx, t, driverWithPool)
		require.NoError(t, listener.Close(ctx))
	})

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		listener, bundle := setupListener(ctx, t, driverWithPool)

		connectListener(ctx, t, listener)

		require.NoError(t, listener.Listen(ctx, "topic1"))
		require.NoError(t, listener.Listen(ctx, "topic2"))

		require.NoError(t, listener.Ping(ctx)) // still alive

		{
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic1", Payload: []string{"payload1_1"}, Schema: listener.Schema()}))
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic2", Payload: []string{"payload2_1"}, Schema: listener.Schema()}))

			notification := waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1_1"}, notification)
			notification = waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic2", Payload: "payload2_1"}, notification)
		}

		require.NoError(t, listener.Unlisten(ctx, "topic2"))

		{
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic1", Payload: []string{"payload1_2"}, Schema: listener.Schema()}))
			require.NoError(t, bundle.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic2", Payload: []string{"payload2_2"}, Schema: listener.Schema()}))

			notification := waitForNotification(ctx, t, listener)
			require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1_2"}, notification)

			requireNoNotification(ctx, t, listener)
		}

		require.NoError(t, listener.Unlisten(ctx, "topic1"))

		require.NoError(t, listener.Close(ctx))
	})

	t.Run("SchemaFromParameter", func(t *testing.T) {
		t.Parallel()

		var (
			driver, _ = driverWithPool(ctx, t, nil)
			listener  = driver.GetListener(&riverdriver.GetListenenerParams{Schema: "my_custom_schema"})
		)

		require.Equal(t, "my_custom_schema", listener.Schema())
	})

	t.Run("SchemaFromSearchPath", func(t *testing.T) {
		t.Parallel()

		var (
			driver, _ = driverWithPool(ctx, t, nil)
			listener  = driver.GetListener(&riverdriver.GetListenenerParams{Schema: ""})
		)

		listener.SetAfterConnectExec("SET search_path TO 'public'")

		connectListener(ctx, t, listener)
		require.Equal(t, "public", listener.Schema())
	})

	t.Run("EmptySchemaFromSearchPath", func(t *testing.T) {
		t.Parallel()

		var (
			driver, _ = driverWithPool(ctx, t, nil)
			listener  = driver.GetListener(&riverdriver.GetListenenerParams{Schema: ""})
		)

		connectListener(ctx, t, listener)
		require.Empty(t, listener.Schema())
	})

	t.Run("TransactionGated", func(t *testing.T) {
		t.Parallel()

		listener, bundle := setupListener(ctx, t, driverWithPool)

		connectListener(ctx, t, listener)

		require.NoError(t, listener.Listen(ctx, "topic1"))

		tx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)

		require.NoError(t, tx.NotifyMany(ctx, &riverdriver.NotifyManyParams{Topic: "topic1", Payload: []string{"payload1"}, Schema: listener.Schema()}))

		// No notification because the transaction hasn't committed yet.
		requireNoNotification(ctx, t, listener)

		require.NoError(t, tx.Commit(ctx))

		// Notification received now that transaction has committed.
		notification := waitForNotification(ctx, t, listener)
		require.Equal(t, &riverdriver.Notification{Topic: "topic1", Payload: "payload1"}, notification)
	})

	t.Run("MultipleReuse", func(t *testing.T) {
		t.Parallel()

		listener, _ := setupListener(ctx, t, driverWithPool)

		connectListener(ctx, t, listener)

		require.NoError(t, listener.Listen(ctx, "topic1"))
		require.NoError(t, listener.Unlisten(ctx, "topic1"))

		require.NoError(t, listener.Close(ctx))
		require.NoError(t, listener.Connect(ctx))

		require.NoError(t, listener.Listen(ctx, "topic1"))
		require.NoError(t, listener.Unlisten(ctx, "topic1"))
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
		require.Regexp(t, fmt.Sprintf(`(pq: relation "%s\.%s" does not exist|no such table: %s\.%s)`, schema, missingRelation, schema, missingRelation), err.Error())
	}
}
