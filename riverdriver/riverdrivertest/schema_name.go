package riverdrivertest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivertype"
)

func exerciseSchemaName[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string),
) {
	t.Helper()

	t.Run("SchemaNameWithSpace", func(t *testing.T) {
		t.Parallel()

		driver, _ := driverWithSchema(ctx, t, nil)

		// In SQLite schemas are files assigned to particular names, so this
		// check isn't relevant in the same way.
		if driver.DatabaseName() != databaseNamePostgres {
			t.Skip("Skipping; schema names with spaces only relevant for Postgres")
		}

		// Schemas should get cleaned up, but still need some randomness in case
		// multiple tests are running in parallel.
		schema := "river test schema " + randutil.Hex(8)

		exec := driver.GetExecutor()

		require.NoError(t, exec.SchemaCreate(ctx, &riverdriver.SchemaCreateParams{Schema: schema}))
		t.Cleanup(func() {
			require.NoError(t, exec.SchemaDrop(ctx, &riverdriver.SchemaDropParams{Schema: schema}))
		})

		migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
			Logger: riversharedtest.Logger(t),
			Schema: schema,
		})
		require.NoError(t, err)

		_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
		require.NoError(t, err)

		// Insert a job and verify it can be read back.
		insertedJobs, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs: []*riverdriver.JobInsertFastParams{{
				EncodedArgs: []byte(`{}`),
				Kind:        "test_kind",
				MaxAttempts: 25,
				Metadata:    []byte(`{}`),
				Priority:    1,
				Queue:       "default",
				State:       rivertype.JobStateAvailable,
			}},
			Schema: schema,
		})
		require.NoError(t, err)
		require.Len(t, insertedJobs, 1)

		fetchedJob, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     insertedJobs[0].Job.ID,
			Schema: schema,
		})
		require.NoError(t, err)
		require.Equal(t, insertedJobs[0].Job.ID, fetchedJob.ID)
		require.Equal(t, "test_kind", fetchedJob.Kind)

		// Verify the table exists in the schema.
		exists, err := exec.TableExists(ctx, &riverdriver.TableExistsParams{
			Schema: schema,
			Table:  "river_job",
		})
		require.NoError(t, err)
		require.True(t, exists)

		// Migrate back down.
		_, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
			TargetVersion: -1,
		})
		require.NoError(t, err)

		// Verify tables are gone.
		exists, err = exec.TableExists(ctx, &riverdriver.TableExistsParams{
			Schema: schema,
			Table:  "river_job",
		})
		require.NoError(t, err)
		require.False(t, exists)
	})
}
