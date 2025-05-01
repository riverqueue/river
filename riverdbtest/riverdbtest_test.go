package riverdbtest

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func TestTestSchema(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
		exec   = driver.GetExecutor()
	)

	// Always use this set of options on the first invocation of TestSchema in
	// each test. Makes sure that the initial check that package name isn't
	// `riverdbtest` is skipped, but it's only needed once because the
	// check's done in a `sync.Once`. Must be used in every test case because
	// we're using `t.Parallel()` and any test could win the first run race.
	firstInvocationOpts := &TestSchemaOpts{skipPackageNameCheck: true}

	t.Run("BasicExerciseAndVisibility", func(t *testing.T) {
		t.Parallel()

		schema1 := TestSchema(ctx, t, driver, firstInvocationOpts)
		require.Regexp(t, `\Ariverdbtest_`, schema1)

		schema2 := TestSchema(ctx, t, driver, nil)
		require.Regexp(t, `\Ariverdbtest_`, schema2)

		require.NotEqual(t, schema1, schema2)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("schema1_job"), Schema: schema1})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("schema2_job"), Schema: schema2})

		// Notably, the jobs will actually share an ID because the schemas are
		// brand new an the ID sequence will start from 1 in each one every time
		// this package's tests are run. They'll start at 1 on the first test
		// run, but will increase if `-count` is issued because schemas will
		// start being reused.
		//
		// Know about this shared ID is important because it implies we cannot
		// compare jobs just by ID below. We have to check that another
		// property like their kind also matches.
		require.Equal(t, job1.ID, job2.ID)

		// Each job is found in its appropriate schema. Make sure to check kind
		// because as above, IDs will be identical.
		{
			fetchedJob1, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: schema1})
			require.NoError(t, err)
			require.Equal(t, "schema1_job", fetchedJob1.Kind)

			fetchedJob2, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: schema2})
			require.NoError(t, err)
			require.Equal(t, "schema2_job", fetchedJob2.Kind)
		}

		// Essentially the same check as above, but just looking that jobs are
		// found in each schema by their appropriate kind.
		{
			fetchedJobs1, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{Kind: []string{"schema1_job"}, Schema: schema1})
			require.NoError(t, err)
			require.Len(t, fetchedJobs1, 1)

			fetchedJobs2, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{Kind: []string{"schema2_job"}, Schema: schema2})
			require.NoError(t, err)
			require.Len(t, fetchedJobs2, 1)
		}

		// Invert the schemas on each check to show that no jobs intended for
		// the other schema are found in each other's schema.
		{
			fetchedJobs1, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{Kind: []string{"schema1_job"}, Schema: schema2})
			require.NoError(t, err)
			require.Empty(t, fetchedJobs1)

			fetchedJobs2, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{Kind: []string{"schema2_job"}, Schema: schema1})
			require.NoError(t, err)
			require.Empty(t, fetchedJobs2)
		}
	})

	t.Run("EmptyLines", func(t *testing.T) {
		t.Parallel()

		var schema string

		t.Run("FirstCheckout", func(t *testing.T) {
			schema = TestSchema(ctx, t, driver, &TestSchemaOpts{
				Lines:                []string{}, // non-nil empty indicates no migrations should be run
				skipPackageNameCheck: true,
			})

			_, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: 1, Schema: schema})
			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, pgerrcode.UndefinedTable, pgErr.Code)
		})

		// Get another empty schema to make sure that truncating tables with an
		// empty migration line works. This schema is reused because the subtest
		// above will have checked its schema back in when its cleanup hook
		// runs.
		nextSchema := TestSchema(ctx, t, driver, &TestSchemaOpts{
			Lines: []string{},
		})
		require.Equal(t, schema, nextSchema)
	})

	t.Run("LineTargetVersions", func(t *testing.T) {
		t.Parallel()

		var schema string

		t.Run("FirstCheckout", func(t *testing.T) {
			schema = TestSchema(ctx, t, driver, &TestSchemaOpts{
				LineTargetVersions:   map[string]int{riverdriver.MigrationLineMain: 1},
				skipPackageNameCheck: true,
			})

			// we can get a migration
			_, err := exec.MigrationGetAllAssumingMain(ctx, &riverdriver.MigrationGetAllAssumingMainParams{Schema: schema})
			require.NoError(t, err)

			// ... but not a job (because that comes up in version 002
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: 1, Schema: schema})
			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, pgerrcode.UndefinedTable, pgErr.Code)
		})

		// TestSchema can reuse the schema with the same LineTargetVersions. Get
		// another test schema after cleanup has run on the test case above and
		// make sure it's the same.
		nextSchema := TestSchema(ctx, t, driver, &TestSchemaOpts{
			LineTargetVersions: map[string]int{riverdriver.MigrationLineMain: 1},
		})
		require.Equal(t, schema, nextSchema)

		// Another main-only schema without LineTargetVersions must return a
		// different schema because it has a different expected version.
		schemaWithoutLineTargetVersions := TestSchema(ctx, t, driver, &TestSchemaOpts{
			Lines: []string{riverdriver.MigrationLineMain},
		})
		require.NotEqual(t, schema, schemaWithoutLineTargetVersions)
	})
}

func TestPackageFromFunc(t *testing.T) {
	t.Parallel()

	require.Equal(t, "river", packageFromFunc("github.com/riverqueue/river.Test_Client.func1"))
	require.Equal(t, "jobcompleter", packageFromFunc("github.com/riverqueue/river/internal/jobcompleter.testCompleterWait"))
}

func TestTestTx(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
	)

	t.Run("TransactionVisibility", func(t *testing.T) {
		t.Parallel()

		type PoolOrTx interface {
			Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
		}

		var schema string
		checkTestTable := func(ctx context.Context, poolOrTx PoolOrTx) error {
			_, err := poolOrTx.Exec(ctx, fmt.Sprintf("SELECT * FROM %s.river_shared_test_tx_table", schema))
			return err
		}

		// Test cleanups are invoked in the order of last added, first called.
		// When TestTx is called below it adds a cleanup, so we want to make
		// sure that this cleanup, which checks that the database remains
		// pristine, is invoked after the TestTx cleanup, so we add it first.
		t.Cleanup(func() {
			// Tests may inherit context from `t.Context()` which is cancelled
			// after tests run and before calling clean up. We need a
			// non-cancelled context to issue rollback here, so use a bit of a
			// bludgeon to do so with `context.WithoutCancel()`.
			ctx := context.WithoutCancel(ctx)

			err := checkTestTable(ctx, dbPool)
			require.Error(t, err)

			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, pgerrcode.UndefinedTable, pgErr.Code)
		})

		var tx pgx.Tx
		tx, schema = TestTx(ctx, t, driver, &TestTxOpts{skipPackageNameCheck: true})

		_, err := tx.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.river_shared_test_tx_table (id bigint)", schema))
		require.NoError(t, err)

		err = checkTestTable(ctx, tx)
		require.NoError(t, err)
	})

	t.Run("SchemaSharing", func(t *testing.T) {
		t.Parallel()

		_, schema1 := TestTx(ctx, t, driver, &TestTxOpts{skipPackageNameCheck: true})
		_, schema2 := TestTx(ctx, t, driver, &TestTxOpts{skipPackageNameCheck: true})
		require.Equal(t, schema1, schema2)
	})

	t.Run("DisableSchemaSharing", func(t *testing.T) {
		t.Parallel()

		_, schema1 := TestTx(ctx, t, driver, &TestTxOpts{skipPackageNameCheck: true, DisableSchemaSharing: true})
		_, schema2 := TestTx(ctx, t, driver, &TestTxOpts{skipPackageNameCheck: true}) // doesn't specify DisableSchemaSharing, but doesn't have to since first call won't check its schema in
		require.NotEqual(t, schema1, schema2)
	})

	t.Run("EmptyLines", func(t *testing.T) {
		t.Parallel()

		{
			tx, schema := TestTx(ctx, t, driver, &TestTxOpts{
				lines:                []string{}, // non-nil empty indicates no migrations should be run
				skipPackageNameCheck: true,
			})

			_, err := driver.UnwrapExecutor(tx).JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: 1, Schema: schema})
			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, pgerrcode.UndefinedTable, pgErr.Code)
		}

		// Get another test transaction with empty schema to make sure that
		// rollback with an empty migration line works. This schema is reused
		// because the subtest above will have added to test transaction schema
		// to testTxSchemas.
		{
			tx, schema := TestTx(ctx, t, driver, &TestTxOpts{
				lines: []string{},
			})
			_, err := driver.UnwrapExecutor(tx).JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: 1, Schema: schema})
			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, pgerrcode.UndefinedTable, pgErr.Code)
		}

		// A test transaction with default options uses the main schema and has a jobs table.
		{
			tx, schema := TestTx(ctx, t, driver, nil)
			_, err := driver.UnwrapExecutor(tx).JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: 1, Schema: schema})
			require.ErrorIs(t, rivertype.ErrNotFound, err)
		}
	})

	// Simulates a bunch of parallel processes using `TestTx` simultaneously.
	// With the help of `go test -race`, should identify mutex/locking/parallel
	// access problems if there are any.
	t.Run("ConcurrentAccess", func(t *testing.T) {
		t.Parallel()

		// Don't open more than maximum pool size transactions at once because
		// that would deadlock.
		const numGoroutines = 4

		var (
			ctx = context.Background()
			wg  sync.WaitGroup
		)

		dbPool := riversharedtest.DBPoolClone(ctx, t)

		wg.Add(4)
		for i := range numGoroutines {
			workerNum := i
			go func() {
				_, _ = TestTx(ctx, t, riverpgxv5.New(dbPool), &TestTxOpts{skipPackageNameCheck: true})
				t.Logf("Opened transaction: %d", workerNum)
				wg.Done()
			}()
		}

		wg.Wait()
	})
}
