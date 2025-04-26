// Package riverdbtest contains testing infrastructure for the River project
// itself that creates isolated schemas suitable for use within a single case.
//
// This package is for internal use and should not be considered stable. Changes
// to functions and types in this package WILL NOT be considered breaking
// changes for purposes of River's semantic versioning.
package riverdbtest

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

const schemaDateFormat = "2006_01_02t15_04_05" // everything here needs to be lowercase because Postgres forces schema names to lowercase

var (
	genSchemaBase       sync.Once                   //nolint:gochecknoglobals
	idleSchemas         = make(map[string][]string) //nolint:gochecknoglobals
	idleSchemasMu       sync.Mutex                  //nolint:gochecknoglobals
	initialCleanup      sync.Once                   //nolint:gochecknoglobals
	nextSchemaNum       atomic.Int32                //nolint:gochecknoglobals
	packageName         string                      //nolint:gochecknoglobals
	schemaBaseName      string                      //nolint:gochecknoglobals
	schemaExpireHorizon string                      //nolint:gochecknoglobals
	schemaRandSuffix    string                      //nolint:gochecknoglobals
	stats               struct {                    //nolint:gochecknoglobals
		numGenerated atomic.Int32
		numReused    atomic.Int32
	}
)

// TestSchemaOpts are options for TestSchema. Most of the time these can be left
// as nil.
type TestSchemaOpts struct {
	// Lines are migration lines to run. By default, the migration lines
	// specified by the driver's GetMigrationDefaultLines function are run.
	//
	// Set to an empty non-nil slice like `[]string{}` to run no migrations.
	Lines []string

	// Schema will not be checked in for reuse at the end of tests.
	noReuse bool

	// skipPackageNameCheck skips the check that package name doesn't resolve to
	// `riverdbtest`. Normally we want this to make sure that we're skipping
	// the right number of frames back to the caller package, but it needs to be
	// skipped for tests _in_ `riverdbtest`. That's also why it's not
	// exported.
	skipPackageNameCheck bool

	skipExtraFrames int
}

// TestSchema generates an isolated schema for use during a single test run.
// Migrations are run in the schema (this adds ~50 ms of overhead) to prepare it
// for River testing. AFter a test run, the schema in use is checked back into a
// pool for potential reuse. When a schema is reused, tables in TruncateTables
// are truncated to leave a clean state for the next test.
//
// Use of a schema doesn't happen in River clients automatically. TestSchema
// returns the name of a schema to use. This should be set in River clients,
// used as a parameter for testfactory functions, and anywhere else where
// database operations are issued.
//
// Despite reasonably fast migrations and efficient reuse, test schemas still
// have more overhead than a test transaction, so prefer the use of
// riversharedtest.TestTx where a full database isn't needed (TestSchema is
// commonly needed where tests rely on database notifications).
//
// tb is an interface that tolerates not having a cleanup hook so constructs
// like testutil.PanicTB can be used. If Cleanup isn't available, schemas aren't
// checked back in for reuse.
//
// Where Cleanup is available, the function also performs a post-flight check
// that makes sure all tables in TruncateTables are empty. This helps detect
// problems where test cases accidentally inject data into the default schema
// rather than the one returned by this function.
func TestSchema[TTx any](ctx context.Context, tb testutil.TestingTB, driver riverdriver.Driver[TTx], opts *TestSchemaOpts) string {
	tb.Helper()

	require.NotNil(tb, driver, "driver should not be nil")

	if opts == nil {
		opts = &TestSchemaOpts{}
	}

	// An initial pass to calculate a friendly package name that'll be used to
	// prefix this package's schemas so that it won't clash with packages
	// running their own tests in parallel. Generated name is like `river` or
	// `jobcompleter` or `riverpro`.
	genSchemaBase.Do(func() {
		var (
			programCounterAddr, _, _, _ = runtime.Caller(4 + opts.skipExtraFrames)     // skip `TestSchema.func1` (closure) + `sync.(*Once).doSlow` + `sync.(*Once).Do` + `TestSchema` and end up at `TestSchema`'s caller
			funcName                    = runtime.FuncForPC(programCounterAddr).Name() // like: github.com/riverqueue/river.Test_Client.func1
		)

		packageName = packageFromFunc(funcName) // like: `river` (or `jobcompleter`, or `riverpro`)

		// Check to make sure we're skipping the right number of frames above.
		// If the location of `runtime.Caller` is changed at all (a single new
		// function is added to the stack), the reported package will be
		// completely wrong, so we try to take precautions about it.
		if packageName == "riverdbtest" && !opts.skipPackageNameCheck {
			panic("package name should not resolve to riverdbtest")
		}

		// Notification topics are prefixed with schemas. The max Postgres
		// length of topics is 63, and "river_leadership" is the longest topic
		// name. If the package suffix is longer than the max that could fit
		// into 63 when combined with a schema name and leadership, trim it down
		// a bit. The only package where this is needed as I write this is
		// `riverencrypt_test`. If this happens in too many places we may want
		// to trim "_schema_" to an abbreviation or shorten "river_leadership".
		const maxLength = 63 - len("_2025_04_20t16_00_20_schema_01.river_leadership") - 1
		if len(packageName) > maxLength {
			// Where truncation is necessary, we also end up with a high
			// possibility of contention between schema names. For example,
			// `example_job_cancel` and `example_job_cancel_from_client` resolve
			// to exactly the same thing and may run in parallel.
			//
			// Correct this so that if truncation was necessary, generate a
			// random suffix. This is added to the schema name separately so
			// that cleanup still works correctly (if a random suffix was
			// included in cleanup names, it wouldn't match anything).
			const numRandChars = 5
			packageName = packageName[0 : maxLength-numRandChars-1]
			schemaRandSuffix = randBase62(numRandChars) + "_"
		}

		schemaBaseName = packageName + "_" + time.Now().Format(schemaDateFormat) + "_schema_"
		schemaExpireHorizon = packageName + "_" + time.Now().Add(-1*time.Minute).Format(schemaDateFormat) + "_schema_"
	})

	exec := driver.GetExecutor()

	// Schemas aren't dropped after a package test run. Instead, we drop them
	// before starting a test run. This happens in a `sync.Once` to minimize the
	// amount of work that needs to be done (it has to run once, but all other
	// TestSchema invocations skip it).
	initialCleanup.Do(func() {
		expiredSchemas := func() []string {
			// We only expire schemas in our package prefix (e.g. `river_*`) so
			// that in case other package tests are running in parallel we don't
			// end up contending with them as they also try to clean their old
			// schemas.
			expiredSchemas, err := driver.GetExecutor().SchemaGetExpired(ctx, &riverdriver.SchemaGetExpiredParams{
				BeforeName: schemaExpireHorizon,
				Prefix:     packageName + "_%",
			})
			require.NoError(tb, err)

			return expiredSchemas
		}()

		start := time.Now()

		for _, schema := range expiredSchemas {
			_, err := exec.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schema))
			require.NoError(tb, err)
		}

		tb.Logf("Dropped %d expired schema(s) in %s", len(expiredSchemas), time.Since(start))
	})

	lines := driver.GetMigrationDefaultLines()
	if opts.Lines != nil {
		lines = opts.Lines
	}

	// Idle schemas must be managed by which migration lines were run within
	// them. i.e. A schema with no migrations obviously cannot be reused for a
	// test expecting the `main` migration line.
	//
	// linesKey acts as key specific to this migrations set for idleSchemas.
	slices.Sort(lines)
	linesKey := strings.Join(lines, ",")

	// All tables to truncate when reusing a schema for this set of lines. Also
	// used to perform the post-flight cleanup check to make sure tests didn't
	// leave any detritus in the default schema.
	var truncateTables []string
	for _, line := range lines {
		truncateTables = append(truncateTables, driver.GetMigrationTruncateTables(line)...)
	}

	// See if there are any idle schemas that were previously generated during
	// this run and have since been checked back into the pool. If so, pop it
	// off and run cleanup on it. If not, continue on to generating a new schema
	// below. This function never blocks, so we'll prefer generating extra
	// schemas rather than optimizing amongst a minimal set that's already there.
	if schema := func() string {
		idleSchemasMu.Lock()
		defer idleSchemasMu.Unlock()

		linesIdleSchemas := idleSchemas[linesKey]

		if len(linesIdleSchemas) < 1 {
			return ""
		}

		schema := linesIdleSchemas[0]
		idleSchemas[linesKey] = linesIdleSchemas[1:]
		return schema
	}(); schema != "" {
		start := time.Now()

		if len(truncateTables) > 0 {
			_, err := exec.Exec(ctx, "TRUNCATE TABLE "+
				strings.Join(
					sliceutil.Map(
						truncateTables,
						func(table string) string { return schema + "." + table },
					),
					", ",
				),
			)
			require.NoError(tb, err)
		}

		tb.Logf("Reusing idle schema %q after cleaning in %s [%d generated] [%d reused]",
			schema, time.Since(start), stats.numGenerated.Load(), stats.numReused.Add(1))

		return schema
	}

	// e.g. river_2025_04_14t22_13_58_schema_10
	//
	// Or where a package name had to truncated so a random suffix was
	// generated:
	//
	// e.g. river_2025_04_14t22_13_58_schema_kwo78x_10
	schema := schemaBaseName + schemaRandSuffix + fmt.Sprintf("%02d", nextSchemaNum.Add(1))

	_, err := exec.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(tb, err)

	for _, line := range lines {
		// Migrate the new schema. This takes somewhere in the neighborhood of 10 to
		// 50ms on my machine which is already pretty fast, but we still prefer to
		// use an already created schema if available.
		migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
			Line:   line,
			Logger: riversharedtest.LoggerWarn(tb), // set to warn level to make migrate logs a little quieter since as we'll be migrating a lot
			Schema: schema,
		})
		require.NoError(tb, err)

		start := time.Now()

		migrateRes, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{})
		require.NoError(tb, err)

		tb.Logf("Generated schema %q with migrations %+v on line %q in %s [%d generated] [%d reused]",
			schema,
			sliceutil.Map(migrateRes.Versions, func(v rivermigrate.MigrateVersion) int { return v.Version }),
			line,
			time.Since(start),
			stats.numGenerated.Add(1),
			stats.numReused.Load(),
		)
	}

	// Use an interface here so that callers can pass in `testutil.PanicTB`,
	// which doesn't have a Cleanup implementation, but also won't care about
	// having to check schemas back in (it's used in example tests).
	type testingTBWithCleanup interface {
		Cleanup(cleanupFunc func())
	}

	if withCleanup, ok := tb.(testingTBWithCleanup); ok {
		if !opts.noReuse {
			withCleanup.Cleanup(func() {
				idleSchemasMu.Lock()
				defer idleSchemasMu.Unlock()

				idleSchemas[linesKey] = append(idleSchemas[linesKey], schema)

				tb.Logf("Checked in schema %q; %d idle schema(s) [%d generated] [%d reused]",
					schema, len(idleSchemas), stats.numGenerated.Load(), stats.numReused.Load())
			})
		}
	} else {
		tb.Logf("tb does not implement Cleanup; schema not checked in for reuse")
	}

	return schema
}

// Gets a "friendly package name" from a fully qualified function name.
//
// Most effectively demonstrated by example:
//
//   - `github.com/riverqueue/river.Test_Client.func1` -> `river`
//   - `github.com/riverqueue/river/internal/jobcompleter.testCompleterWait` -> `jobcompleter`
//
// This is then used as a root for constructive schema names. It's convenient
// because it's not too long (schemas have a max length of 64 characters), human
// friendly, and won't have any special characters.
func packageFromFunc(funcName string) string {
	var (
		packagePathLastSlashIndex = strings.LastIndex(funcName, "/")        // index of last slash in path, so starting at `/river.Test_Client.func1`
		funcNameFromLastSlash     = funcName[packagePathLastSlashIndex+1:]  // like: `/river.Test_Client.func1`
		packageName, _, _         = strings.Cut(funcNameFromLastSlash, ".") // cut around first dot to extract `river`
	)

	return packageName
}

var base62Runes = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789") //nolint:gochecknoglobals

func randBase62(numRandChars int) string {
	randChars := make([]rune, numRandChars)
	for i := range numRandChars {
		randChars[i] = base62Runes[rand.IntN(len(base62Runes))]
	}
	return string(randChars)
}

// TestTx starts a test transaction that's rolled back automatically as the test
// case is cleaning itself up.
//
// This variant starts a transaction for the standard pgx/v5 driver most
// commonly used throughout most of River.
func TestTxPgx(ctx context.Context, tb testing.TB) pgx.Tx {
	tb.Helper()

	return TestTx(ctx, tb, riverpgxv5.New(riversharedtest.DBPool(ctx, tb)), &TestTxOpts{
		IsTestTxHelper: true,
	})
}

// TestTxOpts are options for TestTx. Most of the time these can be left as nil.
type TestTxOpts struct {
	// IsTestTxHelper should be set to true for if TestTx is being called from
	// within a secondary helper that's in a common testing package. This causes
	// an extra stack frame to be skipped when determining the name of the test
	// schema being used for test transactions. So instead of `riverdbtest` or
	// `riverprodbtest` we get the real name of the package being tested (e.g.
	// `river` or `riverpro`).
	IsTestTxHelper bool

	// Lines are migration lines to run. By default, the migration lines
	// specified by the driver's GetMigrationDefaultLines function are run.
	//
	// Set to an empty non-nil slice like `[]string{}` to run no migrations.
	//
	// This is currently not exported because it hasn't been needed anywhere yet
	// for test transactions.
	lines []string

	// skipPackageNameCheck skips the check that package name doesn't resolve to
	// `riverdbtest`. Normally we want this to make sure that we're skipping
	// the right number of frames back to the caller package, but it needs to be
	// skipped for tests _in_ `riverdbtest`. That's also why it's not exported.
	skipPackageNameCheck bool
}

// TestTx starts a test transaction that's rolled back automatically as the test
// case is cleaning itself up.
//
// The function invokes TestSchema to create a single schema where this test
// transaction and all future test transactions for this package test run will
// run.
//
// `search_path` is set to the name of the transaction schema so that it's not
// necessary to specify an explicit schema for database operations. (This is
// somewhat of a legacy decision
//
// The included driver determines what migrations are run to prepare the test
// transaction schema.
func TestTx[TTx any](ctx context.Context, tb testing.TB, driver riverdriver.Driver[TTx], opts *TestTxOpts) TTx {
	tb.Helper()

	schema := testTxSchemaForMigrationLines(ctx, tb, driver, opts)
	tb.Logf("TestTx using schema: " + schema)

	tx, err := driver.GetExecutor().Begin(ctx)
	require.NoError(tb, err)

	_, err = tx.Exec(ctx, "SET search_path TO '"+schema+"'")
	require.NoError(tb, err)

	tb.Cleanup(func() {
		// Tests may inerit context from `t.Context()` which is cancelled after
		// tests run and before calling clean up. We need a non-cancelled
		// context to issue rollback here, so use a bit of a bludgeon to do so
		// with `context.WithoutCancel()`.
		ctx := context.WithoutCancel(ctx)

		err := tx.Rollback(ctx)

		if err == nil {
			return
		}

		// Try to look for an error on rollback because it does occasionally
		// reveal a real problem in the way a test is written. However, allow
		// tests to roll back their transaction early if they like, so ignore
		// `ErrTxClosed`.
		if errors.Is(err, pgx.ErrTxClosed) {
			return
		}

		// In case of a cancelled context during a database operation, which
		// happens in many tests, pgx seems to not only roll back the
		// transaction, but closes the connection, and returns this error on
		// rollback. Allow this error since it's hard to prevent it in our flows
		// that use contexts heavily.
		if err.Error() == "conn closed" {
			return
		}

		// Similar to the above, but a newly appeared error that wraps the
		// above. As far as I can tell, no error variables are available to use
		// with `errors.Is`.
		if err.Error() == "failed to deallocate cached statement(s): conn closed" {
			return
		}

		require.NoError(tb, err)
	})

	return driver.UnwrapTx(tx)
}

var (
	testTxSchemas   = make(map[string]string) //nolint:gochecknoglobals
	testTxSchemasMu sync.RWMutex              //nolint:gochecknoglobals
)

func testTxSchemaForMigrationLines[TTx any](ctx context.Context, tb testing.TB, driver riverdriver.Driver[TTx], opts *TestTxOpts) string {
	tb.Helper()

	if opts == nil {
		opts = &TestTxOpts{}
	}

	lines := driver.GetMigrationDefaultLines()
	if opts.lines != nil {
		lines = opts.lines
	}

	// Transaction schemas must be managed by which migration lines were run
	// within them, which is determined by the included driver. i.e. A schema
	// with no migrations obviously cannot be reused for a test expecting the
	// `main` migration line.
	//
	// linesKey acts as key specific to this migrations set for testTxSchemas.
	slices.Sort(lines)
	linesKey := strings.Join(lines, ",")

	testTxSchemasMu.RLock()
	schema := testTxSchemas[linesKey]
	testTxSchemasMu.RUnlock()

	if schema != "" {
		return schema
	}

	testTxSchemasMu.Lock()
	defer testTxSchemasMu.Unlock()

	// Check for a schema once more in case there was a race to acquire the
	// mutex lock and another TestTx invocation did it first.
	if schema = testTxSchemas[linesKey]; schema != "" {
		return schema
	}

	// If called from a transaction helper like `TestTxPgx`, skip one more frame
	// for purposes of schema naming.
	skipExtraFrames := 2
	if opts.IsTestTxHelper {
		skipExtraFrames++
	}

	schema = TestSchema(ctx, tb, driver, &TestSchemaOpts{
		Lines:                lines,
		noReuse:              true,
		skipExtraFrames:      skipExtraFrames,
		skipPackageNameCheck: opts.skipPackageNameCheck,
	})
	testTxSchemas[linesKey] = schema
	return schema
}
