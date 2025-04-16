// Package riverschematest contains testing infrastructure for the River project
// itself that creates isolated schemas suitable for use within a single case.
//
// This package is for internal use and should not be considered stable. Changes
// to functions and types in this package WILL NOT be considered breaking
// changes for purposes of River's semantic versioning.
package riverschematest

import (
	"context"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

const schemaDateFormat = "2006_01_02t15_04_05" // everything here needs to be lowercase because Postgres forces schema names to lowercase

var (
	genSchemaBase  sync.Once                   //nolint:gochecknoglobals
	idleSchemas    = make(map[string][]string) //nolint:gochecknoglobals
	idleSchemasMu  sync.Mutex                  //nolint:gochecknoglobals
	initialCleanup sync.Once                   //nolint:gochecknoglobals
	nextSchemaNum  atomic.Int32                //nolint:gochecknoglobals
	packageName    string                      //nolint:gochecknoglobals
	schemaBaseName string                      //nolint:gochecknoglobals
	stats          struct {                    //nolint:gochecknoglobals
		numGenerated atomic.Int32
		numReused    atomic.Int32
	}
)

// TestSchemaOpts are options for TestSchema. Most of the time these can be left
// as nil.
type TestSchemaOpts struct {
	// Lines are migration lines to run. By default, the main migration line is
	// run, but an alternative set can be specified in case more are needed.
	//
	// Set to an empty non-nil slice like `[]string{}` to run no migrations.
	//
	// Defaults to `[]string{"main"}`.
	Lines []string

	// skipPackageNameCheck skips the check that package name doesn't resolve to
	// `riverschematest`. Normally we want this to make sure that we're skipping
	// the right number of frames back to the caller package, but it needs to be
	// skipped for tests _in_ `riverschematest`. That's also why it's not
	// exported.
	skipPackageNameCheck bool
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
			programCounterAddr, _, _, _ = runtime.Caller(4)                            // skip `TestSchema.func1` (closure) + `sync.(*Once).doSlow` + `sync.(*Once).Do` + `TestSchema` and end up at `TestSchema`'s caller
			funcName                    = runtime.FuncForPC(programCounterAddr).Name() // like: github.com/riverqueue/river.Test_Client.func1
		)

		packageName = packageFromFunc(funcName) // like: `river` (or `jobcompleter`, or `riverpro`)

		// Check to make sure we're skipping the right number of frames above.
		// If the location of `runtime.Caller` is changed at all (a single new
		// function is added to the stack), the reported package will be
		// completely wrong, so we try to take precautions about it.
		if packageName == "riverschematest" && !opts.skipPackageNameCheck {
			panic("package name should not resolve to riverschematest")
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
			packageName = packageName[0:maxLength]
		}

		schemaBaseName = packageName + "_" + time.Now().Format(schemaDateFormat) + "_schema_"
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
				BeforeName: schemaBaseName,
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
	schema := schemaBaseName + fmt.Sprintf("%02d", nextSchemaNum.Add(1))

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
		withCleanup.Cleanup(func() {
			idleSchemasMu.Lock()
			defer idleSchemasMu.Unlock()

			idleSchemas[linesKey] = append(idleSchemas[linesKey], schema)

			tb.Logf("Checked in schema %q; %d idle schema(s) [%d generated] [%d reused]",
				schema, len(idleSchemas), stats.numGenerated.Load(), stats.numReused.Load())
		})

		// Add separately to make sure it doesn't hold idleSchemasMu.
		withCleanup.Cleanup(func() { checkExtraneousRows[TTx](ctx, tb, exec, linesKey, truncateTables) })
	} else {
		tb.Logf("tb does not implement Cleanup; schema not checked in for reuse")
	}

	return schema
}

// If a test uses TestSchema but forgets to apply the schema to the client in
// use, the client will leave debris in the database's default schema, which
// will likely screw up a run for a subsequent test or the next time the test
// suite is run. This check helps protect against that problem by checking known
// interestig tables post run.
func checkExtraneousRows[TTx any](ctx context.Context, tb testutil.TestingTB, exec riverdriver.Executor, linesKey string, truncateTables []string) {
	if ctx.Err() != nil {
		tb.Logf("Did not check default schema for extraneous rows because context error: %s", ctx.Err())
		return
	}

	checkExtraneousRowsMu.RLock()
	memo := checkExtraneousRowsMemo[linesKey]
	checkExtraneousRowsMu.RUnlock()
	if memo == nil {
		memo = checkExtraneousRowsLinesMemoFor(truncateTables)
		checkExtraneousRowsMu.Lock()
		checkExtraneousRowsMemo[linesKey] = memo
		checkExtraneousRowsMu.Unlock()
	}

	var (
		allNumRows     = make([]int, len(truncateTables))
		allNumRowsPtrs = make([]any, len(truncateTables))
		start          = time.Now()
	)
	for i := range allNumRowsPtrs {
		allNumRowsPtrs[i] = &(allNumRows[i])
	}
	err := exec.QueryRow(ctx, memo.remnantCheckSQL).Scan(allNumRowsPtrs...)
	if err != nil && (err.Error() == "closed pool" || err.Error() == "sql: database is closed") {
		tb.Logf("Did not check default schema for extraneous rows because database pool is closed")
		return
	} else {
		require.NoError(tb, err)
	}

	actualNumRowsMap := maps.Clone(memo.expectedNumRowsMap)
	for i, numRows := range allNumRows {
		actualNumRowsMap[truncateTables[i]] = numRows
	}
	require.Equal(tb, memo.expectedNumRowsMap, actualNumRowsMap, "Found non-zero number of extraneous rows in default schema table")

	tb.Logf("Checked default schema for extraneous rows in %s", time.Since(start))
}

// This isn't strictly necessary, but since a few items will always be the same
// for a specific set of migration lines no matter how many thousands of test
// cases are invoked, memoize them so that the objects can be reused.
var (
	checkExtraneousRowsMemo = make(map[string]*checkExtraneousRowsLinesMemo) //nolint:gochecknoglobals
	checkExtraneousRowsMu   sync.RWMutex                                     //nolint:gochecknoglobals
)

type checkExtraneousRowsLinesMemo struct {
	expectedNumRowsMap map[string]int
	remnantCheckSQL    string
}

func checkExtraneousRowsLinesMemoFor(truncateTables []string) *checkExtraneousRowsLinesMemo {
	expectedNumRowsMap := make(map[string]int, len(truncateTables))
	for _, table := range truncateTables {
		expectedNumRowsMap[table] = 0
	}

	tableSelects := sliceutil.Map(
		truncateTables,
		func(table string) string { return "(SELECT count(*) FROM public." + table + ")" },
	)

	return &checkExtraneousRowsLinesMemo{
		expectedNumRowsMap: expectedNumRowsMap,
		remnantCheckSQL:    "SELECT " + strings.Join(tableSelects, ", "),
	}
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
