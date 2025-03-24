// Package riverinternaltest contains shared testing utilities for tests
// throughout the rest of the project.
package riverinternaltest

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/riverqueue/river/internal/testdb"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/valutil"
)

// SchedulerShortInterval is an artificially short interval for the scheduler
// that's used in the tests of various components to make sure that errored jobs
// always end up in a `retryable` state rather than `available`. Normally, the
// job executor sets `available` if the retry delay is smaller than the
// scheduler's interval. To simplify things so errors are always `retryable`,
// this time is picked to be smaller than any retry delay that the default
// retry policy will ever produce. It's shared so we can document/explain it all
// in one place.
const SchedulerShortInterval = 500 * time.Millisecond

var (
	dbManager *testdb.Manager //nolint:gochecknoglobals

	// Maximum number of connections for the connection pool. This is the same
	// default that pgxpool uses (the larger of 4 or number of CPUs), but made a
	// variable here so that we can reference it from the test suite and not
	// rely on implicit knowledge of pgxpool implementation details that could
	// change in the future. If changing this value, also change the number of
	// databases to create in `testdbman`.
	dbPoolMaxConns = int32(max(4, runtime.NumCPU())) //nolint:gochecknoglobals
)

func DatabaseConfig(databaseName string) *pgxpool.Config {
	config, err := pgxpool.ParseConfig(DatabaseURL(databaseName))
	if err != nil {
		panic(fmt.Sprintf("error parsing database URL: %v", err))
	}
	config.MaxConns = dbPoolMaxConns
	// Use a short conn timeout here to attempt to quickly cancel attempts that
	// are unlikely to succeed even with more time:
	config.ConnConfig.ConnectTimeout = 2 * time.Second
	config.ConnConfig.RuntimeParams["timezone"] = "UTC"
	return config
}

// DatabaseURL gets a test database URL from TEST_DATABASE_URL or falls back on
// a default pointing to `river_test`. If databaseName is set, it replaces the
// database in the URL, although the host and other parameters are preserved.
//
// Most of the time DatabaseConfig should be used instead of this function, but
// it may be useful in non-pgx situations like for examples showing the use of
// `database/sql`.
func DatabaseURL(databaseName string) string {
	parsedURL, err := url.Parse(valutil.ValOrDefault(
		os.Getenv("TEST_DATABASE_URL"),
		"postgres://localhost/river_test?sslmode=disable"),
	)
	if err != nil {
		panic(err)
	}

	if databaseName != "" {
		parsedURL.Path = databaseName
	}

	return parsedURL.String()
}

// DiscardContinuously drains continuously out of the given channel and discards
// anything that comes out of it. Returns a stop function that should be invoked
// to stop draining. Stop must be invoked before tests finish to stop an
// internal goroutine.
func DiscardContinuously[T any](drainChan <-chan T) func() {
	var (
		stop     = make(chan struct{})
		stopped  = make(chan struct{})
		stopOnce sync.Once
	)

	go func() {
		defer close(stopped)

		for {
			select {
			case <-drainChan:
			case <-stop:
				return
			}
		}
	}()

	return func() {
		stopOnce.Do(func() {
			close(stop)
			<-stopped
		})
	}
}

// DrainContinuously drains continuously out of the given channel and
// accumulates items that are received from it. Returns a get function that can
// be called to retrieve the current set of received items, and which will also
// cause the function to shut down and stop draining. This function must be
// invoked before tests finish to stop an internal goroutine. It's safe to call
// it multiple times.
func DrainContinuously[T any](drainChan <-chan T) func() []T {
	var (
		items    []T
		stop     = make(chan struct{})
		stopped  = make(chan struct{})
		stopOnce sync.Once
	)

	go func() {
		defer close(stopped)

		for {
			select {
			case item := <-drainChan:
				items = append(items, item)
			case <-stop:
				// Drain until empty
				for {
					select {
					case item := <-drainChan:
						items = append(items, item)
					default:
						return
					}
				}
			}
		}
	}()

	return func() []T {
		stopOnce.Do(func() {
			close(stop)
			<-stopped
		})

		return items
	}
}

// TestDB acquires a dedicated test database for the duration of the test. If an
// error occurs, the test fails. The test database will be automatically
// returned to the pool at the end of the test. If the pool was closed, it will
// be recreated.
func TestDB(ctx context.Context, tb testing.TB) *pgxpool.Pool {
	tb.Helper()

	ctx, cancel := context.WithTimeout(ctx, riversharedtest.WaitTimeout())
	defer cancel()

	testPool, err := dbManager.Acquire(ctx)
	if err != nil {
		tb.Fatalf("Failed to acquire pool for test DB: %v", err)
	}
	tb.Cleanup(testPool.Release)

	return testPool.Pool()
}

// A pool and mutex to protect it, lazily initialized by TestTx. Once open, this
// pool is never explicitly closed, instead closing implicitly as the package
// tests finish.
var (
	dbPool   *pgxpool.Pool //nolint:gochecknoglobals
	dbPoolMu sync.RWMutex  //nolint:gochecknoglobals
)

// TestTx starts a test transaction that's rolled back automatically as the test
// case is cleaning itself up. This can be used as a lighter weight alternative
// to `testdb.Manager` in components where it's not necessary to have many
// connections open simultaneously.
func TestTx(ctx context.Context, tb testing.TB) pgx.Tx {
	tb.Helper()

	tryPool := func() *pgxpool.Pool {
		dbPoolMu.RLock()
		defer dbPoolMu.RUnlock()
		return dbPool
	}

	getPool := func() *pgxpool.Pool {
		if dbPool := tryPool(); dbPool != nil {
			return dbPool
		}

		dbPoolMu.Lock()
		defer dbPoolMu.Unlock()

		// Multiple goroutines may have passed the initial `nil` check on start
		// up, so check once more to make sure pool hasn't been set yet.
		if dbPool != nil {
			return dbPool
		}

		var err error
		dbPool, err = pgxpool.NewWithConfig(ctx, DatabaseConfig("river_test"))
		require.NoError(tb, err)

		return dbPool
	}

	return riversharedtest.TestTxPool(ctx, tb, getPool())
}

// TruncateRiverTables truncates River tables in the target database. This is
// for test cleanup and should obviously only be used in tests.
func TruncateRiverTables(ctx context.Context, pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tables := []string{"river_job", "river_leader", "river_queue"}

	for _, table := range tables {
		if _, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s;", table)); err != nil {
			return fmt.Errorf("error truncating %q: %w", table, err)
		}
	}

	return nil
}

// WrapTestMain performs some common setup and teardown that should be shared
// amongst all packages. e.g. Configures a manager for test databases on setup,
// and checks for no goroutine leaks on teardown.
func WrapTestMain(m *testing.M) {
	poolConfig := DatabaseConfig("river_test")
	// Use a smaller number of conns per pool, because otherwise we could have
	// NUM_CPU pools, each with NUM_CPU connections, and that's a lot of
	// connections if there are many CPUs.
	poolConfig.MaxConns = 4
	// Pre-initialize 1 connection per pool.
	poolConfig.MinConns = 1

	var err error
	// Allow up to one database per concurrent test, plus two for overhead:
	maxTestDBs := int32(runtime.GOMAXPROCS(0)) + 2 //nolint:gosec
	dbManager, err = testdb.NewManager(poolConfig, maxTestDBs, nil, TruncateRiverTables)
	if err != nil {
		log.Fatal(err)
	}

	status := m.Run()

	dbManager.Close()

	if status == 0 {
		if err := goleak.Find(riversharedtest.IgnoredKnownGoroutineLeaks...); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)
			status = 1
		}
	}

	os.Exit(status)
}
