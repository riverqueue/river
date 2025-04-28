package riversharedtest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/slogtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

// BaseServiceArchetype returns a new base service suitable for use in tests.
// Returns a new instance so that it's not possible to accidentally taint a
// shared object.
func BaseServiceArchetype(tb testing.TB) *baseservice.Archetype {
	tb.Helper()

	return &baseservice.Archetype{
		Logger: Logger(tb),
		Time:   &TimeStub{},
	}
}

// A pool and sync.Once to initialize it, invoked by TestTx. Once open, this
// pool is never explicitly closed, instead closing implicitly as the package
// tests finish.
var (
	dbPool     *pgxpool.Pool //nolint:gochecknoglobals
	dbPoolOnce sync.Once     //nolint:gochecknoglobals
)

// DBPool gets a lazily initialized database pool for `TEST_DATABASE_URL` or
// `river_test` if the former isn't specified.
func DBPool(ctx context.Context, tb testing.TB) *pgxpool.Pool {
	tb.Helper()

	dbPoolOnce.Do(func() {
		config, err := pgxpool.ParseConfig(TestDatabaseURL())
		require.NoError(tb, err)

		config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			// Empty the search path so that tests using riverdbtest are
			// forced to pass a schema to clients and any other database
			// operations they invoke. Calls do not accidentally fall back to a
			// default schema, which would potentially hide bugs where we
			// weren't properly referencing a schema explicitly.
			_, err := conn.Exec(ctx, "SET search_path TO ''")

			// This should not be a `require` because the callback may run long
			// after the original test has completed.
			if err != nil && !errors.Is(err, context.Canceled) {
				panic(err)
			}

			return nil
		}

		dbPool, err = pgxpool.NewWithConfig(ctx, config)
		require.NoError(tb, err)
	})
	require.NotNil(tb, dbPool) // die in case initial connect from another test failed

	return dbPool
}

// DBPoolClone returns a disposable clone of DBPool. Share resources by using
// DBPool when possible, but this is useless for areas like stress tests where
// context cancellations are likely to end up closing the pool.
//
// Unlike DBPool, adds a test cleanup hook that closes the pool after run.
func DBPoolClone(ctx context.Context, tb testing.TB) *pgxpool.Pool {
	tb.Helper()

	dbPool := DBPool(ctx, tb)

	config := dbPool.Config()
	config.MaxConns = 4 // dramatically reduce max allowed conns for clones so we they don't clobber the database server

	var err error
	dbPool, err = pgxpool.NewWithConfig(ctx, config)
	require.NoError(tb, err)

	tb.Cleanup(dbPool.Close)

	return dbPool
}

// Logger returns a logger suitable for use in tests.
//
// Defaults to informational verbosity. If env is set with `RIVER_DEBUG=true`,
// debug level verbosity is activated.
func Logger(tb testing.TB) *slog.Logger {
	tb.Helper()

	if os.Getenv("RIVER_DEBUG") == "1" || os.Getenv("RIVER_DEBUG") == "true" {
		return slogtest.NewLogger(tb, &slog.HandlerOptions{Level: slog.LevelDebug})
	}

	return slogtest.NewLogger(tb, nil)
}

// Logger returns a logger suitable for use in tests which outputs only at warn
// or above. Useful in tests where particularly noisy log output is expected.
func LoggerWarn(tb testutil.TestingTB) *slog.Logger {
	tb.Helper()
	return slogtest.NewLogger(tb, &slog.HandlerOptions{Level: slog.LevelWarn})
}

// TestDatabaseURL returns `TEST_DATABASE_URL` or a default URL pointing to
// `river_test` and with suitable connection configuration defaults.
func TestDatabaseURL() string {
	return cmp.Or(
		os.Getenv("TEST_DATABASE_URL"),

		// 100 conns is the default maximum for Homebrew.
		//
		// It'd be nice to be able to set this number really high because it'd
		// mean less waiting time acquiring connections in tests, but with
		// default settings, contention between tests/test packages leading to
		// exhausion on the Postgres server is definitely a problem. At numbers
		// >75 I started seeing a lot of errors between tests within a single
		// package, and worse yet, at numbers >=20 I saw major problems between
		// packages (i.e. as parallel packages run at the same time).
		//
		// 15 is about as high as I found I could set it while keeping test runs
		// stable. This could be much higher in areas where we know Postgres is
		// configured with more allowed max connections.
		"postgres://localhost:5432/river_test?pool_max_conns=15&sslmode=disable",
	)
}

// TimeStub implements baseservice.TimeGeneratorWithStub to allow time to be
// stubbed in tests.
//
// It exists separately from rivertest.TimeStub to avoid a circular dependency.
type TimeStub struct {
	mu     sync.RWMutex
	nowUTC *time.Time
}

func (t *TimeStub) NowUTC() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.nowUTC == nil {
		return time.Now().UTC()
	}

	return *t.nowUTC
}

func (t *TimeStub) NowUTCOrNil() *time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.nowUTC
}

func (t *TimeStub) StubNowUTC(nowUTC time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nowUTC = &nowUTC
	return nowUTC
}

// WaitOrTimeout tries to wait on the given channel for a value to come through,
// and returns it if one does, but times out after a reasonable amount of time.
// Useful to guarantee that test cases don't hang forever, even in the event of
// something wrong.
func WaitOrTimeout[T any](tb testing.TB, waitChan <-chan T) T {
	tb.Helper()

	timeout := WaitTimeout()

	select {
	case value := <-waitChan:
		return value
	case <-time.After(timeout):
		require.FailNowf(tb, "WaitOrTimeout timed out",
			"WaitOrTimeout timed out after waiting %s", timeout)
	}
	return *new(T) // unreachable
}

// WaitOrTimeoutN tries to wait on the given channel for N values to come
// through, and returns it if they do, but times out after a reasonable amount
// of time.  Useful to guarantee that test cases don't hang forever, even in the
// event of something wrong.
func WaitOrTimeoutN[T any](tb testutil.TestingTB, waitChan <-chan T, numValues int) []T {
	tb.Helper()

	var (
		timeout  = WaitTimeout()
		deadline = time.Now().Add(timeout)
		values   = make([]T, 0, numValues)
	)

	for {
		select {
		case value := <-waitChan:
			values = append(values, value)

			if len(values) >= numValues {
				return values
			}

		case <-time.After(time.Until(deadline)):
			require.FailNowf(tb, "WaitOrTimeout timed out",
				"WaitOrTimeout timed out after waiting %s (received %d value(s), wanted %d)", timeout, len(values), numValues)
			return nil
		}
	}
}

// WaitTimeout returns a duration broadly appropriate for waiting on an expected
// event in a test, and which is used for `TestSignal.WaitOrTimeout` in the main
// package and `WaitOrTimeout` above. Its main purpose is to allow a little
// extra leeway in GitHub Actions where we occasionally seem to observe subpar
// performance which leads to timeouts and test intermittency, while still
// keeping a tight a timeout for local test runs where this is never a problem.
func WaitTimeout() time.Duration {
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		return 10 * time.Second
	}

	return 3 * time.Second
}

var IgnoredKnownGoroutineLeaks = []goleak.Option{ //nolint:gochecknoglobals
	// This goroutine contains a 500 ms uninterruptible sleep that may still be
	// running by the time the test suite finishes and cause a failure. This
	// might be something that should be fixed in pgx, but ignore it for the
	// time being lest we have intermittent tests.
	//
	// We opened an issue on pgx, but it may or may not be one that gets fixed:
	//
	// https://github.com/jackc/pgx/issues/1641
	goleak.IgnoreTopFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),

	// Similar to the above, may be sitting in a sleep when the program finishes
	// and there's not much we can do about it.
	goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),
}

// WrapTestMain performs some common setup and teardown that should be shared
// amongst all packages. e.g. Configures a manager for test databases on setup,
// and checks for no goroutine leaks on teardown.
func WrapTestMain(m *testing.M) {
	status := m.Run()

	if status == 0 {
		if err := goleak.Find(IgnoredKnownGoroutineLeaks...); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)
			status = 1
		}
	}

	os.Exit(status)
}
