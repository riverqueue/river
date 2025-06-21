package riverdrivertest_test

import (
	"context"
	"database/sql"
	"log/slog"

	_ "github.com/tursodatabase/libsql-client-go/libsql"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

// Example_libSQL demonstrates use of River's SQLite driver with libSQL (a
// SQLite fork).
func Example_libSQL() { //nolint:dupl
	ctx := context.Background()

	dbPool, err := sql.Open("libsql", "file:./example_libsql_test.libsql")
	if err != nil {
		panic(err)
	}
	dbPool.SetMaxOpenConns(1)
	defer dbPool.Close()

	driver := riversqlite.New(dbPool)

	if err := migrateDB(ctx, driver); err != nil {
		panic(err)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &SortWorker{})

	riverClient, err := river.NewClient(driver, &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		TestOnly: true, // suitable only for use in tests; remove for live environments
		Workers:  workers,
	})
	if err != nil {
		panic(err)
	}

	// Out of example scope, but used to wait until a job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	_, err = riverClient.Insert(ctx, SortArgs{
		Strings: []string{
			"whale", "tiger", "bear",
		},
	}, nil)
	if err != nil {
		panic(err)
	}

	// Wait for jobs to complete. Only needed for purposes of the example test.
	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Sorted strings: [bear tiger whale]
}
