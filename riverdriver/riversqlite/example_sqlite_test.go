package riversqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"

	_ "modernc.org/sqlite"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

type SortArgs struct {
	// Strings is a slice of strings to sort.
	Strings []string `json:"strings"`
}

func (SortArgs) Kind() string { return "sort" }

type SortWorker struct {
	river.WorkerDefaults[SortArgs]
}

func (w *SortWorker) Work(ctx context.Context, job *river.Job[SortArgs]) error {
	sort.Strings(job.Args.Strings)
	fmt.Printf("Sorted strings: %+v\n", job.Args.Strings)
	return nil
}

// Example_sqlite demonstrates use of River's SQLite driver.
func Example_sqlite() {
	ctx := context.Background()

	dbPool, err := sql.Open("sqlite", ":memory:")
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

func migrateDB(ctx context.Context, driver riverdriver.Driver[*sql.Tx]) error {
	// We're using an in-memory SQLite database here, so we need to migrate it
	// up before use. This won't generally be needed outside of tests.
	migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
	})
	if err != nil {
		return err
	}
	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	if err != nil {
		return err
	}

	return nil
}
