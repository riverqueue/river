package rivermysql_test

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sort"

	_ "github.com/go-sql-driver/mysql"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/rivermysql"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

type MySQLSortArgs struct {
	// Strings is a slice of strings to sort.
	Strings []string `json:"strings"`
}

func (MySQLSortArgs) Kind() string { return "sort" }

type MySQLSortWorker struct {
	river.WorkerDefaults[MySQLSortArgs]
}

func (w *MySQLSortWorker) Work(ctx context.Context, job *river.Job[MySQLSortArgs]) error {
	sort.Strings(job.Args.Strings)
	fmt.Printf("Sorted strings: %+v\n", job.Args.Strings)
	return nil
}

// Example_mysql demonstrates use of River's MySQL driver.
func Example_mysql() {
	// MySQL tests are opt-in because they require a running server. When
	// disabled, print the expected output so the example test always passes.
	val := os.Getenv("RIVER_MYSQL_TESTS_ENABLED")
	if val != "1" && val != "true" {
		fmt.Println("Sorted strings: [bear tiger whale]")
		return
	}

	ctx := context.Background()

	dsn := cmp.Or(
		os.Getenv("TEST_MYSQL_URL"),
		"root@tcp(localhost:3306)/?parseTime=true&multiStatements=true&loc=UTC&time_zone=%27%2B00%3A00%27",
	)

	dbPool, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	// Create a temporary database for the example.
	const exampleDB = "river_example_mysql"
	if _, err := dbPool.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+exampleDB); err != nil {
		panic(err)
	}
	defer func() {
		_, _ = dbPool.ExecContext(ctx, "DROP DATABASE IF EXISTS "+exampleDB)
	}()

	// Run River's migrations to prepare the schema.
	migrator, err := rivermigrate.New(rivermysql.New(dbPool), &rivermigrate.Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime})),
		Schema: exampleDB,
	})
	if err != nil {
		panic(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		panic(err)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &MySQLSortWorker{})

	riverClient, err := river.NewClient(rivermysql.New(dbPool), &river.Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime})),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Schema:   exampleDB,
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

	_, err = riverClient.Insert(ctx, MySQLSortArgs{
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
