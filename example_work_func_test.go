package river_test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/slogutil"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type WorkFuncArgs struct {
	Message string `json:"message"`
}

func (WorkFuncArgs) Kind() string { return "work_func" }

// Example_workFunc demonstrates the use of river.WorkFunc, which can be used to
// easily add a worker with only a function instead of having to implement a
// full worker struct.
func Example_workFunc() {
	ctx := context.Background()

	dbPool, err := pgxpool.NewWithConfig(ctx, riverinternaltest.DatabaseConfig("river_testdb_example"))
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	// Required for the purpose of this test, but not necessary in real usage.
	if err := riverinternaltest.TruncateRiverTables(ctx, dbPool); err != nil {
		panic(err)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[WorkFuncArgs]) error {
		fmt.Printf("Message: %s", job.Args.Message)
		return nil
	}))

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Workers: workers,
	})
	if err != nil {
		panic(err)
	}

	// Out of example scope, but used to make wait until a job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	_, err = riverClient.Insert(ctx, WorkFuncArgs{
		Message: "hello from a function!",
	}, nil)
	if err != nil {
		panic(err)
	}

	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Message: hello from a function!
}
