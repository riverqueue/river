package river_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/util/slogutil"
)

type ContextClientArgs struct{}

func (args ContextClientArgs) Kind() string { return "ContextClientWorker" }

type ContextClientWorker struct {
	river.WorkerDefaults[ContextClientArgs]
}

func (w *ContextClientWorker) Work(ctx context.Context, job *river.Job[ContextClientArgs]) error {
	client := river.ClientFromContext[pgx.Tx](ctx)
	if client == nil {
		fmt.Println("client not found in context")
		return errors.New("client not found in context")
	}

	fmt.Printf("client found in context, id=%s\n", client.ID())
	return nil
}

// ExampleClientFromContext_pgx demonstrates how to extract the River client
// from the worker context when using the pgx/v5 driver.
// ([github.com/riverqueue/river/riverdriver/riverpgxv5]).
func ExampleClientFromContext_pgx() {
	ctx := context.Background()

	dbPool, err := pgxpool.NewWithConfig(ctx, riverinternaltest.DatabaseConfig("river_test_example"))
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	// Required for the purpose of this test, but not necessary in real usage.
	if err := riverinternaltest.TruncateRiverTables(ctx, dbPool); err != nil {
		panic(err)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &ContextClientWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		ID:     "ClientFromContextClient",
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		TestOnly: true, // suitable only for use in tests; remove for live environments
		Workers:  workers,
	})
	if err != nil {
		panic(err)
	}

	// Not strictly needed, but used to help this test wait until job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}
	if _, err = riverClient.Insert(ctx, ContextClientArgs{}, nil); err != nil {
		panic(err)
	}

	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// client found in context, id=ClientFromContextClient
}
