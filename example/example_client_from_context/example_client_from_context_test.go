package example_client_from_context

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
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

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &ContextClientWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		ID:     "ClientFromContextClient",
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		Schema:   riverdbtest.TestSchema(ctx, testutil.PanicTB(), riverpgxv5.New(dbPool), nil), // only necessary for the example test
		TestOnly: true,                                                                         // suitable only for use in tests; remove for live environments
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

	// Wait for jobs to complete. Only needed for purposes of the example test.
	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// client found in context, id=ClientFromContextClient
}
