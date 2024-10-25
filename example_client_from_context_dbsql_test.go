package river_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/rivershared/util/slogutil"
)

type ContextClientSQLArgs struct{}

func (args ContextClientSQLArgs) Kind() string { return "ContextClientSQLWorker" }

type ContextClientSQLWorker struct {
	river.WorkerDefaults[ContextClientSQLArgs]
}

func (w *ContextClientSQLWorker) Work(ctx context.Context, job *river.Job[ContextClientSQLArgs]) error {
	client := river.ClientFromContext[*sql.Tx](ctx)
	if client == nil {
		fmt.Println("client not found in context")
		return errors.New("client not found in context")
	}

	fmt.Printf("client found in context, id=%s\n", client.ID())
	return nil
}

// ExampleClientFromContext_databaseSQL demonstrates how to extract the River
// client from the worker context when using the [database/sql] driver.
// ([github.com/riverqueue/river/riverdriver/riverdatabasesql])
func ExampleClientFromContext_databaseSQL() {
	ctx := context.Background()

	config := riverinternaltest.DatabaseConfig("river_test_example")
	db, err := sql.Open("pgx", config.ConnString())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &ContextClientSQLWorker{})

	riverClient, err := river.NewClient(riverdatabasesql.New(db), &river.Config{
		ID:     "ClientFromContextClientSQL",
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		FetchCooldown:     10 * time.Millisecond,
		FetchPollInterval: 10 * time.Millisecond,
		TestOnly:          true, // suitable only for use in tests; remove for live environments
		Workers:           workers,
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
	if _, err := riverClient.Insert(ctx, ContextClientSQLArgs{}, nil); err != nil {
		panic(err)
	}

	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// client found in context, id=ClientFromContextClientSQL
}
