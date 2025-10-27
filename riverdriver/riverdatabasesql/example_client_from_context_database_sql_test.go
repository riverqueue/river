package riverdatabasesql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/urlutil"
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
// ([github.com/riverqueue/river/riverdriver/riverdatabasesql]).
func ExampleClientFromContext_databaseSQL() {
	ctx := context.Background()

	db, err := sql.Open("pgx", urlutil.DatabaseSQLCompatibleURL(riversharedtest.TestDatabaseURL()))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &ContextClientSQLWorker{})

	riverClient, err := river.NewClient(riverdatabasesql.New(db), &river.Config{
		ID:     "ClientFromContextClientSQL",
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime})),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		FetchCooldown:     10 * time.Millisecond,
		FetchPollInterval: 10 * time.Millisecond,
		Schema:            riverdbtest.TestSchema(ctx, testutil.PanicTB(), riverdatabasesql.New(db), nil), // only necessary for the example test
		TestOnly:          true,                                                                           // suitable only for use in tests; remove for live environments
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

	// Wait for jobs to complete. Only needed for purposes of the example test.
	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// client found in context, id=ClientFromContextClientSQL
}
