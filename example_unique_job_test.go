package river_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/slogutil"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// Account represents a minimal account containing a unique identifier, recent
// expenditures, and a remaining total.
type Account struct {
	ID                 int
	RecentExpenditures int
	AccountTotal       int
}

var allAccounts = []Account{ //nolint:gochecknoglobals
	{ID: 1, RecentExpenditures: 100, AccountTotal: 1_000},
	{ID: 2, RecentExpenditures: 999, AccountTotal: 1_000},
}

type ReconcileAllAccountsArgs struct{}

func (ReconcileAllAccountsArgs) Kind() string { return "reconcile_all_accounts" }

// InsertOpts returns custom insert options that every job of this type will
// inherit by default, including unique options.
func (ReconcileAllAccountsArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		UniqueOpts: river.UniqueOpts{
			ByPeriod: 24 * time.Hour,
		},
	}
}

type ReconcileAllAccountsWorker struct {
	river.WorkerDefaults[ReconcileAllAccountsArgs]
}

func (w *ReconcileAllAccountsWorker) Work(ctx context.Context, job *river.Job[ReconcileAllAccountsArgs]) error {
	for _, account := range allAccounts {
		account.AccountTotal -= account.RecentExpenditures
		account.RecentExpenditures = 0

		fmt.Printf("Reconciled account %d; new total: %d\n", account.ID, account.AccountTotal)
	}
	return nil
}

// Example_uniqueJob demonstrates the use of a job with custom
// job-specific insertion options.
func Example_uniqueJob() {
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
	river.AddWorker(workers, &ReconcileAllAccountsWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.DefaultQueue: {MaxWorkers: 100},
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

	_, err = riverClient.Insert(ctx, ReconcileAllAccountsArgs{}, nil)
	if err != nil {
		panic(err)
	}

	// Job is inserted a second time, but it doesn't matter because its unique
	// args ensure that it'll only run once in a 24 hour period.
	_, err = riverClient.Insert(ctx, ReconcileAllAccountsArgs{}, nil)
	if err != nil {
		panic(err)
	}

	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Reconciled account 1; new total: 900
	// Reconciled account 2; new total: 1
}
