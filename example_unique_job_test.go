package river_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"weavelab.xyz/river"
	"weavelab.xyz/river/internal/riverinternaltest"
	"weavelab.xyz/river/internal/util/slogutil"
	"weavelab.xyz/river/riverdriver/riverpgxv5"
)

// Account represents a minimal account including recent expenditures and a
// remaining total.
type Account struct {
	RecentExpenditures int
	AccountTotal       int
}

// Map of account ID -> account.
var allAccounts = map[int]Account{ //nolint:gochecknoglobals
	1: {RecentExpenditures: 100, AccountTotal: 1_000},
	2: {RecentExpenditures: 999, AccountTotal: 1_000},
}

type ReconcileAccountArgs struct {
	AccountID int `json:"account_id"`
}

func (ReconcileAccountArgs) Kind() string { return "reconcile_account" }

// InsertOpts returns custom insert options that every job of this type will
// inherit, including unique options.
func (ReconcileAccountArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		UniqueOpts: river.UniqueOpts{
			ByArgs:   true,
			ByPeriod: 24 * time.Hour,
		},
	}
}

type ReconcileAccountWorker struct {
	river.WorkerDefaults[ReconcileAccountArgs]
}

func (w *ReconcileAccountWorker) Work(ctx context.Context, job *river.Job[ReconcileAccountArgs]) error {
	account := allAccounts[job.Args.AccountID]

	account.AccountTotal -= account.RecentExpenditures
	account.RecentExpenditures = 0

	fmt.Printf("Reconciled account %d; new total: %d\n", job.Args.AccountID, account.AccountTotal)

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
	river.AddWorker(workers, &ReconcileAccountWorker{})

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

	// Out of example scope, but used to wait until a job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	// First job insertion for account 1.
	_, err = riverClient.Insert(ctx, ReconcileAccountArgs{AccountID: 1}, nil)
	if err != nil {
		panic(err)
	}

	// Job is inserted a second time, but it doesn't matter because its unique
	// args cause the insertion to be skipped because it's meant to only run
	// once per account per 24 hour period.
	_, err = riverClient.Insert(ctx, ReconcileAccountArgs{AccountID: 1}, nil)
	if err != nil {
		panic(err)
	}

	// Cheat a little by waiting for the first job to come back so we can
	// guarantee that this example's output comes out in order.
	waitForNJobs(subscribeChan, 1)

	// Because the job is unique ByArgs, another job for account 2 is allowed.
	_, err = riverClient.Insert(ctx, ReconcileAccountArgs{AccountID: 2}, nil)
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
