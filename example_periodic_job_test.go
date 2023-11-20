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

type PeriodicJobArgs struct{}

// Kind is the unique string name for this job.
func (PeriodicJobArgs) Kind() string { return "periodic" }

// PeriodicJobWorker is a job worker for sorting strings.
type PeriodicJobWorker struct {
	river.WorkerDefaults[PeriodicJobArgs]
}

func (w *PeriodicJobWorker) Work(ctx context.Context, job *river.Job[PeriodicJobArgs]) error {
	fmt.Printf("This job will run once immediately then approximately once every 15 minutes\n")
	return nil
}

// Example_periodicJob demonstrates the use of a periodic job.
func Example_periodicJob() {
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
	river.AddWorker(workers, &PeriodicJobWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		PeriodicJobs: []*river.PeriodicJob{
			river.NewPeriodicJob(
				river.PeriodicInterval(15*time.Minute),
				func() (river.JobArgs, *river.InsertOpts) {
					return PeriodicJobArgs{}, nil
				},
				&river.PeriodicJobOpts{RunOnStart: true},
			),
		},
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

	// There's no need to explicitly insert a periodic job. One will be inserted
	// (and worked soon after) as the client starts up.
	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// This job will run once immediately then approximately once every 15 minutes
}
