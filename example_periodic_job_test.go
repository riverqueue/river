package river_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
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

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &PeriodicJobWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime})),
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
		Schema:   riverdbtest.TestSchema(ctx, testutil.PanicTB(), riverpgxv5.New(dbPool), nil), // only necessary for the example test
		TestOnly: true,                                                                         // suitable only for use in tests; remove for live environments
		Workers:  workers,
	})
	if err != nil {
		panic(err)
	}

	// Out of example scope, but used to wait until a job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	// There's no need to explicitly insert a periodic job. One will be inserted
	// (and worked soon after) as the client starts up.
	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	// Wait for jobs to complete. Only needed for purposes of the example test.
	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	// Periodic jobs can also be configured dynamically after a client has
	// already started. Added jobs are scheduled for run immediately.
	riverClient.PeriodicJobs().Clear()
	riverClient.PeriodicJobs().Add(
		river.NewPeriodicJob(
			river.PeriodicInterval(15*time.Minute),
			func() (river.JobArgs, *river.InsertOpts) {
				return PeriodicJobArgs{}, nil
			},
			nil,
		),
	)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// This job will run once immediately then approximately once every 15 minutes
}
