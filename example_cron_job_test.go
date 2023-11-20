package river_test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/slogutil"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type CronJobArgs struct{}

// Kind is the unique string name for this job.
func (CronJobArgs) Kind() string { return "cron" }

// CronJobWorker is a job worker for sorting strings.
type CronJobWorker struct {
	river.WorkerDefaults[CronJobArgs]
}

func (w *CronJobWorker) Work(ctx context.Context, job *river.Job[CronJobArgs]) error {
	fmt.Printf("This job will run once immediately then every hour on the half hour\n")
	return nil
}

// Example_cronJob demonstrates how to create a cron job with a more complex
// schedule using a third party cron package to parse more elaborate crontab
// syntax.
func Example_cronJob() {
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
	river.AddWorker(workers, &CronJobWorker{})

	schedule, err := cron.ParseStandard("30 * * * *") // every hour on the half hour
	if err != nil {
		panic(err)
	}

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		PeriodicJobs: []*river.PeriodicJob{
			river.NewPeriodicJob(
				schedule,
				func() (river.JobArgs, *river.InsertOpts) {
					return CronJobArgs{}, nil
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
	// This job will run once immediately then every hour on the half hour
}
