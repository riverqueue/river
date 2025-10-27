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

type ScheduledArgs struct {
	Message string `json:"message"`
}

func (ScheduledArgs) Kind() string { return "scheduled" }

type ScheduledWorker struct {
	river.WorkerDefaults[ScheduledArgs]
}

func (w *ScheduledWorker) Work(ctx context.Context, job *river.Job[ScheduledArgs]) error {
	fmt.Printf("Message: %s\n", job.Args.Message)
	return nil
}

// Example_scheduledJob demonstrates how to schedule a job to be worked in the
// future.
func Example_scheduledJob() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &ScheduledWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime})),
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

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	_, err = riverClient.Insert(ctx,
		ScheduledArgs{
			Message: "hello from the future",
		},
		&river.InsertOpts{
			// Schedule the job to be worked in three hours.
			ScheduledAt: time.Now().Add(3 * time.Hour),
		})
	if err != nil {
		panic(err)
	}

	// Unlike most other examples, we don't wait for the job to be worked since
	// doing so would require making the job's scheduled time contrived, and the
	// example therefore less realistic/useful.

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
}
