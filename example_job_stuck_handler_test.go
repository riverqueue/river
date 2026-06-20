package river_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

type StuckJobHandlerArgs struct{}

func (StuckJobHandlerArgs) Kind() string { return "stuck_job_handler" }

type StuckJobHandlerWorker struct {
	river.WorkerDefaults[StuckJobHandlerArgs]

	releaseJobs chan struct{}
	started     chan struct{}
}

func (w *StuckJobHandlerWorker) Work(ctx context.Context, job *river.Job[StuckJobHandlerArgs]) error {
	w.started <- struct{}{}

	// Ignore ctx.Done() to simulate a job that doesn't respond to cancellation.
	<-w.releaseJobs

	return nil
}

// Example_jobStuckHandler demonstrates how to use JobStuckHandler to stop a
// client when too many jobs are stuck so the process can be restarted. For the
// first couple stuck jobs it uses AddWorkerSlot to add additional worker slots
// to replace those occupied by stuck jobs, but after maxStuckJobsBeforeRestart
// it gives up and exits so it can be restarted.
func Example_jobStuckHandler() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	var riverClient *river.Client[pgx.Tx]

	const maxStuckJobsBeforeRestart = 2

	var (
		releaseJobs      = make(chan struct{})
		restartRequested = make(chan struct{})
		started          = make(chan struct{}, maxStuckJobsBeforeRestart+1)
		stopOnce         sync.Once
	)

	workers := river.NewWorkers()
	river.AddWorker(workers, &StuckJobHandlerWorker{releaseJobs: releaseJobs, started: started})

	riverClient, err = river.NewClient(riverpgxv5.New(dbPool), initTestConfig(ctx, dbPool, &river.Config{
		JobStuckHandler: func(ctx context.Context, params river.JobStuckHandlerParams) river.JobStuckHandlerResult {
			if params.TotalStuckJobs <= maxStuckJobsBeforeRestart {
				fmt.Printf("stuck jobs: %d; opening replacement worker slot\n", params.TotalStuckJobs)
				return river.JobStuckHandlerResult{AddWorkerSlot: true}
			}

			stopOnce.Do(func() {
				fmt.Printf("too many stuck jobs: %d; shutting down so the process can restart\n", params.TotalStuckJobs)
				close(restartRequested)

				shutdownCtx := context.WithoutCancel(ctx)
				go func() {
					if err := riverClient.Stop(shutdownCtx); err != nil {
						panic(err)
					}
				}()
			})

			return river.JobStuckHandlerResult{}
		},
		JobStuckThreshold: time.Millisecond,
		JobTimeout:        10 * time.Millisecond,
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 1},
		},
		Workers: workers,
	}))
	if err != nil {
		panic(err)
	}

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	for range maxStuckJobsBeforeRestart + 1 {
		if _, err := riverClient.Insert(ctx, StuckJobHandlerArgs{}, nil); err != nil {
			panic(err)
		}
	}

	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), started, maxStuckJobsBeforeRestart+1)

	<-restartRequested
	close(releaseJobs)
	<-riverClient.Stopped()

	// Output:
	// stuck jobs: 1; opening replacement worker slot
	// stuck jobs: 2; opening replacement worker slot
	// too many stuck jobs: 3; shutting down so the process can restart
}
