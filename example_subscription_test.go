package river_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/rivershared/riversharedtest"
	"github.com/riverqueue/rivershared/util/slogutil"
)

type SubscriptionArgs struct {
	Cancel bool `json:"cancel"`
	Fail   bool `json:"fail"`
}

func (SubscriptionArgs) Kind() string { return "subscription" }

type SubscriptionWorker struct {
	river.WorkerDefaults[SubscriptionArgs]
}

func (w *SubscriptionWorker) Work(ctx context.Context, job *river.Job[SubscriptionArgs]) error {
	switch {
	case job.Args.Cancel:
		return river.JobCancel(errors.New("cancelling job"))
	case job.Args.Fail:
		return errors.New("failing job")
	}
	return nil
}

// Example_subscription demonstrates the use of client subscriptions to receive
// events containing information about worked jobs.
func Example_subscription() {
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
	river.AddWorker(workers, &SubscriptionWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: 9}), // Suppress logging so example output is cleaner (9 > slog.LevelError).
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Workers: workers,
	})
	if err != nil {
		panic(err)
	}

	// Subscribers tell the River client the kinds of events they'd like to receive.
	completedChan, completedSubscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer completedSubscribeCancel()

	// Multiple simultaneous subscriptions are allowed.
	failedChan, failedSubscribeCancel := riverClient.Subscribe(river.EventKindJobFailed)
	defer failedSubscribeCancel()

	otherChan, otherSubscribeCancel := riverClient.Subscribe(river.EventKindJobCancelled, river.EventKindJobSnoozed)
	defer otherSubscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	// Insert one job for each subscription above: one to succeed, one to fail,
	// and one that's cancelled that'll arrive on the "other" channel.
	_, err = riverClient.Insert(ctx, SubscriptionArgs{}, nil)
	if err != nil {
		panic(err)
	}
	_, err = riverClient.Insert(ctx, SubscriptionArgs{Fail: true}, nil)
	if err != nil {
		panic(err)
	}
	_, err = riverClient.Insert(ctx, SubscriptionArgs{Cancel: true}, nil)
	if err != nil {
		panic(err)
	}

	waitForJob := func(subscribeChan <-chan *river.Event) {
		select {
		case event := <-subscribeChan:
			if event == nil {
				fmt.Printf("Channel is closed\n")
				return
			}

			fmt.Printf("Got job with state: %s\n", event.Job.State)
		case <-time.After(riversharedtest.WaitTimeout()):
			panic("timed out waiting for job")
		}
	}

	waitForJob(completedChan)
	waitForJob(failedChan)
	waitForJob(otherChan)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	fmt.Printf("Client stopped\n")

	// Try waiting again, but none of these work because stopping the client
	// closed all subscription channels automatically.
	waitForJob(completedChan)
	waitForJob(failedChan)
	waitForJob(otherChan)

	// Output:
	// Got job with state: completed
	// Got job with state: available
	// Got job with state: cancelled
	// Client stopped
	// Channel is closed
	// Channel is closed
	// Channel is closed
}
