package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"weavelab.xyz/river"
	"weavelab.xyz/river/internal/riverinternaltest" //nolint:depguard
	"weavelab.xyz/river/riverdriver/riverpgxv5"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	if err := prepareAndRunBenchmark(context.Background(), logger); err != nil {
		logger.Error("failed", "error", err.Error())
	}
}

func prepareAndRunBenchmark(ctx context.Context, logger *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	const total = 1000000
	const workerCount = 500
	const insertBatchSize = 10000
	const poolLimit = 10
	const fetchInterval = 2 * time.Millisecond

	fmt.Printf(
		"--- benchmarking throughput: jobs=%d workers=%d pool_limit=%d fetch_interval=%s\n",
		total,
		workerCount,
		poolLimit,
		fetchInterval,
	)

	dbPool := mustGetDBPool(ctx, poolLimit)
	fmt.Println("--- truncating DB…")
	if err := truncateDB(ctx, dbPool); err != nil {
		return fmt.Errorf("failed to truncate DB: %w", err)
	}

	counterWorker := &CounterWorker{}
	workers := river.NewWorkers()
	river.AddWorker(workers, counterWorker)

	client, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		FetchCooldown:     fetchInterval,
		FetchPollInterval: fetchInterval,
		Logger:            logger,
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: workerCount},
		},
		Workers: workers,
	})
	if err != nil {
		return fmt.Errorf("failed to create river client: %w", err)
	}

	// Insert jobs in batches until we hit the total:
	fmt.Printf("--- inserting %d jobs in batches of %d…\n", total, insertBatchSize)
	batch := make([]river.InsertManyParams, 0, insertBatchSize)
	for i := 0; i < total; i += insertBatchSize {
		batch = batch[:0]
		for j := 0; j < insertBatchSize && i+j < total; j++ {
			batch = append(batch, river.InsertManyParams{Args: CounterArgs{Number: i + j}})
		}
		if err := insertBatch(ctx, client, batch); err != nil {
			return fmt.Errorf("failed to insert batch: %w", err)
		}
	}
	fmt.Printf("\n")
	logger.Info("done inserting jobs, sleeping 5s")
	time.Sleep(5 * time.Second)
	logger.Info("starting client")

	startTime := time.Now()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				jobsWorked := counterWorker.Counter.Load()
				now := time.Now()
				fmt.Printf("stats: %d jobs worked in %s (%.2f jobs/sec)\n",
					counterWorker.Counter.Load(),
					now.Sub(startTime),
					float64(jobsWorked)/now.Sub(startTime).Seconds(),
				)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				jobsWorked := counterWorker.Counter.Load()
				if jobsWorked == total {
					logger.Info("--- all jobs worked")
					cancel()
					return
				}
			}
		}
	}()

	if err := client.Start(ctx); err != nil {
		return fmt.Errorf("failed to run river client: %w", err)
	}

	logger.Info("client started")

	<-ctx.Done()
	logger.Info("initiating shutdown")
	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 10*time.Second)
	defer shutdownCancel()

	if err = client.Stop(shutdownCtx); err != nil {
		return fmt.Errorf("error shutting down client: %w", err)
	}

	logger.Info("client shutdown complete", "jobs_worked_count", counterWorker.Counter.Load())

	endTime := time.Now()
	fmt.Printf("final stats: %d jobs worked in %s (%.2f jobs/sec)\n",
		counterWorker.Counter.Load(),
		endTime.Sub(startTime),
		float64(counterWorker.Counter.Load())/endTime.Sub(startTime).Seconds(),
	)
	return nil
}

func mustGetDBPool(ctx context.Context, connCount int32) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	config := riverinternaltest.DatabaseConfig("river_testdb_example")
	config.MaxConns = connCount
	config.MinConns = connCount
	dbPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		panic(err)
	}
	return dbPool
}

func truncateDB(ctx context.Context, pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err := pool.Exec(ctx, "TRUNCATE river_job")
	return err
}

func insertBatch(ctx context.Context, client *river.Client[pgx.Tx], params []river.InsertManyParams) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	insertedCount, err := client.InsertMany(ctx, params)
	if err != nil {
		return err
	}

	if insertedCount != int64(len(params)) {
		return fmt.Errorf("inserted %d jobs, expected %d", insertedCount, len(params))
	}

	fmt.Printf(".")
	return nil
}

// CounterArgs are arguments for CounterWorker.
type CounterArgs struct {
	// Number is the number of this job.
	Number int `json:"number"`
}

func (CounterArgs) Kind() string { return "counter_worker" }

// CounterWorker is a job worker for counting the number of worked jobs.
type CounterWorker struct {
	river.WorkerDefaults[CounterArgs]
	Counter atomic.Uint64
}

func (w *CounterWorker) Work(ctx context.Context, j *river.Job[CounterArgs]) error {
	w.Counter.Add(1)
	return nil
}
