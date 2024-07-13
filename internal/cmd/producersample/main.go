package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

const (
	jobCount   = 10_000 // how many jobs total to insert
	numWorkers = 500    // how many workers to spin up
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := (&producerSample{logger: logger, name: reflect.TypeOf(producerSample{}).Name()}).produce(context.Background()); err != nil {
		logger.Error("failed", "error", err.Error())
	}
}

type producerSample struct {
	logger *slog.Logger
	name   string
}

func (p *producerSample) produce(ctx context.Context) error {
	p.logger.Info(p.name+": Running", "num_workers", numWorkers, "num_jobs", jobCount)
	poolConfig, err := pgxpool.ParseConfig(getDatabaseURL())
	if err != nil {
		return fmt.Errorf("error parsing pool config: %w", err)
	}
	poolConfig.ConnConfig.ConnectTimeout = 10 * time.Second
	poolConfig.MaxConns = int32(runtime.NumCPU()) * 2

	dbPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("error opening pool config: %w", err)
	}

	workers := river.NewWorkers()
	myJobWorker := &MyJobWorker{logger: p.logger}
	river.AddWorker(workers, myJobWorker)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Clean jobs from previous runs so that job stats don't look completely
	// messed up.
	if err := cleanDatabase(ctx, dbPool); err != nil {
		return err
	}

	client, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		FetchCooldown: 2 * time.Millisecond,
		Logger:        p.logger,
		Queues:        map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: numWorkers}},
		Workers:       workers,
	})
	if err != nil {
		return fmt.Errorf("error creating river client: %w", err)
	}

	if err := p.insertBulkJobs(ctx, client, jobCount); err != nil {
		return err
	}

	p.logger.Info(p.name + ": Finished inserting jobs; working")

	if err := client.Start(ctx); err != nil {
		return fmt.Errorf("error running river client: %w", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 10*time.Second)
		defer shutdownCancel()
		err := client.Stop(shutdownCtx)
		if err != nil {
			p.logger.Error(p.name+": Error shutting down client", "error", err.Error())
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			sig := <-sigChan
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				p.logger.Info(p.name+": Initiating graceful shutdown", "signal", sig)
				if err := p.initiateGracefulShutdown(ctx, client); err != nil {
					fmt.Fprintf(os.Stderr, "failed: %s", err.Error())
					os.Exit(1)
				}
			}
		}
	}()

	done := make(chan struct{})

	go func() {
		shutdownTimer := time.After(60 * time.Second)
		for {
			select {
			case <-time.After(5 * time.Second):
				jobsRun := atomic.LoadUint64(&myJobWorker.jobsPerformed)
				p.logger.Info(p.name+": Working", "num_jobs_run", jobsRun)
			case <-shutdownTimer:
				if err := p.initiateGracefulShutdown(ctx, client); err != nil {
					fmt.Fprintf(os.Stderr, "failed: %s", err.Error())
					os.Exit(1)
				}
				close(done)
			}
		}
	}()

	<-done
	p.logger.Info(p.name + ": Client done")

	// TODO: need to wait until completer has finished all pending completions

	return nil
}

func (p *producerSample) initiateGracefulShutdown(ctx context.Context, client *river.Client[pgx.Tx]) error {
	p.logger.Info(p.name + ": Initiating graceful shutdown")

	// Wait 10 seconds for existing work to complete:
	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 10*time.Second)
	defer shutdownCancel()

	if err := client.Stop(shutdownCtx); errors.Is(err, context.DeadlineExceeded) {
		p.logger.Info(p.name + ": Gave up waiting for graceful shutdown, getting more aggressive")
		return p.initiateHardShutdown(ctx, client)
	}

	return nil
}

func (p *producerSample) initiateHardShutdown(ctx context.Context, client *river.Client[pgx.Tx]) error {
	shutDownNowCtx, shutdownNowCancel := context.WithTimeout(ctx, 5*time.Second)
	defer shutdownNowCancel()

	if err := client.StopAndCancel(shutDownNowCtx); err != nil {
		return fmt.Errorf("error shutting down: %w", err)
	}

	p.logger.Info(p.name + ": Aggressive graceful shutdown complete")
	return nil
}

func (p *producerSample) insertBulkJobs(ctx context.Context, client *river.Client[pgx.Tx], jobCount int) error {
	p.logger.Info(p.name+": Inserting jobs", "num_jobs", jobCount)

	insertParams := make([]river.InsertManyParams, jobCount)
	for i := 0; i < jobCount; i++ {
		insertParams[i] = river.InsertManyParams{Args: &MyJobArgs{JobNum: i}}
	}
	inserted, err := client.InsertMany(ctx, insertParams)
	if err != nil {
		return fmt.Errorf("error inserting jobs: %w", err)
	}
	if inserted != jobCount {
		return fmt.Errorf("expected to insert %d jobs, but only inserted %d", jobCount, inserted)
	}

	return nil
}

func cleanDatabase(ctx context.Context, dbPool *pgxpool.Pool) error {
	_, err := dbPool.Exec(ctx, "TRUNCATE TABLE river_job")
	if err != nil {
		return fmt.Errorf("error cleaning database: %w", err)
	}
	return nil
}

func getDatabaseURL() string {
	if envURL := os.Getenv("DATABASE_URL"); envURL != "" {
		return envURL
	}
	return "postgres:///river_test_example?sslmode=disable"
}

type MyJobArgs struct {
	JobNum int `json:"job_num"`
}

func (MyJobArgs) Kind() string { return "MyJob" }

func (MyJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Priority: 2,
	}
}

type MyJobWorker struct {
	river.WorkerDefaults[MyJobArgs]
	jobsPerformed uint64
	logger        *slog.Logger
}

func (w *MyJobWorker) Work(ctx context.Context, job *river.Job[MyJobArgs]) error {
	atomic.AddUint64(&w.jobsPerformed, 1)
	// fmt.Printf("performing job %d with args %+v\n", job.ID, job.Args)
	if job.ID%1000 == 0 {
		w.logger.Info("simulating stalled job, blocked on ctx.Done()")
		<-ctx.Done()
		w.logger.Info("stalled job exiting")
		return ctx.Err()
		// return fmt.Errorf("random failure")
	}
	return nil
}
