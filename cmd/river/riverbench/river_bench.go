package riverbench

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
)

type Benchmarker[TTx any] struct {
	driver       riverdriver.Driver[TTx]
	duration     time.Duration
	logger       *slog.Logger
	name         string
	numTotalJobs int
}

func NewBenchmarker[TTx any](driver riverdriver.Driver[TTx], logger *slog.Logger, duration time.Duration, numTotalJobs int) *Benchmarker[TTx] {
	return &Benchmarker[TTx]{
		driver:       driver,
		duration:     duration,
		logger:       logger,
		name:         "Benchmarker",
		numTotalJobs: numTotalJobs,
	}
}

// Run starts the benchmarking loop. Stops upon receiving SIGINT/SIGTERM, or
// when reaching maximum configured run duration.
func (b *Benchmarker[TTx]) Run(ctx context.Context) error {
	var (
		numJobsInserted atomic.Int64
		numJobsLeft     atomic.Int64
		numJobsWorked   atomic.Int64
		shutdown        = make(chan struct{})
		shutdownClosed  bool
	)

	// Prevents double-close on shutdown channel.
	closeShutdown := func() {
		if !shutdownClosed {
			b.logger.InfoContext(ctx, "Closing shutdown channel")
			close(shutdown)
		}
		shutdownClosed = true
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Installing signals allows us to try and stop the client cleanly, and also
	// to produce a final summary log line for th whole bench run (by default,
	// Go will terminate programs abruptly and not even defers will run).
	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-ctx.Done():
		case <-signalChan:
			closeShutdown()

			// Wait again since the client may take an absurd amount of time to
			// shut down. If we receive another signal in the intervening
			// period, cancel context, thereby forcing a hard shut down.
			select {
			case <-ctx.Done():
			case <-signalChan:
				fmt.Printf("second signal received; canceling context\n")
				cancel()
			}
		}
	}()

	if err := b.resetJobsTable(ctx); err != nil {
		return err
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &BenchmarkWorker{})

	client, err := river.NewClient(b.driver, &river.Config{
		// When benchmarking to maximize job throughput these numbers have an
		// outsized effect on results. The ones chosen here could possibly be
		// optimized further, but based on my tests of throwing a lot of random
		// values against the wall, they perform quite well. Much better than
		// the client's default values at any rate.
		FetchCooldown:     2 * time.Millisecond,
		FetchPollInterval: 5 * time.Millisecond,

		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})),
		Queues: map[string]river.QueueConfig{
			// This could probably use more refinement, but in my quick and
			// dirty tests I found that roughly 1k workers was most optimal. 500
			// and 2,000 performed a little more poorly, and jumping up to the
			// maximum of 10k performed quite badly (scheduler contention?).
			// There may be a more optimal number than 1,000, but it seems close
			// enough to target for now.
			river.QueueDefault: {MaxWorkers: 1_000},
		},
		Workers: workers,
	})
	if err != nil {
		return err
	}

	// Notably, we use a subscribe channel to track how many jobs have been
	// worked instead of using telemetry from the worker itself because the
	// subscribe channel accounts for the job moving through the completer while
	// the worker does not.
	subscribeChan, subscribeCancel := client.Subscribe(
		river.EventKindJobCancelled,
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
	)
	defer subscribeCancel()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case <-shutdown:
				return

			case <-subscribeChan:
				numJobsLeft.Add(-1)
				numJobsWorked := numJobsWorked.Add(1)

				const logBatchSize = 5_000
				if numJobsWorked%logBatchSize == 0 {
					b.logger.InfoContext(ctx, b.name+": Worked job batch", "num_worked", logBatchSize)
				}
			}
		}
	}()

	minJobsReady := make(chan struct{})

	if b.numTotalJobs != 0 {
		b.insertJobs(ctx, client, minJobsReady, &numJobsInserted, &numJobsLeft, shutdown)
	} else {
		insertJobsFinished := make(chan struct{})
		defer func() { <-insertJobsFinished }()

		go func() {
			defer close(insertJobsFinished)
			b.insertJobsContinuously(ctx, client, minJobsReady, &numJobsInserted, &numJobsLeft, shutdown)
		}()
	}

	// Must appear after we wait for insert jobs to finish before so that the
	// defers run in the right order.
	defer closeShutdown()

	// Don't start measuring until the first batch of jobs is confirmed ready.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-minJobsReady:
		// okay
	case <-shutdown:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("timed out waiting for minimum starting jobs to be inserted")
	}

	b.logger.InfoContext(ctx, b.name+": Minimum jobs inserted; starting iteration")

	b.logger.InfoContext(ctx, b.name+": Client starting")
	if err := client.Start(ctx); err != nil {
		return err
	}

	defer func() {
		b.logger.InfoContext(ctx, b.name+": Client stopping")
		if err := client.Stop(ctx); err != nil {
			b.logger.ErrorContext(ctx, b.name+": Error stopping client", "err", err)
		}
		b.logger.InfoContext(ctx, b.name+": Client stopped")
	}()

	// Prints one last log line before exit summarizing all operations.
	start := time.Now()
	defer func() {
		runPeriod := time.Since(start)
		jobsPerSecond := float64(numJobsWorked.Load()) / runPeriod.Seconds()

		fmt.Printf("bench: total jobs worked [ %10d ], total jobs inserted [ %10d ], overall job/sec [ %10.1f ], running %s\n",
			numJobsWorked.Load(), numJobsInserted.Load(), jobsPerSecond, runPeriod)
	}()

	const iterationPeriod = 2 * time.Second

	var (
		firstRun            = true
		numJobsInsertedLast int64
		numJobsWorkedLast   int64
		ticker              = time.NewTicker(iterationPeriod)
	)
	defer ticker.Stop()

	for numIterations := 0; ; numIterations++ {
		// Use iterations multiplied by period time instead of actual elapsed
		// time to allow a precise, predictable run duration to be specified.
		if b.duration != 0 && time.Duration(numIterations)*iterationPeriod >= b.duration {
			return nil
		}

		var (
			numJobsInsertedSinceLast = numJobsInserted.Load() - numJobsInsertedLast
			numJobsWorkedSinceLast   = numJobsWorked.Load() - numJobsWorkedLast
		)

		jobsPerSecond := float64(numJobsWorkedSinceLast) / iterationPeriod.Seconds()

		// On first run, show iteration period as 0s because no time was given
		// for jobs to be worked.
		period := iterationPeriod
		if firstRun {
			period = 0 * time.Second
		}

		fmt.Printf("bench: jobs worked [ %10d ], inserted [ %10d ], job/sec [ %10.1f ] [%s]\n",
			numJobsWorkedSinceLast, numJobsInsertedSinceLast, jobsPerSecond, period)

		firstRun = false
		numJobsInsertedLast = numJobsInserted.Load()
		numJobsWorkedLast = numJobsWorked.Load()

		// If working in the mode where we're burning jobs down and there are no
		// jobs left, end.
		if b.numTotalJobs != 0 && numJobsLeft.Load() < 1 {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil

		case <-shutdown:
			return nil

		case <-ticker.C:
		}
	}
}

const (
	insertBatchSize = 2_000
	minJobs         = 50_000
)

// Inserts `b.numTotalJobs` in batches. This variant inserts a bulk of initial
// jobs and ends, and is used in cases the `-n`/`--num-total-jobs` flag is
// specified.
func (b *Benchmarker[TTx]) insertJobs(
	ctx context.Context,
	client *river.Client[TTx],
	minJobsReady chan struct{},
	numJobsInserted *atomic.Int64,
	numJobsLeft *atomic.Int64,
	shutdown chan struct{},
) {
	defer close(minJobsReady)

	var (
		// We'll be reusing the same batch for all inserts because (1) we can
		// get away with it, and (2) to avoid needless allocations.
		insertParamsBatch = make([]river.InsertManyParams, insertBatchSize)
		jobArgsBatch      = make([]BenchmarkArgs, insertBatchSize)

		jobNum int
	)

	var numInsertedThisRound int

	for {
		for _, jobArgs := range jobArgsBatch {
			jobNum++
			jobArgs.Num = jobNum
		}

		for i := range insertParamsBatch {
			insertParamsBatch[i].Args = jobArgsBatch[i]
		}

		numLeft := b.numTotalJobs - numInsertedThisRound
		if numLeft < insertBatchSize {
			insertParamsBatch = insertParamsBatch[0:numLeft]
		}

		if _, err := client.InsertMany(ctx, insertParamsBatch); err != nil {
			b.logger.ErrorContext(ctx, b.name+": Error inserting jobs", "err", err)
		}

		numJobsInserted.Add(int64(len(insertParamsBatch)))
		numJobsLeft.Add(int64(len(insertParamsBatch)))
		numInsertedThisRound += len(insertParamsBatch)

		if numJobsLeft.Load() >= int64(b.numTotalJobs) {
			b.logger.InfoContext(ctx, b.name+": Finished inserting jobs",
				"num_inserted", numInsertedThisRound)
			return
		}

		// Will be very unusual, but break early if done between batches.
		select {
		case <-ctx.Done():
			return
		case <-shutdown:
			return
		default:
		}
	}
}

// Inserts jobs continuously, but only if it notices that the number of jobs
// left is below a minimum threshold. This has the effect of keeping enough job
// slack in the pool to be worked, but keeping the total number of jobs being
// inserted roughly matched with the rate at which the benchmark can work them.
func (b *Benchmarker[TTx]) insertJobsContinuously(
	ctx context.Context,
	client *river.Client[TTx],
	minJobsReady chan struct{},
	numJobsInserted *atomic.Int64,
	numJobsLeft *atomic.Int64,
	shutdown chan struct{},
) {
	var (
		// We'll be reusing the same batch for all inserts because (1) we can
		// get away with it, and (2) to avoid needless allocations.
		insertParamsBatch = make([]river.InsertManyParams, insertBatchSize)
		jobArgsBatch      = make([]BenchmarkArgs, insertBatchSize)

		jobNum int
	)

	for {
		select {
		case <-ctx.Done():
			return

		case <-shutdown:
			return

		case <-time.After(50 * time.Millisecond):
		}

		if numJobsLeft.Load() >= minJobs {
			continue
		}

		var numInsertedThisRound int

		for {
			for _, jobArgs := range jobArgsBatch {
				jobNum++
				jobArgs.Num = jobNum
			}

			for i := range insertParamsBatch {
				insertParamsBatch[i].Args = jobArgsBatch[i]
			}

			if _, err := client.InsertMany(ctx, insertParamsBatch); err != nil {
				b.logger.ErrorContext(ctx, b.name+": Error inserting jobs", "err", err)
			}

			numJobsInserted.Add(int64(len(insertParamsBatch)))
			numJobsLeft.Add(int64(len(insertParamsBatch)))
			numInsertedThisRound += len(insertParamsBatch)

			if numJobsLeft.Load() >= minJobs {
				b.logger.InfoContext(ctx, b.name+": Finished inserting batch of jobs",
					"num_inserted", numInsertedThisRound)
				break // break inner loop to go back to sleep
			}

			// Will be very unusual, but break early if done between batches.
			select {
			case <-ctx.Done():
				return
			case <-shutdown:
				return
			default:
			}
		}

		// Close the first time we insert a full batch to tell the main loop it
		// can start benchmarking.
		if minJobsReady != nil {
			close(minJobsReady)
			minJobsReady = nil
		}
	}
}

// Truncates and `VACUUM FULL`s the jobs table to guarantee as little state
// related job variance as possible.
func (b *Benchmarker[TTx]) resetJobsTable(ctx context.Context) error {
	b.logger.InfoContext(ctx, b.name+": Truncating and vacuuming jobs table")

	_, err := b.driver.GetExecutor().Exec(ctx, "TRUNCATE river_job")
	if err != nil {
		return err
	}
	_, err = b.driver.GetExecutor().Exec(ctx, "VACUUM FULL river_job")
	if err != nil {
		return err
	}

	return nil
}

type BenchmarkArgs struct {
	Num int `json:"num"`
}

func (BenchmarkArgs) Kind() string { return "benchmark" }

// BenchmarkWorker is a job worker for counting the number of worked jobs.
type BenchmarkWorker struct {
	river.WorkerDefaults[BenchmarkArgs]
}

func (w *BenchmarkWorker) Work(ctx context.Context, j *river.Job[BenchmarkArgs]) error {
	return nil
}
