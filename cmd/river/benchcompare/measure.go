package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

type benchArgs struct {
	Payload string `json:"payload"`
}

func (benchArgs) Kind() string { return "benchcompare" }

type benchWorker struct {
	river.WorkerDefaults[benchArgs]
}

func (*benchWorker) Work(context.Context, *river.Job[benchArgs]) error { return nil }

func measure[TTx any](ctx context.Context, driver riverdriver.Driver[TTx], schema string, cfg config, sample *trial) (err error) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	migrator, err := rivermigrate.New(driver, &rivermigrate.Config{Logger: logger, Schema: schema})
	if err != nil {
		return err
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return err
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &benchWorker{})
	client, err := river.NewClient(driver, &river.Config{
		CompletedJobRetentionPeriod: -1,
		FetchCooldown:               2 * time.Millisecond,
		FetchPollInterval:           20 * time.Millisecond,
		Logger:                      logger,
		Queues:                      map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: cfg.Workers}},
		Schema:                      schema,
		Workers:                     workers,
	})
	if err != nil {
		return err
	}
	events, cancel := client.SubscribeConfig(&river.SubscribeConfig{
		ChanSize: cfg.Jobs,
		Kinds:    []river.EventKind{river.EventKindJobCompleted, river.EventKindJobFailed, river.EventKindJobCancelled},
	})
	defer cancel()
	params := make([]river.InsertManyParams, min(cfg.BatchSize, cfg.Jobs))
	for i := range params {
		params[i].Args = benchArgs{Payload: strings.Repeat("x", cfg.PayloadBytes)}
	}
	start := time.Now()
	for inserted := 0; inserted < cfg.Jobs; {
		batch := params[:min(len(params), cfg.Jobs-inserted)]
		var count int
		if cfg.InsertMode == "fast" {
			count, err = client.InsertManyFast(ctx, batch)
		} else {
			var rows []*rivertype.JobInsertResult
			rows, err = client.InsertMany(ctx, batch)
			count = len(rows)
		}
		if err != nil {
			return fmt.Errorf("insert: %w", err)
		}
		if count != len(batch) {
			return fmt.Errorf("insert returned %d jobs, expected %d", count, len(batch))
		}
		inserted += count
	}
	sample.InsertSeconds = time.Since(start).Seconds()
	sample.InsertJobsPerSecond = float64(cfg.Jobs) / sample.InsertSeconds

	// Include client startup in drain time. Count completion events after the
	// database acknowledgement, rather than counting calls to the no-op worker.
	start = time.Now()
	if err := client.Start(ctx); err != nil {
		return err
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer stopCancel()
		err = errors.Join(err, client.StopAndCancel(stopCtx))
	}()
	for sample.Completed < cfg.Jobs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				return errors.New("completion subscription closed early")
			}
			if event.Kind != river.EventKindJobCompleted || event.Job.Attempt != 1 {
				return fmt.Errorf("unexpected job outcome: %s, attempt %d", event.Kind, event.Job.Attempt)
			}
			sample.Completed++
		}
	}
	sample.DrainSeconds = time.Since(start).Seconds()
	sample.DrainJobsPerSecond = float64(cfg.Jobs) / sample.DrainSeconds
	if err := client.Stop(ctx); err != nil {
		return err
	}
	completed, err := driver.GetExecutor().JobCountByState(ctx, &riverdriver.JobCountByStateParams{
		Schema: schema,
		State:  rivertype.JobStateCompleted,
	})
	if err != nil {
		return err
	}
	if completed != cfg.Jobs {
		return fmt.Errorf("database has %d completed jobs, expected %d", completed, cfg.Jobs)
	}
	return nil
}
