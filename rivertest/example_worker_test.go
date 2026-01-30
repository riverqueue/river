package rivertest_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertest"
)

type ExampleArgs struct {
	Name string `json:"name"`
}

func (ExampleArgs) Kind() string { return "example" }

// ExampleNewWorker demonstrates using rivertest.NewWorker to execute a job.
func ExampleNewWorker() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, testutil.PanicTB(), driver, nil)

	tx, err := dbPool.Begin(ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "SET search_path TO '"+schema+"'")
	if err != nil {
		panic(err)
	}

	worker := river.WorkFunc(func(ctx context.Context, job *river.Job[ExampleArgs]) error {
		fmt.Printf("Hello, %s!\n", job.Args.Name)
		return nil
	})

	t := &testing.T{}

	testWorker := rivertest.NewWorker(t, driver, &river.Config{
		Logger:   slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime})),
		Schema:   schema,
		TestOnly: true,
	}, worker)

	res, err := testWorker.Work(ctx, t, tx, ExampleArgs{Name: "River"}, nil)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.EventKind)

	// Output:
	// Hello, River!
	// job_completed
}
