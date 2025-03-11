package river_test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivertype"
)

type BothInsertAndWorkBeginHook struct{ river.HookDefaults }

func (BothInsertAndWorkBeginHook) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	fmt.Printf("BothInsertAndWorkBeginHook.InsertBegin ran\n")
	return nil
}

func (BothInsertAndWorkBeginHook) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	fmt.Printf("BothInsertAndWorkBeginHook.WorkBegin ran\n")
	return nil
}

type InsertBeginHook struct{ river.HookDefaults }

func (InsertBeginHook) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	fmt.Printf("InsertBeginHook.InsertBegin ran\n")
	return nil
}

type WorkBeginHook struct{ river.HookDefaults }

func (WorkBeginHook) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	fmt.Printf("WorkBeginHook.WorkBegin ran\n")
	return nil
}

// Verify interface compliance. It's recommended that these are included in your
// test suite to make sure that your hooks are complying to the specific
// interface hooks that you expected them to be.
var (
	_ rivertype.HookInsertBegin = &BothInsertAndWorkBeginHook{}
	_ rivertype.HookWorkBegin   = &BothInsertAndWorkBeginHook{}
	_ rivertype.HookInsertBegin = &InsertBeginHook{}
	_ rivertype.HookWorkBegin   = &WorkBeginHook{}
)

// Example_globalHooks demonstrates the use of hooks to modify River behavior
// which are global to a River client.
func Example_globalHooks() {
	ctx := context.Background()

	dbPool, err := pgxpool.NewWithConfig(ctx, riverinternaltest.DatabaseConfig("river_test_example"))
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	// Required for the purpose of this test, but not necessary in real usage.
	if err := riverinternaltest.TruncateRiverTables(ctx, dbPool); err != nil {
		panic(err)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &NoOpWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		// Order is significant. See output below.
		Hooks: []rivertype.Hook{
			&BothInsertAndWorkBeginHook{},
			&InsertBeginHook{},
			&WorkBeginHook{},
		},
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		TestOnly: true, // suitable only for use in tests; remove for live environments
		Workers:  workers,
	})
	if err != nil {
		panic(err)
	}

	// Out of example scope, but used to wait until a job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	_, err = riverClient.Insert(ctx, NoOpArgs{}, nil)
	if err != nil {
		panic(err)
	}

	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// BothInsertAndWorkBeginHook.InsertBegin ran
	// InsertBeginHook.InsertBegin ran
	// BothInsertAndWorkBeginHook.WorkBegin ran
	// WorkBeginHook.WorkBegin ran
	// NoOpWorker.Work ran
}
