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

type JobWithHooksArgs struct{}

func (JobWithHooksArgs) Kind() string { return "job_with_hooks" }

// Warning: Hooks is only called once per job insert or work and its return
// value is memoized. It should not vary based on the contents of any particular
// args because changes will be ignored.
func (JobWithHooksArgs) Hooks() []rivertype.Hook {
	// Order is significant. See output below.
	return []rivertype.Hook{
		&JobWithHooksBothInsertAndWorkBeginHook{},
		&JobWithHooksInsertBeginHook{},
		&JobWithHooksWorkBeginHook{},
	}
}

type JobWithHooksWorker struct {
	river.WorkerDefaults[JobWithHooksArgs]
}

func (w *JobWithHooksWorker) Work(ctx context.Context, job *river.Job[JobWithHooksArgs]) error {
	fmt.Printf("JobWithHooksWorker.Work ran\n")
	return nil
}

type JobWithHooksBothInsertAndWorkBeginHook struct{ river.HookDefaults }

func (JobWithHooksBothInsertAndWorkBeginHook) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	fmt.Printf("JobWithHooksInsertAndWorkBeginHook.InsertBegin ran\n")
	return nil
}

func (JobWithHooksBothInsertAndWorkBeginHook) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	fmt.Printf("JobWithHooksInsertAndWorkBeginHook.WorkBegin ran\n")
	return nil
}

type JobWithHooksInsertBeginHook struct{ river.HookDefaults }

func (JobWithHooksInsertBeginHook) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	fmt.Printf("JobWithHooksInsertBeginHook.InsertBegin ran\n")
	return nil
}

type JobWithHooksWorkBeginHook struct{ river.HookDefaults }

func (JobWithHooksWorkBeginHook) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	fmt.Printf("JobWithHooksWorkBeginHook.WorkBegin ran\n")
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

// Example_jobArgsHooks demonstrates the use of hooks to modify River behavior.
func Example_jobArgsHooks() {
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
	river.AddWorker(workers, &JobWithHooksWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
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

	_, err = riverClient.Insert(ctx, JobWithHooksArgs{}, nil)
	if err != nil {
		panic(err)
	}

	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// JobWithHooksInsertAndWorkBeginHook.InsertBegin ran
	// JobWithHooksInsertBeginHook.InsertBegin ran
	// JobWithHooksInsertAndWorkBeginHook.WorkBegin ran
	// JobWithHooksWorkBeginHook.WorkBegin ran
	// JobWithHooksWorker.Work ran
}
