package river

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

func TestWork(t *testing.T) {
	t.Parallel()

	workers := NewWorkers()

	AddWorker(workers, &noOpWorker{})
	require.Contains(t, workers.workersMap, (noOpArgs{}).Kind())

	require.PanicsWithError(t, `worker for kind "noOp" is already registered`, func() {
		AddWorker(workers, &noOpWorker{})
	})

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	AddWorker(workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
		return nil
	}))
	require.Contains(t, workers.workersMap, (JobArgs{}).Kind())

	AddWorker(workers, WorkFunc(func(ctx context.Context, job *Job[withKindAliasesArgs]) error {
		return nil
	}))
	require.Contains(t, workers.workersMap, (withKindAliasesArgs{}).Kind())
	require.Contains(t, workers.workersMap, (withKindAliasesArgs{}).KindAliases()[0])

	require.PanicsWithError(t, `worker for kind "with_kind_alternate_alternate" is already registered`, func() {
		AddWorker(workers, WorkFunc(func(ctx context.Context, job *Job[withKindAliasesCollisionArgs]) error {
			return nil
		}))
	})

	type OtherJobArgs struct {
		testutil.JobArgsReflectKind[OtherJobArgs]
	}

	var jobArgs OtherJobArgs
	AddWorkerArgs(workers, jobArgs, WorkFunc(func(ctx context.Context, job *Job[OtherJobArgs]) error {
		return nil
	}))
	require.Contains(t, workers.workersMap, (OtherJobArgs{}).Kind())
}

type configurableArgs struct {
	uniqueOpts UniqueOpts
}

func (a configurableArgs) Kind() string { return "configurable" }
func (a configurableArgs) InsertOpts() InsertOpts {
	return InsertOpts{UniqueOpts: a.uniqueOpts}
}

type configurableWorker struct {
	WorkerDefaults[configurableArgs]
}

func (w *configurableWorker) Work(ctx context.Context, job *Job[configurableArgs]) error {
	return nil
}

type withKindAliasesArgs struct{}

func (a withKindAliasesArgs) Kind() string          { return "with_kind_alternate" }
func (a withKindAliasesArgs) KindAliases() []string { return []string{"with_kind_alternate_alternate"} }

type withKindAliasesCollisionArgs struct{}

func (a withKindAliasesCollisionArgs) Kind() string {
	return (withKindAliasesArgs{}).KindAliases()[0]
}

func TestWorkers_add(t *testing.T) {
	t.Parallel()

	workers := NewWorkers()

	err := workers.add(noOpArgs{}, &workUnitFactoryWrapper[noOpArgs]{worker: &noOpWorker{}})
	require.NoError(t, err)

	// Different worker kind.
	err = workers.add(configurableArgs{}, &workUnitFactoryWrapper[configurableArgs]{worker: &configurableWorker{}})
	require.NoError(t, err)

	err = workers.add(noOpArgs{}, &workUnitFactoryWrapper[noOpArgs]{worker: &noOpWorker{}})
	require.EqualError(t, err, `worker for kind "noOp" is already registered`)
}

type WorkFuncArgs struct{}

func (WorkFuncArgs) Kind() string { return "work_func" }

type StructWithFunc struct {
	WorkChan chan struct{}
}

func (s *StructWithFunc) Work(ctx context.Context, job *Job[WorkFuncArgs]) error {
	s.WorkChan <- struct{}{}
	return nil
}

func TestWorkFunc(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		startClient(ctx, t, client)

		return client, &testBundle{}
	}

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		workChan := make(chan struct{})
		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[WorkFuncArgs]) error {
			workChan <- struct{}{}
			return nil
		}))

		_, err := client.Insert(ctx, &WorkFuncArgs{}, nil)
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, workChan)
	})

	t.Run("StructFunction", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		structWithFunc := &StructWithFunc{
			WorkChan: make(chan struct{}),
		}

		AddWorker(client.config.Workers, WorkFunc(structWithFunc.Work))

		_, err := client.Insert(ctx, &WorkFuncArgs{}, nil)
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, structWithFunc.WorkChan)
	})

	t.Run("JobArgsReflectKind", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type InFuncWorkFuncArgs struct {
			testutil.JobArgsReflectKind[InFuncWorkFuncArgs]
		}

		workChan := make(chan struct{})
		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[InFuncWorkFuncArgs]) error {
			workChan <- struct{}{}
			return nil
		}))

		_, err := client.Insert(ctx, &InFuncWorkFuncArgs{}, nil)
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, workChan)
	})
}
