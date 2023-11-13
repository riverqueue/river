package river

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
)

func TestWork(t *testing.T) {
	t.Parallel()

	workers := NewWorkers()

	AddWorker(workers, &noOpWorker{})
	require.Contains(t, workers.workersMap, (noOpArgs{}).Kind())

	require.PanicsWithError(t, `worker for kind "noOp" is already registered`, func() {
		AddWorker(workers, &noOpWorker{})
	})

	fn := func(ctx context.Context, j *Job[callbackArgs]) error { return nil }
	ch := callbackWorker{fn: fn}

	// function worker
	AddWorker(workers, &ch)
	require.Contains(t, workers.workersMap, (callbackArgs{}).Kind())
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

func (w *configurableWorker) Work(ctx context.Context, j *Job[configurableArgs]) error {
	return nil
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

func (s *StructWithFunc) Work(ctx context.Context, j *Job[WorkFuncArgs]) error {
	s.WorkChan <- struct{}{}
	return nil
}

func TestWorkFunc(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		client := newTestClient(ctx, t, newTestConfig(t, nil))
		startClient(ctx, t, client)

		return client, &testBundle{}
	}

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		workChan := make(chan struct{})
		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, j *Job[WorkFuncArgs]) error {
			workChan <- struct{}{}
			return nil
		}))

		_, err := client.Insert(ctx, &WorkFuncArgs{}, nil)
		require.NoError(t, err)

		riverinternaltest.WaitOrTimeout(t, workChan)
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

		riverinternaltest.WaitOrTimeout(t, structWithFunc.WorkChan)
	})

	t.Run("JobArgsReflectKind", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type InFuncWorkFuncArgs struct {
			JobArgsReflectKind[InFuncWorkFuncArgs]
		}

		workChan := make(chan struct{})
		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, j *Job[InFuncWorkFuncArgs]) error {
			workChan <- struct{}{}
			return nil
		}))

		_, err := client.Insert(ctx, &InFuncWorkFuncArgs{}, nil)
		require.NoError(t, err)

		riverinternaltest.WaitOrTimeout(t, workChan)
	})
}
