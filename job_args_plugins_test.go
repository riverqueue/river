package river

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

type jobArgsPluginsTestArgs func() []rivertype.Plugin

func (jobArgsPluginsTestArgs) Kind() string { return "job_args_plugins_test" }

func (jobArgsPluginsTestArgs) MarshalJSON() ([]byte, error) { return []byte("{}"), nil }

func (f jobArgsPluginsTestArgs) Plugins() []rivertype.Plugin { return f() }

func (jobArgsPluginsTestArgs) UnmarshalJSON([]byte) error { return nil }

type jobArgsPluginsTestPlugin struct {
	baseservice.BaseService
	PluginDefaults

	insertBeginCount     atomic.Int32
	insertManyCount      atomic.Int32
	insertManyParamCount atomic.Int32
	workBeginCount       atomic.Int32
	workMiddlewareCount  atomic.Int32
}

func (p *jobArgsPluginsTestPlugin) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	p.insertBeginCount.Add(1)
	return nil
}

func (p *jobArgsPluginsTestPlugin) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	p.insertManyCount.Add(1)
	p.insertManyParamCount.Store(int32(len(manyParams))) //nolint:gosec // test-only count will be small
	return doInner(ctx)
}

func (p *jobArgsPluginsTestPlugin) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) error {
	p.workMiddlewareCount.Add(1)
	return doInner(ctx)
}

func (p *jobArgsPluginsTestPlugin) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	p.workBeginCount.Add(1)
	return nil
}

var (
	_ JobArgsWithPlugins            = jobArgsPluginsTestArgs(nil)
	_ rivertype.HookInsertBegin     = &jobArgsPluginsTestPlugin{}
	_ rivertype.JobInsertMiddleware = &jobArgsPluginsTestPlugin{}
	_ rivertype.Plugin              = &jobArgsPluginsTestPlugin{}
	_ rivertype.WorkerMiddleware    = &jobArgsPluginsTestPlugin{}
)

func TestJobArgsWithPlugins(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)
	plugin := &jobArgsPluginsTestPlugin{}
	args := jobArgsPluginsTestArgs(func() []rivertype.Plugin { return []rivertype.Plugin{plugin} })

	config := newTestConfig(t, schema)
	AddWorkerArgs(config.Workers, args, WorkFunc(func(ctx context.Context, job *Job[jobArgsPluginsTestArgs]) error {
		return nil
	}))

	client, err := NewClient(driver, config)
	require.NoError(t, err)
	require.NotEmpty(t, plugin.Name)

	subscribeChan := subscribe(t, client)
	startClient(ctx, t, client)

	insertResults, err := client.InsertMany(ctx, []InsertManyParams{
		{Args: args},
		{Args: noOpArgs{}},
		{Args: args},
	})
	require.NoError(t, err)
	require.Len(t, insertResults, 3)

	events := riversharedtest.WaitOrTimeoutN(t, subscribeChan, 3)
	require.Equal(t, EventKindJobCompleted, events[0].Kind)
	require.Equal(t, EventKindJobCompleted, events[1].Kind)
	require.Equal(t, EventKindJobCompleted, events[2].Kind)
	require.Equal(t, int32(2), plugin.insertBeginCount.Load())
	require.Equal(t, int32(1), plugin.insertManyCount.Load())
	require.Equal(t, int32(3), plugin.insertManyParamCount.Load())
	require.Equal(t, int32(2), plugin.workBeginCount.Load())
	require.Equal(t, int32(2), plugin.workMiddlewareCount.Load())
}
