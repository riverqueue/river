package pluginlookup

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestEmptyPluginLookup(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*emptyPluginLookup, *testBundle) {
		t.Helper()

		lookup, isEmptyLookup := NewPluginLookup(nil).(*emptyPluginLookup)
		require.True(t, isEmptyLookup)

		return lookup, &testBundle{}
	}

	t.Run("AlwaysReturnsNil", func(t *testing.T) {
		t.Parallel()

		pluginLookup, _ := setup(t)

		require.Nil(t, pluginLookup.ByKind(HookKindInsertBegin))
		require.Nil(t, pluginLookup.ByKind(HookKindWorkBegin))
		require.Nil(t, pluginLookup.ByKind(MiddlewareKindJobInsert))
		require.Nil(t, pluginLookup.ByKind(MiddlewareKindWorker))
	})
}

func TestJobPluginLookup(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*JobPluginLookup, *testBundle) { //nolint:unparam
		t.Helper()

		return NewJobPluginLookup(), &testBundle{}
	}

	t.Run("LooksUpHooks", func(t *testing.T) {
		t.Parallel()

		jobPluginLookup, _ := setup(t)

		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(HookKindInsertBegin))
		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(HookKindWorkBegin))
		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(HookKindWorkEnd))
		require.Equal(t, []rivertype.Plugin{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(HookKindInsertBegin))
		require.Equal(t, []rivertype.Plugin{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(HookKindWorkBegin))
		require.Equal(t, []rivertype.Plugin{
			&testHookWorkEnd{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(HookKindWorkEnd))

		require.Len(t, jobPluginLookup.pluginLookupByKind, 2)

		// Repeat lookups to make sure we get the same result.
		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(HookKindInsertBegin))
		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(HookKindWorkBegin))
		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(HookKindWorkEnd))
		require.Equal(t, []rivertype.Plugin{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(HookKindInsertBegin))
		require.Equal(t, []rivertype.Plugin{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(HookKindWorkBegin))
		require.Equal(t, []rivertype.Plugin{
			&testHookWorkEnd{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(HookKindWorkEnd))
	})

	t.Run("Stress", func(t *testing.T) {
		t.Parallel()

		jobPluginLookup, _ := setup(t)

		var wg sync.WaitGroup

		parallelLookupLoop := func(args rivertype.JobArgs) {
			wg.Go(func() {
				for range 50 {
					jobPluginLookup.ByJobArgs(args)
				}
			})
		}

		parallelLookupLoop(&jobArgsNoHooks{})
		parallelLookupLoop(&jobArgsWithCustomHooks{})
		parallelLookupLoop(&jobArgsNoHooks{})
		parallelLookupLoop(&jobArgsWithCustomHooks{})

		wg.Wait()
	})
}

func TestPluginLookup(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*pluginLookup, *testBundle) { //nolint:unparam
		t.Helper()

		lookup, isPluginLookup := NewPluginLookup([]rivertype.Plugin{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
			&testHookWorkBegin{},
			&testHookWorkEnd{},
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareJobInsert{},
			&testMiddlewareWorker{},
		}).(*pluginLookup)
		require.True(t, isPluginLookup)

		return lookup, &testBundle{}
	}

	t.Run("LooksUpPlugins", func(t *testing.T) {
		t.Parallel()

		pluginLookup, _ := setup(t)

		require.Equal(t, []rivertype.Plugin{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, pluginLookup.ByKind(HookKindInsertBegin))
		require.Equal(t, []rivertype.Plugin{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, pluginLookup.ByKind(HookKindWorkBegin))
		require.Equal(t, []rivertype.Plugin{
			&testHookWorkEnd{},
		}, pluginLookup.ByKind(HookKindWorkEnd))

		require.Equal(t, []rivertype.Plugin{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareJobInsert{},
		}, pluginLookup.ByKind(MiddlewareKindJobInsert))
		require.Equal(t, []rivertype.Plugin{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareWorker{},
		}, pluginLookup.ByKind(MiddlewareKindWorker))

		require.Len(t, pluginLookup.pluginsByKind, 5)

		// Repeat lookups to make sure we get the same result.
		require.Equal(t, []rivertype.Plugin{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, pluginLookup.ByKind(HookKindInsertBegin))
		require.Equal(t, []rivertype.Plugin{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, pluginLookup.ByKind(HookKindWorkBegin))
		require.Equal(t, []rivertype.Plugin{
			&testHookWorkEnd{},
		}, pluginLookup.ByKind(HookKindWorkEnd))
		require.Equal(t, []rivertype.Plugin{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareJobInsert{},
		}, pluginLookup.ByKind(MiddlewareKindJobInsert))
		require.Equal(t, []rivertype.Plugin{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareWorker{},
		}, pluginLookup.ByKind(MiddlewareKindWorker))
	})

	t.Run("Stress", func(t *testing.T) {
		t.Parallel()

		pluginLookup, _ := setup(t)

		var wg sync.WaitGroup

		parallelLookupLoop := func(kind HookKind) {
			wg.Go(func() {
				for range 50 {
					pluginLookup.ByKind(kind)
				}
			})
		}

		parallelLookupLoop(HookKindInsertBegin)
		parallelLookupLoop(HookKindWorkBegin)
		parallelLookupLoop(HookKindInsertBegin)
		parallelLookupLoop(HookKindWorkBegin)

		wg.Wait()
	})
}

//
// jobArgsNoHooks
//

var _ rivertype.JobArgs = &jobArgsNoHooks{}

type jobArgsNoHooks struct{}

func (jobArgsNoHooks) Kind() string { return "no_hooks" }

//
// jobArgsWithHooks
//

var (
	_ rivertype.JobArgs = &jobArgsWithCustomHooks{}
	_ jobArgsWithHooks  = &jobArgsWithCustomHooks{}
)

type jobArgsWithCustomHooks struct{}

func (jobArgsWithCustomHooks) Hooks() []rivertype.Hook {
	return []rivertype.Hook{
		&testHookInsertAndWorkBegin{},
		&testHookInsertBegin{},
		&testHookWorkBegin{},
		&testHookWorkEnd{},
	}
}

func (jobArgsWithCustomHooks) Kind() string { return "with_custom_hooks" }

//
// testHookInsertAndWorkBegin
//

var (
	_ rivertype.HookInsertBegin = &testHookInsertAndWorkBegin{}
	_ rivertype.HookWorkBegin   = &testHookInsertAndWorkBegin{}
)

type testHookInsertAndWorkBegin struct{ rivertype.Hook }

func (t *testHookInsertAndWorkBegin) IsPlugin() bool { return true }

func (t *testHookInsertAndWorkBegin) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	return nil
}

func (t *testHookInsertAndWorkBegin) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	return nil
}

//
// testHookInsertBegin
//

var _ rivertype.HookInsertBegin = &testHookInsertBegin{}

type testHookInsertBegin struct{ rivertype.Hook }

func (t *testHookInsertBegin) IsPlugin() bool { return true }

func (t *testHookInsertBegin) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	return nil
}

//
// testHookWorkBegin
//

var _ rivertype.HookWorkBegin = &testHookWorkBegin{}

type testHookWorkBegin struct{ rivertype.Hook }

func (t *testHookWorkBegin) IsPlugin() bool { return true }

func (t *testHookWorkBegin) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	return nil
}

//
// testHookWorkEnd
//

var _ rivertype.HookWorkEnd = &testHookWorkEnd{}

type testHookWorkEnd struct{ rivertype.Hook }

func (t *testHookWorkEnd) IsPlugin() bool { return true }

func (t *testHookWorkEnd) WorkEnd(ctx context.Context, job *rivertype.JobRow, err error) error {
	return nil
}

//
// testMiddlewareInsertAndWorkBegin
//

var (
	_ rivertype.JobInsertMiddleware = &testMiddlewareJobInsertAndWorker{}
	_ rivertype.WorkerMiddleware    = &testMiddlewareJobInsertAndWorker{}
)

type testMiddlewareJobInsertAndWorker struct{ rivertype.Middleware }

func (t *testMiddlewareJobInsertAndWorker) IsPlugin() bool { return true }

func (t *testMiddlewareJobInsertAndWorker) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	return doInner(ctx)
}

func (t *testMiddlewareJobInsertAndWorker) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) error {
	return doInner(ctx)
}

//
// testMiddlewareJobInsert
//

var _ rivertype.JobInsertMiddleware = &testMiddlewareJobInsert{}

type testMiddlewareJobInsert struct{ rivertype.Middleware }

func (t *testMiddlewareJobInsert) IsPlugin() bool { return true }

func (t *testMiddlewareJobInsert) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	return doInner(ctx)
}

//
// testMiddlewareWorker
//

var _ rivertype.WorkerMiddleware = &testMiddlewareWorker{}

type testMiddlewareWorker struct{ rivertype.Middleware }

func (t *testMiddlewareWorker) IsPlugin() bool { return true }

func (t *testMiddlewareWorker) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) error {
	return doInner(ctx)
}
