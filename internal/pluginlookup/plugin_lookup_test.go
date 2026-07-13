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

		require.Nil(t, pluginLookup.ByKind(PluginKindHookInsertBegin))
		require.Nil(t, pluginLookup.ByKind(PluginKindHookWorkBegin))
		require.Nil(t, pluginLookup.ByKind(PluginKindMiddlewareJobInsert))
		require.Nil(t, pluginLookup.ByKind(PluginKindMiddlewareWorker))
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

		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(PluginKindHookInsertBegin))
		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(PluginKindHookWorkBegin))
		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(PluginKindHookWorkEnd))
		require.Equal(t, []any{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(PluginKindHookInsertBegin))
		require.Equal(t, []any{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(PluginKindHookWorkBegin))
		require.Equal(t, []any{
			&testHookWorkEnd{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(PluginKindHookWorkEnd))

		require.Len(t, jobPluginLookup.pluginLookupByKind, 2)

		// Repeat lookups to make sure we get the same result.
		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(PluginKindHookInsertBegin))
		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(PluginKindHookWorkBegin))
		require.Nil(t, jobPluginLookup.ByJobArgs(&jobArgsNoHooks{}).ByKind(PluginKindHookWorkEnd))
		require.Equal(t, []any{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(PluginKindHookInsertBegin))
		require.Equal(t, []any{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(PluginKindHookWorkBegin))
		require.Equal(t, []any{
			&testHookWorkEnd{},
		}, jobPluginLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByKind(PluginKindHookWorkEnd))
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

func TestNormalizePlugins(t *testing.T) {
	t.Parallel()

	t.Run("DistinctZeroSizedPointersArePreserved", func(t *testing.T) {
		t.Parallel()

		plugins := NormalizePlugins(nil, nil, []rivertype.Plugin{
			&testZeroSizedHookMiddlewarePlugin{},
			&testZeroSizedHookMiddlewarePlugin{},
		})

		require.Len(t, plugins, 2)
	})

	t.Run("DuplicatesWithinAGroupArePreserved", func(t *testing.T) {
		t.Parallel()

		plugin := &testHookMiddlewarePlugin{}
		plugins := NormalizePlugins(nil, nil, []rivertype.Plugin{plugin, plugin})

		require.Equal(t, []any{plugin, plugin}, plugins)
	})

	t.Run("PluginsPrecedeHooksAndMiddleware", func(t *testing.T) {
		t.Parallel()

		hookPlugin := &testHookMiddlewarePlugin{}
		middlewarePlugin := &testHookMiddlewarePlugin{}
		plugin := &testHookMiddlewarePlugin{}

		plugins := NormalizePlugins(
			[]rivertype.Hook{hookPlugin},
			[]rivertype.Middleware{middlewarePlugin},
			[]rivertype.Plugin{plugin},
		)

		lookup, isPluginLookup := NewPluginLookup(plugins).(*pluginLookup)
		require.True(t, isPluginLookup)

		require.Equal(t, []any{
			plugin,
			hookPlugin,
			middlewarePlugin,
		}, lookup.ByKind(PluginKindHookInsertBegin))
		require.Equal(t, []any{
			plugin,
			hookPlugin,
			middlewarePlugin,
		}, lookup.ByKind(PluginKindMiddlewareJobInsert))
	})

	t.Run("SamePointerAcrossGroupsIsPreserved", func(t *testing.T) {
		t.Parallel()

		plugin := &testHookMiddlewarePlugin{}
		plugins := NormalizePlugins(
			[]rivertype.Hook{plugin},
			[]rivertype.Middleware{plugin},
			[]rivertype.Plugin{plugin},
		)

		require.Equal(t, []any{plugin, plugin, plugin}, plugins)
	})

	t.Run("SameZeroSizedPointerAcrossGroupsIsPreserved", func(t *testing.T) {
		t.Parallel()

		plugin := &testZeroSizedHookMiddlewarePlugin{}
		plugins := NormalizePlugins(
			[]rivertype.Hook{plugin},
			[]rivertype.Middleware{plugin},
			nil,
		)

		require.Equal(t, []any{plugin, plugin}, plugins)
	})
}

func TestPluginLookup(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*pluginLookup, *testBundle) { //nolint:unparam
		t.Helper()

		lookup, isPluginLookup := NewPluginLookup([]any{
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

		require.Equal(t, []any{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, pluginLookup.ByKind(PluginKindHookInsertBegin))
		require.Equal(t, []any{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, pluginLookup.ByKind(PluginKindHookWorkBegin))
		require.Equal(t, []any{
			&testHookWorkEnd{},
		}, pluginLookup.ByKind(PluginKindHookWorkEnd))

		require.Equal(t, []any{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareJobInsert{},
		}, pluginLookup.ByKind(PluginKindMiddlewareJobInsert))
		require.Equal(t, []any{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareWorker{},
		}, pluginLookup.ByKind(PluginKindMiddlewareWorker))

		require.Len(t, pluginLookup.pluginsByKind, 5)

		// Repeat lookups to make sure we get the same result.
		require.Equal(t, []any{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, pluginLookup.ByKind(PluginKindHookInsertBegin))
		require.Equal(t, []any{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, pluginLookup.ByKind(PluginKindHookWorkBegin))
		require.Equal(t, []any{
			&testHookWorkEnd{},
		}, pluginLookup.ByKind(PluginKindHookWorkEnd))
		require.Equal(t, []any{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareJobInsert{},
		}, pluginLookup.ByKind(PluginKindMiddlewareJobInsert))
		require.Equal(t, []any{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareWorker{},
		}, pluginLookup.ByKind(PluginKindMiddlewareWorker))
	})

	t.Run("Stress", func(t *testing.T) {
		t.Parallel()

		pluginLookup, _ := setup(t)

		var wg sync.WaitGroup

		parallelLookupLoop := func(kind PluginKind) {
			wg.Go(func() {
				for range 50 {
					pluginLookup.ByKind(kind)
				}
			})
		}

		parallelLookupLoop(PluginKindHookInsertBegin)
		parallelLookupLoop(PluginKindHookWorkBegin)
		parallelLookupLoop(PluginKindHookInsertBegin)
		parallelLookupLoop(PluginKindHookWorkBegin)

		wg.Wait()
	})

	t.Run("LooksUpLegacyHooksAndMiddleware", func(t *testing.T) {
		t.Parallel()

		legacyHook := &testLegacyHookInsertBegin{}
		legacyMiddleware := &testLegacyMiddlewareJobInsert{}

		plugins := NormalizePlugins(
			[]rivertype.Hook{legacyHook},
			[]rivertype.Middleware{legacyMiddleware},
			nil,
		)

		lookup, isPluginLookup := NewPluginLookup(plugins).(*pluginLookup)
		require.True(t, isPluginLookup)

		hookPlugins := lookup.ByKind(PluginKindHookInsertBegin)
		require.Len(t, hookPlugins, 1)

		hook, isHookInsertBegin := hookPlugins[0].(rivertype.HookInsertBegin)
		require.True(t, isHookInsertBegin)
		require.NoError(t, hook.InsertBegin(context.Background(), &rivertype.JobInsertParams{}))
		require.True(t, legacyHook.insertBeginCalled)

		middlewarePlugins := lookup.ByKind(PluginKindMiddlewareJobInsert)
		require.Len(t, middlewarePlugins, 1)

		middleware, isJobInsertMiddleware := middlewarePlugins[0].(rivertype.JobInsertMiddleware)
		require.True(t, isJobInsertMiddleware)
		_, err := middleware.InsertMany(context.Background(), []*rivertype.JobInsertParams{{}}, func(context.Context) ([]*rivertype.JobInsertResult, error) {
			legacyMiddleware.doInnerCalled = true
			return nil, nil
		})
		require.NoError(t, err)
		require.True(t, legacyMiddleware.doInnerCalled)
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
// testHookMiddlewarePlugin
//

var (
	_ rivertype.HookInsertBegin     = &testHookMiddlewarePlugin{}
	_ rivertype.JobInsertMiddleware = &testHookMiddlewarePlugin{}
)

type testHookMiddlewarePlugin struct {
	_ byte
}

func (t *testHookMiddlewarePlugin) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	return nil
}

func (t *testHookMiddlewarePlugin) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	return doInner(ctx)
}

func (t *testHookMiddlewarePlugin) IsHook() bool       { return true }
func (t *testHookMiddlewarePlugin) IsMiddleware() bool { return true }
func (t *testHookMiddlewarePlugin) IsPlugin() bool     { return true }

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

//
// testZeroSizedHookMiddlewarePlugin
//

var (
	_ rivertype.HookInsertBegin     = &testZeroSizedHookMiddlewarePlugin{}
	_ rivertype.JobInsertMiddleware = &testZeroSizedHookMiddlewarePlugin{}
	_ rivertype.Plugin              = &testZeroSizedHookMiddlewarePlugin{}
)

type testZeroSizedHookMiddlewarePlugin struct{}

func (t *testZeroSizedHookMiddlewarePlugin) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	return nil
}

func (t *testZeroSizedHookMiddlewarePlugin) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	return doInner(ctx)
}

func (t *testZeroSizedHookMiddlewarePlugin) IsHook() bool       { return true }
func (t *testZeroSizedHookMiddlewarePlugin) IsMiddleware() bool { return true }
func (t *testZeroSizedHookMiddlewarePlugin) IsPlugin() bool     { return true }

//
// testLegacyHookInsertBegin
//

var _ rivertype.HookInsertBegin = &testLegacyHookInsertBegin{}

type testLegacyHookInsertBegin struct {
	insertBeginCalled bool
}

func (t *testLegacyHookInsertBegin) IsHook() bool { return true }

func (t *testLegacyHookInsertBegin) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	t.insertBeginCalled = true
	return nil
}

//
// testLegacyMiddlewareJobInsert
//

var _ rivertype.JobInsertMiddleware = &testLegacyMiddlewareJobInsert{}

type testLegacyMiddlewareJobInsert struct {
	doInnerCalled bool
}

func (t *testLegacyMiddlewareJobInsert) IsMiddleware() bool { return true }

func (t *testLegacyMiddlewareJobInsert) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	return doInner(ctx)
}
