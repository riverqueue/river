package hooklookup

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestHookLookup(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*hookLookup, *testBundle) { //nolint:unparam
		t.Helper()

		return NewHookLookup([]rivertype.Hook{ //nolint:forcetypeassert
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
			&testHookWorkBegin{},
		}).(*hookLookup), &testBundle{}
	}

	t.Run("LooksUpHooks", func(t *testing.T) {
		t.Parallel()

		hookLookup, _ := setup(t)

		require.Equal(t, []rivertype.Hook{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, hookLookup.ByHookKind(HookKindInsertBegin))
		require.Equal(t, []rivertype.Hook{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, hookLookup.ByHookKind(HookKindWorkBegin))

		require.Len(t, hookLookup.hooksByKind, 2)

		// Repeat lookups to make sure we get the same result.
		require.Equal(t, []rivertype.Hook{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, hookLookup.ByHookKind(HookKindInsertBegin))
		require.Equal(t, []rivertype.Hook{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, hookLookup.ByHookKind(HookKindWorkBegin))
	})

	t.Run("Stress", func(t *testing.T) {
		t.Parallel()

		hookLookup, _ := setup(t)

		var wg sync.WaitGroup

		parallelLookupLoop := func(kind HookKind) {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for range 50 {
					hookLookup.ByHookKind(kind)
				}
			}()
		}

		parallelLookupLoop(HookKindInsertBegin)
		parallelLookupLoop(HookKindWorkBegin)
		parallelLookupLoop(HookKindInsertBegin)
		parallelLookupLoop(HookKindWorkBegin)

		wg.Wait()
	})
}

func TestEmptyHookLookup(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*emptyHookLookup, *testBundle) {
		t.Helper()

		return NewHookLookup(nil).(*emptyHookLookup), &testBundle{} //nolint:forcetypeassert
	}

	t.Run("AlwaysReturnsNil", func(t *testing.T) {
		t.Parallel()

		hookLookup, _ := setup(t)

		require.Nil(t, hookLookup.ByHookKind(HookKindInsertBegin))
		require.Nil(t, hookLookup.ByHookKind(HookKindWorkBegin))
	})
}

func TestJobHookLookup(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*JobHookLookup, *testBundle) { //nolint:unparam
		t.Helper()

		return NewJobHookLookup(), &testBundle{}
	}

	t.Run("LooksUpHooks", func(t *testing.T) {
		t.Parallel()

		jobHookLookup, _ := setup(t)

		require.Nil(t, jobHookLookup.ByJobArgs(&jobArgsNoHooks{}).ByHookKind(HookKindInsertBegin))
		require.Nil(t, jobHookLookup.ByJobArgs(&jobArgsNoHooks{}).ByHookKind(HookKindWorkBegin))
		require.Equal(t, []rivertype.Hook{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, jobHookLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByHookKind(HookKindInsertBegin))
		require.Equal(t, []rivertype.Hook{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, jobHookLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByHookKind(HookKindWorkBegin))

		require.Len(t, jobHookLookup.hookLookupByKind, 2)

		// Repeat lookups to make sure we get the same result.
		require.Nil(t, jobHookLookup.ByJobArgs(&jobArgsNoHooks{}).ByHookKind(HookKindInsertBegin))
		require.Nil(t, jobHookLookup.ByJobArgs(&jobArgsNoHooks{}).ByHookKind(HookKindWorkBegin))
		require.Equal(t, []rivertype.Hook{
			&testHookInsertAndWorkBegin{},
			&testHookInsertBegin{},
		}, jobHookLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByHookKind(HookKindInsertBegin))
		require.Equal(t, []rivertype.Hook{
			&testHookInsertAndWorkBegin{},
			&testHookWorkBegin{},
		}, jobHookLookup.ByJobArgs(&jobArgsWithCustomHooks{}).ByHookKind(HookKindWorkBegin))
	})

	t.Run("Stress", func(t *testing.T) {
		t.Parallel()

		jobHookLookup, _ := setup(t)

		var wg sync.WaitGroup

		parallelLookupLoop := func(args rivertype.JobArgs) {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for range 50 {
					jobHookLookup.ByJobArgs(args)
				}
			}()
		}

		parallelLookupLoop(&jobArgsNoHooks{})
		parallelLookupLoop(&jobArgsWithCustomHooks{})
		parallelLookupLoop(&jobArgsNoHooks{})
		parallelLookupLoop(&jobArgsWithCustomHooks{})

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

func (t *testHookInsertBegin) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	return nil
}

//
// testHookWorkBegin
//

var _ rivertype.HookWorkBegin = &testHookWorkBegin{}

type testHookWorkBegin struct{ rivertype.Hook }

func (t *testHookWorkBegin) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	return nil
}
