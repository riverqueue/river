package middlewarelookup

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestMiddlewareLookup(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*middlewareLookup, *testBundle) { //nolint:unparam
		t.Helper()

		return NewMiddlewareLookup([]rivertype.Middleware{ //nolint:forcetypeassert
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareJobInsert{},
			&testMiddlewareWorker{},
		}).(*middlewareLookup), &testBundle{}
	}

	t.Run("LooksUpMiddleware", func(t *testing.T) {
		t.Parallel()

		middlewareLookup, _ := setup(t)

		require.Equal(t, []rivertype.Middleware{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareJobInsert{},
		}, middlewareLookup.ByMiddlewareKind(MiddlewareKindJobInsert))
		require.Equal(t, []rivertype.Middleware{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareWorker{},
		}, middlewareLookup.ByMiddlewareKind(MiddlewareKindWorker))

		require.Len(t, middlewareLookup.middlewaresByKind, 2)

		// Repeat lookups to make sure we get the same result.
		require.Equal(t, []rivertype.Middleware{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareJobInsert{},
		}, middlewareLookup.ByMiddlewareKind(MiddlewareKindJobInsert))
		require.Equal(t, []rivertype.Middleware{
			&testMiddlewareJobInsertAndWorker{},
			&testMiddlewareWorker{},
		}, middlewareLookup.ByMiddlewareKind(MiddlewareKindWorker))
	})

	t.Run("Stress", func(t *testing.T) {
		t.Parallel()

		middlewareLookup, _ := setup(t)

		var wg sync.WaitGroup

		parallelLookupLoop := func(kind MiddlewareKind) {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for range 50 {
					middlewareLookup.ByMiddlewareKind(kind)
				}
			}()
		}

		parallelLookupLoop(MiddlewareKindJobInsert)
		parallelLookupLoop(MiddlewareKindWorker)
		parallelLookupLoop(MiddlewareKindJobInsert)
		parallelLookupLoop(MiddlewareKindWorker)

		wg.Wait()
	})
}

func TestEmptyMiddlewareLookup(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*emptyMiddlewareLookup, *testBundle) {
		t.Helper()

		return NewMiddlewareLookup(nil).(*emptyMiddlewareLookup), &testBundle{} //nolint:forcetypeassert
	}

	t.Run("AlwaysReturnsNil", func(t *testing.T) {
		t.Parallel()

		middlewareLookup, _ := setup(t)

		require.Nil(t, middlewareLookup.ByMiddlewareKind(MiddlewareKindJobInsert))
		require.Nil(t, middlewareLookup.ByMiddlewareKind(MiddlewareKindWorker))
	})
}

//
// testMiddlewareInsertAndWorkBegin
//

var (
	_ rivertype.JobInsertMiddleware = &testMiddlewareJobInsertAndWorker{}
	_ rivertype.WorkerMiddleware    = &testMiddlewareJobInsertAndWorker{}
)

type testMiddlewareJobInsertAndWorker struct{ rivertype.Middleware }

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

func (t *testMiddlewareJobInsert) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	return doInner(ctx)
}

//
// testMiddlewareWorker
//

var _ rivertype.WorkerMiddleware = &testMiddlewareWorker{}

type testMiddlewareWorker struct{ rivertype.Middleware }

func (t *testMiddlewareWorker) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) error {
	return doInner(ctx)
}
