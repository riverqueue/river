package pluginconfig

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestCombinedMiddleware(t *testing.T) {
	t.Parallel()

	t.Run("CombinesMiddlewareInOrder", func(t *testing.T) {
		t.Parallel()

		var (
			middleware          = &testMiddleware{}
			jobInsertMiddleware = &testJobInsertMiddleware{}
			workerMiddleware    = &testWorkerMiddleware{}
		)

		require.Equal(t, []rivertype.Middleware{
			middleware,
			jobInsertMiddleware,
			workerMiddleware,
		}, CombinedMiddleware(
			[]rivertype.Middleware{middleware},
			[]rivertype.JobInsertMiddleware{jobInsertMiddleware},
			[]rivertype.WorkerMiddleware{workerMiddleware},
		))
	})

	t.Run("SameSpecificMiddlewareIncludedOnce", func(t *testing.T) {
		t.Parallel()

		middleware := &testJobInsertAndWorkerMiddleware{}

		require.Equal(t, []rivertype.Middleware{middleware}, CombinedMiddleware(
			nil,
			[]rivertype.JobInsertMiddleware{middleware},
			[]rivertype.WorkerMiddleware{middleware},
		))
	})
}

type testJobInsertAndWorkerMiddleware struct {
	rivertype.JobInsertMiddleware
	rivertype.WorkerMiddleware
}

func (*testJobInsertAndWorkerMiddleware) IsMiddleware() bool { return true }

type testJobInsertMiddleware struct {
	rivertype.JobInsertMiddleware
}

type testMiddleware struct{}

func (*testMiddleware) IsMiddleware() bool { return true }

type testWorkerMiddleware struct {
	rivertype.WorkerMiddleware
}
