package river

import (
	"context"

	"github.com/riverqueue/river/rivertype"
)

type testErrorHandler struct {
	HandleErrorCalled bool
	HandleErrorFunc   func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult

	HandlePanicCalled bool
	HandlePanicFunc   func(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult
}

// Test handler with no-ops for both error handling functions.
func newTestErrorHandler() *testErrorHandler {
	return &testErrorHandler{
		HandleErrorFunc: func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult { return nil },
		HandlePanicFunc: func(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult {
			return nil
		},
	}
}

func (h *testErrorHandler) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult {
	h.HandleErrorCalled = true
	return h.HandleErrorFunc(ctx, job, err)
}

func (h *testErrorHandler) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult {
	h.HandlePanicCalled = true
	return h.HandlePanicFunc(ctx, job, panicVal, trace)
}
