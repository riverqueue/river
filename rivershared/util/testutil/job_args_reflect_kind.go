package testutil

import "reflect"

// JobArgsReflectKind can be embedded on a struct to implement JobArgs such that
// the job's kind will be the name of TKind. Typically, for convenience TKind
// will be the same type that does the embedding. Use of JobArgsReflectKind may
// not be typical, but in combination with WorkFunc, it allows the entirety of a
// job args and worker pair to be implemented inside the body of a function.
//
//	type InFuncWorkFuncArgs struct {
//		testutil.JobArgsReflectKind[InFuncWorkFuncArgs]
//		Message `json:"message"`
//	}
//
//	AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[WorkFuncArgs]) error {
//		...
//
// Its major downside compared to a normal JobArgs implementation is that it's
// possible to easily break things accidentally by renaming its type, deploying,
// and then finding that the worker will no longer work any jobs that were
// inserted before the deploy. It also depends on reflection, which likely makes
// it marginally slower.
//
// We're not sure yet whether it's appropriate to expose this publicly, so for
// now we've localized it to the test suite only. When a test case needs a job
// type that won't be reused, it's preferable to make use of JobArgsReflectKind
// so the type doesn't pollute the global namespace.
type JobArgsReflectKind[TKind any] struct{}

func (a JobArgsReflectKind[TKind]) Kind() string { return reflect.TypeOf(a).Name() }
