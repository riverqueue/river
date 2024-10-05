package river

import "github.com/riverqueue/river/rivertype"

var (
	_ rivertype.JobInsertMiddleware = &JobInsertMiddlewareDefaults{}
	_ rivertype.WorkerMiddleware    = &WorkerMiddlewareDefaults{}
)
