package pluginconfig

import "github.com/riverqueue/river/rivertype"

// Middleware combines middleware from the current and legacy configuration
// fields into one list. Explicit middleware is preserved first, followed by
// legacy job insert and worker middleware. A worker middleware already present
// in the job insert list is included only once.
func Middleware(middleware []rivertype.Middleware, jobInsertMiddleware []rivertype.JobInsertMiddleware, workerMiddleware []rivertype.WorkerMiddleware) []rivertype.Middleware {
	allMiddleware := make([]rivertype.Middleware, 0,
		len(middleware)+len(jobInsertMiddleware)+len(workerMiddleware))
	allMiddleware = append(allMiddleware, middleware...)

	for _, jobInsertMiddlewareItem := range jobInsertMiddleware {
		allMiddleware = append(allMiddleware, jobInsertMiddlewareItem)
	}

outerLoop:
	for _, workerMiddlewareItem := range workerMiddleware {
		if workerMiddlewareAsJobInsertMiddleware, ok := workerMiddlewareItem.(rivertype.JobInsertMiddleware); ok {
			for _, jobInsertMiddlewareItem := range jobInsertMiddleware {
				if workerMiddlewareAsJobInsertMiddleware == jobInsertMiddlewareItem {
					continue outerLoop
				}
			}
		}

		allMiddleware = append(allMiddleware, workerMiddlewareItem)
	}

	return allMiddleware
}
