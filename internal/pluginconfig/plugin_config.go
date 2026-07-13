package pluginconfig

import "github.com/riverqueue/river/rivertype"

// CombinedMiddleware combines middleware from the current and legacy
// configuration fields into one list. Explicit middleware is preserved first,
// followed by legacy job insert and worker middleware.
//
// The reason this exists is that River originally had a seprate configuration
// field for each type of middleware (JobInsertMiddleware, WorkerMiddleware)
// before it was unified to a combined Middleware field so that a new one
// wouldn't be needed every time a new type of middleware was added. Middleware
// then became Plugins. The JobInsertMiddleware and WorkerMiddleware fields have
// been deprecated for quite some time and we should consider just removing them
// to simplify this set up.
func CombinedMiddleware(middleware []rivertype.Middleware, jobInsertMiddleware []rivertype.JobInsertMiddleware, workerMiddleware []rivertype.WorkerMiddleware) []rivertype.Middleware {
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
