package pluginconfig

import "github.com/riverqueue/river/rivertype"

// Hooks returns the effective hook list from configured hooks and middleware.
// Explicit hooks are preserved first, followed by middleware that also
// implement hooks.
func Hooks(hooks []rivertype.Hook, middleware []rivertype.Middleware) []rivertype.Hook {
	effectiveHooks := make([]rivertype.Hook, 0, len(hooks)+len(middleware))

	effectiveHooks = append(effectiveHooks, hooks...)

	for _, middlewareItem := range middleware {
		hook, ok := middlewareItem.(rivertype.Hook)
		if !ok {
			continue
		}

		effectiveHooks = append(effectiveHooks, hook)
	}

	return effectiveHooks
}

// Middleware returns the effective middleware list from configured hooks and
// middleware. Explicit middleware are preserved first, followed by hooks that
// also implement middleware.
func Middleware(hooks []rivertype.Hook, middleware []rivertype.Middleware) []rivertype.Middleware {
	effectiveMiddleware := make([]rivertype.Middleware, 0, len(hooks)+len(middleware))

	effectiveMiddleware = append(effectiveMiddleware, middleware...)

	for _, hook := range hooks {
		middlewareItem, ok := hook.(rivertype.Middleware)
		if !ok {
			continue
		}

		effectiveMiddleware = append(effectiveMiddleware, middlewareItem)
	}

	return effectiveMiddleware
}
