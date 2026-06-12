package pluginconfig

import "github.com/riverqueue/river/rivertype"

// Hooks returns the effective hook list from configured hooks, middleware, and
// plugins. Explicit hooks are preserved first, followed by middleware that also
// implement hooks, then plugins.
func Hooks(hooks []rivertype.Hook, middleware []rivertype.Middleware, plugins []rivertype.Plugin) []rivertype.Hook {
	effectiveHooks := make([]rivertype.Hook, 0, len(hooks)+len(middleware)+len(plugins))

	effectiveHooks = append(effectiveHooks, hooks...)

	for _, middlewareItem := range middleware {
		hook, ok := middlewareItem.(rivertype.Hook)
		if !ok {
			continue
		}

		effectiveHooks = append(effectiveHooks, hook)
	}

	for _, plugin := range plugins {
		effectiveHooks = append(effectiveHooks, plugin)
	}

	return effectiveHooks
}

// Middleware returns the effective middleware list from configured hooks,
// middleware, and plugins. Explicit middleware are preserved first, followed by
// hooks that also implement middleware, then plugins.
func Middleware(hooks []rivertype.Hook, middleware []rivertype.Middleware, plugins []rivertype.Plugin) []rivertype.Middleware {
	effectiveMiddleware := make([]rivertype.Middleware, 0, len(hooks)+len(middleware)+len(plugins))

	effectiveMiddleware = append(effectiveMiddleware, middleware...)

	for _, hook := range hooks {
		middlewareItem, ok := hook.(rivertype.Middleware)
		if !ok {
			continue
		}

		effectiveMiddleware = append(effectiveMiddleware, middlewareItem)
	}

	for _, plugin := range plugins {
		effectiveMiddleware = append(effectiveMiddleware, plugin)
	}

	return effectiveMiddleware
}
