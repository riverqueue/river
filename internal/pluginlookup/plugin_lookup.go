package pluginlookup

import (
	"sync"

	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

//
// PluginKind
//

type PluginKind string

const (
	PluginKindHookInsertBegin       PluginKind = "hook_insert_begin"
	PluginKindHookMetricEmit        PluginKind = "hook_metric_emit"
	PluginKindHookPeriodicJobsStart PluginKind = "hook_periodic_jobs_start"
	PluginKindHookWorkBegin         PluginKind = "hook_work_begin"
	PluginKindHookWorkEnd           PluginKind = "hook_work_end"
	PluginKindMiddlewareJobInsert   PluginKind = "middleware_job_insert"
	PluginKindMiddlewareWorker      PluginKind = "middleware_worker"
)

// InitBaseServices initializes base services embedded in plugins.
func InitBaseServices[T any](archetype *baseservice.Archetype, plugins []T) {
	for _, plugin := range plugins {
		if withBaseService, ok := any(plugin).(baseservice.WithBaseService); ok {
			baseservice.Init(archetype, withBaseService)
		}
	}
}

//
// PluginLookupInterface
//

// PluginLookupInterface looks up plugins by kind. It's commonly implemented by
// PluginLookup, but may also be EmptyPluginLookup as a memory allocation
// optimization for bundles where no plugins are present.
type PluginLookupInterface interface {
	ByKind(kind PluginKind) []any
}

// NewPluginLookup returns a new plugin lookup interface based on the given
// plugins that satisfies PluginLookupInterface. This is often pluginLookup,
// but may be emptyPluginLookup as an optimization for the common case of an
// empty plugin bundle. Each input is considered for every hook and middleware
// kind it implements.
//
// The plugins parameter is []any rather than []rivertype.Plugin because the
// lookup may contain legacy hooks and middleware that don't implement
// rivertype.Plugin. Keeping their original concrete values avoids compatibility
// wrappers that would need to forward every operation-specific interface.
func NewPluginLookup(plugins []any) PluginLookupInterface {
	return newPluginLookup(plugins, plugins)
}

// NewPluginLookupFromConfig returns a plugin lookup from separately configured
// hooks, middleware, and plugins. Explicit plugins may participate as either
// hooks or middleware, while entries from the legacy Hooks and Middleware
// configuration fields participate only as the kind they were configured as.
func NewPluginLookupFromConfig(hooks []rivertype.Hook, middlewares []rivertype.Middleware, plugins []rivertype.Plugin) PluginLookupInterface {
	pluginValues := toAnySlice(plugins)

	hookValues := make([]any, 0, len(plugins)+len(hooks))
	hookValues = append(hookValues, pluginValues...)
	hookValues = append(hookValues, toAnySlice(hooks)...)

	middlewareValues := make([]any, 0, len(plugins)+len(middlewares))
	middlewareValues = append(middlewareValues, pluginValues...)
	middlewareValues = append(middlewareValues, toAnySlice(middlewares)...)

	return newPluginLookup(hookValues, middlewareValues)
}

func newPluginLookup(hooks, middlewares []any) PluginLookupInterface {
	if len(hooks) < 1 && len(middlewares) < 1 {
		return &emptyPluginLookup{}
	}

	pluginsByKind := make(map[PluginKind][]any)

	for _, plugin := range hooks {
		if plugin == nil {
			continue
		}

		if _, ok := plugin.(rivertype.HookInsertBegin); ok {
			pluginsByKind[PluginKindHookInsertBegin] = append(pluginsByKind[PluginKindHookInsertBegin], plugin)
		}
		if _, ok := plugin.(rivertype.HookMetricEmit); ok {
			pluginsByKind[PluginKindHookMetricEmit] = append(pluginsByKind[PluginKindHookMetricEmit], plugin)
		}
		if _, ok := plugin.(rivertype.HookPeriodicJobsStart); ok {
			pluginsByKind[PluginKindHookPeriodicJobsStart] = append(pluginsByKind[PluginKindHookPeriodicJobsStart], plugin)
		}
		if _, ok := plugin.(rivertype.HookWorkBegin); ok {
			pluginsByKind[PluginKindHookWorkBegin] = append(pluginsByKind[PluginKindHookWorkBegin], plugin)
		}
		if _, ok := plugin.(rivertype.HookWorkEnd); ok {
			pluginsByKind[PluginKindHookWorkEnd] = append(pluginsByKind[PluginKindHookWorkEnd], plugin)
		}
	}

	for _, plugin := range middlewares {
		if plugin == nil {
			continue
		}

		if _, ok := plugin.(rivertype.JobInsertMiddleware); ok {
			pluginsByKind[PluginKindMiddlewareJobInsert] = append(pluginsByKind[PluginKindMiddlewareJobInsert], plugin)
		}
		if _, ok := plugin.(rivertype.WorkerMiddleware); ok {
			pluginsByKind[PluginKindMiddlewareWorker] = append(pluginsByKind[PluginKindMiddlewareWorker], plugin)
		}
	}

	return &pluginLookup{pluginsByKind: pluginsByKind}
}

//
// pluginLookup
//

// pluginLookup looks up and caches plugins based on their kind, saving work
// when looking up plugin bundles for specific operations, a common operation
// that gets repeated over and over again. This struct may be used as a lookup
// for globally installed plugins or plugins for specific job kinds through the
// use of JobPluginLookup.
type pluginLookup struct {
	pluginsByKind map[PluginKind][]any
}

func (c *pluginLookup) ByKind(kind PluginKind) []any {
	return c.pluginsByKind[kind]
}

//
// emptyPluginLookup
//

// emptyPluginLookup is an empty version of PluginLookup that's zero
// allocation. For most applications, most job args won't have plugins, so this
// prevents us from allocating dozens/hundreds of small PluginLookup objects
// that go unused.
type emptyPluginLookup struct{}

func (c *emptyPluginLookup) ByKind(kind PluginKind) []any {
	return nil
}

func toAnySlice[T any](values []T) []any {
	plugins := make([]any, 0, len(values))
	for _, value := range values {
		plugins = append(plugins, value)
	}
	return plugins
}

//
// JobPluginLookup
//

type JobPluginLookup struct {
	pluginLookupByKind map[string]PluginLookupInterface
	mu                 sync.RWMutex
}

func NewJobPluginLookup() *JobPluginLookup {
	return &JobPluginLookup{
		pluginLookupByKind: make(map[string]PluginLookupInterface),
	}
}

// ByJobArgs returns a PluginLookupInterface by job args, which is a
// PluginLookup if the job args had specific hooks (i.e. implements
// JobArgsWithHooks and returns a non-empty set of hooks), or an
// EmptyPluginLookup otherwise.
func (c *JobPluginLookup) ByJobArgs(args rivertype.JobArgs) PluginLookupInterface {
	kind := args.Kind()

	c.mu.RLock()
	lookup, ok := c.pluginLookupByKind[kind]
	c.mu.RUnlock()
	if ok {
		return lookup
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var hooks []rivertype.Hook
	if argsWithHooks, ok := args.(jobArgsWithHooks); ok {
		hooks = argsWithHooks.Hooks()
	}

	lookup = newPluginLookup(toAnySlice(hooks), nil)
	c.pluginLookupByKind[kind] = lookup
	return lookup
}

// Same as river.JobArgsWithHooks, but duplicated here so that can still live in
// the top level package.
type jobArgsWithHooks interface {
	Hooks() []rivertype.Hook
}
