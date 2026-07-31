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

//
// PluginLookup
//

// PluginLookup looks up plugins by kind. Its zero value is an empty lookup.
type PluginLookup struct {
	hooks         []rivertype.Hook
	pluginsByKind map[PluginKind][]any
}

// NewPluginLookup returns a new plugin lookup based on the given plugins. Each
// input is considered for every hook and middleware kind it implements.
//
// The plugins parameter is []any rather than []rivertype.Plugin because the
// lookup may contain legacy hooks and middleware that don't implement
// rivertype.Plugin. Keeping their original concrete values avoids compatibility
// wrappers that would need to forward every operation-specific interface.
func NewPluginLookup(plugins []any) *PluginLookup {
	return newPluginLookup(plugins, plugins)
}

// NewPluginLookupFromConfig returns a plugin lookup from separately configured
// hooks, middleware, and plugins. Explicit plugins may participate as either
// hooks or middleware, while entries from the legacy Hooks and Middleware
// configuration fields participate only as the kind they were configured as.
// Base services embedded in any configured extension are initialized with
// archetype when it's non-nil.
func NewPluginLookupFromConfig(archetype *baseservice.Archetype, hooks []rivertype.Hook, middlewares []rivertype.Middleware, plugins []rivertype.Plugin) *PluginLookup {
	if archetype != nil {
		initBaseServices(archetype, hooks)
		initBaseServices(archetype, middlewares)
		initBaseServices(archetype, plugins)
	}

	pluginValues := toAnySlice(plugins)

	hookValues := make([]any, 0, len(plugins)+len(hooks))
	hookValues = append(hookValues, pluginValues...)
	hookValues = append(hookValues, toAnySlice(hooks)...)

	middlewareValues := make([]any, 0, len(plugins)+len(middlewares))
	middlewareValues = append(middlewareValues, pluginValues...)
	middlewareValues = append(middlewareValues, toAnySlice(middlewares)...)

	return newPluginLookup(hookValues, middlewareValues)
}

func (c *PluginLookup) ByKind(kind PluginKind) []any {
	return c.pluginsByKind[kind]
}

// Hooks returns all the hooks in the lookup in configuration order.
func (c *PluginLookup) Hooks() []rivertype.Hook {
	return c.hooks
}

func initBaseServices[T any](archetype *baseservice.Archetype, plugins []T) {
	for _, plugin := range plugins {
		if withBaseService, ok := any(plugin).(baseservice.WithBaseService); ok {
			baseservice.Init(archetype, withBaseService)
		}
	}
}

func newPluginLookup(hooks, middlewares []any) *PluginLookup {
	lookup := &PluginLookup{}
	if len(hooks) < 1 && len(middlewares) < 1 {
		return lookup
	}

	lookup.pluginsByKind = make(map[PluginKind][]any)

	for _, plugin := range hooks {
		if plugin == nil {
			continue
		}

		if hook, ok := plugin.(rivertype.Hook); ok {
			lookup.hooks = append(lookup.hooks, hook)
		}
		if _, ok := plugin.(rivertype.HookInsertBegin); ok {
			lookup.pluginsByKind[PluginKindHookInsertBegin] = append(lookup.pluginsByKind[PluginKindHookInsertBegin], plugin)
		}
		if _, ok := plugin.(rivertype.HookMetricEmit); ok {
			lookup.pluginsByKind[PluginKindHookMetricEmit] = append(lookup.pluginsByKind[PluginKindHookMetricEmit], plugin)
		}
		if _, ok := plugin.(rivertype.HookPeriodicJobsStart); ok {
			lookup.pluginsByKind[PluginKindHookPeriodicJobsStart] = append(lookup.pluginsByKind[PluginKindHookPeriodicJobsStart], plugin)
		}
		if _, ok := plugin.(rivertype.HookWorkBegin); ok {
			lookup.pluginsByKind[PluginKindHookWorkBegin] = append(lookup.pluginsByKind[PluginKindHookWorkBegin], plugin)
		}
		if _, ok := plugin.(rivertype.HookWorkEnd); ok {
			lookup.pluginsByKind[PluginKindHookWorkEnd] = append(lookup.pluginsByKind[PluginKindHookWorkEnd], plugin)
		}
	}

	for _, plugin := range middlewares {
		if plugin == nil {
			continue
		}

		if _, ok := plugin.(rivertype.JobInsertMiddleware); ok {
			lookup.pluginsByKind[PluginKindMiddlewareJobInsert] = append(lookup.pluginsByKind[PluginKindMiddlewareJobInsert], plugin)
		}
		if _, ok := plugin.(rivertype.WorkerMiddleware); ok {
			lookup.pluginsByKind[PluginKindMiddlewareWorker] = append(lookup.pluginsByKind[PluginKindMiddlewareWorker], plugin)
		}
	}

	return lookup
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
	archetype *baseservice.Archetype

	mu                 sync.RWMutex
	pluginLookupByKind map[string]*PluginLookup
}

func NewJobPluginLookup(archetype *baseservice.Archetype) *JobPluginLookup {
	return &JobPluginLookup{
		archetype:          archetype,
		pluginLookupByKind: make(map[string]*PluginLookup),
	}
}

// ByJobArgs returns a plugin lookup for the given job args.
func (c *JobPluginLookup) ByJobArgs(args rivertype.JobArgs) *PluginLookup {
	kind := args.Kind()

	c.mu.RLock()
	entry, ok := c.pluginLookupByKind[kind]
	c.mu.RUnlock()
	if ok {
		return entry
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.pluginLookupByKind[kind]; ok {
		return entry
	}

	var (
		hooks   []rivertype.Hook
		plugins []rivertype.Plugin
	)
	if argsWithHooks, ok := args.(jobArgsWithHooks); ok {
		hooks = argsWithHooks.Hooks()
	}
	if argsWithPlugins, ok := args.(jobArgsWithPlugins); ok {
		plugins = argsWithPlugins.Plugins()
	}

	entry = NewPluginLookupFromConfig(c.archetype, hooks, nil, plugins)
	c.pluginLookupByKind[kind] = entry
	return entry
}

// Same as river.JobArgsWithHooks, but duplicated here so that can still live in
// the top level package.
type jobArgsWithHooks interface {
	Hooks() []rivertype.Hook
}

// Same as river.JobArgsWithPlugins, but duplicated here so that can still live
// in the top level package.
type jobArgsWithPlugins interface {
	Plugins() []rivertype.Plugin
}
