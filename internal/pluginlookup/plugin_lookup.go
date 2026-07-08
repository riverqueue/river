package pluginlookup

import (
	"sync"

	"github.com/riverqueue/river/rivertype"
)

//
// PluginKind
//

type PluginKind string

const (
	PluginKindInsertBegin       PluginKind = "insert_begin"
	PluginKindJobInsert         PluginKind = "job_insert"
	PluginKindPeriodicJobsStart PluginKind = "periodic_job_start"
	PluginKindWorkBegin         PluginKind = "work_begin"
	PluginKindWorkEnd           PluginKind = "work_end"
	PluginKindWorker            PluginKind = "worker"
)

type (
	HookKind       = PluginKind
	MiddlewareKind = PluginKind
)

const (
	HookKindInsertBegin       HookKind       = PluginKindInsertBegin
	HookKindPeriodicJobsStart HookKind       = PluginKindPeriodicJobsStart
	HookKindWorkBegin         HookKind       = PluginKindWorkBegin
	HookKindWorkEnd           HookKind       = PluginKindWorkEnd
	MiddlewareKindJobInsert   MiddlewareKind = PluginKindJobInsert
	MiddlewareKindWorker      MiddlewareKind = PluginKindWorker
)

//
// PluginLookupInterface
//

// PluginLookupInterface looks up plugins by kind. It's commonly implemented by
// PluginLookup, but may also be EmptyPluginLookup as a memory allocation
// optimization for bundles where no plugins are present.
type PluginLookupInterface interface {
	ByKind(kind PluginKind) []rivertype.Plugin
}

// NewPluginLookup returns a new plugin lookup interface based on the given
// plugins that satisfies PluginLookupInterface. This is often pluginLookup,
// but may be emptyPluginLookup as an optimization for the common case of an
// empty plugin bundle.
func NewPluginLookup(plugins []rivertype.Plugin) PluginLookupInterface {
	if len(plugins) < 1 {
		return &emptyPluginLookup{}
	}

	pluginsByKind := make(map[PluginKind][]rivertype.Plugin)

	for _, plugin := range plugins {
		if plugin == nil {
			continue
		}

		if _, ok := plugin.(rivertype.HookInsertBegin); ok {
			pluginsByKind[PluginKindInsertBegin] = append(pluginsByKind[PluginKindInsertBegin], plugin)
		}
		if _, ok := plugin.(rivertype.HookPeriodicJobsStart); ok {
			pluginsByKind[PluginKindPeriodicJobsStart] = append(pluginsByKind[PluginKindPeriodicJobsStart], plugin)
		}
		if _, ok := plugin.(rivertype.HookWorkBegin); ok {
			pluginsByKind[PluginKindWorkBegin] = append(pluginsByKind[PluginKindWorkBegin], plugin)
		}
		if _, ok := plugin.(rivertype.HookWorkEnd); ok {
			pluginsByKind[PluginKindWorkEnd] = append(pluginsByKind[PluginKindWorkEnd], plugin)
		}
		if _, ok := plugin.(rivertype.JobInsertMiddleware); ok {
			pluginsByKind[PluginKindJobInsert] = append(pluginsByKind[PluginKindJobInsert], plugin)
		}
		if _, ok := plugin.(rivertype.WorkerMiddleware); ok {
			pluginsByKind[PluginKindWorker] = append(pluginsByKind[PluginKindWorker], plugin)
		}
	}

	return &pluginLookup{pluginsByKind: pluginsByKind}
}

// NormalizePlugins converts hook, middleware, and plugin registrations into a
// single plugin slice. Hook and middleware registrations must already satisfy
// rivertype.Plugin to participate.
func NormalizePlugins(hooks []rivertype.Hook, middlewares []rivertype.Middleware, plugins []rivertype.Plugin) []rivertype.Plugin {
	normalizedPlugins := make([]rivertype.Plugin, 0, len(hooks)+len(middlewares)+len(plugins))

	for _, hook := range hooks {
		if plugin, ok := hook.(rivertype.Plugin); ok {
			normalizedPlugins = append(normalizedPlugins, plugin)
		}
	}
	for _, middleware := range middlewares {
		if plugin, ok := middleware.(rivertype.Plugin); ok {
			normalizedPlugins = append(normalizedPlugins, plugin)
		}
	}
	for _, plugin := range plugins {
		if plugin != nil {
			normalizedPlugins = append(normalizedPlugins, plugin)
		}
	}

	return normalizedPlugins
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
	pluginsByKind map[PluginKind][]rivertype.Plugin
}

func (c *pluginLookup) ByKind(kind PluginKind) []rivertype.Plugin {
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

func (c *emptyPluginLookup) ByKind(kind PluginKind) []rivertype.Plugin {
	return nil
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

	lookup = NewPluginLookup(NormalizePlugins(hooks, nil, nil))
	c.pluginLookupByKind[kind] = lookup
	return lookup
}

// Same as river.JobArgsWithHooks, but duplicated here so that can still live in
// the top level package.
type jobArgsWithHooks interface {
	Hooks() []rivertype.Hook
}
