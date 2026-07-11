package pluginlookup

import (
	"context"
	"reflect"
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
	PluginKindHookPeriodicJobsStart PluginKind = "hook_periodic_jobs_start"
	PluginKindHookWorkBegin         PluginKind = "hook_work_begin"
	PluginKindHookWorkEnd           PluginKind = "hook_work_end"
	PluginKindMiddlewareJobInsert   PluginKind = "middleware_job_insert"
	PluginKindMiddlewareWorker      PluginKind = "middleware_worker"
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

// InitBaseServices initializes base services embedded in plugins, including
// those hidden behind wrappers for legacy hooks and middleware.
func InitBaseServices(archetype *baseservice.Archetype, plugins []rivertype.Plugin) {
	for _, plugin := range plugins {
		if legacyPlugin, ok := plugin.(*legacyPlugin); ok {
			legacyPlugin.initBaseService(archetype)
			continue
		}

		if withBaseService, ok := plugin.(baseservice.WithBaseService); ok {
			baseservice.Init(archetype, withBaseService)
		}
	}
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

		extension := any(plugin)
		if legacyPlugin, ok := plugin.(*legacyPlugin); ok {
			extension = legacyPlugin.extension
		}

		if _, ok := extension.(rivertype.HookInsertBegin); ok {
			pluginsByKind[PluginKindHookInsertBegin] = append(pluginsByKind[PluginKindHookInsertBegin], plugin)
		}
		if _, ok := extension.(rivertype.HookPeriodicJobsStart); ok {
			pluginsByKind[PluginKindHookPeriodicJobsStart] = append(pluginsByKind[PluginKindHookPeriodicJobsStart], plugin)
		}
		if _, ok := extension.(rivertype.HookWorkBegin); ok {
			pluginsByKind[PluginKindHookWorkBegin] = append(pluginsByKind[PluginKindHookWorkBegin], plugin)
		}
		if _, ok := extension.(rivertype.HookWorkEnd); ok {
			pluginsByKind[PluginKindHookWorkEnd] = append(pluginsByKind[PluginKindHookWorkEnd], plugin)
		}
		if _, ok := extension.(rivertype.JobInsertMiddleware); ok {
			pluginsByKind[PluginKindMiddlewareJobInsert] = append(pluginsByKind[PluginKindMiddlewareJobInsert], plugin)
		}
		if _, ok := extension.(rivertype.WorkerMiddleware); ok {
			pluginsByKind[PluginKindMiddlewareWorker] = append(pluginsByKind[PluginKindMiddlewareWorker], plugin)
		}
	}

	return &pluginLookup{pluginsByKind: pluginsByKind}
}

// NormalizePlugins converts hook, middleware, and plugin registrations into a
// single plugin slice while preserving legacy hook and middleware registrations
// that don't yet opt into Plugin.
//
// Plugins passed explicitly are ordered before hooks and middleware, so
// Config.Plugins entries take precedence over plugins bridged from Config.Hooks
// or Config.Middleware. Relative order is preserved within each input slice.
// Repeated registrations of the same non-zero-sized pointer are collapsed so a
// legacy hybrid registered as both a hook and middleware runs only once for
// each operation. Registrations repeated within one input slice are preserved.
func NormalizePlugins(hooks []rivertype.Hook, middlewares []rivertype.Middleware, plugins []rivertype.Plugin) []rivertype.Plugin {
	var (
		normalizedPlugins = make([]rivertype.Plugin, 0, len(hooks)+len(middlewares)+len(plugins))
		seenPointers      = make(map[pluginPointerIdentity]struct{})
	)

	appendGroup := func(group []pluginRegistration) {
		groupPointers := make(map[pluginPointerIdentity]struct{})
		for _, registration := range group {
			if identity, ok := pluginPointerIdentityFor(registration.original); ok {
				if _, ok := seenPointers[identity]; ok {
					continue
				}
				groupPointers[identity] = struct{}{}
			}

			normalizedPlugins = append(normalizedPlugins, registration.plugin)
		}

		for identity := range groupPointers {
			seenPointers[identity] = struct{}{}
		}
	}

	pluginRegistrations := make([]pluginRegistration, 0, len(plugins))
	for _, plugin := range plugins {
		pluginRegistrations = append(pluginRegistrations, pluginRegistration{original: plugin, plugin: plugin})
	}
	appendGroup(pluginRegistrations)

	hookRegistrations := make([]pluginRegistration, 0, len(hooks))
	for _, hook := range hooks {
		if plugin, ok := hook.(rivertype.Plugin); ok {
			hookRegistrations = append(hookRegistrations, pluginRegistration{original: hook, plugin: plugin})
		} else {
			hookRegistrations = append(hookRegistrations, pluginRegistration{original: hook, plugin: newLegacyPlugin(hook)})
		}
	}
	appendGroup(hookRegistrations)

	middlewareRegistrations := make([]pluginRegistration, 0, len(middlewares))
	for _, middleware := range middlewares {
		if plugin, ok := middleware.(rivertype.Plugin); ok {
			middlewareRegistrations = append(middlewareRegistrations, pluginRegistration{original: middleware, plugin: plugin})
		} else {
			middlewareRegistrations = append(middlewareRegistrations, pluginRegistration{original: middleware, plugin: newLegacyPlugin(middleware)})
		}
	}
	appendGroup(middlewareRegistrations)

	return normalizedPlugins
}

// pluginPointerIdentity identifies a non-zero-sized pointer-backed extension so
// NormalizePlugins can collapse the same instance registered through multiple
// plugin, hook, or middleware config fields. Zero-sized pointers are excluded
// because Go allows distinct zero-sized values to have the same address.
type pluginPointerIdentity struct {
	pointer uintptr
	typeOf  reflect.Type
}

func pluginPointerIdentityFor(plugin any) (pluginPointerIdentity, bool) {
	value := reflect.ValueOf(plugin)
	if !value.IsValid() || value.Kind() != reflect.Ptr || value.Type().Elem().Size() == 0 {
		return pluginPointerIdentity{}, false
	}

	return pluginPointerIdentity{pointer: value.Pointer(), typeOf: value.Type()}, true
}

type pluginRegistration struct {
	original any
	plugin   rivertype.Plugin
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
// legacyPlugin
//

// legacyPlugin wraps a legacy hook or middleware so it can participate in
// plugin lookup. It forwards both hook and middleware capabilities so a legacy
// extension that implements both still works when registered through either
// legacy config field.
type legacyPlugin struct {
	extension any
}

func newLegacyPlugin(extension any) rivertype.Plugin {
	return &legacyPlugin{extension: extension}
}

func (p *legacyPlugin) initBaseService(archetype *baseservice.Archetype) {
	if withBaseService, ok := p.extension.(baseservice.WithBaseService); ok {
		baseservice.Init(archetype, withBaseService)
	}
}

func (p *legacyPlugin) IsHook() bool       { return true }
func (p *legacyPlugin) IsMiddleware() bool { return true }
func (p *legacyPlugin) IsPlugin() bool     { return true }

func (p *legacyPlugin) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	hook, ok := p.extension.(rivertype.HookInsertBegin)
	if !ok {
		return nil
	}
	return hook.InsertBegin(ctx, params)
}

func (p *legacyPlugin) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	middleware, ok := p.extension.(rivertype.JobInsertMiddleware)
	if !ok {
		return doInner(ctx)
	}
	return middleware.InsertMany(ctx, manyParams, doInner)
}

func (p *legacyPlugin) Start(ctx context.Context, params *rivertype.HookPeriodicJobsStartParams) error {
	hook, ok := p.extension.(rivertype.HookPeriodicJobsStart)
	if !ok {
		return nil
	}
	return hook.Start(ctx, params)
}

func (p *legacyPlugin) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) error {
	middleware, ok := p.extension.(rivertype.WorkerMiddleware)
	if !ok {
		return doInner(ctx)
	}
	return middleware.Work(ctx, job, doInner)
}

func (p *legacyPlugin) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	hook, ok := p.extension.(rivertype.HookWorkBegin)
	if !ok {
		return nil
	}
	return hook.WorkBegin(ctx, job)
}

func (p *legacyPlugin) WorkEnd(ctx context.Context, job *rivertype.JobRow, err error) error {
	hook, ok := p.extension.(rivertype.HookWorkEnd)
	if !ok {
		return err
	}
	return hook.WorkEnd(ctx, job, err)
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
