package hooklookup

import (
	"sync"

	"github.com/riverqueue/river/rivertype"
)

//
// HookKind
//

type HookKind string

const (
	HookKindInsertBegin HookKind = "insert_begin"
	HookKindWorkBegin   HookKind = "work_begin"
	HookKindWorkEnd     HookKind = "work_end"
)

//
// HookLookupInterface
//

// HookLookupInterface is an interface to look up hooks by hook kind. It's
// commonly implemented by HookLookup, but may also be EmptyHookLookup as a
// memory allocation optimization for bundles where no hooks are present.
type HookLookupInterface interface {
	ByHookKind(kind HookKind) []rivertype.Hook
}

// NewHookLookup returns a new hook lookup interface based on the given hooks
// that satisfies HookLookupInterface. This is often hookLookup, but may be
// emptyHookLookup as an optimization for the common case of an empty hook
// bundle.
func NewHookLookup(hooks []rivertype.Hook) HookLookupInterface {
	if len(hooks) < 1 {
		return &emptyHookLookup{}
	}

	return &hookLookup{
		hooks:       hooks,
		hooksByKind: make(map[HookKind][]rivertype.Hook),
		mu:          &sync.RWMutex{},
	}
}

//
// hookLookup
//

// hookLookup looks up and caches hooks based on a HookKind, saving work when
// looking up hooks for specific operations, a common operation that gets
// repeated over and over again. This struct may be used as a lookup for
// globally installed hooks or hooks for specific job kinds through the use of
// JobHookLookup.
type hookLookup struct {
	hooks       []rivertype.Hook
	hooksByKind map[HookKind][]rivertype.Hook
	mu          *sync.RWMutex
}

func (c *hookLookup) ByHookKind(kind HookKind) []rivertype.Hook {
	c.mu.RLock()
	cache, ok := c.hooksByKind[kind]
	c.mu.RUnlock()
	if ok {
		return cache
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Even if this ends up being empty, make sure there's an entry for the next
	// time the cache gets invoked for this kind.
	c.hooksByKind[kind] = nil

	// Rely on exhaustlint to find any missing hook kinds here.
	switch kind {
	case HookKindInsertBegin:
		for _, hook := range c.hooks {
			if typedHook, ok := hook.(rivertype.HookInsertBegin); ok {
				c.hooksByKind[kind] = append(c.hooksByKind[kind], typedHook)
			}
		}
	case HookKindWorkBegin:
		for _, hook := range c.hooks {
			if typedHook, ok := hook.(rivertype.HookWorkBegin); ok {
				c.hooksByKind[kind] = append(c.hooksByKind[kind], typedHook)
			}
		}
	case HookKindWorkEnd:
		for _, hook := range c.hooks {
			if typedHook, ok := hook.(rivertype.HookWorkEnd); ok {
				c.hooksByKind[kind] = append(c.hooksByKind[kind], typedHook)
			}
		}
	}

	return c.hooksByKind[kind]
}

//
// emptyHookLookup
//

// emptyHookLookup is an empty version of HookLookup that's zero allocation. For
// most applications, most job args won't have hooks, so this prevents us from
// allocating dozens/hundreds of small HookLookup objects that go unused.
type emptyHookLookup struct{}

func (c *emptyHookLookup) ByHookKind(kind HookKind) []rivertype.Hook { return nil }

//
// JobHookLookup
//

type JobHookLookup struct {
	hookLookupByKind map[string]HookLookupInterface
	mu               sync.RWMutex
}

func NewJobHookLookup() *JobHookLookup {
	return &JobHookLookup{
		hookLookupByKind: make(map[string]HookLookupInterface),
	}
}

// ByJobArgs returns a HookLookupInterface by job args, which is a HookLookup if
// the job args had specific hooks (i.e. implements JobArgsWithHooks and returns
// a non-empty set of hooks), or an EmptyHashLookup otherwise.
func (c *JobHookLookup) ByJobArgs(args rivertype.JobArgs) HookLookupInterface {
	kind := args.Kind()

	c.mu.RLock()
	lookup, ok := c.hookLookupByKind[kind]
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

	lookup = NewHookLookup(hooks)
	c.hookLookupByKind[kind] = lookup
	return lookup
}

// Same as river.JobArgsWithHooks, but duplicated here so that can still live in
// the top level package.
type jobArgsWithHooks interface {
	Hooks() []rivertype.Hook
}
