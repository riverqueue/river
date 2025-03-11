package middlewarelookup

import (
	"sync"

	"github.com/riverqueue/river/rivertype"
)

//
// MiddlewareKind
//

type MiddlewareKind string

const (
	MiddlewareKindJobInsert MiddlewareKind = "job_insert"
	MiddlewareKindWorker    MiddlewareKind = "worker"
)

//
// MiddlewareLookupInterface
//

// MiddlewareLookupInterface is an interface to look up middlewares by
// middleware kind. It's commonly implemented by MiddlewareLookup, but may also
// be EmptyMiddlewareLookup as a memory allocation optimization for bundles
// where no middlewares are present.
type MiddlewareLookupInterface interface {
	ByMiddlewareKind(kind MiddlewareKind) []rivertype.Middleware
}

// NewMiddlewareLookup returns a new middleware lookup interface based on the given middlewares
// that satisfies MiddlewareLookupInterface. This is often middlewareLookup, but may be
// emptyMiddlewareLookup as an optimization for the common case of an empty middleware
// bundle.
func NewMiddlewareLookup(middlewares []rivertype.Middleware) MiddlewareLookupInterface {
	if len(middlewares) < 1 {
		return &emptyMiddlewareLookup{}
	}

	return &middlewareLookup{
		middlewares:       middlewares,
		middlewaresByKind: make(map[MiddlewareKind][]rivertype.Middleware),
		mu:                &sync.RWMutex{},
	}
}

//
// middlewareLookup
//

// middlewareLookup looks up and caches middlewares based on a MiddlewareKind, saving work when
// looking up middlewares for specific operations, a common operation that gets
// repeated over and over again. This struct may be used as a lookup for
// globally installed middlewares or middlewares for specific job kinds through the use of
// JobMiddlewareLookup.
type middlewareLookup struct {
	middlewares       []rivertype.Middleware
	middlewaresByKind map[MiddlewareKind][]rivertype.Middleware
	mu                *sync.RWMutex
}

func (c *middlewareLookup) ByMiddlewareKind(kind MiddlewareKind) []rivertype.Middleware {
	c.mu.RLock()
	cache, ok := c.middlewaresByKind[kind]
	c.mu.RUnlock()
	if ok {
		return cache
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Even if this ends up being empty, make sure there's an entry for the next
	// time the cache gets invoked for this kind.
	c.middlewaresByKind[kind] = nil

	// Rely on exhaustlint to find any missing middleware kinds here.
	switch kind {
	case MiddlewareKindJobInsert:
		for _, middleware := range c.middlewares {
			if typedMiddleware, ok := middleware.(rivertype.JobInsertMiddleware); ok {
				c.middlewaresByKind[kind] = append(c.middlewaresByKind[kind], typedMiddleware)
			}
		}
	case MiddlewareKindWorker:
		for _, middleware := range c.middlewares {
			if typedMiddleware, ok := middleware.(rivertype.WorkerMiddleware); ok {
				c.middlewaresByKind[kind] = append(c.middlewaresByKind[kind], typedMiddleware)
			}
		}
	}

	return c.middlewaresByKind[kind]
}

//
// emptyMiddlewareLookup
//

// emptyMiddlewareLookup is an empty version of MiddlewareLookup that's zero allocation. For
// most applications, most job args won't have middlewares, so this prevents us from
// allocating dozens/hundreds of small MiddlewareLookup objects that go unused.
type emptyMiddlewareLookup struct{}

func (c *emptyMiddlewareLookup) ByMiddlewareKind(kind MiddlewareKind) []rivertype.Middleware {
	return nil
}
