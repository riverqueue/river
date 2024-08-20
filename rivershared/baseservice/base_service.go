// Package baseservice contains structs and initialization functions for
// "service-like" objects that provide commonly needed facilities so that they
// don't have to be redefined on every struct. The word "service" is used quite
// loosely here in that it may be applied to many long-lived object that aren't
// strictly services (e.g. adapters).
package baseservice

import (
	"log/slog"
	"math/rand"
	"reflect"
	"regexp"
	"time"

	"github.com/riverqueue/river/rivershared/util/randutil"
)

// Archetype contains the set of base service properties that are immutable, or
// otherwise safe for services to copy from another service. The struct is also
// embedded in BaseService, so these properties are available on services
// directly.
type Archetype struct {
	// Logger is a structured logger.
	Logger *slog.Logger

	// Rand is a random source safe for concurrent access and seeded with a
	// cryptographically random seed to ensure good distribution between nodes
	// and services. The random source itself is _not_ cryptographically secure,
	// and therefore should not be used anywhere security-related. This is a
	// deliberate choice because Go's non-crypto rand source is about twenty
	// times faster, and so far none of our uses of random require cryptographic
	// secure randomness.
	//
	// TODO: When we drop Go 1.21 support, drop this in favor of just using top
	// level rand functions which are already safe for concurrent use.
	Rand *rand.Rand

	// Time returns a time generator to get the current time in UTC. Normally
	// it's implemented as UnStubbableTimeGenerator which just calls through to
	// `time.Now().UTC()`, but is riverinternaltest.timeStub in tests to allow
	// the current time to be stubbed.  Services should try to use this function
	// instead of the vanilla ones from the `time` package for testing purposes.
	Time TimeGenerator
}

// NewArchetype returns a new archetype. This function is most suitable for
// non-test usage wherein nothing should be stubbed.
func NewArchetype(logger *slog.Logger) *Archetype {
	return &Archetype{
		Logger: logger,
		Rand:   randutil.NewCryptoSeededConcurrentSafeRand(),
		Time:   &UnStubbableTimeGenerator{},
	}
}

// BaseService is a struct that's meant to be embedded on "service-like" objects
// (e.g. client, producer, queue maintainer) and which provides a number of
// convenient properties that are widely needed so that they don't have to be
// defined on every individual service and can easily be copied from each other.
//
// An initial Archetype should be defined near the program's entrypoint
// (currently in Client), and then each service should invoke Init along with
// the archetype to initialize its own base service. This is often done in the
// service's constructor, but if it doesn't have one, it's the job of the caller
// which instantiates it to invoke Init.
type BaseService struct {
	Archetype

	// Name is a name of the service. It should generally be used to prefix all
	// log lines the service emits.
	Name string
}

func (s *BaseService) GetBaseService() *BaseService { return s }

// withBaseService is an interface to a struct that embeds BaseService. An
// implementation is provided automatically by BaseService, and it's largely
// meant for internal use.
type withBaseService interface {
	GetBaseService() *BaseService
}

// Init initializes a base service from an archetype. It returns the same
// service that was passed into it for convenience.
func Init[TService withBaseService](archetype *Archetype, service TService) TService {
	baseService := service.GetBaseService()

	baseService.Logger = archetype.Logger
	baseService.Name = simplifyLogName(reflect.TypeOf(service).Elem().Name())
	baseService.Rand = archetype.Rand
	baseService.Time = archetype.Time

	return service
}

// TimeGenerator generates a current time in UTC. In test environments it's
// implemented by riverinternaltest.timeStub which lets the current time be
// stubbed. Otherwise, it's implemented as UnStubbableTimeGenerator which
// doesn't allow stubbing.
type TimeGenerator interface {
	// NowUTC returns the current time. This may be a stubbed time if the time
	// has been actively stubbed in a test.
	NowUTC() time.Time

	// NowUTCOrNil returns if the currently stubbed time _if_ the current time
	// is stubbed, and returns nil otherwise. This is generally useful in cases
	// where a component may want to use a stubbed time if the time is stubbed,
	// but to fall back to a database time default otherwise.
	NowUTCOrNil() *time.Time

	// StubNowUTC stubs the current time. It will panic if invoked outside of
	// tests. Returns the same time passed as parameter for convenience.
	StubNowUTC(nowUTC time.Time) time.Time
}

// UnStubbableTimeGenerator is a TimeGenerator implementation that can't be
// stubbed. It's always the generator used outside of tests.
type UnStubbableTimeGenerator struct{}

func (g *UnStubbableTimeGenerator) NowUTC() time.Time       { return time.Now() }
func (g *UnStubbableTimeGenerator) NowUTCOrNil() *time.Time { return nil }
func (g *UnStubbableTimeGenerator) StubNowUTC(nowUTC time.Time) time.Time {
	panic("time not stubbable outside tests")
}

var stripGenericTypePathRE = regexp.MustCompile(`\[([\[\]\*]*).*/([^/]+)\]`)

// Simplies the name of a Go type that uses generics for cleaner logging output.
//
// So this:
//
//	QueryCacher[[]*github.com/riverqueue/riverui/internal/dbsqlc.JobCountByStateRow]
//
// Becomes this:
//
//	QueryCacher[[]*dbsqlc.JobCountByStateRow]
func simplifyLogName(name string) string {
	return stripGenericTypePathRE.ReplaceAllString(name, `[$1$2]`)
}
