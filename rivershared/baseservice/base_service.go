// Package baseservice contains structs and initialization functions for
// "service-like" objects that provide commonly needed facilities so that they
// don't have to be redefined on every struct. The word "service" is used quite
// loosely here in that it may be applied to many long-lived object that aren't
// strictly services (e.g. adapters).
package baseservice

import (
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/riverqueue/river/rivertype"
)

// Archetype contains the set of base service properties that are immutable, or
// otherwise safe for services to copy from another service. The struct is also
// embedded in BaseService, so these properties are available on services
// directly.
type Archetype struct {
	// Logger is a structured logger.
	Logger *slog.Logger

	// Time returns a time generator for in-process deadline/duration math and
	// optional stubbed wall-clock timestamps in tests.
	//
	// The production path intentionally uses `time.Now()` rather than
	// `time.Now().UTC()`: per the Go time package's monotonic clock semantics,
	// normalizing through `UTC()` strips the monotonic reading. Services should
	// use this clock for local timing math and normalize to UTC only at
	// database or serialization boundaries.
	Time TimeGeneratorWithStub
}

// NewArchetype returns a new archetype. This function is most suitable for
// non-test usage wherein nothing should be stubbed.
func NewArchetype(logger *slog.Logger) *Archetype {
	return &Archetype{
		Logger: logger,
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

// WithBaseService is an interface to a struct that embeds BaseService. An
// implementation is provided automatically by BaseService, and it's largely
// meant for internal use.
type WithBaseService interface {
	GetBaseService() *BaseService
}

// Init initializes a base service from an archetype. It returns the same
// service that was passed into it for convenience.
func Init[TService WithBaseService](archetype *Archetype, service TService) TService {
	var (
		baseService = service.GetBaseService()
		serviceType = reflect.TypeOf(service).Elem()
	)

	baseService.Logger = archetype.Logger
	baseService.Name = lastPkgPathSegmentIfNotRiver(serviceType.PkgPath()) + simplifyLogName(serviceType.Name())
	baseService.Time = archetype.Time

	return service
}

type TimeGeneratorWithStub interface {
	rivertype.TimeGenerator

	// StubNow stubs the current wall-clock time. It will panic if invoked
	// outside of tests. Returns the same time passed as parameter for
	// convenience.
	StubNow(now time.Time) time.Time
}

// TimeGeneratorWithStubWrapper provides a wrapper around TimeGenerator that
// implements missing TimeGeneratorWithStub functions. This is used so that we
// only need to expose the minimal TimeGenerator interface publicly, but can
// keep a stubbable version of widely available for internal use.
type TimeGeneratorWithStubWrapper struct {
	rivertype.TimeGenerator
}

func (g *TimeGeneratorWithStubWrapper) StubNow(now time.Time) time.Time {
	panic("time not stubbable outside tests")
}

// UnStubbableTimeGenerator is a TimeGenerator implementation that can't be
// stubbed. It's always the generator used outside of tests.
type UnStubbableTimeGenerator struct{}

// Now intentionally returns `time.Now()` without calling `.UTC()`. River uses
// this clock for in-process duration and deadline math, and Go strips the
// monotonic clock reading when changing a Time's location with methods like
// `UTC()`. Normalize at database or serialization boundaries instead.
func (g *UnStubbableTimeGenerator) Now() time.Time       { return time.Now() }
func (g *UnStubbableTimeGenerator) NowOrNil() *time.Time { return nil }

func (g *UnStubbableTimeGenerator) StubNow(now time.Time) time.Time {
	panic("time not stubbable outside tests")
}

// Takes a package path and extracts the last part of it to use in a service
// name for logging purposes. If the package is the top-level `river` returns an
// empty string so that top-level structs aren't prefixed but sub-packages
// structs are.
//
//   - github.com/riverqueue/river          -> ""
//   - github.com/riverqueue/river/riverlog -> "riverlog."
//   - github.com/riverqueue/riverui        -> "riverui."
//
// Helps produce log-friendly service names like `riverlog.Middleware`.
func lastPkgPathSegmentIfNotRiver(pkgPath string) string {
	lastSlashIndex := strings.LastIndex(pkgPath, "/")
	if lastSlashIndex == -1 {
		return ""
	}

	lastPart := pkgPath[lastSlashIndex+1:]
	if lastPart == "" || lastPart == "river" {
		return ""
	}

	return lastPart + "."
}

var stripGenericTypePathRE = regexp.MustCompile(`\[([\[\]\*]*).*/([^/]+)\]`)

// Simplifies the name of a Go type that uses generics for cleaner logging output.
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
