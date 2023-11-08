// Package baseservice contains structs and initialization functions for
// "service-like" objects that provide commonly needed facilities so that they
// don't have to be redefined on every struct. The word "service" is used quite
// loosely here in that it may be applied to many long-lived object that aren't
// strictly services (e.g. adapters).
package baseservice

import (
	"context"
	"log/slog"
	"math/rand"
	"reflect"
	"time"

	"github.com/riverqueue/river/internal/util/randutil"
)

// Archetype contains the set of base service properties that are immutable, or
// otherwise safe for services to copy from another service. The struct is also
// embedded in BaseService, so these properties are available on services
// directly.
type Archetype struct {
	// DisableSleep disables sleep in services as long as they're sleeping via
	// `BaseService`'s `CancellableSleep` functions. This is meant to provide a
	// convenient way to disable sleep in one place and which will automatically
	// propagate from a parent into its subcomponents as they initialize
	// themselves from its archetype. `riverinternaltest.BaseServiceArchetype`
	// returns an archetype with sleep disabled.
	DisableSleep bool

	// Logger is a structured logger.
	Logger *slog.Logger

	// TimeNowUTC returns the current time as UTC. Normally it's implemented as
	// a call to `time.Now().UTC()`, but may be overridden in tests for time
	// injection. Services should try to use this function instead of the
	// vanilla ones from the `time` package for testing purposes.
	TimeNowUTC func() time.Time
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

	// Rand is a random source safe for concurrent access and seeded with a
	// cryptographically random seed to ensure good distribution between nodes
	// and services. The random source itself is _not_ cryptographically secure,
	// and therefore should not be used anywhere security-related. This is a
	// deliberate choice because Go's non-crypto rand source is about twenty
	// times faster, and so far none of our uses of random require cryptographic
	// secure randomness.
	Rand *rand.Rand
}

// CancellableSleep sleeps for the given duration, but returns early if context
// has been cancelled. If the service's `DisableSleep` option is set (usually
// appropriate in tests), sleep is skipped automatically.
func (s *BaseService) CancellableSleep(ctx context.Context, sleepDuration time.Duration) {
	if s.DisableSleep {
		return
	}

	timer := time.NewTimer(sleepDuration)

	select {
	case <-ctx.Done():
		timer.Stop()
	case <-timer.C:
	}
}

// CancellableSleepRandomBetween sleeps for a random duration between the given
// bounds (max bound is exclusive), but returns early if context has been
// cancelled. If the service's `DisableSleep` option is set (usually appropriate
// in tests), sleep is skipped automatically.
func (s *BaseService) CancellableSleepRandomBetween(ctx context.Context, sleepDurationMin, sleepDurationMax time.Duration) {
	s.CancellableSleep(ctx, time.Duration(randutil.IntBetween(s.Rand, int(sleepDurationMin), int(sleepDurationMax))))
}

func (s *BaseService) GetBaseService() *BaseService {
	return s
}

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

	baseService.DisableSleep = archetype.DisableSleep
	baseService.Logger = archetype.Logger
	baseService.Name = reflect.TypeOf(service).Elem().Name()
	baseService.Rand = randutil.NewCryptoSeededConcurrentSafeRand()
	baseService.TimeNowUTC = archetype.TimeNowUTC

	return service
}
