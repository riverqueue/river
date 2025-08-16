package circuitbreaker

import (
	"time"

	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

// CircuitBreakerOptions are options for CircuitBreaker.
type CircuitBreakerOptions struct {
	// Limit is the maximum number of trips/actions allowed within Window
	// before the circuit breaker opens.
	Limit int

	// Window is the window of time during which Limit number of trips/actions
	// can occur before the circuit breaker opens. The window is sliding, and
	// actions are reaped as they fall outside of Window compared to the current
	// time.
	Window time.Duration
}

func (o *CircuitBreakerOptions) mustValidate() *CircuitBreakerOptions {
	if o.Limit < 1 {
		panic("CircuitBreakerOptions.Limit must be greater than 0")
	}
	if o.Window < 1 {
		panic("CircuitBreakerOptions.Window must be greater than 0")
	}
	return o
}

// CircuitBreaker is a basic implementation of the circuit breaker pattern. Trip
// is called a number of times until the circuit breaker reaches its defined
// limit inside its allowed window at which point the circuit breaker
// opens. Once open, this version of the circuit breaker does not close again
// and stays open indefinitely. The window of time in question is sliding, and
// actions are repeaed as they fall outside of it compared to the current time
// (and don't count towards the breaker's limit).
type CircuitBreaker struct {
	open          bool
	opts          *CircuitBreakerOptions
	timeGenerator rivertype.TimeGenerator
	trips         []time.Time
}

func NewCircuitBreaker(opts *CircuitBreakerOptions) *CircuitBreaker {
	return &CircuitBreaker{
		opts:          opts.mustValidate(),
		timeGenerator: &baseservice.UnStubbableTimeGenerator{},
	}
}

// Limit returns the configured limit of the circuit breaker.
func (b *CircuitBreaker) Limit() int {
	return b.opts.Limit
}

// Open returns true if the circuit breaker is open (i.e. is broken).
func (b *CircuitBreaker) Open() bool {
	return b.open
}

// ResetIfNotOpen resets the circuit breaker to its empty state if it's not
// already open. i.e. As if no calls to Trip have been invoked at all. If the
// circuit breaker is open, it has no effect.
//
// This may be useful for example in cases where we're trying to track a number
// of consecutive failures before deciding to open. In the case of a success, we
// want to indicate that the series of consecutive failures has ended, so we
// call ResetIfNotOpen to trip it.
//
// Returns true if the breaker was reset (i.e. it had not been open), and false
// otherwise.
func (b *CircuitBreaker) ResetIfNotOpen() bool {
	if !b.open {
		b.trips = nil
	}
	return !b.open
}

// Trip "trips" the circuit breaker by counting an action towards opening it. If
// the action causes the breaker to reach its limit within its window, the
// breaker opens and the function returns true. Subsequent calls to Trip after
// the breaker is open will also return true.
func (b *CircuitBreaker) Trip() bool {
	if b.open {
		return true
	}

	var (
		horizonIndex = -1
		now          = b.timeGenerator.NowUTC()
	)
	for i := len(b.trips) - 1; i >= 0; i-- {
		if b.trips[i].Before(now.Add(-b.opts.Window)) {
			horizonIndex = i
			break
		}
	}

	if horizonIndex != -1 {
		b.trips = b.trips[horizonIndex+1:]
	}

	b.trips = append(b.trips, now)
	if len(b.trips) >= b.opts.Limit {
		b.open = true
	}

	return b.open
}
