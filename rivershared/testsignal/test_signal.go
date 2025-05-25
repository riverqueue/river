package testsignal

import (
	"time"

	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

// TestSignalWaiter provides an interface for TestSignal which only exposes
// waiting on the signal. This is useful for minimizing functionality across
// package boundaries.
type TestSignalWaiter[T any] interface {
	WaitOrTimeout() T
}

// TestSignal is a channel wrapper designed to allow tests to wait on certain
// events (to test difficult concurrent conditions without intermittency) while
// also having minimal impact on the production code that calls into it.
//
// Its default value produces a state where its safe to call Signal to signal
// into it, but where doing so will have no effect. Entities that embed it
// should by convention provide a TestSignalsInit function that tests can invoke
// and which calls Init on all member test signals, after which it becomes
// possible for tests to WaitOrTimeout on them.
type TestSignal[T any] struct {
	internalChan chan T
	tb           testutil.TestingTB
}

const testSignalInternalChanSize = 50

// Init initializes the test signal for use. This should only ever be called
// from tests.
func (s *TestSignal[T]) Init(tb testutil.TestingTB) {
	s.internalChan = make(chan T, testSignalInternalChanSize)
	s.tb = tb
}

// RequireEmpty requires that the test signal be empty (i.e. have not received
// any values).
func (s *TestSignal[T]) RequireEmpty() {
	if s.internalChan == nil {
		panic("test only signal is not initialized; called outside of tests?")
	}

	select {
	case val := <-s.internalChan:
		s.tb.Errorf("test signal should be empty, but wasn't\ngot value: %v\n", val)
	default:
	}
}

// Signal signals the test signal. In production where the signal hasn't been
// initialized, this no ops harmlessly. In tests, the value is written to an
// internal asynchronous channel which can be waited with WaitOrTimeout.
func (s *TestSignal[T]) Signal(val T) {
	// Occurs in the case of a raw signal that hasn't been initialized (which is
	// what should always be happening outside of tests).
	if s.internalChan == nil {
		return
	}

	select { // never block on send
	case s.internalChan <- val:
	default:
		s.tb.Errorf("test only signal channel is full")
	}
}

// WaitC returns a channel on which a value from the test signal can be waited
// upon.
func (s TestSignal[T]) WaitC() <-chan T {
	if s.internalChan == nil {
		panic("test only signal is not initialized; called outside of tests?")
	}

	return s.internalChan
}

// WaitOrTimeout waits on the next value injected by Signal. This should only be
// used in tests, and can only be used if Init has been invoked on the test
// signal.
func (s *TestSignal[T]) WaitOrTimeout() T {
	if s.internalChan == nil {
		panic("test only signal is not initialized; called outside of tests?")
	}

	timeout := riversharedtest.WaitTimeout()

	select {
	case val := <-s.internalChan:
		return val
	case <-time.After(timeout):
		s.tb.Errorf("timed out waiting on test signal after %s", timeout)
	}

	var val T
	return val
}
