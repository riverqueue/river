package rivercommon

import (
	"fmt"
	"os"
	"time"
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
}

const testSignalInternalChanSize = 50

// Init initializes the test signal for use. This should only ever be called
// from tests.
func (s *TestSignal[T]) Init() {
	s.internalChan = make(chan T, testSignalInternalChanSize)
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
		panic("test only signal channel is full")
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

	timeout := WaitTimeout()

	select {
	case value := <-s.internalChan:
		return value
	case <-time.After(timeout):
		panic(fmt.Sprintf("timed out waiting on test signal after %s", timeout))
	}
}

// WaitTimeout returns a duration broadly appropriate for waiting on an expected
// event in a test, and which is used for `TestSignal.WaitOrTimeout` and
// `riverinternaltest.WaitOrTimeout`. It's main purpose is to allow a little
// extra leeway in GitHub Actions where we occasionally seem to observe subpar
// performance which leads to timeouts and test intermittency, while still
// keeping a tight a timeout for local test runs where this is never a problem.
func WaitTimeout() time.Duration {
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		return 10 * time.Second
	}

	return 3 * time.Second
}
