package startstop

import (
	"context"
	"sync"
)

// BaseStartStop is a helper that can be embedded on a queue maintenance service
// and which will provide the basic necessities to safely implement the Service
// interface in a way that's not racy and can tolerate a number of edge cases.
// It's packaged separately so that it doesn't leak its internal variables into
// services that use it.
//
// Services should implement their own Start function which invokes StartInit
// first thing, return if told not to start, spawn a goroutine with their main
// run block otherwise, and make sure to defer a close on the stop channel
// returned by StartInit within that goroutine.
//
// A Stop implementation is provided automatically and it's not necessary to
// override it.
type BaseStartStop struct {
	cancelFunc context.CancelFunc
	mu         sync.Mutex
	started    bool
	stopped    chan struct{}
}

// StartInit should be invoked at the beginning of a service's Start function.
// It returns a context for the service to use, a boolean indicating whether it
// should start (which will be false if the service is already started), and a
// stopped channel. Services should defer a close on the stop channel in their
// main run loop.
//
//	ctx, shouldStart, stopped := s.StartInit(ctx)
//	if !shouldStart {
//	    return nil
//	}
//
//	go func() {
//	     defer close(stopped)
//
//	     ...
func (s *BaseStartStop) StartInit(ctx context.Context) (context.Context, bool, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return ctx, false, nil
	}

	s.started = true
	s.stopped = make(chan struct{})
	ctx, s.cancelFunc = context.WithCancel(ctx)

	return ctx, true, s.stopped
}

// Stop is an automatically provided implementation for the maintenance Service
// interface's Stop.
func (s *BaseStartStop) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Tolerate being told to stop without having been started.
	if s.stopped == nil {
		return
	}

	s.cancelFunc()

	<-s.stopped
	s.started = false
	s.stopped = nil
}

// Stopped returns a channel that can be waited on for the service to be
// stopped. This function is only safe to invoke after successfully waiting on
// a service's Start.
func (s *BaseStartStop) Stopped() <-chan struct{} {
	return s.stopped
}
