package river_test

import (
	"fmt"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/rivercommon"
)

//
// This file used as a holding place for test helpers for examples so that the
// helpers aren't included in Godoc and keep each example more succinct.
//

// Wait on the given subscription channel for numJobs. Times out with a panic if
// jobs take too long to be received.
func waitForNJobs(subscribeChan <-chan *river.Event, numJobs int) {
	var (
		timeout  = rivercommon.WaitTimeout()
		deadline = time.Now().Add(timeout)
		events   = make([]*river.Event, 0, numJobs)
	)

	for {
		select {
		case event := <-subscribeChan:
			events = append(events, event)

			if len(events) >= numJobs {
				return
			}

		case <-time.After(time.Until(deadline)):
			panic(fmt.Sprintf("WaitOrTimeout timed out after waiting %s (received %d job(s), wanted %d)",
				timeout, len(events), numJobs))
		}
	}
}
