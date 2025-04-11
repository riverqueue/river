package riverlog_test

import (
	"fmt"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

//
// This file used as a holding place for test helpers for examples so that the
// helpers aren't included in Godoc and keep each example more succinct.
//

// Wait on the given subscription channel for numJobs. Times out with a panic if
// jobs take too long to be received.
func waitForNJobs(subscribeChan <-chan *river.Event, numJobs int) []*rivertype.JobRow {
	var (
		timeout  = riversharedtest.WaitTimeout()
		deadline = time.Now().Add(timeout)
		events   = make([]*river.Event, 0, numJobs)
	)

	for {
		select {
		case event := <-subscribeChan:
			events = append(events, event)

			if len(events) >= numJobs {
				return sliceutil.Map(events, func(e *river.Event) *rivertype.JobRow { return e.Job })
			}

		case <-time.After(time.Until(deadline)):
			panic(fmt.Sprintf("waitForNJobs timed out after waiting %s (received %d job(s), wanted %d)",
				timeout, len(events), numJobs))
		}
	}
}
