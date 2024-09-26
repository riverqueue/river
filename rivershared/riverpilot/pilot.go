package riverpilot

import (
	"context"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

// A Pilot bridges the gap between the River client and the driver, implementing
// higher level functionality on top of the driver's underlying queries. It
// tracks closely to the underlying driver's API, but may add additional
// functionality or logic wrapping the queries.
type Pilot interface {
	JobInsertMany(
		ctx context.Context,
		tx riverdriver.ExecutorTx,
		params []*riverdriver.JobInsertFastParams,
	) ([]*riverdriver.JobInsertFastResult, error)

	JobSetStateIfRunningMany(ctx context.Context, tx riverdriver.ExecutorTx, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error)

	PilotInit(archetype *baseservice.Archetype)
}
