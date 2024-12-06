package riverpilot

import (
	"context"

	"github.com/google/uuid"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

// A Pilot bridges the gap between the River client and the driver, implementing
// higher level functionality on top of the driver's underlying queries. It
// tracks closely to the underlying driver's API, but may add additional
// functionality or logic wrapping the queries.
//
// This should be considered a River internal API and its stability is not
// guaranteed. DO NOT USE.
type Pilot interface {
	JobGetAvailable(
		ctx context.Context,
		exec riverdriver.Executor,
		state ProducerState,
		params *riverdriver.JobGetAvailableParams,
	) ([]*rivertype.JobRow, error)

	JobInsertMany(
		ctx context.Context,
		tx riverdriver.ExecutorTx,
		params []*riverdriver.JobInsertFastParams,
	) ([]*riverdriver.JobInsertFastResult, error)

	JobSetStateIfRunningMany(ctx context.Context, tx riverdriver.ExecutorTx, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error)

	PilotInit(archetype *baseservice.Archetype)

	// ProducerInit is called when a producer is started. It should return
	// a new state object that will be used to track the producer's state.
	ProducerInit(ctx context.Context, exec riverdriver.Executor, clientID string, producerID uuid.UUID, queue string) (ProducerState, error)

	ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error

	ProducerShutdown(ctx context.Context, exec riverdriver.Executor, producerID uuid.UUID, state ProducerState) error
}

type ProducerState interface {
	JobFinish(job *rivertype.JobRow)
}
