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
		params *riverdriver.JobInsertFastManyParams,
	) ([]*riverdriver.JobInsertFastResult, error)

	JobSetStateIfRunningMany(ctx context.Context, tx riverdriver.ExecutorTx, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error)

	PilotInit(archetype *baseservice.Archetype)

	// ProducerInit is called when a producer is started. It should return the ID
	// of the new producer, a new state object that will be used to track the
	// producer's state, and an error if the producer could not be initialized.
	ProducerInit(ctx context.Context, exec riverdriver.Executor, params *ProducerInitParams) (int64, ProducerState, error)

	ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error

	ProducerShutdown(ctx context.Context, exec riverdriver.Executor, params *ProducerShutdownParams) error

	QueueMetadataChanged(ctx context.Context, exec riverdriver.Executor, params *QueueMetadataChangedParams) error
}

type ProducerState interface {
	JobFinish(job *rivertype.JobRow)
}

type ProducerInitParams struct {
	ClientID      string
	ProducerID    int64
	Queue         string
	QueueMetadata []byte
	Schema        string
}

type ProducerShutdownParams struct {
	ProducerID int64
	Schema     string
	State      ProducerState
}

type QueueMetadataChangedParams struct {
	Metadata []byte
	Schema   string
	State    ProducerState
}
