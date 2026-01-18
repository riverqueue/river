package riverpilot

import (
	"context"
	"time"

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
	PilotPeriodicJob

	JobCancel(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error)

	// JobCleanerQueuesExcluded returns queues that should be excluded from the
	// main River client's JobCleaner. If no queues should be omitted, this
	// function should return nil as opposed to an empty array. (Because the
	// underlying database query uses an `IS NULL` check, though this could
	// conceivably be changed.)
	JobCleanerQueuesExcluded() []string

	JobGetAvailable(
		ctx context.Context,
		exec riverdriver.Executor,
		state ProducerState,
		params *riverdriver.JobGetAvailableParams,
	) ([]*rivertype.JobRow, error)

	JobInsertMany(
		ctx context.Context,
		exec riverdriver.Executor,
		params *riverdriver.JobInsertFastManyParams,
	) ([]*riverdriver.JobInsertFastResult, error)

	JobRetry(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error)

	JobSetStateIfRunningMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error)

	PilotInit(archetype *baseservice.Archetype, params *PilotInitParams)

	// ProducerInit is called when a producer is started. It should return the ID
	// of the new producer, a new state object that will be used to track the
	// producer's state, and an error if the producer could not be initialized.
	ProducerInit(ctx context.Context, exec riverdriver.Executor, params *ProducerInitParams) (int64, ProducerState, error)

	ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error

	ProducerShutdown(ctx context.Context, exec riverdriver.Executor, params *ProducerShutdownParams) error

	QueueMetadataChanged(ctx context.Context, exec riverdriver.Executor, params *QueueMetadataChangedParams) error
}

// PilotInitParams are parameters for initializing a pilot.
//
// API is not stable. DO NOT USE.
type PilotInitParams struct {
	// Insert is the insert implementation from the main client. This is
	// used as a low-level insert that shouldn't be accessible via public API,
	// but should be accessible to deep integrations.
	Insert func(ctx context.Context, tx riverdriver.ExecutorTx, insertParams []*rivertype.JobInsertParams) ([]*rivertype.JobInsertResult, error)

	// NotifyNonTxJobInsert is a special function that should be invoked when a
	// client knows that a job has become available and the transaction that
	// committed it has finished so that it's possible for a producer to fetch
	// it. This is used in special cases like poll-only clients to improve
	// latency between job insert and when a job is worked.
	NotifyNonTxJobInsert func(ctx context.Context, res []*rivertype.JobInsertResult)

	// WorkerMetadata is metadata about registered workers as received from the
	// client's worker bundle. Only available when a client will work jobs (i.e.
	// has Workers configured), so while it's safe to assume the presence of
	// this value in places like maintenance services, it's not in all contexts.
	WorkerMetadata []*rivertype.WorkerMetadata
}

func (p *PilotInitParams) Validate() *PilotInitParams {
	if p.Insert == nil {
		panic("need PilotInitParams.Insert")
	}
	if p.NotifyNonTxJobInsert == nil {
		panic("need PilotInitParams.NotifyNonTxJobInsert ")
	}
	return p
}

// PilotPeriodicJob contains pilot functions related to periodic jobs. This is
// extracted as its own interface so there's less surface area to mock in places
// like the periodic job enqueuer where that's needed.
type PilotPeriodicJob interface {
	// PeriodicJobGetAll gets all currently known periodic jobs.
	//
	// API is not stable. DO NOT USE.
	PeriodicJobGetAll(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobGetAllParams) ([]*PeriodicJob, error)

	// PeriodicJobTouchMany updates the `updated_at` timestamp on many jobs at
	// once to keep them alive and reaps any jobs that haven't been seen in some
	// time.
	//
	// API is not stable. DO NOT USE.
	PeriodicJobKeepAliveAndReap(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobKeepAliveAndReapParams) ([]*PeriodicJob, error)

	// PeriodicJobUpsertMany upserts many periodic jobs.
	//
	// API is not stable. DO NOT USE.
	PeriodicJobUpsertMany(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobUpsertManyParams) ([]*PeriodicJob, error)
}

// PeriodicJob represents a durable periodic job.
//
// TODO: Get rid of this in favor of rivertype.PeriodicJob the next time we're
// making River <-> River Pro API contract changes.
type PeriodicJob struct {
	ID        string
	CreatedAt time.Time
	NextRunAt time.Time
	UpdatedAt time.Time
}

type PeriodicJobGetAllParams struct {
	Schema string
}

type PeriodicJobKeepAliveAndReapParams struct {
	ID     []string
	Schema string
}

type PeriodicJobUpsertManyParams struct {
	Jobs   []*PeriodicJobUpsertParams
	Schema string
}

type PeriodicJobUpsertParams struct {
	ID        string
	NextRunAt time.Time
	UpdatedAt time.Time
}

type ProducerState interface {
	JobFinish(job *rivertype.JobRow)
}

type ProducerInitParams struct {
	ClientID   string
	ProducerID int64
	Queue      string
	Schema     string
}

type ProducerShutdownParams struct {
	ProducerID int64
	Queue      string
	Schema     string
}

type QueueMetadataChangedParams struct {
	Queue    string
	Metadata []byte
}
