package riverpilot

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivertype"
)

type StandardPilot struct {
	seq atomic.Int64
}

func (p *StandardPilot) JobCleanerQueuesExcluded() []string { return nil }

func (p *StandardPilot) JobGetAvailable(ctx context.Context, exec riverdriver.Executor, state ProducerState, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	if params.MaxToLock <= 0 {
		return nil, nil
	}

	// Set an outer context timeout on locking jobs, and where possible (i.e. in
	// Postgres, but not SQLite), set an inner `statement_timeout` inside a
	// transaction so the configuration isn't durable. The error from the
	// Postgres version will be better, so try to have that trigger first. It
	// also minimizes the chances of a successful operation that locks jobs but
	// then accidentally errors because it's run time was so close to the Go
	// timeout.
	const timeout = 30 * time.Second

	ctx, cancel := context.WithTimeoutCause(ctx, timeout, context.DeadlineExceeded)
	defer cancel()

	return dbutil.WithTxV(ctx, exec, func(ctx context.Context, execTx riverdriver.ExecutorTx) ([]*rivertype.JobRow, error) {
		if err := execTx.SetLocalStatementTimeout(ctx, timeout-1*time.Second); err != nil {
			return nil, fmt.Errorf("error setting statement timeout: %w", err)
		}
		return execTx.JobGetAvailable(ctx, params)
	})
}

func (p *StandardPilot) JobCancel(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	return exec.JobCancel(ctx, params)
}

func (p *StandardPilot) JobInsertMany(
	ctx context.Context,
	exec riverdriver.Executor,
	params *riverdriver.JobInsertFastManyParams,
) ([]*riverdriver.JobInsertFastResult, error) {
	return exec.JobInsertFastMany(ctx, params)
}

func (p *StandardPilot) JobRetry(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error) {
	return exec.JobRetry(ctx, params)
}

func (p *StandardPilot) JobSetStateIfRunningMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	return exec.JobSetStateIfRunningMany(ctx, params)
}

func (p *StandardPilot) PeriodicJobKeepAliveAndReap(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobKeepAliveAndReapParams) ([]*PeriodicJob, error) {
	return nil, nil
}

func (p *StandardPilot) PeriodicJobGetAll(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobGetAllParams) ([]*PeriodicJob, error) {
	return nil, nil
}

func (p *StandardPilot) PeriodicJobUpsertMany(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobUpsertManyParams) ([]*PeriodicJob, error) {
	return nil, nil
}

func (p *StandardPilot) PilotInit(archetype *baseservice.Archetype, params *PilotInitParams) {
	// No-op
}

func (p *StandardPilot) ProducerInit(ctx context.Context, exec riverdriver.Executor, params *ProducerInitParams) (int64, ProducerState, error) {
	id := p.seq.Add(1)
	return id, &standardProducerState{}, nil
}

func (p *StandardPilot) ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error {
	return nil
}

func (p *StandardPilot) ProducerShutdown(ctx context.Context, exec riverdriver.Executor, params *ProducerShutdownParams) error {
	return nil
}

func (p *StandardPilot) QueueMetadataChanged(ctx context.Context, exec riverdriver.Executor, params *QueueMetadataChangedParams) error {
	return nil
}

type standardProducerState struct{}

func (s *standardProducerState) JobFinish(job *rivertype.JobRow) {
	// No-op
}
