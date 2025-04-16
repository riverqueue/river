package riverpilot

import (
	"context"
	"sync/atomic"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

type StandardPilot struct {
	seq atomic.Int64
}

func (p *StandardPilot) JobGetAvailable(ctx context.Context, exec riverdriver.Executor, state ProducerState, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	if params.Max <= 0 {
		return nil, nil
	}
	return exec.JobGetAvailable(ctx, params)
}

func (p *StandardPilot) JobInsertMany(
	ctx context.Context,
	tx riverdriver.ExecutorTx,
	params *riverdriver.JobInsertFastManyParams,
) ([]*riverdriver.JobInsertFastResult, error) {
	return tx.JobInsertFastMany(ctx, params)
}

func (p *StandardPilot) JobSetStateIfRunningMany(ctx context.Context, tx riverdriver.ExecutorTx, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	return tx.JobSetStateIfRunningMany(ctx, params)
}

func (p *StandardPilot) PilotInit(archetype *baseservice.Archetype) {
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
