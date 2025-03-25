package riverpilot

import (
	"context"

	"github.com/google/uuid"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

type StandardPilot struct{}

func (p *StandardPilot) JobGetAvailable(ctx context.Context, exec riverdriver.Executor, state ProducerState, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	return exec.JobGetAvailable(ctx, params)
}

func (p *StandardPilot) JobInsertMany(
	ctx context.Context,
	tx riverdriver.ExecutorTx,
	params []*riverdriver.JobInsertFastParams,
) ([]*riverdriver.JobInsertFastResult, error) {
	return tx.JobInsertFastMany(ctx, params)
}

func (p *StandardPilot) JobSetStateIfRunningMany(ctx context.Context, tx riverdriver.ExecutorTx, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	return tx.JobSetStateIfRunningMany(ctx, params)
}

func (p *StandardPilot) PilotInit(archetype *baseservice.Archetype) {
	// No-op
}

func (p *StandardPilot) ProducerInit(ctx context.Context, exec riverdriver.Executor, clientID string, producerID uuid.UUID, queue string) (ProducerState, error) {
	return &standardProducerState{}, nil
}

func (p *StandardPilot) ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error {
	return nil
}

func (p *StandardPilot) ProducerShutdown(ctx context.Context, exec riverdriver.Executor, producerID uuid.UUID, state ProducerState) error {
	return nil
}

func (p *StandardPilot) QueueMetadataChanged(ctx context.Context, exec riverdriver.Executor, state ProducerState, metadata []byte) error {
	return nil
}

type standardProducerState struct{}

func (s *standardProducerState) JobFinish(job *rivertype.JobRow) {
	// No-op
}
