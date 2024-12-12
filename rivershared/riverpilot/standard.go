package riverpilot

import (
	"context"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

type StandardPilot struct{}

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
	// Noop
}
