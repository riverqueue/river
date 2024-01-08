package dbadaptertest

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/riverqueue/river/internal/dbadapter"
	"github.com/riverqueue/river/internal/dbsqlc"
)

// TestAdapter is an Adapter that allows any of its methods to be overridden,
// automatically falling back to the fallthroughAdapter if the method is not
// overridden.
type TestAdapter struct {
	fallthroughAdapter dbadapter.Adapter
	mu                 sync.Mutex

	JobCancelCalled              bool
	JobCancelTxCalled            bool
	JobInsertCalled              bool
	JobInsertTxCalled            bool
	JobInsertManyCalled          bool
	JobInsertManyTxCalled        bool
	JobGetAvailableCalled        bool
	JobGetAvailableTxCalled      bool
	JobSetStateIfRunningCalled   bool
	LeadershipAttemptElectCalled bool
	LeadershipResignedCalled     bool

	JobCancelFunc              func(ctx context.Context, id int64) (*dbsqlc.RiverJob, error)
	JobCancelTxFunc            func(ctx context.Context, tx pgx.Tx, id int64) (*dbsqlc.RiverJob, error)
	JobInsertFunc              func(ctx context.Context, params *dbadapter.JobInsertParams) (*dbadapter.JobInsertResult, error)
	JobInsertTxFunc            func(ctx context.Context, tx pgx.Tx, params *dbadapter.JobInsertParams) (*dbadapter.JobInsertResult, error)
	JobInsertManyFunc          func(ctx context.Context, params []*dbadapter.JobInsertParams) (int64, error)
	JobInsertManyTxFunc        func(ctx context.Context, tx pgx.Tx, params []*dbadapter.JobInsertParams) (int64, error)
	JobGetAvailableFunc        func(ctx context.Context, queueName string, limit int32) ([]*dbsqlc.RiverJob, error)
	JobGetAvailableTxFunc      func(ctx context.Context, tx pgx.Tx, queueName string, limit int32) ([]*dbsqlc.RiverJob, error)
	JobSetStateIfRunningFunc   func(ctx context.Context, params *dbadapter.JobSetStateIfRunningParams) (*dbsqlc.RiverJob, error)
	LeadershipAttemptElectFunc func(ctx context.Context) (bool, error)
	LeadershipResignFunc       func(ctx context.Context, name string, leaderID string) error
}

func (ta *TestAdapter) JobCancel(ctx context.Context, id int64) (*dbsqlc.RiverJob, error) {
	ta.atomicSetBoolTrue(&ta.JobCancelCalled)

	if ta.JobCancelFunc != nil {
		return ta.JobCancelFunc(ctx, id)
	}

	return ta.fallthroughAdapter.JobCancel(ctx, id)
}

func (ta *TestAdapter) JobCancelTx(ctx context.Context, tx pgx.Tx, id int64) (*dbsqlc.RiverJob, error) {
	ta.atomicSetBoolTrue(&ta.JobCancelTxCalled)

	if ta.JobCancelTxFunc != nil {
		return ta.JobCancelTxFunc(ctx, tx, id)
	}

	return ta.fallthroughAdapter.JobCancel(ctx, id)
}

func (ta *TestAdapter) JobInsert(ctx context.Context, params *dbadapter.JobInsertParams) (*dbadapter.JobInsertResult, error) {
	ta.atomicSetBoolTrue(&ta.JobInsertCalled)

	if ta.JobInsertFunc != nil {
		return ta.JobInsertFunc(ctx, params)
	}

	return ta.fallthroughAdapter.JobInsert(ctx, params)
}

func (ta *TestAdapter) JobInsertTx(ctx context.Context, tx pgx.Tx, params *dbadapter.JobInsertParams) (*dbadapter.JobInsertResult, error) {
	ta.atomicSetBoolTrue(&ta.JobInsertTxCalled)

	if ta.JobInsertTxFunc != nil {
		return ta.JobInsertTxFunc(ctx, tx, params)
	}

	return ta.fallthroughAdapter.JobInsertTx(ctx, tx, params)
}

func (ta *TestAdapter) JobInsertMany(ctx context.Context, params []*dbadapter.JobInsertParams) (int64, error) {
	ta.atomicSetBoolTrue(&ta.JobInsertManyCalled)

	if ta.JobInsertManyFunc != nil {
		return ta.JobInsertManyFunc(ctx, params)
	}

	return ta.fallthroughAdapter.JobInsertMany(ctx, params)
}

func (ta *TestAdapter) JobInsertManyTx(ctx context.Context, tx pgx.Tx, params []*dbadapter.JobInsertParams) (int64, error) {
	ta.atomicSetBoolTrue(&ta.JobInsertManyTxCalled)

	if ta.JobInsertManyTxFunc != nil {
		return ta.JobInsertManyTxFunc(ctx, tx, params)
	}

	return ta.fallthroughAdapter.JobInsertManyTx(ctx, tx, params)
}

func (ta *TestAdapter) JobGetAvailable(ctx context.Context, queueName string, limit int32) ([]*dbsqlc.RiverJob, error) {
	ta.atomicSetBoolTrue(&ta.JobGetAvailableCalled)

	if ta.JobGetAvailableFunc != nil {
		return ta.JobGetAvailableFunc(ctx, queueName, limit)
	}

	return ta.fallthroughAdapter.JobGetAvailable(ctx, queueName, limit)
}

func (ta *TestAdapter) JobGetAvailableTx(ctx context.Context, tx pgx.Tx, queueName string, limit int32) ([]*dbsqlc.RiverJob, error) {
	ta.atomicSetBoolTrue(&ta.JobGetAvailableTxCalled)

	if ta.JobGetAvailableTxFunc != nil {
		return ta.JobGetAvailableTxFunc(ctx, tx, queueName, limit)
	}

	return ta.fallthroughAdapter.JobGetAvailableTx(ctx, tx, queueName, limit)
}

func (ta *TestAdapter) JobSetStateIfRunning(ctx context.Context, params *dbadapter.JobSetStateIfRunningParams) (*dbsqlc.RiverJob, error) {
	ta.atomicSetBoolTrue(&ta.JobSetStateIfRunningCalled)

	if ta.JobSetStateIfRunningFunc != nil {
		return ta.JobSetStateIfRunningFunc(ctx, params)
	}

	return ta.fallthroughAdapter.JobSetStateIfRunning(ctx, params)
}

func (ta *TestAdapter) LeadershipAttemptElect(ctx context.Context, alreadyElected bool, name, leaderID string, ttl time.Duration) (bool, error) {
	ta.atomicSetBoolTrue(&ta.LeadershipAttemptElectCalled)

	if ta.LeadershipAttemptElectFunc != nil {
		return ta.LeadershipAttemptElectFunc(ctx)
	}

	return ta.fallthroughAdapter.LeadershipAttemptElect(ctx, alreadyElected, name, leaderID, ttl)
}

func (ta *TestAdapter) LeadershipResign(ctx context.Context, name, leaderID string) error {
	ta.atomicSetBoolTrue(&ta.LeadershipResignedCalled)

	if ta.LeadershipResignFunc != nil {
		return ta.LeadershipResignFunc(ctx, name, leaderID)
	}

	return ta.fallthroughAdapter.LeadershipResign(ctx, name, leaderID)
}

func (ta *TestAdapter) atomicSetBoolTrue(b *bool) {
	ta.mu.Lock()
	*b = true
	ta.mu.Unlock()
}
