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

	JobCompleteManyCalled          bool
	JobCompleteManyTxCalled        bool
	JobInsertCalled                bool
	JobInsertTxCalled              bool
	JobInsertManyCalled            bool
	JobInsertManyTxCalled          bool
	JobGetAvailableCalled          bool
	JobGetAvailableTxCalled        bool
	JobSetCancelledIfRunningCalled bool
	JobSetCompletedIfRunningCalled bool
	JobSetCompletedTxCalled        bool
	JobSetDiscardedIfRunningCalled bool
	JobSetErroredIfRunningCalled   bool
	JobSetSnoozedIfRunningCalled   bool
	LeadershipAttemptElectCalled   bool
	LeadershipResignedCalled       bool

	JobCompleteManyFunc          func(ctx context.Context, jobs ...dbadapter.JobToComplete) error
	JobCompleteManyTxFunc        func(ctx context.Context, tx pgx.Tx, jobs ...dbadapter.JobToComplete) error
	JobInsertFunc                func(ctx context.Context, params *dbadapter.JobInsertParams) (*dbadapter.JobInsertResult, error)
	JobInsertTxFunc              func(ctx context.Context, tx pgx.Tx, params *dbadapter.JobInsertParams) (*dbadapter.JobInsertResult, error)
	JobInsertManyFunc            func(ctx context.Context, params []*dbadapter.JobInsertParams) (int64, error)
	JobInsertManyTxFunc          func(ctx context.Context, tx pgx.Tx, params []*dbadapter.JobInsertParams) (int64, error)
	JobGetAvailableFunc          func(ctx context.Context, queueName string, limit int32) ([]*dbsqlc.RiverJob, error)
	JobGetAvailableTxFunc        func(ctx context.Context, tx pgx.Tx, queueName string, limit int32) ([]*dbsqlc.RiverJob, error)
	JobSetCancelledIfRunningFunc func(ctx context.Context, id int64, finalizedAt time.Time, err []byte) (*dbsqlc.RiverJob, error)
	JobSetCompletedIfRunningFunc func(ctx context.Context, job dbadapter.JobToComplete) (*dbsqlc.RiverJob, error)
	JobSetCompletedTxFunc        func(ctx context.Context, tx pgx.Tx, id int64, completedAt time.Time) (*dbsqlc.RiverJob, error)
	JobSetDiscardedIfRunningFunc func(ctx context.Context, id int64, finalizedAt time.Time, err []byte) (*dbsqlc.RiverJob, error)
	JobSetErroredIfRunningFunc   func(ctx context.Context, id int64, scheduledAt time.Time, err []byte) (*dbsqlc.RiverJob, error)
	JobSetSnoozedIfRunningFunc   func(ctx context.Context, id int64, scheduledAt time.Time) (*dbsqlc.RiverJob, error)
	LeadershipAttemptElectFunc   func(ctx context.Context) (bool, error)
	LeadershipResignFunc         func(ctx context.Context, name string, leaderID string) error
}

func (ta *TestAdapter) JobCompleteMany(ctx context.Context, jobs ...dbadapter.JobToComplete) error {
	ta.atomicSetBoolTrue(&ta.JobCompleteManyCalled)

	if ta.JobCompleteManyFunc != nil {
		return ta.JobCompleteManyFunc(ctx, jobs...)
	}

	return ta.fallthroughAdapter.JobCompleteMany(ctx, jobs...)
}

func (ta *TestAdapter) JobCompleteManyTx(ctx context.Context, tx pgx.Tx, jobs ...dbadapter.JobToComplete) error {
	ta.atomicSetBoolTrue(&ta.JobCompleteManyTxCalled)

	if ta.JobCompleteManyTxFunc != nil {
		return ta.JobCompleteManyTxFunc(ctx, tx, jobs...)
	}

	return ta.fallthroughAdapter.JobCompleteManyTx(ctx, tx, jobs...)
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

func (ta *TestAdapter) JobSetCancelledIfRunning(ctx context.Context, id int64, finalizedAt time.Time, err []byte) (*dbsqlc.RiverJob, error) {
	ta.atomicSetBoolTrue(&ta.JobSetCancelledIfRunningCalled)

	if ta.JobSetCancelledIfRunningFunc != nil {
		return ta.JobSetCancelledIfRunningFunc(ctx, id, finalizedAt, err)
	}

	return ta.fallthroughAdapter.JobSetCancelledIfRunning(ctx, id, finalizedAt, err)
}

func (ta *TestAdapter) JobSetCompletedIfRunning(ctx context.Context, job dbadapter.JobToComplete) (*dbsqlc.RiverJob, error) {
	ta.atomicSetBoolTrue(&ta.JobSetCompletedIfRunningCalled)

	if ta.JobSetCompletedIfRunningFunc != nil {
		return ta.JobSetCompletedIfRunningFunc(ctx, job)
	}

	return ta.fallthroughAdapter.JobSetCompletedIfRunning(ctx, job)
}

func (ta *TestAdapter) JobSetCompletedTx(ctx context.Context, tx pgx.Tx, id int64, completedAt time.Time) (*dbsqlc.RiverJob, error) {
	ta.atomicSetBoolTrue(&ta.JobSetCompletedTxCalled)

	if ta.JobSetCompletedTxFunc != nil {
		return ta.JobSetCompletedTxFunc(ctx, tx, id, completedAt)
	}

	return ta.fallthroughAdapter.JobSetCompletedTx(ctx, tx, id, completedAt)
}

func (ta *TestAdapter) JobSetDiscardedIfRunning(ctx context.Context, id int64, finalizedAt time.Time, err []byte) (*dbsqlc.RiverJob, error) {
	ta.atomicSetBoolTrue(&ta.JobSetDiscardedIfRunningCalled)

	if ta.JobSetDiscardedIfRunningFunc != nil {
		return ta.JobSetDiscardedIfRunningFunc(ctx, id, finalizedAt, err)
	}

	return ta.fallthroughAdapter.JobSetDiscardedIfRunning(ctx, id, finalizedAt, err)
}

func (ta *TestAdapter) JobSetErroredIfRunning(ctx context.Context, id int64, scheduledAt time.Time, err []byte) (*dbsqlc.RiverJob, error) {
	ta.atomicSetBoolTrue(&ta.JobSetErroredIfRunningCalled)

	if ta.JobSetErroredIfRunningFunc != nil {
		return ta.JobSetErroredIfRunningFunc(ctx, id, scheduledAt, err)
	}

	return ta.fallthroughAdapter.JobSetErroredIfRunning(ctx, id, scheduledAt, err)
}

func (ta *TestAdapter) JobSetSnoozedIfRunning(ctx context.Context, id int64, scheduledAt time.Time) (*dbsqlc.RiverJob, error) {
	ta.atomicSetBoolTrue(&ta.JobSetSnoozedIfRunningCalled)

	if ta.JobSetErroredIfRunningFunc != nil {
		return ta.JobSetSnoozedIfRunningFunc(ctx, id, scheduledAt)
	}

	return ta.fallthroughAdapter.JobSetSnoozedIfRunning(ctx, id, scheduledAt)
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
