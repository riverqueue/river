// Package riverpgxv5 provides a River driver implementation for Pgx v5.
//
// This is currently the only supported driver for River and will therefore be
// used by all projects using River, but the code is organized this way so that
// other database packages can be supported in future River versions.
package riverpgxv5

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5/internal/dbsqlc"
	"github.com/riverqueue/river/rivertype"
)

// Driver is an implementation of riverdriver.Driver for Pgx v5.
type Driver struct {
	dbPool  *pgxpool.Pool
	queries *dbsqlc.Queries
}

// New returns a new Pgx v5 River driver for use with River.
//
// It takes a pgxpool.Pool to use for use with River. The pool should already be
// configured to use the schema specified in the client's Schema field. The pool
// must not be closed while associated River objects are running.
//
// The database pool may be nil. If it is, a client that it's sent into will not
// be able to start up (calls to Start will error) and the Insert and InsertMany
// functions will be disabled, but the transactional-variants InsertTx and
// InsertManyTx continue to function. This behavior may be particularly useful
// in testing so that inserts can be performed and verified on a test
// transaction that will be rolled back.
func New(dbPool *pgxpool.Pool) *Driver {
	return &Driver{dbPool: dbPool, queries: dbsqlc.New()}
}

func (d *Driver) GetExecutor() riverdriver.Executor { return &Executor{d.dbPool, dbsqlc.New()} }
func (d *Driver) GetListener() riverdriver.Listener { return &Listener{dbPool: d.dbPool} }
func (d *Driver) HasPool() bool                     { return d.dbPool != nil }
func (d *Driver) SupportsListener() bool            { return true }

func (d *Driver) UnwrapExecutor(tx pgx.Tx) riverdriver.ExecutorTx {
	return &ExecutorTx{Executor: Executor{tx, dbsqlc.New()}, tx: tx}
}

type Executor struct {
	dbtx interface {
		dbsqlc.DBTX
		Begin(ctx context.Context) (pgx.Tx, error)
	}
	queries *dbsqlc.Queries
}

func (e *Executor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	tx, err := e.dbtx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &ExecutorTx{Executor: Executor{tx, e.queries}, tx: tx}, nil
}

func (e *Executor) Exec(ctx context.Context, sql string) (struct{}, error) {
	_, err := e.dbtx.Exec(ctx, sql)
	return struct{}{}, interpretError(err)
}

func (e *Executor) JobCancel(ctx context.Context, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	cancelledAt, err := params.CancelAttemptedAt.MarshalJSON()
	if err != nil {
		return nil, err
	}

	job, err := e.queries.JobCancel(ctx, e.dbtx, &dbsqlc.JobCancelParams{
		ID:                params.ID,
		CancelAttemptedAt: cancelledAt,
		ControlTopic:      params.ControlTopic,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobCountByState(ctx context.Context, state rivertype.JobState) (int, error) {
	numJobs, err := e.queries.JobCountByState(ctx, e.dbtx, dbsqlc.RiverJobState(state))
	if err != nil {
		return 0, err
	}
	return int(numJobs), nil
}

func (e *Executor) JobDeleteBefore(ctx context.Context, params *riverdriver.JobDeleteBeforeParams) (int, error) {
	numDeleted, err := e.queries.JobDeleteBefore(ctx, e.dbtx, &dbsqlc.JobDeleteBeforeParams{
		CancelledFinalizedAtHorizon: params.CancelledFinalizedAtHorizon,
		CompletedFinalizedAtHorizon: params.CompletedFinalizedAtHorizon,
		DiscardedFinalizedAtHorizon: params.DiscardedFinalizedAtHorizon,
		Max:                         int64(params.Max),
	})
	return int(numDeleted), interpretError(err)
}

func (e *Executor) JobGetAvailable(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	jobs, err := e.queries.JobGetAvailable(ctx, e.dbtx, &dbsqlc.JobGetAvailableParams{
		AttemptedBy: params.AttemptedBy,
		Max:         int32(params.Max),
		Queue:       params.Queue,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetByID(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	job, err := e.queries.JobGetByID(ctx, e.dbtx, id)
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobGetByIDMany(ctx context.Context, id []int64) ([]*rivertype.JobRow, error) {
	jobs, err := e.queries.JobGetByIDMany(ctx, e.dbtx, id)
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetByKindAndUniqueProperties(ctx context.Context, params *riverdriver.JobGetByKindAndUniquePropertiesParams) (*rivertype.JobRow, error) {
	job, err := e.queries.JobGetByKindAndUniqueProperties(ctx, e.dbtx, (*dbsqlc.JobGetByKindAndUniquePropertiesParams)(params))
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobGetByKindMany(ctx context.Context, kind []string) ([]*rivertype.JobRow, error) {
	jobs, err := e.queries.JobGetByKindMany(ctx, e.dbtx, kind)
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetStuck(ctx context.Context, params *riverdriver.JobGetStuckParams) ([]*rivertype.JobRow, error) {
	jobs, err := e.queries.JobGetStuck(ctx, e.dbtx, &dbsqlc.JobGetStuckParams{Max: int32(params.Max), StuckHorizon: params.StuckHorizon})
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobInsertFast(ctx context.Context, params *riverdriver.JobInsertFastParams) (*rivertype.JobRow, error) {
	job, err := e.queries.JobInsertFast(ctx, e.dbtx, &dbsqlc.JobInsertFastParams{
		Args:        params.EncodedArgs,
		Kind:        params.Kind,
		MaxAttempts: int16(min(params.MaxAttempts, math.MaxInt16)),
		Metadata:    params.Metadata,
		Priority:    int16(min(params.Priority, math.MaxInt16)),
		Queue:       params.Queue,
		ScheduledAt: params.ScheduledAt,
		State:       dbsqlc.RiverJobState(params.State),
		Tags:        params.Tags,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobInsertFastMany(ctx context.Context, params []*riverdriver.JobInsertFastParams) (int, error) {
	insertJobsParams := make([]*dbsqlc.JobInsertFastManyCopyFromParams, len(params))
	now := time.Now()

	for i := 0; i < len(params); i++ {
		params := params[i]

		metadata := params.Metadata
		if metadata == nil {
			metadata = []byte("{}")
		}

		scheduledAt := now
		if params.ScheduledAt != nil {
			scheduledAt = *params.ScheduledAt
		}

		tags := params.Tags
		if tags == nil {
			tags = []string{}
		}

		insertJobsParams[i] = &dbsqlc.JobInsertFastManyCopyFromParams{
			Args:        params.EncodedArgs,
			Kind:        params.Kind,
			MaxAttempts: int16(min(params.MaxAttempts, math.MaxInt16)),
			Metadata:    metadata,
			Priority:    int16(min(params.Priority, math.MaxInt16)),
			Queue:       params.Queue,
			ScheduledAt: scheduledAt,
			State:       dbsqlc.RiverJobState(params.State),
			Tags:        tags,
		}
	}

	numInserted, err := e.queries.JobInsertFastManyCopyFrom(ctx, e.dbtx, insertJobsParams)
	if err != nil {
		return 0, interpretError(err)
	}

	return int(numInserted), nil
}

func (e *Executor) JobInsertFull(ctx context.Context, params *riverdriver.JobInsertFullParams) (*rivertype.JobRow, error) {
	job, err := e.queries.JobInsertFull(ctx, e.dbtx, &dbsqlc.JobInsertFullParams{
		Attempt:     int16(params.Attempt),
		AttemptedAt: params.AttemptedAt,
		Args:        params.EncodedArgs,
		CreatedAt:   params.CreatedAt,
		Errors:      params.Errors,
		FinalizedAt: params.FinalizedAt,
		Kind:        params.Kind,
		MaxAttempts: int16(min(params.MaxAttempts, math.MaxInt16)),
		Metadata:    params.Metadata,
		Priority:    int16(min(params.Priority, math.MaxInt16)),
		Queue:       params.Queue,
		ScheduledAt: params.ScheduledAt,
		State:       dbsqlc.RiverJobState(params.State),
		Tags:        params.Tags,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobList(ctx context.Context, query string, namedArgs map[string]any) ([]*rivertype.JobRow, error) {
	rows, err := e.dbtx.Query(ctx, query, pgx.NamedArgs(namedArgs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []*dbsqlc.RiverJob
	for rows.Next() {
		var i dbsqlc.RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			&i.AttemptedBy,
			&i.CreatedAt,
			&i.Errors,
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			&i.Tags,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, interpretError(err)
	}

	return mapSliceError(items, jobRowFromInternal)
}

func (e *Executor) JobListFields() string {
	return "id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags"
}

func (e *Executor) JobRescueMany(ctx context.Context, params *riverdriver.JobRescueManyParams) (*struct{}, error) {
	err := e.queries.JobRescueMany(ctx, e.dbtx, (*dbsqlc.JobRescueManyParams)(params))
	if err != nil {
		return nil, interpretError(err)
	}
	return &struct{}{}, nil
}

func (e *Executor) JobRetry(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	job, err := e.queries.JobRetry(ctx, e.dbtx, id)
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobSchedule(ctx context.Context, params *riverdriver.JobScheduleParams) ([]*rivertype.JobRow, error) {
	jobs, err := e.queries.JobSchedule(ctx, e.dbtx, &dbsqlc.JobScheduleParams{
		Max: int64(params.Max),
		Now: params.Now,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobSetCompleteIfRunningMany(ctx context.Context, params *riverdriver.JobSetCompleteIfRunningManyParams) ([]*rivertype.JobRow, error) {
	jobs, err := e.queries.JobSetCompleteIfRunningMany(ctx, e.dbtx, &dbsqlc.JobSetCompleteIfRunningManyParams{
		ID:          params.ID,
		FinalizedAt: params.FinalizedAt,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobSetStateIfRunning(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
	var maxAttempts int16
	if params.MaxAttempts != nil {
		maxAttempts = int16(*params.MaxAttempts)
	}

	job, err := e.queries.JobSetStateIfRunning(ctx, e.dbtx, &dbsqlc.JobSetStateIfRunningParams{
		ID:                  params.ID,
		ErrorDoUpdate:       params.ErrData != nil,
		Error:               params.ErrData,
		FinalizedAtDoUpdate: params.FinalizedAt != nil,
		FinalizedAt:         params.FinalizedAt,
		MaxAttemptsUpdate:   params.MaxAttempts != nil,
		MaxAttempts:         maxAttempts,
		ScheduledAtDoUpdate: params.ScheduledAt != nil,
		ScheduledAt:         params.ScheduledAt,
		State:               dbsqlc.RiverJobState(params.State),
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobUpdate(ctx context.Context, params *riverdriver.JobUpdateParams) (*rivertype.JobRow, error) {
	job, err := e.queries.JobUpdate(ctx, e.dbtx, &dbsqlc.JobUpdateParams{
		ID:                  params.ID,
		AttemptedAtDoUpdate: params.AttemptedAtDoUpdate,
		AttemptedAt:         params.AttemptedAt,
		AttemptDoUpdate:     params.AttemptDoUpdate,
		Attempt:             int16(params.Attempt),
		ErrorsDoUpdate:      params.ErrorsDoUpdate,
		Errors:              params.Errors,
		FinalizedAtDoUpdate: params.FinalizedAtDoUpdate,
		FinalizedAt:         params.FinalizedAt,
		StateDoUpdate:       params.StateDoUpdate,
		State:               dbsqlc.RiverJobState(params.State),
	})
	if err != nil {
		return nil, interpretError(err)
	}

	return jobRowFromInternal(job)
}

func (e *Executor) LeaderAttemptElect(ctx context.Context, params *riverdriver.LeaderElectParams) (bool, error) {
	numElectionsWon, err := e.queries.LeaderAttemptElect(ctx, e.dbtx, &dbsqlc.LeaderAttemptElectParams{
		LeaderID: params.LeaderID,
		TTL:      params.TTL,
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numElectionsWon > 0, nil
}

func (e *Executor) LeaderAttemptReelect(ctx context.Context, params *riverdriver.LeaderElectParams) (bool, error) {
	numElectionsWon, err := e.queries.LeaderAttemptReelect(ctx, e.dbtx, &dbsqlc.LeaderAttemptReelectParams{
		LeaderID: params.LeaderID,
		TTL:      params.TTL,
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numElectionsWon > 0, nil
}

func (e *Executor) LeaderDeleteExpired(ctx context.Context) (int, error) {
	numDeleted, err := e.queries.LeaderDeleteExpired(ctx, e.dbtx)
	if err != nil {
		return 0, interpretError(err)
	}
	return int(numDeleted), nil
}

func (e *Executor) LeaderGetElectedLeader(ctx context.Context) (*riverdriver.Leader, error) {
	leader, err := e.queries.LeaderGetElectedLeader(ctx, e.dbtx)
	if err != nil {
		return nil, interpretError(err)
	}
	return leaderFromInternal(leader), nil
}

func (e *Executor) LeaderInsert(ctx context.Context, params *riverdriver.LeaderInsertParams) (*riverdriver.Leader, error) {
	leader, err := e.queries.LeaderInsert(ctx, e.dbtx, &dbsqlc.LeaderInsertParams{
		ElectedAt: params.ElectedAt,
		ExpiresAt: params.ExpiresAt,
		LeaderID:  params.LeaderID,
		TTL:       params.TTL,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return leaderFromInternal(leader), nil
}

func (e *Executor) LeaderResign(ctx context.Context, params *riverdriver.LeaderResignParams) (bool, error) {
	numResigned, err := e.queries.LeaderResign(ctx, e.dbtx, &dbsqlc.LeaderResignParams{
		LeaderID:        params.LeaderID,
		LeadershipTopic: params.LeadershipTopic,
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numResigned > 0, nil
}

func (e *Executor) MigrationDeleteByVersionMany(ctx context.Context, versions []int) ([]*riverdriver.Migration, error) {
	migrations, err := e.queries.RiverMigrationDeleteByVersionMany(ctx, e.dbtx,
		mapSlice(versions, func(v int) int64 { return int64(v) }))
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSlice(migrations, migrationFromInternal), nil
}

func (e *Executor) MigrationGetAll(ctx context.Context) ([]*riverdriver.Migration, error) {
	migrations, err := e.queries.RiverMigrationGetAll(ctx, e.dbtx)
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSlice(migrations, migrationFromInternal), nil
}

func (e *Executor) MigrationInsertMany(ctx context.Context, versions []int) ([]*riverdriver.Migration, error) {
	migrations, err := e.queries.RiverMigrationInsertMany(ctx, e.dbtx,
		mapSlice(versions, func(v int) int64 { return int64(v) }))
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSlice(migrations, migrationFromInternal), nil
}

func (e *Executor) NotifyMany(ctx context.Context, params *riverdriver.NotifyManyParams) error {
	return e.queries.PGNotifyMany(ctx, e.dbtx, &dbsqlc.PGNotifyManyParams{
		Payload: params.Payload,
		Topic:   params.Topic,
	})
}

func (e *Executor) PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error) {
	err := e.queries.PGAdvisoryXactLock(ctx, e.dbtx, key)
	return &struct{}{}, interpretError(err)
}

func (e *Executor) QueueCreateOrSetUpdatedAt(ctx context.Context, params *riverdriver.QueueCreateOrSetUpdatedAtParams) (*rivertype.Queue, error) {
	queue, err := e.queries.QueueCreateOrSetUpdatedAt(ctx, e.dbtx, &dbsqlc.QueueCreateOrSetUpdatedAtParams{
		Metadata:  params.Metadata,
		Name:      params.Name,
		PausedAt:  params.PausedAt,
		UpdatedAt: params.UpdatedAt,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return queueFromInternal(queue), nil
}

func (e *Executor) QueueDeleteExpired(ctx context.Context, params *riverdriver.QueueDeleteExpiredParams) ([]string, error) {
	queues, err := e.queries.QueueDeleteExpired(ctx, e.dbtx, &dbsqlc.QueueDeleteExpiredParams{
		Max:              int64(params.Max),
		UpdatedAtHorizon: params.UpdatedAtHorizon,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	queueNames := make([]string, len(queues))
	for i, q := range queues {
		queueNames[i] = q.Name
	}
	return queueNames, nil
}

func (e *Executor) QueueGet(ctx context.Context, name string) (*rivertype.Queue, error) {
	queue, err := e.queries.QueueGet(ctx, e.dbtx, name)
	if err != nil {
		return nil, interpretError(err)
	}
	return queueFromInternal(queue), nil
}

func (e *Executor) QueueList(ctx context.Context, limit int) ([]*rivertype.Queue, error) {
	internalQueues, err := e.queries.QueueList(ctx, e.dbtx, int32(limit))
	if err != nil {
		return nil, interpretError(err)
	}
	queues := make([]*rivertype.Queue, len(internalQueues))
	for i, q := range internalQueues {
		queues[i] = queueFromInternal(q)
	}
	return queues, nil
}

func (e *Executor) QueuePause(ctx context.Context, name string) error {
	res, err := e.queries.QueuePause(ctx, e.dbtx, name)
	if err != nil {
		return interpretError(err)
	}
	if res.RowsAffected() == 0 {
		return rivertype.ErrNotFound
	}
	return nil
}

func (e *Executor) QueueResume(ctx context.Context, name string) error {
	res, err := e.queries.QueueResume(ctx, e.dbtx, name)
	if err != nil {
		return interpretError(err)
	}
	if res.RowsAffected() == 0 {
		return rivertype.ErrNotFound
	}
	return nil
}

func (e *Executor) TableExists(ctx context.Context, tableName string) (bool, error) {
	exists, err := e.queries.TableExists(ctx, e.dbtx, tableName)
	return exists, interpretError(err)
}

type ExecutorTx struct {
	Executor
	tx pgx.Tx
}

func (t *ExecutorTx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *ExecutorTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

type Listener struct {
	conn   *pgxpool.Conn
	dbPool *pgxpool.Pool
	prefix string
	mu     sync.Mutex
}

// NewListener returns a Listener that can be used to enable other drivers
// (notably `riverdatabasesql`) to listen for notifications when their
// abstraction doesn't allow direct access to pgx connections, even though they
// are using pgx under the hood. This constructor is not applicable to
// applications which only use pgx directly.
//
// Users of `database/sql` or Bun can use this with the
// `riverdatabasesql.NewWithListener` constructor to enable listener support in
// that driver.
//
// The dbPool will solely be used for acquiring new connections for `LISTEN`
// commands. As such, a pool must be provided that supports that command. Users
// of pgbouncer should ensure that this specific pool is configured with session
// pooling, even if their main application does not use session pooling.
//
// A single Client will never use more than one listener connection at a time,
// no matter how many topics are being listened to.
func NewListener(dbPool *pgxpool.Pool) riverdriver.Listener {
	return &Listener{dbPool: dbPool}
}

func (l *Listener) Close(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn == nil {
		return nil
	}

	// Release below would take care of cleanup and potentially put the
	// connection back into rotation, but in case a Listen was invoked without a
	// subsequent Unlisten on the same tpic, close the connection explicitly to
	// guarantee no other caller will receive a partially tainted connection.
	err := l.conn.Conn().Close(ctx)

	// Even in the event of an error, make sure conn is set back to nil so that
	// the listener can be reused.
	l.conn.Release()
	l.conn = nil

	return err
}

func (l *Listener) Connect(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn != nil {
		return errors.New("connection already established")
	}

	conn, err := l.dbPool.Acquire(ctx)
	if err != nil {
		return err
	}

	var schema string
	if err := conn.QueryRow(ctx, "SELECT current_schema();").Scan(&schema); err != nil {
		conn.Release()
		return err
	}

	l.prefix = schema + "."
	l.conn = conn
	return nil
}

func (l *Listener) Listen(ctx context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, err := l.conn.Exec(ctx, "LISTEN \""+l.prefix+topic+"\"")
	return err
}

func (l *Listener) Ping(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.conn.Ping(ctx)
}

func (l *Listener) Unlisten(ctx context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, err := l.conn.Exec(ctx, "UNLISTEN \""+l.prefix+topic+"\"")
	return err
}

func (l *Listener) WaitForNotification(ctx context.Context) (*riverdriver.Notification, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	notification, err := l.conn.Conn().WaitForNotification(ctx)
	if err != nil {
		return nil, err
	}

	return &riverdriver.Notification{
		Topic:   strings.TrimPrefix(notification.Channel, l.prefix),
		Payload: notification.Payload,
	}, nil
}

func interpretError(err error) error {
	if errors.Is(err, puddle.ErrClosedPool) {
		return riverdriver.ErrClosedPool
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return rivertype.ErrNotFound
	}
	return err
}

func jobRowFromInternal(internal *dbsqlc.RiverJob) (*rivertype.JobRow, error) {
	var attemptedAt *time.Time
	if internal.AttemptedAt != nil {
		t := internal.AttemptedAt.UTC()
		attemptedAt = &t
	}

	errors := make([]rivertype.AttemptError, len(internal.Errors))
	for i, rawError := range internal.Errors {
		if err := json.Unmarshal(rawError, &errors[i]); err != nil {
			return nil, err
		}
	}

	var finalizedAt *time.Time
	if internal.FinalizedAt != nil {
		t := internal.FinalizedAt.UTC()
		finalizedAt = &t
	}

	return &rivertype.JobRow{
		ID:          internal.ID,
		Attempt:     max(int(internal.Attempt), 0),
		AttemptedAt: attemptedAt,
		AttemptedBy: internal.AttemptedBy,
		CreatedAt:   internal.CreatedAt.UTC(),
		EncodedArgs: internal.Args,
		Errors:      errors,
		FinalizedAt: finalizedAt,
		Kind:        internal.Kind,
		MaxAttempts: max(int(internal.MaxAttempts), 0),
		Metadata:    internal.Metadata,
		Priority:    max(int(internal.Priority), 0),
		Queue:       internal.Queue,
		ScheduledAt: internal.ScheduledAt.UTC(),
		State:       rivertype.JobState(internal.State),
		Tags:        internal.Tags,
	}, nil
}

func leaderFromInternal(internal *dbsqlc.RiverLeader) *riverdriver.Leader {
	return &riverdriver.Leader{
		ElectedAt: internal.ElectedAt.UTC(),
		ExpiresAt: internal.ExpiresAt.UTC(),
		LeaderID:  internal.LeaderID,
	}
}

// mapSlice manipulates a slice and transforms it to a slice of another type.
func mapSlice[T any, R any](collection []T, mapFunc func(T) R) []R {
	if collection == nil {
		return nil
	}

	result := make([]R, len(collection))

	for i, item := range collection {
		result[i] = mapFunc(item)
	}

	return result
}

// mapSliceError manipulates a slice and transforms it to a slice of another
// type, returning the first error that occurred invoking the map function, if
// there was one.
func mapSliceError[T any, R any](collection []T, mapFunc func(T) (R, error)) ([]R, error) {
	if collection == nil {
		return nil, nil
	}

	result := make([]R, len(collection))

	for i, item := range collection {
		var err error
		result[i], err = mapFunc(item)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func migrationFromInternal(internal *dbsqlc.RiverMigration) *riverdriver.Migration {
	return &riverdriver.Migration{
		ID:        int(internal.ID),
		CreatedAt: internal.CreatedAt.UTC(),
		Version:   int(internal.Version),
	}
}

func queueFromInternal(internal *dbsqlc.RiverQueue) *rivertype.Queue {
	var pausedAt *time.Time
	if internal.PausedAt != nil {
		t := internal.PausedAt.UTC()
		pausedAt = &t
	}
	return &rivertype.Queue{
		CreatedAt: internal.CreatedAt.UTC(),
		Metadata:  internal.Metadata,
		Name:      internal.Name,
		PausedAt:  pausedAt,
		UpdatedAt: internal.UpdatedAt.UTC(),
	}
}
