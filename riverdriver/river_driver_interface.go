// Package riverdriver exposes generic constructs to be implemented by specific
// drivers that wrap third party database packages, with the aim being to keep
// the main River interface decoupled from a specific database package so that
// other packages or other major versions of packages can be supported in future
// River versions.
//
// River currently only supports Pgx v5, and the interface here wrap it with
// only the thinnest possible layer. Adding support for alternate packages will
// require the interface to change substantially, and therefore it should not be
// implemented or invoked by user code. Changes to interfaces in this package
// WILL NOT be considered breaking changes for purposes of River's semantic
// versioning.
package riverdriver

import (
	"context"
	"errors"
	"io/fs"
	"time"

	"github.com/riverqueue/river/rivertype"
)

const AllQueuesString = "*"

const MigrationLineMain = "main"

var (
	ErrClosedPool     = errors.New("underlying driver pool is closed")
	ErrNotImplemented = errors.New("driver does not implement this functionality")
)

// Driver provides a database driver for use with river.Client.
//
// Its purpose is to wrap the interface of a third party database package, with
// the aim being to keep the main River interface decoupled from a specific
// database package so that other packages or major versions of packages can be
// supported in future River versions.
//
// River currently only supports Pgx v5, and this interface wraps it with only
// the thinnest possible layer. Adding support for alternate packages will
// require it to change substantially, and therefore it should not be
// implemented or invoked by user code. Changes to this interface WILL NOT be
// considered breaking changes for purposes of River's semantic versioning.
//
// API is not stable. DO NOT IMPLEMENT.
type Driver[TTx any] interface {
	// GetExecutor gets an executor for the driver.
	//
	// API is not stable. DO NOT USE.
	GetExecutor() Executor

	// GetListener gets a listener for purposes of receiving notifications.
	//
	// API is not stable. DO NOT USE.
	GetListener() Listener

	// GetMigrationFS gets a filesystem containing migrations for the driver.
	//
	// Each set of migration files is expected to exist within the filesystem as
	// `migration/<line>/`. For example:
	//
	//     migration/main/001_create_river_migration.up.sql
	//
	// API is not stable. DO NOT USE.
	GetMigrationFS(line string) fs.FS

	// GetMigrationLines gets supported migration lines from the driver. Most
	// drivers will only support a single line: MigrationLineMain.
	//
	// API is not stable. DO NOT USE.
	GetMigrationLines() []string

	// HasPool returns true if the driver is configured with a database pool.
	//
	// API is not stable. DO NOT USE.
	HasPool() bool

	// SupportsListener gets whether this driver supports a listener. Drivers
	// that don't support a listener support poll only mode only.
	//
	// API is not stable. DO NOT USE.
	SupportsListener() bool

	// UnwrapExecutor gets an executor from a driver transaction.
	//
	// API is not stable. DO NOT USE.
	UnwrapExecutor(tx TTx) ExecutorTx
}

// Executor provides River operations against a database. It may be a database
// pool or transaction.
//
// API is not stable. DO NOT IMPLEMENT.
type Executor interface {
	// Begin begins a new subtransaction. ErrSubTxNotSupported may be returned
	// if the executor is a transaction and the driver doesn't support
	// subtransactions (like riverdriver/riverdatabasesql for database/sql).
	Begin(ctx context.Context) (ExecutorTx, error)

	// ColumnExists checks whether a column for a particular table exists for
	// the schema in the current search schema.
	ColumnExists(ctx context.Context, tableName, columnName string) (bool, error)

	// Exec executes raw SQL. Used for migrations.
	Exec(ctx context.Context, sql string) (struct{}, error)

	JobCancel(ctx context.Context, params *JobCancelParams) (*rivertype.JobRow, error)
	JobCountByState(ctx context.Context, state rivertype.JobState) (int, error)
	JobDelete(ctx context.Context, id int64) (*rivertype.JobRow, error)
	JobDeleteBefore(ctx context.Context, params *JobDeleteBeforeParams) (int, error)
	JobGetAvailable(ctx context.Context, params *JobGetAvailableParams) ([]*rivertype.JobRow, error)
	JobGetByID(ctx context.Context, id int64) (*rivertype.JobRow, error)
	JobGetByIDMany(ctx context.Context, id []int64) ([]*rivertype.JobRow, error)
	JobGetByKindAndUniqueProperties(ctx context.Context, params *JobGetByKindAndUniquePropertiesParams) (*rivertype.JobRow, error)
	JobGetByKindMany(ctx context.Context, kind []string) ([]*rivertype.JobRow, error)
	JobGetStuck(ctx context.Context, params *JobGetStuckParams) ([]*rivertype.JobRow, error)
	JobInsertFast(ctx context.Context, params *JobInsertFastParams) (*JobInsertFastResult, error)
	JobInsertFastMany(ctx context.Context, params []*JobInsertFastParams) ([]*JobInsertFastResult, error)
	JobInsertFastManyNoReturning(ctx context.Context, params []*JobInsertFastParams) (int, error)
	JobInsertFull(ctx context.Context, params *JobInsertFullParams) (*rivertype.JobRow, error)
	JobList(ctx context.Context, query string, namedArgs map[string]any) ([]*rivertype.JobRow, error)
	JobListFields() string
	JobRescueMany(ctx context.Context, params *JobRescueManyParams) (*struct{}, error)
	JobRetry(ctx context.Context, id int64) (*rivertype.JobRow, error)
	JobSchedule(ctx context.Context, params *JobScheduleParams) ([]*JobScheduleResult, error)
	JobSetCompleteIfRunningMany(ctx context.Context, params *JobSetCompleteIfRunningManyParams) ([]*rivertype.JobRow, error)
	JobSetStateIfRunning(ctx context.Context, params *JobSetStateIfRunningParams) (*rivertype.JobRow, error)
	JobSetStateIfRunningMany(ctx context.Context, params *JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error)
	JobUpdate(ctx context.Context, params *JobUpdateParams) (*rivertype.JobRow, error)
	LeaderAttemptElect(ctx context.Context, params *LeaderElectParams) (bool, error)
	LeaderAttemptReelect(ctx context.Context, params *LeaderElectParams) (bool, error)
	LeaderDeleteExpired(ctx context.Context) (int, error)
	LeaderGetElectedLeader(ctx context.Context) (*Leader, error)
	LeaderInsert(ctx context.Context, params *LeaderInsertParams) (*Leader, error)
	LeaderResign(ctx context.Context, params *LeaderResignParams) (bool, error)

	// MigrationDeleteAssumingMainMany deletes many migrations assuming
	// everything is on the main line. This is suitable for use in databases on
	// a version before the `line` column exists.
	MigrationDeleteAssumingMainMany(ctx context.Context, versions []int) ([]*Migration, error)

	// MigrationDeleteByLineAndVersionMany deletes many migration versions on a
	// particular line.
	MigrationDeleteByLineAndVersionMany(ctx context.Context, line string, versions []int) ([]*Migration, error)

	// MigrationGetAllAssumingMain gets all migrations assuming everything is on
	// the main line. This is suitable for use in databases on a version before
	// the `line` column exists.
	MigrationGetAllAssumingMain(ctx context.Context) ([]*Migration, error)

	// MigrationGetByLine gets all currently applied migrations.
	MigrationGetByLine(ctx context.Context, line string) ([]*Migration, error)

	// MigrationInsertMany inserts many migration versions.
	MigrationInsertMany(ctx context.Context, line string, versions []int) ([]*Migration, error)

	// MigrationInsertManyAssumingMain inserts many migrations, assuming they're
	// on the main line. This operation is necessary for compatibility before
	// the `line` column was added to the migrations table.
	MigrationInsertManyAssumingMain(ctx context.Context, versions []int) ([]*Migration, error)

	NotifyMany(ctx context.Context, params *NotifyManyParams) error
	PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error)

	QueueCreateOrSetUpdatedAt(ctx context.Context, params *QueueCreateOrSetUpdatedAtParams) (*rivertype.Queue, error)
	QueueDeleteExpired(ctx context.Context, params *QueueDeleteExpiredParams) ([]string, error)
	QueueGet(ctx context.Context, name string) (*rivertype.Queue, error)
	QueueList(ctx context.Context, limit int) ([]*rivertype.Queue, error)
	QueuePause(ctx context.Context, name string) error
	QueueResume(ctx context.Context, name string) error

	// TableExists checks whether a table exists for the schema in the current
	// search schema.
	TableExists(ctx context.Context, tableName string) (bool, error)
}

// ExecutorTx is an executor which is a transaction. In addition to standard
// Executor operations, it may be committed or rolled back.
//
// API is not stable. DO NOT IMPLEMENT.
type ExecutorTx interface {
	Executor

	// Commit commits the transaction.
	//
	// API is not stable. DO NOT USE.
	Commit(ctx context.Context) error

	// Rollback rolls back the transaction.
	//
	// API is not stable. DO NOT USE.
	Rollback(ctx context.Context) error
}

// Listener listens for notifications. In Postgres, this is a database
// connection where `LISTEN` has been run.
//
// API is not stable. DO NOT IMPLEMENT.
type Listener interface {
	Close(ctx context.Context) error
	Connect(ctx context.Context) error
	Listen(ctx context.Context, topic string) error
	Ping(ctx context.Context) error
	Unlisten(ctx context.Context, topic string) error
	WaitForNotification(ctx context.Context) (*Notification, error)
}

type Notification struct {
	Payload string
	Topic   string
}

type JobCancelParams struct {
	CancelAttemptedAt time.Time
	ControlTopic      string
	ID                int64
}

type JobDeleteBeforeParams struct {
	CancelledFinalizedAtHorizon time.Time
	CompletedFinalizedAtHorizon time.Time
	DiscardedFinalizedAtHorizon time.Time
	Max                         int
}

type JobGetAvailableParams struct {
	AttemptedBy string
	Max         int
	Queue       string
}

type JobGetByKindAndUniquePropertiesParams struct {
	Kind           string
	ByArgs         bool
	Args           []byte
	ByCreatedAt    bool
	CreatedAtBegin time.Time
	CreatedAtEnd   time.Time
	ByQueue        bool
	Queue          string
	ByState        bool
	State          []string
}

type JobGetStuckParams struct {
	Max          int
	StuckHorizon time.Time
}

type JobInsertFastParams struct {
	// Args contains the raw underlying job arguments struct. It has already been
	// encoded into EncodedArgs, but the original is kept here for to leverage its
	// struct tags and interfaces, such as for use in unique key generation.
	Args         rivertype.JobArgs
	CreatedAt    *time.Time
	EncodedArgs  []byte
	Kind         string
	MaxAttempts  int
	Metadata     []byte
	Priority     int
	Queue        string
	ScheduledAt  *time.Time
	State        rivertype.JobState
	Tags         []string
	UniqueKey    []byte
	UniqueStates byte
}

type JobInsertFastResult struct {
	Job                      *rivertype.JobRow
	UniqueSkippedAsDuplicate bool
}

type JobInsertFullParams struct {
	Attempt      int
	AttemptedAt  *time.Time
	CreatedAt    *time.Time
	EncodedArgs  []byte
	Errors       [][]byte
	FinalizedAt  *time.Time
	Kind         string
	MaxAttempts  int
	Metadata     []byte
	Priority     int
	Queue        string
	ScheduledAt  *time.Time
	State        rivertype.JobState
	Tags         []string
	UniqueKey    []byte
	UniqueStates byte
}

type JobRescueManyParams struct {
	ID          []int64
	Error       [][]byte
	FinalizedAt []time.Time
	ScheduledAt []time.Time
	State       []string
}

type JobScheduleParams struct {
	Max int
	Now time.Time
}

type JobScheduleResult struct {
	Job               rivertype.JobRow
	ConflictDiscarded bool
}

// JobSetCompleteIfRunningManyParams are parameters to set many running jobs to
// `complete` all at once for improved throughput and efficiency.
type JobSetCompleteIfRunningManyParams struct {
	ID          []int64
	FinalizedAt []time.Time
}

// JobSetStateIfRunningParams are parameters to update the state of a currently
// running job. Use one of the constructors below to ensure a correct
// combination of parameters.
type JobSetStateIfRunningParams struct {
	ID          int64
	ErrData     []byte
	FinalizedAt *time.Time
	MaxAttempts *int
	ScheduledAt *time.Time
	State       rivertype.JobState
}

func JobSetStateCancelled(id int64, finalizedAt time.Time, errData []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, ErrData: errData, FinalizedAt: &finalizedAt, State: rivertype.JobStateCancelled}
}

func JobSetStateCompleted(id int64, finalizedAt time.Time) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, FinalizedAt: &finalizedAt, State: rivertype.JobStateCompleted}
}

func JobSetStateDiscarded(id int64, finalizedAt time.Time, errData []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, ErrData: errData, FinalizedAt: &finalizedAt, State: rivertype.JobStateDiscarded}
}

func JobSetStateErrorAvailable(id int64, scheduledAt time.Time, errData []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, ErrData: errData, ScheduledAt: &scheduledAt, State: rivertype.JobStateAvailable}
}

func JobSetStateErrorRetryable(id int64, scheduledAt time.Time, errData []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, ErrData: errData, ScheduledAt: &scheduledAt, State: rivertype.JobStateRetryable}
}

func JobSetStateSnoozed(id int64, scheduledAt time.Time, maxAttempts int) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, MaxAttempts: &maxAttempts, ScheduledAt: &scheduledAt, State: rivertype.JobStateScheduled}
}

func JobSetStateSnoozedAvailable(id int64, scheduledAt time.Time, maxAttempts int) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, MaxAttempts: &maxAttempts, ScheduledAt: &scheduledAt, State: rivertype.JobStateAvailable}
}

// JobSetStateIfRunningManyParams are parameters to update the state of
// currently running jobs. Use one of the constructors below to ensure a correct
// combination of parameters.
type JobSetStateIfRunningManyParams struct {
	ID          []int64
	ErrData     [][]byte
	FinalizedAt []*time.Time
	MaxAttempts []*int
	ScheduledAt []*time.Time
	State       []rivertype.JobState
}

type JobUpdateParams struct {
	ID                  int64
	AttemptDoUpdate     bool
	Attempt             int
	AttemptedAtDoUpdate bool
	AttemptedAt         *time.Time
	ErrorsDoUpdate      bool
	Errors              [][]byte
	FinalizedAtDoUpdate bool
	FinalizedAt         *time.Time
	StateDoUpdate       bool
	State               rivertype.JobState
	// Deprecated and will be removed when advisory lock unique path is removed.
	UniqueKeyDoUpdate bool
	// Deprecated and will be removed when advisory lock unique path is removed.
	UniqueKey []byte
}

// Leader represents a River leader.
//
// API is not stable. DO NOT USE.
type Leader struct {
	ElectedAt time.Time
	ExpiresAt time.Time
	LeaderID  string
}

type LeaderInsertParams struct {
	ElectedAt *time.Time
	ExpiresAt *time.Time
	LeaderID  string
	TTL       time.Duration
}

type LeaderElectParams struct {
	LeaderID string
	TTL      time.Duration
}

type LeaderResignParams struct {
	LeaderID        string
	LeadershipTopic string
}

// Migration represents a River migration.
//
// API is not stable. DO NOT USE.
type Migration struct {
	// CreatedAt is when the migration was initially created.
	//
	// API is not stable. DO NOT USE.
	CreatedAt time.Time

	// Line is the migration line that the migration belongs to.
	//
	// API is not stable. DO NOT USE.
	Line string

	// Version is the version of the migration.
	//
	// API is not stable. DO NOT USE.
	Version int
}

// NotifyManyParams are parameters to issue many pubsub notifications all at
// once for a single topic.
type NotifyManyParams struct {
	Payload []string
	Topic   string
}

type QueueCreateOrSetUpdatedAtParams struct {
	Metadata  []byte
	Name      string
	PausedAt  *time.Time
	UpdatedAt *time.Time
}

type QueueDeleteExpiredParams struct {
	Max              int
	UpdatedAtHorizon time.Time
}
