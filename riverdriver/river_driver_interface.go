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
	// ArgPlaceholder is the placeholder character used in query positional
	// arguments, so "$" for "$1", "$2", "$3", etc. This is a "$" for Postgres
	// and "?" for SQLite.
	//
	// API is not stable. DO NOT USE.
	ArgPlaceholder() string

	// DatabaseName is the name of the database that the driver targets like
	// "postgres" or "sqlite". This is used for purposes like a cache key prefix
	// in riverdbtest so that multiple drivers may share schemas as long as they
	// target the same database.
	//
	// API is not stable. DO NOT USE.
	DatabaseName() string

	// GetExecutor gets an executor for the driver.
	//
	// API is not stable. DO NOT USE.
	GetExecutor() Executor

	// GetListener gets a listener for purposes of receiving notifications.
	//
	// API is not stable. DO NOT USE.
	GetListener(params *GetListenenerParams) Listener

	// GetMigrationDefaultLines gets default migration lines that should be
	// applied when using this driver. This is mainly used by riverdbtest to
	// figure out what migration lines should be available by default for new
	// test schemas.
	//
	// API is not stable. DO NOT USE.
	GetMigrationDefaultLines() []string

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

	// GetMigrationTruncateTables gets the tables that should be truncated
	// before or after tests for a specific migration line returned by this
	// driver. Tables to truncate doesn't need to consider intermediary states,
	// and should return tables for the latest migration version.
	//
	// API is not stable. DO NOT USE.
	GetMigrationTruncateTables(line string, version int) []string

	// PoolIsSet returns true if the driver is configured with a database pool.
	//
	// API is not stable. DO NOT USE.
	PoolIsSet() bool

	// PoolSet sets a database pool into a driver will a nil pool. This is meant
	// only for use in testing, and only in specific circumstances where it's
	// needed. The pool in a driver should generally be treated as immutable
	// because it's inherited by driver executors, and changing if when active
	// executors exist will cause problems.
	//
	// Most drivers don't implement this function and return ErrNotImplemented.
	//
	// Drivers should only set a pool if the previous pool was nil (to help root
	// out bugs where something unexpected is happening), and panic in case a
	// pool is set to a driver twice.
	//
	// API is not stable. DO NOT USE.
	PoolSet(dbPool any) error

	// SQLFragmentColumnIn generates an SQL fragment to be included as a
	// predicate in a `WHERE` query for the existence of a set of values in a
	// column like `id IN (...)`. The actual implementation depends on support
	// for specific data types. Postgres uses arrays while SQLite uses a JSON
	// fragment with `json_each`.
	//
	// API is not stable. DO NOT USE.
	SQLFragmentColumnIn(column string, values any) (string, any, error)

	// SupportsListener gets whether this driver supports a listener. Drivers
	// that don't support a listener support poll only mode only.
	//
	// API is not stable. DO NOT USE.
	SupportsListener() bool

	// SupportsListenNotify indicates whether the underlying database supports
	// listen/notify. This differs from SupportsListener in that even if a
	// driver doesn't a support a listener but the database supports the
	// underlying listen/notify mechanism, it will still broadcast in case there
	// are other clients/drivers on the database that do support a listener. If
	// listen/notify can't be supported at all, no broadcast attempt is made.
	//
	// API is not stable. DO NOT USE.
	SupportsListenNotify() bool

	// TimePrecision returns the maximum time resolution supported by the
	// database. This is used in test assertions when checking round trips on
	// timestamps.
	//
	// API is not stable. DO NOT USE.
	TimePrecision() time.Duration

	// UnwrapExecutor gets an executor from a driver transaction.
	//
	// API is not stable. DO NOT USE.
	UnwrapExecutor(tx TTx) ExecutorTx

	// UnwrapTx gets a driver transaction from an executor. This is currently
	// only needed for test transaction helpers.
	//
	// API is not stable. DO NOT USE.
	UnwrapTx(execTx ExecutorTx) TTx
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
	ColumnExists(ctx context.Context, params *ColumnExistsParams) (bool, error)

	// Exec executes raw SQL. Used for migrations.
	Exec(ctx context.Context, sql string, args ...any) error

	// IndexDropIfExists drops a database index if exists. This abstraction is a
	// little leaky right now because Postgres runs this `CONCURRENTLY` and
	// that's not possible in SQLite.
	//
	// API is not stable. DO NOT USE.
	IndexDropIfExists(ctx context.Context, params *IndexDropIfExistsParams) error
	IndexExists(ctx context.Context, params *IndexExistsParams) (bool, error)
	IndexesExist(ctx context.Context, params *IndexesExistParams) (map[string]bool, error)

	// IndexReindex reindexes a database index. This abstraction is a little
	// leaky right now because Postgres runs this `CONCURRENTLY` and that's not
	// possible in SQLite.
	//
	// API is not stable. DO NOT USE.
	IndexReindex(ctx context.Context, params *IndexReindexParams) error

	JobCancel(ctx context.Context, params *JobCancelParams) (*rivertype.JobRow, error)
	JobCountByAllStates(ctx context.Context, params *JobCountByAllStatesParams) (map[rivertype.JobState]int, error)
	JobCountByQueueAndState(ctx context.Context, params *JobCountByQueueAndStateParams) ([]*JobCountByQueueAndStateResult, error)
	JobCountByState(ctx context.Context, params *JobCountByStateParams) (int, error)
	JobDelete(ctx context.Context, params *JobDeleteParams) (*rivertype.JobRow, error)
	JobDeleteBefore(ctx context.Context, params *JobDeleteBeforeParams) (int, error)
	JobDeleteMany(ctx context.Context, params *JobDeleteManyParams) ([]*rivertype.JobRow, error)
	JobGetAvailable(ctx context.Context, params *JobGetAvailableParams) ([]*rivertype.JobRow, error)
	JobGetByID(ctx context.Context, params *JobGetByIDParams) (*rivertype.JobRow, error)
	JobGetByIDMany(ctx context.Context, params *JobGetByIDManyParams) ([]*rivertype.JobRow, error)
	JobGetByKindMany(ctx context.Context, params *JobGetByKindManyParams) ([]*rivertype.JobRow, error)
	JobGetStuck(ctx context.Context, params *JobGetStuckParams) ([]*rivertype.JobRow, error)
	JobInsertFastMany(ctx context.Context, params *JobInsertFastManyParams) ([]*JobInsertFastResult, error)
	JobInsertFastManyNoReturning(ctx context.Context, params *JobInsertFastManyParams) (int, error)
	JobInsertFull(ctx context.Context, params *JobInsertFullParams) (*rivertype.JobRow, error)
	JobInsertFullMany(ctx context.Context, jobs *JobInsertFullManyParams) ([]*rivertype.JobRow, error)
	JobKindList(ctx context.Context, params *JobKindListParams) ([]string, error)
	JobList(ctx context.Context, params *JobListParams) ([]*rivertype.JobRow, error)
	JobRescueMany(ctx context.Context, params *JobRescueManyParams) (*struct{}, error)
	JobRetry(ctx context.Context, params *JobRetryParams) (*rivertype.JobRow, error)
	JobSchedule(ctx context.Context, params *JobScheduleParams) ([]*JobScheduleResult, error)
	JobSetStateIfRunningMany(ctx context.Context, params *JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error)
	JobUpdate(ctx context.Context, params *JobUpdateParams) (*rivertype.JobRow, error)
	LeaderAttemptElect(ctx context.Context, params *LeaderElectParams) (bool, error)
	LeaderAttemptReelect(ctx context.Context, params *LeaderElectParams) (bool, error)
	LeaderDeleteExpired(ctx context.Context, params *LeaderDeleteExpiredParams) (int, error)
	LeaderGetElectedLeader(ctx context.Context, params *LeaderGetElectedLeaderParams) (*Leader, error)
	LeaderInsert(ctx context.Context, params *LeaderInsertParams) (*Leader, error)
	LeaderResign(ctx context.Context, params *LeaderResignParams) (bool, error)

	// MigrationDeleteAssumingMainMany deletes many migrations assuming
	// everything is on the main line. This is suitable for use in databases on
	// a version before the `line` column exists.
	MigrationDeleteAssumingMainMany(ctx context.Context, params *MigrationDeleteAssumingMainManyParams) ([]*Migration, error)

	// MigrationDeleteByLineAndVersionMany deletes many migration versions on a
	// particular line.
	MigrationDeleteByLineAndVersionMany(ctx context.Context, params *MigrationDeleteByLineAndVersionManyParams) ([]*Migration, error)

	// MigrationGetAllAssumingMain gets all migrations assuming everything is on
	// the main line. This is suitable for use in databases on a version before
	// the `line` column exists.
	MigrationGetAllAssumingMain(ctx context.Context, params *MigrationGetAllAssumingMainParams) ([]*Migration, error)

	// MigrationGetByLine gets all currently applied migrations.
	MigrationGetByLine(ctx context.Context, params *MigrationGetByLineParams) ([]*Migration, error)

	// MigrationInsertMany inserts many migration versions.
	MigrationInsertMany(ctx context.Context, params *MigrationInsertManyParams) ([]*Migration, error)

	// MigrationInsertManyAssumingMain inserts many migrations, assuming they're
	// on the main line. This operation is necessary for compatibility before
	// the `line` column was added to the migrations table.
	MigrationInsertManyAssumingMain(ctx context.Context, params *MigrationInsertManyAssumingMainParams) ([]*Migration, error)

	NotifyMany(ctx context.Context, params *NotifyManyParams) error
	PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error)

	QueueCreateOrSetUpdatedAt(ctx context.Context, params *QueueCreateOrSetUpdatedAtParams) (*rivertype.Queue, error)
	QueueDeleteExpired(ctx context.Context, params *QueueDeleteExpiredParams) ([]string, error)
	QueueGet(ctx context.Context, params *QueueGetParams) (*rivertype.Queue, error)
	QueueList(ctx context.Context, params *QueueListParams) ([]*rivertype.Queue, error)
	QueueNameList(ctx context.Context, params *QueueNameListParams) ([]string, error)
	QueuePause(ctx context.Context, params *QueuePauseParams) error
	QueueResume(ctx context.Context, params *QueueResumeParams) error
	QueueUpdate(ctx context.Context, params *QueueUpdateParams) (*rivertype.Queue, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row

	SchemaCreate(ctx context.Context, params *SchemaCreateParams) error
	SchemaDrop(ctx context.Context, params *SchemaDropParams) error
	SchemaGetExpired(ctx context.Context, params *SchemaGetExpiredParams) ([]string, error)

	// TableExists checks whether a table exists for the schema in the current
	// search schema.
	TableExists(ctx context.Context, params *TableExistsParams) (bool, error)
	TableTruncate(ctx context.Context, params *TableTruncateParams) error
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

type GetListenenerParams struct {
	Schema string
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
	Schema() string
	SetAfterConnectExec(sql string) // should only ever be used in testing
	Unlisten(ctx context.Context, topic string) error
	WaitForNotification(ctx context.Context) (*Notification, error)
}

type Notification struct {
	Payload string
	Topic   string
}

type ColumnExistsParams struct {
	Column string
	Schema string
	Table  string
}

type IndexDropIfExistsParams struct {
	Index  string
	Schema string
}

type IndexExistsParams struct {
	Index  string
	Schema string
}

type IndexesExistParams struct {
	IndexNames []string
	Schema     string
}

type JobCancelParams struct {
	ID                int64
	CancelAttemptedAt time.Time
	ControlTopic      string
	Now               *time.Time
	Schema            string
}

type JobCountByAllStatesParams struct {
	Schema string
}

type JobCountByQueueAndStateParams struct {
	QueueNames []string
	Schema     string
}

type JobCountByQueueAndStateResult struct {
	CountAvailable int64
	CountRunning   int64
	Queue          string
}

type JobCountByStateParams struct {
	Schema string
	State  rivertype.JobState
}

type JobDeleteParams struct {
	ID     int64
	Schema string
}

type JobDeleteBeforeParams struct {
	CancelledDoDelete           bool
	CancelledFinalizedAtHorizon time.Time
	CompletedDoDelete           bool
	CompletedFinalizedAtHorizon time.Time
	DiscardedDoDelete           bool
	DiscardedFinalizedAtHorizon time.Time
	Max                         int
	QueuesExcluded              []string
	QueuesIncluded              []string
	Schema                      string
}

type JobDeleteManyParams struct {
	Max           int32
	NamedArgs     map[string]any
	OrderByClause string
	Schema        string
	WhereClause   string
}

type JobGetAvailableParams struct {
	ClientID       string
	MaxAttemptedBy int
	MaxToLock      int
	Now            *time.Time
	ProducerID     int64
	Queue          string
	Schema         string
}

type JobGetByIDParams struct {
	ID     int64
	Schema string
}

type JobGetByIDManyParams struct {
	ID     []int64
	Schema string
}

type JobGetByKindManyParams struct {
	Kind   []string
	Schema string
}

type JobGetStuckParams struct {
	Max          int
	Schema       string
	StuckHorizon time.Time
}

type JobInsertFastParams struct {
	ID *int64
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

type JobInsertFastManyParams struct {
	Jobs   []*JobInsertFastParams
	Schema string
}

type JobInsertFastResult struct {
	Job                      *rivertype.JobRow
	UniqueSkippedAsDuplicate bool
}

type JobInsertFullParams struct {
	Attempt      int
	AttemptedAt  *time.Time
	AttemptedBy  []string
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
	Schema       string
	State        rivertype.JobState
	Tags         []string
	UniqueKey    []byte
	UniqueStates byte
}

type JobInsertFullManyParams struct {
	Jobs   []*JobInsertFullParams
	Schema string
}

type JobKindListParams struct {
	After   string
	Exclude []string
	Match   string
	Max     int
	Schema  string
}

type JobListParams struct {
	Max           int32
	NamedArgs     map[string]any
	OrderByClause string
	Schema        string
	WhereClause   string
}

type JobRescueManyParams struct {
	ID          []int64
	Error       [][]byte
	FinalizedAt []*time.Time
	ScheduledAt []time.Time
	Schema      string
	State       []string
}

type JobRetryParams struct {
	ID     int64
	Now    *time.Time
	Schema string
}

type JobScheduleParams struct {
	Max    int
	Now    *time.Time
	Schema string
}

type JobScheduleResult struct {
	Job               rivertype.JobRow
	ConflictDiscarded bool
}

// JobSetStateIfRunningParams are parameters to update the state of a currently
// running job. Use one of the constructors below to ensure a correct
// combination of parameters.
type JobSetStateIfRunningParams struct {
	ID              int64
	Attempt         *int
	ErrData         []byte
	FinalizedAt     *time.Time
	MetadataDoMerge bool
	MetadataUpdates []byte
	ScheduledAt     *time.Time
	Schema          string // added by completer
	Snoozed         bool
	State           rivertype.JobState
}

func JobSetStateCancelled(id int64, finalizedAt time.Time, errData []byte, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		ID:              id,
		ErrData:         errData,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		FinalizedAt:     &finalizedAt,
		State:           rivertype.JobStateCancelled,
	}
}

func JobSetStateCompleted(id int64, finalizedAt time.Time, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		FinalizedAt:     &finalizedAt,
		ID:              id,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		State:           rivertype.JobStateCompleted,
	}
}

func JobSetStateDiscarded(id int64, finalizedAt time.Time, errData []byte, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		ID:              id,
		ErrData:         errData,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		FinalizedAt:     &finalizedAt,
		State:           rivertype.JobStateDiscarded,
	}
}

func JobSetStateErrorAvailable(id int64, scheduledAt time.Time, errData []byte, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		ID:              id,
		ErrData:         errData,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		ScheduledAt:     &scheduledAt,
		State:           rivertype.JobStateAvailable,
	}
}

func JobSetStateErrorRetryable(id int64, scheduledAt time.Time, errData []byte, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		ID:              id,
		ErrData:         errData,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		ScheduledAt:     &scheduledAt,
		State:           rivertype.JobStateRetryable,
	}
}

func JobSetStateSnoozed(id int64, scheduledAt time.Time, attempt int, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		Attempt:         &attempt,
		ID:              id,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		ScheduledAt:     &scheduledAt,
		Snoozed:         true,
		State:           rivertype.JobStateScheduled,
	}
}

func JobSetStateSnoozedAvailable(id int64, scheduledAt time.Time, attempt int, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		Attempt:         &attempt,
		ID:              id,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		ScheduledAt:     &scheduledAt,
		Snoozed:         true,
		State:           rivertype.JobStateAvailable,
	}
}

// JobSetStateIfRunningManyParams are parameters to update the state of
// currently running jobs. Use one of the constructors below to ensure a correct
// combination of parameters.
type JobSetStateIfRunningManyParams struct {
	ID              []int64
	Attempt         []*int
	ErrData         [][]byte
	FinalizedAt     []*time.Time
	MetadataDoMerge []bool
	MetadataUpdates [][]byte
	Now             *time.Time
	ScheduledAt     []*time.Time
	Schema          string
	State           []rivertype.JobState
}

type JobUpdateParams struct {
	ID                  int64
	AttemptDoUpdate     bool
	Attempt             int
	AttemptedAtDoUpdate bool
	AttemptedAt         *time.Time
	AttemptedByDoUpdate bool
	AttemptedBy         []string
	ErrorsDoUpdate      bool
	Errors              [][]byte
	FinalizedAtDoUpdate bool
	FinalizedAt         *time.Time
	MaxAttemptsDoUpdate bool
	MaxAttempts         int
	MetadataDoUpdate    bool
	Metadata            []byte
	Schema              string
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

type LeaderDeleteExpiredParams struct {
	Now    *time.Time
	Schema string
}

type LeaderGetElectedLeaderParams struct {
	Schema string
}

type LeaderInsertParams struct {
	ElectedAt *time.Time
	ExpiresAt *time.Time
	LeaderID  string
	Now       *time.Time
	Schema    string
	TTL       time.Duration
}

type LeaderElectParams struct {
	LeaderID string
	Now      *time.Time
	Schema   string
	TTL      time.Duration
}

type LeaderResignParams struct {
	LeaderID        string
	LeadershipTopic string
	Schema          string
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

type MigrationDeleteAssumingMainManyParams struct {
	Schema   string
	Versions []int
}

type MigrationDeleteByLineAndVersionManyParams struct {
	Line     string
	Schema   string
	Versions []int
}

type MigrationGetAllAssumingMainParams struct {
	Schema string
}

type MigrationGetByLineParams struct {
	Line   string
	Schema string
}

type MigrationInsertManyParams struct {
	Line     string
	Schema   string
	Versions []int
}

type MigrationInsertManyAssumingMainParams struct {
	Schema   string
	Versions []int
}

// NotifyManyParams are parameters to issue many pubsub notifications all at
// once for a single topic.
type NotifyManyParams struct {
	Payload []string
	Topic   string
	Schema  string
}

type ProducerKeepAliveParams struct {
	ID                    int64
	QueueName             string
	Schema                string
	StaleUpdatedAtHorizon time.Time
}

type QueueCreateOrSetUpdatedAtParams struct {
	Metadata  []byte
	Name      string
	Now       *time.Time
	PausedAt  *time.Time
	Schema    string
	UpdatedAt *time.Time
}

type QueueDeleteExpiredParams struct {
	Max              int
	Schema           string
	UpdatedAtHorizon time.Time
}

type QueueGetParams struct {
	Name   string
	Schema string
}

type QueueListParams struct {
	Max    int
	Schema string
}

type QueueNameListParams struct {
	After   string
	Exclude []string
	Match   string
	Max     int
	Schema  string
}

type QueuePauseParams struct {
	Name   string
	Now    *time.Time
	Schema string
}

type QueueResumeParams struct {
	Name   string
	Now    *time.Time
	Schema string
}

type QueueUpdateParams struct {
	Metadata         []byte
	MetadataDoUpdate bool
	Name             string
	Schema           string
}

type Row interface {
	Scan(dest ...any) error
}

type IndexReindexParams struct {
	Index  string
	Schema string
}

type Schema struct {
	Name string
}

type SchemaCreateParams struct {
	Schema string
}

type SchemaDropParams struct {
	Schema string
}

type SchemaGetExpiredParams struct {
	BeforeName string
	Prefix     string
}

type TableExistsParams struct {
	Schema string
	Table  string
}

type TableTruncateParams struct {
	Schema string
	Table  []string
}

// MigrationLineMainTruncateTables is a shared helper that produces tables to
// truncate for the main migration line. It's reused across all drivers.
//
// API is not stable. DO NOT USE.
func MigrationLineMainTruncateTables(version int) []string {
	switch version {
	case 1:
		return nil // don't truncate `river_migrate`
	case 2, 3:
		return []string{"river_job", "river_leader"}
	case 4:
		return []string{"river_job", "river_leader", "river_queue"}
	}

	// 0 (zero value), 5, 6
	return []string{"river_job", "river_leader", "river_queue", "river_client", "river_client_queue"}
}
