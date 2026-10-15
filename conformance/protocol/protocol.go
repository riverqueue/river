// Package protocol defines the contract between the conformance harness and
// an implementation's adapter. The Go types here are the contract: the
// harness encodes requests with them, the Go reference adapter decodes them,
// and the Rust and JavaScript adapters mirror them by hand.
//
// # Transport
//
// An adapter is a process that reads one JSON-RPC 2.0 request per line on
// stdin and writes one response per line on stdout, in order. Requests are
// sequential: the harness waits for each response before sending the next
// request to the same process. Anything an adapter writes to stderr is shown
// when a scenario fails.
//
// The harness configures an adapter through its environment:
//
//   - RIVER_CONFORMANCE_DRIVER is "postgres" or "sqlite".
//   - RIVER_CONFORMANCE_DATABASE_URL is a Postgres URL, or a SQLite file
//     path. A Postgres URL may set search_path through its `options`
//     parameter, which the adapter must honor so River uses that schema when
//     no schema is given.
//   - RIVER_CONFORMANCE_APPLICATION_NAME is the Postgres application_name
//     every connection of the adapter must use, so the harness can observe
//     and fault exactly this process.
//
// On Postgres an adapter should hold at most 10 connections. On SQLite it
// should set a busy timeout of at least five seconds and use WAL mode, since
// several processes share the database file.
//
// # Requests
//
// Unknown methods fail with CodeMethodNotFound and params with unknown fields
// with CodeInvalidParams. Missing optional fields take River's defaults. A
// request River rejects, such as an invalid insert option, fails with
// CodeRejected, and one naming a job, queue, or transaction that doesn't
// exist with CodeNotFound.
//
// Every operation that takes a `tx` runs in the named open transaction (see
// MethodTxBegin) instead of its own. Every operation that takes a `schema`
// runs against that schema instead of the connection's default.
//
// # Jobs
//
// Adapters insert jobs of kind KindEcho whose args are always the complete
// object {"behavior": ..., "duration_ms": ..., "message": ...}, with every
// key present, and work them with a built-in worker that follows the
// behavior (see the Behavior constants). Jobs are reported as Job values.
package protocol

import (
	"encoding/json"
	"time"
)

// Methods of the contract.
const (
	// MethodCancel cancels a job with River's job cancel. Params are
	// JobParams; the result is the Job River returns.
	MethodCancel = "cancel"

	// MethodHandshake identifies the adapter. Params are empty; the result is
	// HandshakeResult.
	MethodHandshake = "handshake"

	// MethodInsert inserts a batch of jobs in one call to River's insert
	// many. Params are InsertParams; the result is InsertResult.
	MethodInsert = "insert"

	// MethodList lists jobs with River's job list. Params are ListParams; the
	// result is ListResult.
	MethodList = "list"

	// MethodMigrate runs River's migrator on the main line. Params are
	// MigrateParams; the result is MigrateResult.
	MethodMigrate = "migrate"

	// MethodQueue pauses, resumes, or updates a queue. Params are
	// QueueParams; the result is empty.
	MethodQueue = "queue"

	// MethodRelease releases a barrier that jobs with BehaviorBarrierWait or
	// BehaviorBarrierOutput, or a client started with a claim barrier, wait
	// on. Releasing a barrier before anything waits on it is allowed, and
	// later waits then pass. Params are ReleaseParams; the result is empty.
	MethodRelease = "release"

	// MethodRequestResign asks the current leader to resign, through River's
	// notify API. Params are RequestResignParams; the result is empty.
	MethodRequestResign = "request_resign"

	// MethodRetry retries a job with River's job retry. Params are
	// JobParams; the result is the Job River returns.
	MethodRetry = "retry"

	// MethodStart starts the adapter's one worker client. Params are
	// StartParams; the result is empty. Starting a client while one runs is
	// rejected.
	MethodStart = "start"

	// MethodStats reports what the running client observed since it started.
	// Params are empty; the result is StatsResult.
	MethodStats = "stats"

	// MethodStop stops the running client. Params are StopParams; the result
	// is empty.
	MethodStop = "stop"

	// MethodTxBegin opens a named transaction. Params are TxParams; the
	// result is empty.
	MethodTxBegin = "tx_begin"

	// MethodTxEnd commits or rolls back a named transaction. Params are
	// TxEndParams; the result is empty. The transaction is closed even when
	// its commit fails.
	MethodTxEnd = "tx_end"
)

// Error codes. The first four are JSON-RPC 2.0's own.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternal       = -32603

	// CodeNotFound is returned when a job, queue, or transaction named by a
	// request doesn't exist.
	CodeNotFound = -32001

	// CodeRejected is returned when River rejects a request or fails to
	// complete it, such as an invalid insert option or a database error.
	CodeRejected = -32002
)

// Behaviors of the built-in worker, selected by a job's `behavior` arg.
const (
	// BehaviorBarrierOutput waits like BehaviorBarrierWait and then records
	// the output {"race": "worker"}.
	BehaviorBarrierOutput = "barrier_output"

	// BehaviorBarrierWait waits until the barrier named by the job's message
	// is released, then completes.
	BehaviorBarrierWait = "barrier_wait"

	// BehaviorCancel cancels the job with River's job cancel error.
	BehaviorCancel = "cancel"

	// BehaviorComplete completes immediately. It's the empty behavior.
	BehaviorComplete = ""

	// BehaviorCooperativeCancel waits until the work context is cancelled and
	// returns its error. If the context is already cancelled when work
	// starts, it counts StatsResult.CancelledAtStart first.
	BehaviorCooperativeCancel = "cooperative_cancel"

	// BehaviorError fails with the error "conformance retryable error".
	BehaviorError = "error"

	// BehaviorOutput records the output {"message": <message>}.
	BehaviorOutput = "output"

	// BehaviorResumableCursor runs three resumable steps. Step "first" sets
	// metadata "first_attempt" to the attempt. Step "second" is a cursor step:
	// on attempt 1 it sets its cursor to 7 and fails, and on later attempts it
	// requires cursor 7 and sets metadata "cursor_observed" to it. Step
	// "third" fails on attempt 2.
	BehaviorResumableCursor = "resumable_cursor"

	// BehaviorSleep sleeps for duration_ms, then completes.
	BehaviorSleep = "sleep"

	// BehaviorSnoozeOnce snoozes for duration_ms (at least 1 ms) when the
	// job's metadata has no "snoozes" key, and completes otherwise.
	BehaviorSnoozeOnce = "snooze_once"
)

// Values shared by every adapter.
const (
	// ErrorRetryable is the error BehaviorError fails with.
	ErrorRetryable = "conformance retryable error"

	// KindEcho is the kind of every job an adapter inserts, and the kind its
	// built-in worker is registered under unless StartParams.WorkerKinds
	// says otherwise.
	KindEcho = "conformance_echo"

	// KindEchoPeer and KindEchoRenamed are the other kinds the built-in
	// worker can be registered under. KindEchoRenamed keeps KindEcho as a
	// kind alias, as after a safe rename.
	KindEchoPeer    = "conformance_echo_peer"
	KindEchoRenamed = "conformance_echo_renamed"

	// PeriodicJobID is the ID of the periodic job StartParams.PeriodicRunOnStart
	// configures, and PeriodicMarkerJobID that of its marker job.
	PeriodicJobID       = "conformance-periodic"
	PeriodicMarkerJobID = "conformance-periodic-marker"
)

// Queue actions.
const (
	QueueActionPause  = "pause"
	QueueActionResume = "resume"
	QueueActionUpdate = "update"
)

// Args are the args of a KindEcho job.
type Args struct {
	Behavior   string `json:"behavior"`
	DurationMS int64  `json:"duration_ms"`
	Message    string `json:"message"`
}

// AttemptError is one entry of a job's errors.
type AttemptError struct {
	At      time.Time `json:"at"`
	Attempt int       `json:"attempt"`
	Error   string    `json:"error"`
	Trace   string    `json:"trace"`
}

// Error is a JSON-RPC 2.0 error.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Message }

// HandshakeResult identifies an adapter.
type HandshakeResult struct {
	// Driver is "postgres" or "sqlite".
	Driver string `json:"driver"`

	// Implementation is "go", "rust", or "js".
	Implementation string `json:"implementation"`

	// Version is the implementation's version.
	Version string `json:"version"`
}

// InsertJob is one job to insert.
type InsertJob struct {
	Args

	Opts *InsertOpts `json:"opts,omitempty"`
}

// InsertOpts are River's insert options. Zero values take River's defaults.
type InsertOpts struct {
	MaxAttempts int             `json:"max_attempts,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	Pending     bool            `json:"pending,omitempty"`
	Priority    int             `json:"priority,omitempty"`
	Queue       string          `json:"queue,omitempty"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
	Tags        []string        `json:"tags,omitempty"`
	Unique      *UniqueOpts     `json:"unique,omitempty"`
}

// InsertParams are the params of MethodInsert.
type InsertParams struct {
	Jobs   []InsertJob `json:"jobs"`
	Schema string      `json:"schema,omitempty"`
	Tx     string      `json:"tx,omitempty"`
}

// InsertResult is the result of MethodInsert, in input order.
type InsertResult struct {
	Results []JobInsertResult `json:"results"`
}

// Job is a job row as an adapter reports it. Times are RFC 3339 in UTC with the
// precision the database stored. Metadata leaves out "river:unique_nonce",
// which is random. UniqueKey is lowercase hex, and UniqueStates are the
// state names in alphabetical order.
type Job struct {
	Args         map[string]any `json:"args"`
	Attempt      int            `json:"attempt"`
	AttemptedAt  *time.Time     `json:"attempted_at"`
	AttemptedBy  []string       `json:"attempted_by"`
	CreatedAt    time.Time      `json:"created_at"`
	Errors       []AttemptError `json:"errors"`
	FinalizedAt  *time.Time     `json:"finalized_at"`
	ID           int64          `json:"id"`
	Kind         string         `json:"kind"`
	MaxAttempts  int            `json:"max_attempts"`
	Metadata     map[string]any `json:"metadata"`
	Priority     int            `json:"priority"`
	Queue        string         `json:"queue"`
	ScheduledAt  time.Time      `json:"scheduled_at"`
	State        string         `json:"state"`
	Tags         []string       `json:"tags"`
	UniqueKey    *string        `json:"unique_key"`
	UniqueStates []string       `json:"unique_states"`
}

// JobInsertResult is the outcome of inserting one job.
type JobInsertResult struct {
	Job                      Job  `json:"job"`
	UniqueSkippedAsDuplicate bool `json:"unique_skipped_as_duplicate"`
}

// JobParams name one job, for MethodCancel and MethodRetry.
type JobParams struct {
	ID     int64  `json:"id"`
	Schema string `json:"schema,omitempty"`
	Tx     string `json:"tx,omitempty"`
}

// ListParams are the params of MethodList, mapped onto River's job list
// params. OrderBy is "id" (the default), "finalized_at", "scheduled_at", or
// "time", and Direction "asc" (the default) or "desc". After is a cursor
// from a previous ListResult. Metadata is a JSON containment filter, which
// SQLite doesn't support.
type ListParams struct {
	After      string          `json:"after,omitempty"`
	Direction  string          `json:"direction,omitempty"`
	IDs        []int64         `json:"ids,omitempty"`
	Kinds      []string        `json:"kinds,omitempty"`
	Limit      int             `json:"limit,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
	OrderBy    string          `json:"order_by,omitempty"`
	Priorities []int           `json:"priorities,omitempty"`
	Queues     []string        `json:"queues,omitempty"`
	Schema     string          `json:"schema,omitempty"`
	States     []string        `json:"states,omitempty"`
	TagsAll    []string        `json:"tags_all,omitempty"`
	Tx         string          `json:"tx,omitempty"`
}

// ListResult is the result of MethodList. Cursor is the text of River's
// cursor after the last job, or nil when no job was listed.
type ListResult struct {
	Cursor *string `json:"cursor"`
	Jobs   []Job   `json:"jobs"`
}

// MigrateParams are the params of MethodMigrate. Direction is "up" (the
// default) or "down". TargetVersion is the version to migrate to; omitted,
// up migrates to the latest version and down one step, and -1 migrates down
// past the first version.
type MigrateParams struct {
	Direction     string `json:"direction,omitempty"`
	Schema        string `json:"schema,omitempty"`
	TargetVersion *int   `json:"target_version,omitempty"`
}

// MigrateResult lists the versions a migration applied, in the order it
// applied them.
type MigrateResult struct {
	Versions []int `json:"versions"`
}

// QueueParams are the params of MethodQueue. Name "*" pauses or resumes
// every queue. Metadata is the new metadata of an update.
type QueueParams struct {
	Action   string          `json:"action"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
	Name     string          `json:"name"`
	Schema   string          `json:"schema,omitempty"`
	Tx       string          `json:"tx,omitempty"`
}

// ReleaseParams name a barrier.
type ReleaseParams struct {
	Name string `json:"name"`
}

// Request is a JSON-RPC 2.0 request.
type Request struct {
	ID      int64           `json:"id"`
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// RequestResignParams are the params of MethodRequestResign.
type RequestResignParams struct {
	Schema string `json:"schema,omitempty"`
	Tx     string `json:"tx,omitempty"`
}

// Response is a JSON-RPC 2.0 response.
type Response struct {
	Error   *Error          `json:"error,omitempty"`
	ID      int64           `json:"id"`
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
}

// StartParams configure the worker client MethodStart starts. Zero values
// take River's defaults, except as noted.
type StartParams struct {
	// ClaimBarrier, if set, makes the client's first fetch that claims jobs
	// hold them, already running, until the barrier is released.
	ClaimBarrier string `json:"claim_barrier,omitempty"`

	// ClientID is River's client ID.
	ClientID string `json:"client_id"`

	// ErrorHandlerCancel installs an error handler that cancels every job
	// whose attempt fails and counts its calls in
	// StatsResult.ErrorHandlerCalls.
	ErrorHandlerCancel bool `json:"error_handler_cancel,omitempty"`

	FetchOnlyKnownKinds    bool  `json:"fetch_only_known_kinds,omitempty"`
	FetchPollIntervalMS    int64 `json:"fetch_poll_interval_ms,omitempty"`
	JobTimeoutMS           int64 `json:"job_timeout_ms,omitempty"`
	LeaderElectionDisabled bool  `json:"leader_election_disabled,omitempty"`

	// MaxWorkers of each queue. The default is 4.
	MaxWorkers int `json:"max_workers,omitempty"`

	// PeriodicRunOnStart configures a periodic job with ID PeriodicJobID,
	// run on start and then hourly, that inserts a KindEcho job with metadata
	// {"periodic": true}, and counts StatsResult.PeriodicStarts each time the
	// client's periodic job enqueuer starts. PeriodicUnique makes that job
	// unique by args and queue, and adds a periodic job with ID
	// PeriodicMarkerJobID, configured after it, that inserts a non-unique
	// job.
	PeriodicRunOnStart bool `json:"periodic_run_on_start,omitempty"`
	PeriodicUnique     bool `json:"periodic_unique,omitempty"`

	PollOnly bool `json:"poll_only,omitempty"`

	// Queues are the queues the client works. The default is "default"
	// alone.
	Queues []string `json:"queues,omitempty"`

	RescueAfterMS int64 `json:"rescue_after_ms,omitempty"`

	// RetryDelayMS installs a retry policy that retries every failed attempt
	// after this delay.
	RetryDelayMS int64 `json:"retry_delay_ms,omitempty"`

	Schema string `json:"schema,omitempty"`

	// Tuning shortens maintenance intervals. An implementation applies what
	// it exposes and ignores the rest, so scenarios can't depend on it.
	Tuning *Tuning `json:"tuning,omitempty"`

	// WorkerKinds are the kinds the built-in worker is registered under. The
	// default is KindEcho alone.
	WorkerKinds []string `json:"worker_kinds,omitempty"`
}

// StatsResult is what a running client observed since it started.
type StatsResult struct {
	// CancelledAtStart counts BehaviorCooperativeCancel jobs whose context was
	// already cancelled when work started.
	CancelledAtStart int `json:"cancelled_at_start"`

	// ErrorHandlerCalls counts calls of the error handler
	// StartParams.ErrorHandlerCancel installs.
	ErrorHandlerCalls int `json:"error_handler_calls"`

	// Events are the kinds of the River events the client emitted, in order:
	// job_cancelled, job_completed, job_failed, job_snoozed, queue_paused,
	// and queue_resumed.
	Events []string `json:"events"`

	// PeriodicStarts counts starts of the client's periodic job enqueuer.
	PeriodicStarts int `json:"periodic_starts"`
}

// StopParams are the params of MethodStop. Cancel stops with River's stop
// and cancel instead of a graceful stop.
type StopParams struct {
	Cancel bool `json:"cancel,omitempty"`
}

// Tuning are optional maintenance intervals.
type Tuning struct {
	ElectIntervalMS     int64 `json:"elect_interval_ms,omitempty"`
	RescuerIntervalMS   int64 `json:"rescuer_interval_ms,omitempty"`
	SchedulerIntervalMS int64 `json:"scheduler_interval_ms,omitempty"`
}

// TxEndParams are the params of MethodTxEnd.
type TxEndParams struct {
	Commit bool   `json:"commit,omitempty"`
	Tx     string `json:"tx"`
}

// TxParams are the params of MethodTxBegin.
type TxParams struct {
	Tx string `json:"tx"`
}

// UniqueOpts are River's unique options. ByState lists state names.
type UniqueOpts struct {
	ByArgs      bool     `json:"by_args,omitempty"`
	ByPeriodMS  int64    `json:"by_period_ms,omitempty"`
	ByQueue     bool     `json:"by_queue,omitempty"`
	ByState     []string `json:"by_state,omitempty"`
	ExcludeKind bool     `json:"exclude_kind,omitempty"`
}
