// Command riverconformanceadapter exposes River Go through the shared
// newline-delimited JSON-RPC conformance protocol.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "modernc.org/sqlite"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

const (
	adapterVersion   = 11
	protocolRevision = 1
)

var adapterMethods = []string{ //nolint:gochecknoglobals
	"barrier_create",
	"barrier_release",
	"benchmark_enqueue",
	"cancel",
	"clock_set",
	"connection_count",
	"delete",
	"delete_many",
	"fault_disconnect_application",
	"fault_disconnect_listeners",
	"fault_expire_leader",
	"get",
	"handshake",
	"insert",
	"insert_many",
	"insert_many_fast",
	"leader",
	"list",
	"listener_count",
	"migrate",
	"queue_add",
	"queue_get",
	"queue_list",
	"queue_pause",
	"queue_remove",
	"queue_resume",
	"queue_update",
	"raw_finalize",
	"raw_insert_full_row",
	"raw_insert_no_notify",
	"raw_job_timestamps",
	"request_resign",
	"reset",
	"retry",
	"retry_delay",
	"rng_seed",
	"runtime_stats",
	"start",
	"stop",
	"tx_begin",
	"tx_cancel",
	"tx_commit",
	"tx_delete",
	"tx_delete_many",
	"tx_fail",
	"tx_get",
	"tx_insert",
	"tx_insert_many",
	"tx_insert_many_fast",
	"tx_list",
	"tx_queue_get",
	"tx_queue_list",
	"tx_queue_pause",
	"tx_queue_resume",
	"tx_queue_update",
	"tx_retry",
	"tx_rollback",
	"tx_update",
	"unique_key",
	"update",
	"wait",
	"work",
}

var capabilities = []string{ //nolint:gochecknoglobals
	"barriers",
	"cancel",
	"custom_schema",
	"deterministic_controls",
	"extensions",
	"fast_insert",
	"fault_injection",
	"get",
	"insert",
	"job_crud",
	"leadership",
	"lifecycle",
	"maintenance",
	"migrate",
	"notifications",
	"periodic_jobs",
	"poll_only",
	"queues",
	"reset",
	"resumable_jobs",
	"retry",
	"subscriptions",
	"transactions",
	"unique_jobs",
	"work",
}

var sqliteAdapterMethods = []string{ //nolint:gochecknoglobals
	"cancel",
	"clock_set",
	"delete",
	"delete_many",
	"get",
	"handshake",
	"insert",
	"insert_many",
	"insert_many_fast",
	"list",
	"migrate",
	"raw_job_timestamps",
	"reset",
	"retry",
	"retry_delay",
	"rng_seed",
	"tx_begin",
	"tx_cancel",
	"tx_commit",
	"tx_delete",
	"tx_delete_many",
	"tx_get",
	"tx_insert",
	"tx_insert_many",
	"tx_insert_many_fast",
	"tx_list",
	"tx_retry",
	"tx_rollback",
	"tx_update",
	"unique_key",
	"update",
}

var sqliteCapabilities = []string{ //nolint:gochecknoglobals
	"cancel",
	"deterministic_controls",
	"fast_insert",
	"get",
	"insert",
	"job_crud",
	"lifecycle",
	"migrate",
	"reset",
	"retry",
	"transactions",
	"unique_jobs",
}

var sqliteRuntimeCapabilities = []string{ //nolint:gochecknoglobals
	"barriers", "cancel", "deterministic_controls", "extensions", "fast_insert", "get", "insert",
	"job_crud", "leadership", "lifecycle", "migrate", "notifications",
	"periodic_jobs", "poll_only", "queues", "reset", "resumable_jobs", "retry", "scheduler",
	"subscriptions", "transactions", "unique_jobs", "work",
}

var sqliteRuntimeMethods = []string{ //nolint:gochecknoglobals
	"barrier_create", "barrier_release", "cancel", "clock_set", "delete", "delete_many", "get",
	"handshake", "insert", "insert_many", "insert_many_fast", "leader", "list", "migrate",
	"queue_add", "queue_get", "queue_list", "queue_pause", "queue_remove", "queue_resume",
	"queue_update", "raw_finalize", "raw_insert_no_notify", "raw_job_timestamps", "request_resign", "reset", "retry", "retry_delay",
	"rng_seed", "runtime_stats", "start", "stop", "tx_begin", "tx_cancel", "tx_commit",
	"tx_delete", "tx_delete_many", "tx_get", "tx_insert", "tx_insert_many", "tx_insert_many_fast",
	"tx_list", "tx_queue_get", "tx_queue_list", "tx_queue_pause", "tx_queue_resume",
	"tx_queue_update", "tx_retry", "tx_rollback", "tx_update", "unique_key", "update", "wait", "work",
}

type request struct {
	ID      any             `json:"id"`
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type response struct {
	Error   *responseError `json:"error,omitempty"`
	ID      any            `json:"id"`
	JSONRPC string         `json:"jsonrpc"`
	Result  any            `json:"result,omitempty"`
}

type responseError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type conformanceArgs struct {
	Behavior   string `json:"behavior"`
	DurationMS uint64 `json:"duration_ms"`
	Message    string `json:"message"`
}

func (conformanceArgs) Kind() string { return "conformance_echo" }

type uniqueAllArgs struct {
	Alpha   string `json:"alpha"`
	Maximum int64  `json:"maximum"`
	Zeta    string `json:"zeta"`
}

func (uniqueAllArgs) Kind() string { return "conformance_all_args" }

type uniqueNumericArgs struct {
	Exponent        float64 `json:"exponent"`
	Fraction        float64 `json:"fraction"`
	Maximum         int64   `json:"maximum"`
	Minimum         int64   `json:"minimum"`
	UnsignedMaximum uint64  `json:"unsigned_maximum"`
}

func (uniqueNumericArgs) Kind() string { return "conformance_numeric_boundaries" }

type uniqueSelectedAccount struct {
	ID      string `json:"id"      river:"unique"`
	Ignored string `json:"ignored"`
}

type uniqueSelectedArgs struct {
	Account uniqueSelectedAccount `json:"account"`
	Ignored bool                  `json:"ignored"`
	Label   string                `json:"label"   river:"unique"`
}

func (uniqueSelectedArgs) Kind() string { return "conformance_selected_args" }

type uniqueSimpleArgs struct {
	ID int64 `json:"id"`
}

func (uniqueSimpleArgs) Kind() string { return "conformance_simple" }

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func (fixedClock) NowOrNil() *time.Time { return nil }

type conformanceWorker struct {
	river.WorkerDefaults[conformanceArgs]

	barriers *barrierRegistry
	pool     *pgxpool.Pool
	probe    *runtimeProbe
}

func (w *conformanceWorker) Work(ctx context.Context, job *river.Job[conformanceArgs]) error {
	switch job.Args.Behavior {
	case "barrier_output", "barrier_wait":
		if err := w.barriers.wait(ctx, job.Args.Message); err != nil {
			return err
		}
		if job.Args.Behavior == "barrier_output" {
			return river.RecordOutput(ctx, map[string]any{"race": "worker"})
		}
		return nil
	case "cancel":
		return river.JobCancel(errors.New("cancelled by conformance worker"))
	case "cooperative_cancel":
		<-ctx.Done()
		return ctx.Err()
	case "discard":
		return errors.New("conformance discard")
	case "error":
		return errors.New("conformance retryable error")
	case "ignored_cancel":
		select {}
	case "output":
		return river.RecordOutput(ctx, map[string]any{"message": job.Args.Message})
	case "panic":
		panic("conformance worker panic")
	case "sleep":
		duration, err := durationFromMilliseconds(job.Args.DurationMS)
		if err != nil {
			return err
		}
		time.Sleep(duration)
	case "snooze_once", "snooze_then_cancel":
		var metadata map[string]any
		if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
			return err
		}
		if _, alreadySnoozed := metadata["snoozes"]; !alreadySnoozed {
			duration, err := durationFromMilliseconds(max(job.Args.DurationMS, 1))
			if err != nil {
				return err
			}
			return river.JobSnooze(duration)
		}
		if job.Args.Behavior == "snooze_then_cancel" {
			<-ctx.Done()
			return ctx.Err()
		}
	case "resumable":
		river.ResumableStep(ctx, "first", nil, func(ctx context.Context) error {
			w.probe.incrementResumableFirst()
			return nil
		})
		river.ResumableStep(ctx, "second", nil, func(ctx context.Context) error {
			w.probe.incrementResumableSecond()
			if job.Attempt == 1 {
				return errors.New("fail second resumable step once")
			}
			return nil
		})
	case "transactional_complete":
		if w.pool == nil {
			return errors.New("transactional completion is unavailable for this backend")
		}
		if err := river.MetadataSet(ctx, "transactional_completion", true); err != nil {
			return err
		}
		tx, err := w.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	return nil
}

type runtimeProbe struct {
	errorHandlerCalls   int
	events              []string
	mu                  sync.Mutex
	periodicStarts      int
	resumableFirstRuns  int
	resumableSecondRuns int
	trace               []string
}

func (p *runtimeProbe) incrementErrorHandlerCalls() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errorHandlerCalls++
}

func (p *runtimeProbe) addEvent(kind river.EventKind) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, string(kind))
}

func (p *runtimeProbe) addTrace(entry string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.trace = append(p.trace, entry)
}

func (p *runtimeProbe) incrementPeriodicStarts() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.periodicStarts++
}

func (p *runtimeProbe) incrementResumableFirst() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resumableFirstRuns++
}

func (p *runtimeProbe) incrementResumableSecond() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resumableSecondRuns++
}

func (p *runtimeProbe) snapshot() map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return map[string]any{
		"error_handler_calls":   p.errorHandlerCalls,
		"events":                slices.Clone(p.events),
		"periodic_starts":       p.periodicStarts,
		"resumable_first_runs":  p.resumableFirstRuns,
		"resumable_second_runs": p.resumableSecondRuns,
		"trace":                 slices.Clone(p.trace),
	}
}

type conformanceErrorHandler struct {
	probe *runtimeProbe
}

func (h *conformanceErrorHandler) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *river.ErrorHandlerResult {
	h.probe.incrementErrorHandlerCalls()
	return &river.ErrorHandlerResult{SetCancelled: true}
}

func (h *conformanceErrorHandler) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *river.ErrorHandlerResult {
	h.probe.incrementErrorHandlerCalls()
	return &river.ErrorHandlerResult{SetCancelled: true}
}

type conformancePlugin struct {
	river.PluginDefaults

	probe *runtimeProbe
}

func (p *conformancePlugin) InsertBegin(_ context.Context, _ *rivertype.JobInsertParams) error { //nolint:unparam // River hook signature requires an error result.
	p.probe.addTrace("hook:insert_begin")
	return nil
}

func (p *conformancePlugin) InsertMany(ctx context.Context, _ []*rivertype.JobInsertParams, doInner func(context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	p.probe.addTrace("middleware:insert_before")
	results, err := doInner(ctx)
	p.probe.addTrace("middleware:insert_after")
	return results, err
}

func (p *conformancePlugin) Start(_ context.Context, _ *rivertype.HookPeriodicJobsStartParams) error { //nolint:unparam // River hook signature requires an error result.
	p.probe.incrementPeriodicStarts()
	p.probe.addTrace("hook:periodic_start")
	return nil
}

func (p *conformancePlugin) WorkBegin(_ context.Context, _ *rivertype.JobRow) error { //nolint:unparam // River hook signature requires an error result.
	p.probe.addTrace("hook:work_begin")
	return nil
}

func (p *conformancePlugin) Work(ctx context.Context, _ *rivertype.JobRow, doInner func(context.Context) error) error {
	p.probe.addTrace("middleware:work_before")
	err := doInner(ctx)
	p.probe.addTrace("middleware:work_after")
	return err
}

func (p *conformancePlugin) WorkEnd(_ context.Context, _ *rivertype.JobRow, err error) error {
	p.probe.addTrace("hook:work_end")
	return err
}

type fixedRetryPolicy struct{ delay time.Duration }

func (p fixedRetryPolicy) NextRetry(job *rivertype.JobRow) time.Time {
	return time.Now().UTC().Add(p.delay)
}

type barrierRegistry struct {
	mu      sync.Mutex
	waiters map[string]chan struct{}
}

func newBarrierRegistry() *barrierRegistry {
	return &barrierRegistry{waiters: make(map[string]chan struct{})}
}

func (r *barrierRegistry) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, waiter := range r.waiters {
		close(waiter)
	}
	r.waiters = make(map[string]chan struct{})
}

func (r *barrierRegistry) create(name string) error {
	if name == "" {
		return errors.New("barrier name is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.waiters[name]; exists {
		return fmt.Errorf("barrier %q already exists", name)
	}
	r.waiters[name] = make(chan struct{})
	return nil
}

func (r *barrierRegistry) release(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	waiter, exists := r.waiters[name]
	if !exists {
		return fmt.Errorf("barrier %q not found", name)
	}
	close(waiter)
	delete(r.waiters, name)
	return nil
}

func (r *barrierRegistry) wait(ctx context.Context, name string) error {
	r.mu.Lock()
	waiter, exists := r.waiters[name]
	r.mu.Unlock()
	if !exists {
		return fmt.Errorf("barrier %q not found", name)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waiter:
		return nil
	}
}

type insertParams struct {
	Behavior   string           `json:"behavior"`
	DurationMS uint64           `json:"duration_ms"`
	Kind       string           `json:"kind"`
	Message    string           `json:"message"`
	Opts       insertOptsParams `json:"opts"`
	Schema     string           `json:"schema"`
}

func (p insertParams) args() conformanceArgs {
	return conformanceArgs{Behavior: p.Behavior, DurationMS: p.DurationMS, Message: p.Message}
}

type insertOptsParams struct {
	MaxAttempts *int             `json:"max_attempts"`
	Metadata    json.RawMessage  `json:"metadata"`
	Pending     bool             `json:"pending"`
	Priority    *int             `json:"priority"`
	Queue       *string          `json:"queue"`
	ScheduledAt *time.Time       `json:"scheduled_at"`
	Tags        []string         `json:"tags"`
	Unique      uniqueOptsParams `json:"unique"`
}

type uniqueOptsParams struct {
	ByArgs      bool                 `json:"by_args"`
	ByPeriodMS  *uint64              `json:"by_period_ms"`
	ByQueue     bool                 `json:"by_queue"`
	ByState     []rivertype.JobState `json:"by_state"`
	ExcludeKind bool                 `json:"exclude_kind"`
}

type uniqueKeyParams struct {
	Args    json.RawMessage `json:"args"`
	Kind    string          `json:"kind"`
	Now     time.Time       `json:"now"`
	Options struct {
		ByArgs        bool                 `json:"by_args"`
		ByPeriodNanos int64                `json:"by_period_nanos"`
		ByQueue       bool                 `json:"by_queue"`
		ByState       []rivertype.JobState `json:"by_state"`
		ExcludeKind   bool                 `json:"exclude_kind"`
	} `json:"options"`
	Queue       string     `json:"queue"`
	ScheduledAt *time.Time `json:"scheduled_at"`
}

func (p uniqueKeyParams) jobArgs() (rivertype.JobArgs, error) {
	var args rivertype.JobArgs
	switch p.Kind {
	case "conformance_all_args":
		args = &uniqueAllArgs{}
	case "conformance_numeric_boundaries":
		var encoded struct {
			Exponent        float64     `json:"exponent"`
			Fraction        float64     `json:"fraction"`
			Maximum         json.Number `json:"maximum"`
			Minimum         json.Number `json:"minimum"`
			UnsignedMaximum json.Number `json:"unsigned_maximum"`
		}
		decoder := json.NewDecoder(bytes.NewReader(p.Args))
		decoder.UseNumber()
		if err := decoder.Decode(&encoded); err != nil {
			return nil, err
		}
		maximum, err := strconv.ParseInt(encoded.Maximum.String(), 10, 64)
		if err != nil {
			return nil, err
		}
		minimum, err := strconv.ParseInt(encoded.Minimum.String(), 10, 64)
		if err != nil {
			return nil, err
		}
		unsignedMaximum, err := strconv.ParseUint(encoded.UnsignedMaximum.String(), 10, 64)
		if err != nil {
			return nil, err
		}
		return &uniqueNumericArgs{
			Exponent: encoded.Exponent, Fraction: encoded.Fraction, Maximum: maximum,
			Minimum: minimum, UnsignedMaximum: unsignedMaximum,
		}, nil
	case "conformance_selected_args":
		args = &uniqueSelectedArgs{}
	case "conformance_simple":
		args = &uniqueSimpleArgs{}
	default:
		return nil, fmt.Errorf("unsupported unique fixture kind %q", p.Kind)
	}
	if err := json.Unmarshal(p.Args, args); err != nil {
		return nil, err
	}
	return args, nil
}

func (p insertOptsParams) opts() (*river.InsertOpts, error) {
	opts := &river.InsertOpts{
		Metadata:    p.Metadata,
		Pending:     p.Pending,
		ScheduledAt: valueOrZero(p.ScheduledAt),
		Tags:        p.Tags,
		UniqueOpts: river.UniqueOpts{
			ByArgs:      p.Unique.ByArgs,
			ByQueue:     p.Unique.ByQueue,
			ByState:     p.Unique.ByState,
			ExcludeKind: p.Unique.ExcludeKind,
		},
	}
	if p.MaxAttempts != nil {
		opts.MaxAttempts = *p.MaxAttempts
	}
	if p.Priority != nil {
		opts.Priority = *p.Priority
	}
	if p.Queue != nil {
		opts.Queue = *p.Queue
	}
	if p.Unique.ByPeriodMS != nil {
		byPeriod, err := durationFromMilliseconds(*p.Unique.ByPeriodMS)
		if err != nil {
			return nil, fmt.Errorf("unique period: %w", err)
		}
		opts.UniqueOpts.ByPeriod = byPeriod
	}
	return opts, nil
}

func handleUniqueKey(rawParams json.RawMessage) (any, error) {
	var params uniqueKeyParams
	if err := json.Unmarshal(rawParams, &params); err != nil {
		return nil, err
	}
	args, err := params.jobArgs()
	if err != nil {
		return nil, err
	}
	opts := &dbunique.UniqueOpts{
		ByArgs:      params.Options.ByArgs,
		ByPeriod:    time.Duration(params.Options.ByPeriodNanos),
		ByQueue:     params.Options.ByQueue,
		ByState:     params.Options.ByState,
		ExcludeKind: params.Options.ExcludeKind,
	}
	key, err := dbunique.UniqueKey(fixedClock{now: params.Now}, opts, &rivertype.JobInsertParams{
		Args:         args,
		EncodedArgs:  params.Args,
		Kind:         params.Kind,
		Queue:        params.Queue,
		ScheduledAt:  params.ScheduledAt,
		UniqueStates: opts.StateBitmask(),
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"sha256":     hex.EncodeToString(key),
		"state_mask": opts.StateBitmask(),
	}, nil
}

func handleQueueAdd(
	ctx context.Context,
	rawParams json.RawMessage,
	addFunc func(string, river.QueueConfig) error,
	removeFunc func(context.Context, string) error,
) (any, error) {
	var params struct {
		MaxWorkers int    `json:"max_workers"`
		Name       string `json:"name"`
	}
	if err := json.Unmarshal(rawParams, &params); err != nil {
		return nil, err
	}
	if params.MaxWorkers == 0 {
		params.MaxWorkers = 1
	}
	err := addFunc(params.Name, river.QueueConfig{MaxWorkers: params.MaxWorkers})
	var alreadyAddedErr *river.QueueAlreadyAddedError
	if errors.As(err, &alreadyAddedErr) {
		if err := removeFunc(ctx, params.Name); err != nil {
			return nil, err
		}
		err = addFunc(params.Name, river.QueueConfig{MaxWorkers: params.MaxWorkers})
	}
	return map[string]any{}, err
}

type runningClient struct {
	client             *river.Client[pgx.Tx]
	probe              *runtimeProbe
	subscription       <-chan *river.Event
	subscriptionCancel func()
}

type adapterState struct {
	barriers     *barrierRegistry
	clock        *time.Time
	pool         *pgxpool.Pool
	rngSeed      uint64
	running      *runningClient
	transactions map[string]pgx.Tx
}

type requestHandler interface {
	handle(ctx context.Context, request *request) (any, error)
}

type sqliteAdapterState struct {
	barriers     *barrierRegistry
	clock        *time.Time
	pool         *sql.DB
	profile      string
	rngSeed      uint64
	running      *sqliteRunningClient
	transactions map[string]*sql.Tx
}

type sqliteRunningClient struct {
	client             *river.Client[*sql.Tx]
	probe              *runtimeProbe
	subscription       <-chan *river.Event
	subscriptionCancel func()
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "River Go conformance adapter:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	if databaseURL == "" {
		return errors.New("RIVER_CONFORMANCE_DATABASE_URL is required")
	}
	databaseKind := os.Getenv("RIVER_CONFORMANCE_DATABASE_KIND")
	if databaseKind == "sqlite" {
		profile := os.Getenv("RIVER_CONFORMANCE_PROFILE")
		if profile == "" {
			profile = "portable-storage-v1"
		}
		if profile != "portable-storage-v1" && profile != "sqlite-runtime-v1" {
			return fmt.Errorf("unsupported SQLite conformance profile %q", profile)
		}
		pool, err := sql.Open("sqlite", databaseURL)
		if err != nil {
			return err
		}
		defer pool.Close()
		pool.SetMaxOpenConns(1)
		if _, err := pool.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
			return fmt.Errorf("enable SQLite WAL: %w", err)
		}
		if _, err := pool.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
			return fmt.Errorf("set SQLite busy timeout: %w", err)
		}
		if _, err := pool.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
			return fmt.Errorf("enable SQLite foreign keys: %w", err)
		}
		state := &sqliteAdapterState{
			barriers:     newBarrierRegistry(),
			pool:         pool,
			profile:      profile,
			transactions: make(map[string]*sql.Tx),
		}
		err = runRequestLoop(ctx, state)
		if state.running != nil {
			stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			_ = state.running.client.StopAndCancel(stopCtx)
			state.running.subscriptionCancel()
		}
		return err
	}
	if databaseKind != "" && databaseKind != "postgres" {
		return fmt.Errorf("unsupported RIVER_CONFORMANCE_DATABASE_KIND %q", databaseKind)
	}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return err
	}
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "river-conformance-go"
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return err
	}
	defer pool.Close()
	state := &adapterState{
		barriers:     newBarrierRegistry(),
		pool:         pool,
		transactions: make(map[string]pgx.Tx),
	}
	err = runRequestLoop(ctx, state)
	if state.running != nil {
		stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_ = state.running.client.StopAndCancel(stopCtx)
		state.running.subscriptionCancel()
	}
	return err
}

func runRequestLoop(ctx context.Context, state requestHandler) error {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	for scanner.Scan() {
		var req request
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			if err := encoder.Encode(errorResponse(nil, -32700, err)); err != nil {
				return err
			}
			continue
		}
		if req.JSONRPC != "2.0" {
			if err := encoder.Encode(errorResponse(req.ID, -32600, errors.New("jsonrpc must be 2.0"))); err != nil {
				return err
			}
			continue
		}
		result, err := state.handle(ctx, &req)
		res := response{ID: req.ID, JSONRPC: "2.0", Result: result}
		if err != nil {
			res = errorResponse(req.ID, -32000, err)
		}
		if err := encoder.Encode(&res); err != nil {
			return err
		}
	}
	return scanner.Err()
}

//nolint:cyclop,funlen,gocognit,maintidx
func (s *adapterState) handle(ctx context.Context, req *request) (any, error) {
	switch req.Method {
	case "handshake":
		return map[string]any{
			"adapter_version":        adapterVersion,
			"backend":                "postgres",
			"capabilities":           capabilities,
			"implementation":         "go",
			"implementation_version": "0.44.0-development",
			"methods":                adapterMethods,
			"migration_lines":        map[string]int{"main": 7},
			"profile":                "postgres-full-v1",
			"protocol_revision":      protocolRevision,
		}, nil

	case "migrate":
		var params struct {
			Direction     string `json:"direction"`
			DryRun        bool   `json:"dry_run"`
			MaxSteps      *int   `json:"max_steps"`
			Schema        string `json:"schema"`
			TargetVersion *int   `json:"target_version"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.Schema != "" {
			if _, err := s.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{params.Schema}.Sanitize()); err != nil {
				return nil, err
			}
		}
		migrator, err := rivermigrate.New(riverpgxv5.New(s.pool), &rivermigrate.Config{
			Logger: adapterLogger(),
			Schema: params.Schema,
		})
		if err != nil {
			return nil, err
		}
		direction := rivermigrate.DirectionUp
		if params.Direction != "" {
			direction = rivermigrate.Direction(params.Direction)
		}
		var opts *rivermigrate.MigrateOpts
		if params.DryRun || params.MaxSteps != nil || params.TargetVersion != nil {
			opts = &rivermigrate.MigrateOpts{DryRun: params.DryRun}
			if params.MaxSteps != nil {
				opts.MaxSteps = *params.MaxSteps
			}
			if params.TargetVersion != nil {
				opts.TargetVersion = *params.TargetVersion
			}
		}
		result, err := migrator.Migrate(ctx, direction, opts)
		if err != nil {
			return nil, err
		}
		versions := make([]int, len(result.Versions))
		for i, version := range result.Versions {
			versions[i] = version.Version
		}
		existingMigrations, err := migrator.ExistingVersions(ctx)
		if err != nil {
			return nil, err
		}
		existing := make([]int, len(existingMigrations))
		for i, migration := range existingMigrations {
			existing[i] = migration.Version
		}
		validation, err := migrator.Validate(ctx, nil)
		if err != nil {
			return nil, err
		}
		return map[string]any{"applied": versions, "existing": existing, "valid": validation.OK}, nil

	case "reset":
		if s.running != nil || len(s.transactions) > 0 {
			return nil, errors.New("reset requires no running client or open transaction")
		}
		s.barriers.clear()
		var params struct {
			Schema string `json:"schema"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		table := func(name string) string {
			if params.Schema == "" {
				return pgx.Identifier{name}.Sanitize()
			}
			return pgx.Identifier{params.Schema, name}.Sanitize()
		}
		_, err := s.pool.Exec(ctx, "TRUNCATE "+table("river_job")+", "+table("river_notification")+", "+table("river_queue")+", "+table("river_leader")+" RESTART IDENTITY CASCADE")
		return map[string]any{}, err

	case "clock_set":
		var params struct {
			Now time.Time `json:"now"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		s.clock = &params.Now
		return map[string]any{}, nil

	case "rng_seed":
		var params struct {
			Seed uint64 `json:"seed"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		s.rngSeed = params.Seed
		return map[string]any{}, nil

	case "retry_delay":
		if s.clock == nil {
			return nil, errors.New("clock_set is required before retry_delay")
		}
		var params struct {
			ErrorCount uint32 `json:"error_count"`
			JobID      int64  `json:"job_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.ErrorCount < 1 {
			return nil, errors.New("error_count must be positive")
		}
		delay := deterministicRetryDelay(*s.clock, params.JobID, params.ErrorCount, s.rngSeed)
		return map[string]any{"delay_ns": delay.Nanoseconds()}, nil

	case "unique_key":
		return handleUniqueKey(req.Params)

	case "barrier_create", "barrier_release":
		var params struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if req.Method == "barrier_create" {
			return map[string]any{}, s.barriers.create(params.Name)
		}
		return map[string]any{}, s.barriers.release(params.Name)

	case "insert":
		var params insertParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		client, err := s.clientForSchema(params.Schema)
		if err != nil {
			return nil, err
		}
		opts, err := params.Opts.opts()
		if err != nil {
			return nil, err
		}
		result, err := client.Insert(ctx, params.args(), opts)
		if err != nil {
			return nil, err
		}
		return normalizeJob(result.Job), nil

	case "insert_many":
		jobs, err := decodeInsertManyParams(req.Params)
		if err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		results, err := client.InsertMany(ctx, jobs)
		if err != nil {
			return nil, err
		}
		return normalizeInsertManyResults(results), nil

	case "benchmark_enqueue":
		var params struct {
			Jobs int `json:"jobs"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.Jobs < 1 {
			return nil, errors.New("jobs must be positive")
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		latencies := make([]time.Duration, 0, params.Jobs)
		startedAt := time.Now()
		for index := range params.Jobs {
			insertedAt := time.Now()
			if _, err := client.Insert(ctx, conformanceArgs{Message: fmt.Sprintf("benchmark-enqueue-%d", index)}, nil); err != nil {
				return nil, err
			}
			latencies = append(latencies, time.Since(insertedAt))
		}
		duration := time.Since(startedAt)
		slices.Sort(latencies)
		p95 := latencies[max(0, (len(latencies)*95+99)/100-1)]
		return map[string]any{"duration_ns": duration.Nanoseconds(), "p95_ns": p95.Nanoseconds()}, nil

	case "insert_many_fast":
		var params struct {
			Jobs []insertParams `json:"jobs"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		jobs := make([]river.InsertManyParams, len(params.Jobs))
		for i, job := range params.Jobs {
			opts, err := job.Opts.opts()
			if err != nil {
				return nil, err
			}
			jobs[i] = river.InsertManyParams{Args: job.args(), InsertOpts: opts}
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		count, err := client.InsertManyFast(ctx, jobs)
		return map[string]any{"count": count}, err

	case "get": //nolint:usestdlibvars // JSON-RPC method names are lowercase protocol values.
		var params struct {
			ID     int64  `json:"id"`
			Schema string `json:"schema"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.ID < 1 {
			return nil, errors.New("id must be positive")
		}
		client, err := s.clientForSchema(params.Schema)
		if err != nil {
			return nil, err
		}
		job, err := client.JobGet(ctx, params.ID)
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "list":
		params, err := makeJobListParams(req.Params)
		if err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		result, err := client.JobList(ctx, params)
		if err != nil {
			return nil, err
		}
		return normalizeJobListResult(result)

	case "cancel", "delete", "retry": //nolint:usestdlibvars // JSON-RPC method names are lowercase protocol values.
		id, err := requestID(req.Params)
		if err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		var job *rivertype.JobRow
		switch req.Method {
		case "cancel":
			job, err = client.JobCancel(ctx, id)
		case "delete": //nolint:usestdlibvars // JSON-RPC method names are lowercase protocol values.
			job, err = client.JobDelete(ctx, id)
		case "retry":
			job, err = client.JobRetry(ctx, id)
		}
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "delete_many":
		params, err := makeJobDeleteManyParams(req.Params)
		if err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		result, err := client.JobDeleteMany(ctx, params)
		if err != nil {
			return nil, err
		}
		return map[string]any{"jobs": normalizeJobs(result.Jobs)}, nil

	case "update":
		var params struct {
			ID     int64 `json:"id"`
			Output any   `json:"output"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		job, err := client.JobUpdate(ctx, params.ID, &river.JobUpdateParams{Output: params.Output})
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "raw_finalize":
		var params struct {
			ID       int64          `json:"id"`
			Metadata map[string]any `json:"metadata"`
			State    string         `json:"state"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.State != "completed" && params.State != "discarded" {
			return nil, errors.New("state must be completed or discarded")
		}
		metadata, err := json.Marshal(params.Metadata)
		if err != nil {
			return nil, err
		}
		attemptError, err := json.Marshal(map[string]any{
			"at":      "2026-02-03T04:05:06.789Z",
			"attempt": 1,
			"error":   "external discard",
			"trace":   "external trace",
		})
		if err != nil {
			return nil, err
		}
		commandTag, err := s.pool.Exec(ctx, `
			UPDATE river_job
			SET errors = CASE WHEN $2 = 'discarded' THEN array_append(errors, $4::jsonb) ELSE errors END,
				finalized_at = $5::timestamptz,
				metadata = metadata || $3::jsonb,
				state = $2::river_job_state
			WHERE id = $1 AND state = 'running'`,
			params.ID, params.State, metadata, attemptError, "2026-02-03T04:05:06.789Z",
		)
		if err != nil {
			return nil, err
		}
		if commandTag.RowsAffected() != 1 {
			return nil, errors.New("running job not found")
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		job, err := client.JobGet(ctx, params.ID)
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "queue_add":
		if s.running == nil {
			return nil, errors.New("queue_add requires a running client")
		}
		return handleQueueAdd(ctx, req.Params, s.running.client.Queues().Add, s.running.client.Queues().Remove)

	case "queue_get":
		var params struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		queue, err := client.QueueGet(ctx, params.Name)
		if err != nil {
			return nil, err
		}
		return normalizeQueue(queue), nil

	case "queue_list":
		var params struct {
			Limit int `json:"limit"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.Limit == 0 {
			params.Limit = 100
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		result, err := client.QueueList(ctx, river.NewQueueListParams().First(params.Limit))
		if err != nil {
			return nil, err
		}
		queues := make([]any, len(result.Queues))
		for i, queue := range result.Queues {
			queues[i] = normalizeQueue(queue)
		}
		return map[string]any{"queues": queues}, nil

	case "queue_pause", "queue_resume":
		var params struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		if req.Method == "queue_pause" {
			err = client.QueuePause(ctx, params.Name, nil)
		} else {
			err = client.QueueResume(ctx, params.Name, nil)
		}
		return map[string]any{}, err

	case "queue_remove":
		if s.running == nil {
			return nil, errors.New("queue_remove requires a running client")
		}
		var params struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		return map[string]any{}, s.running.client.Queues().Remove(ctx, params.Name)

	case "queue_update":
		var params struct {
			Metadata json.RawMessage `json:"metadata"`
			Name     string          `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		queue, err := client.QueueUpdate(ctx, params.Name, &river.QueueUpdateParams{Metadata: params.Metadata})
		if err != nil {
			return nil, err
		}
		return normalizeQueue(queue), nil

	case "request_resign":
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		return map[string]any{}, client.Notify().RequestResign(ctx)

	case "leader":
		var leaderID string
		var electedAt time.Time
		err := s.pool.QueryRow(ctx, "SELECT leader_id, elected_at FROM river_leader WHERE name = 'default' AND expires_at >= now()").Scan(&leaderID, &electedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return map[string]any{"elected_at": nil, "leader_id": nil}, nil
		}
		return map[string]any{"elected_at": formatTime(electedAt), "leader_id": leaderID}, err

	case "listener_count":
		var count int
		err := s.pool.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'river-conformance-go' AND query LIKE 'LISTEN %'").Scan(&count)
		return map[string]any{"count": count}, err

	case "connection_count":
		var count int
		err := s.pool.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'river-conformance-go'").Scan(&count)
		return map[string]any{"count": count}, err

	case "fault_disconnect_listeners":
		var count int
		err := s.pool.QueryRow(ctx, "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'river-conformance-go' AND query LIKE 'LISTEN %' AND pid != pg_backend_pid()) AS terminated").Scan(&count)
		return map[string]any{"count": count}, err

	case "fault_disconnect_application":
		var params struct {
			ApplicationName string `json:"application_name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.ApplicationName != "river-conformance-go" && params.ApplicationName != "river-conformance-rust" {
			return nil, errors.New("unsupported conformance application_name")
		}
		var count int
		err := s.pool.QueryRow(ctx, "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1 AND pid != pg_backend_pid()) AS terminated", params.ApplicationName).Scan(&count)
		return map[string]any{"count": count}, err

	case "fault_expire_leader":
		_, err := s.pool.Exec(ctx, "UPDATE river_leader SET expires_at = now() - interval '1 second'")
		return map[string]any{}, err

	case "raw_insert_no_notify":
		var params insertParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		encodedArgs, err := json.Marshal(params.args())
		if err != nil {
			return nil, err
		}
		if params.Kind == "" {
			params.Kind = "conformance_echo"
		}
		maxAttempts := river.MaxAttemptsDefault
		if params.Opts.MaxAttempts != nil {
			maxAttempts = *params.Opts.MaxAttempts
		}
		var id int64
		err = s.pool.QueryRow(ctx, "INSERT INTO river_job (args, kind, max_attempts) VALUES ($1, $2, $3) RETURNING id", encodedArgs, params.Kind, maxAttempts).Scan(&id)
		if err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		job, err := client.JobGet(ctx, id)
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "raw_insert_full_row":
		var id int64
		err := s.pool.QueryRow(ctx, `
			INSERT INTO river_job (
				args, attempt, attempted_at, attempted_by, created_at, errors,
				finalized_at, kind, max_attempts, metadata, priority, queue,
				scheduled_at, state, tags, unique_key, unique_states
			) VALUES (
				'{"nested":{"enabled":true},"values":[1,"two",null]}'::jsonb,
				3, '2026-01-02T03:04:06.123456Z', ARRAY['go-client','rust-client'],
				'2026-01-02T03:04:05.6789Z',
				ARRAY['{"at":"2026-01-02T03:04:06.123456Z","attempt":3,"error":"worker failed: escaped \"detail\"","trace":"frame one\nframe two"}'::jsonb],
				'2026-01-02T03:04:07.000001Z', 'conformance_full_row', 4,
				'{"output":{"ok":true},"river:rescue_count":2,"user":"metadata"}'::jsonb,
				2, 'priority_jobs', '2026-01-02T03:04:05.999999Z', 'discarded',
				ARRAY['alpha_tag','beta_tag'], decode(repeat('ab', 32), 'hex'), B'11110101'
			) RETURNING id`).Scan(&id)
		if err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		job, err := client.JobGet(ctx, id)
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "raw_job_timestamps":
		id, err := requestID(req.Params)
		if err != nil {
			return nil, err
		}
		var createdAt, scheduledAt string
		err = s.pool.QueryRow(ctx, `
			SELECT created_at::text, scheduled_at::text
			FROM river_job
			WHERE id = $1`, id).Scan(&createdAt, &scheduledAt)
		return map[string]any{"created_at": createdAt, "scheduled_at": scheduledAt}, err

	case "start":
		if s.running != nil {
			return nil, errors.New("client already running")
		}
		var params struct {
			ClientID            string  `json:"client_id"`
			ErrorHandlerCancel  bool    `json:"error_handler_cancel"`
			FetchPollIntervalMS *uint64 `json:"fetch_poll_interval_ms"`
			Instrumented        bool    `json:"instrumented"`
			JobStuckThresholdMS *uint64 `json:"job_stuck_threshold_ms"`
			JobTimeoutMS        *uint64 `json:"job_timeout_ms"`
			MaxWorkers          int     `json:"max_workers"`
			PeriodicRunOnStart  bool    `json:"periodic_run_on_start"`
			PollOnly            bool    `json:"poll_only"`
			Queue               string  `json:"queue"`
			RetryDelayMS        *uint64 `json:"retry_delay_ms"`
			Schema              string  `json:"schema"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.MaxWorkers == 0 {
			params.MaxWorkers = 4
		}
		if params.Queue == "" {
			params.Queue = river.QueueDefault
		}
		probe := &runtimeProbe{}
		client, err := newWorkerClient(s.pool, s.barriers, workerClientConfig{
			errorHandlerCancel:  params.ErrorHandlerCancel,
			fetchPollIntervalMS: params.FetchPollIntervalMS,
			id:                  params.ClientID,
			instrumented:        params.Instrumented,
			jobStuckThresholdMS: params.JobStuckThresholdMS,
			jobTimeoutMS:        params.JobTimeoutMS,
			maxWorkers:          params.MaxWorkers,
			periodicRunOnStart:  params.PeriodicRunOnStart,
			pollOnly:            params.PollOnly,
			probe:               probe,
			queue:               params.Queue,
			retryDelayMS:        params.RetryDelayMS,
			schema:              params.Schema,
		})
		if err != nil {
			return nil, err
		}
		subscription, subscriptionCancel := client.Subscribe(
			river.EventKindJobCancelled,
			river.EventKindJobCompleted,
			river.EventKindJobFailed,
			river.EventKindJobInterrupted,
			river.EventKindJobSnoozed,
			river.EventKindQueuePaused,
			river.EventKindQueueResumed,
		)
		if err := client.Start(ctx); err != nil {
			subscriptionCancel()
			return nil, err
		}
		s.running = &runningClient{
			client:             client,
			probe:              probe,
			subscription:       subscription,
			subscriptionCancel: subscriptionCancel,
		}
		return map[string]any{}, nil

	case "stop":
		if s.running == nil {
			return nil, errors.New("client is not running")
		}
		var params struct {
			Cancel bool `json:"cancel"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if params.Cancel {
			err := s.running.client.StopAndCancel(stopCtx)
			s.running.subscriptionCancel()
			s.running = nil
			return map[string]any{}, err
		}
		err := s.running.client.Stop(stopCtx)
		s.running.subscriptionCancel()
		s.running = nil
		return map[string]any{}, err

	case "runtime_stats":
		if s.running == nil {
			return nil, errors.New("runtime_stats requires a running client")
		}
		for {
			select {
			case event := <-s.running.subscription:
				if event != nil {
					s.running.probe.addEvent(event.Kind)
				}
			default:
				return s.running.probe.snapshot(), nil
			}
		}

	case "wait":
		var params struct {
			ID     int64                `json:"id"`
			States []rivertype.JobState `json:"states"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		job, err := waitForStates(ctx, client, params.ID, params.States)
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "work":
		var params struct {
			ClientID string `json:"client_id"`
			ID       int64  `json:"id"`
			Schema   string `json:"schema"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.ID < 1 {
			return nil, errors.New("id must be positive")
		}
		if params.ClientID == "" {
			params.ClientID = "go-conformance-adapter"
		}
		probe := &runtimeProbe{}
		client, err := newWorkerClient(s.pool, s.barriers, workerClientConfig{
			id:         params.ClientID,
			maxWorkers: 1,
			probe:      probe,
			queue:      river.QueueDefault,
			schema:     params.Schema,
		})
		if err != nil {
			return nil, err
		}
		if err := client.Start(ctx); err != nil {
			return nil, err
		}
		job, waitErr := waitForStates(ctx, client, params.ID, nil)
		stopCtx, stopCancel := context.WithTimeout(ctx, 10*time.Second)
		defer stopCancel()
		stopErr := client.Stop(stopCtx)
		if waitErr != nil {
			return nil, waitErr
		}
		if stopErr != nil {
			return nil, stopErr
		}
		return normalizeJob(job), nil

	case "tx_begin":
		handle, err := requestHandle(req.Params)
		if err != nil {
			return nil, err
		}
		if _, ok := s.transactions[handle]; ok {
			return nil, fmt.Errorf("transaction %q already exists", handle)
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		s.transactions[handle] = tx
		return map[string]any{}, nil

	case "tx_insert":
		var params struct {
			Handle string       `json:"handle"`
			Job    insertParams `json:"job"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		opts, err := params.Job.Opts.opts()
		if err != nil {
			return nil, err
		}
		result, err := client.InsertTx(ctx, tx, params.Job.args(), opts)
		if err != nil {
			return nil, err
		}
		return normalizeJob(result.Job), nil

	case "tx_insert_many", "tx_insert_many_fast":
		var params struct {
			Handle string          `json:"handle"`
			Jobs   json.RawMessage `json:"jobs"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		jobs, err := decodeInsertManyParams(params.Jobs)
		if err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		if req.Method == "tx_insert_many_fast" {
			count, err := client.InsertManyFastTx(ctx, tx, jobs)
			return map[string]any{"count": count}, err
		}
		results, err := client.InsertManyTx(ctx, tx, jobs)
		if err != nil {
			return nil, err
		}
		return normalizeInsertManyResults(results), nil

	case "tx_get", "tx_cancel", "tx_delete", "tx_retry":
		var params struct {
			Handle string `json:"handle"`
			ID     int64  `json:"id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		var job *rivertype.JobRow
		switch req.Method {
		case "tx_cancel":
			job, err = client.JobCancelTx(ctx, tx, params.ID)
		case "tx_delete":
			job, err = client.JobDeleteTx(ctx, tx, params.ID)
		case "tx_get":
			job, err = client.JobGetTx(ctx, tx, params.ID)
		case "tx_retry":
			job, err = client.JobRetryTx(ctx, tx, params.ID)
		}
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "tx_update":
		var params struct {
			Handle string `json:"handle"`
			ID     int64  `json:"id"`
			Output any    `json:"output"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		job, err := client.JobUpdateTx(ctx, tx, params.ID, &river.JobUpdateParams{Output: params.Output})
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "tx_list":
		handle, err := requestHandle(req.Params)
		if err != nil {
			return nil, err
		}
		tx, ok := s.transactions[handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", handle)
		}
		params, err := makeJobListParams(req.Params)
		if err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		result, err := client.JobListTx(ctx, tx, params)
		if err != nil {
			return nil, err
		}
		return normalizeJobListResult(result)

	case "tx_delete_many":
		handle, err := requestHandle(req.Params)
		if err != nil {
			return nil, err
		}
		tx, ok := s.transactions[handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", handle)
		}
		params, err := makeJobDeleteManyParams(req.Params)
		if err != nil {
			return nil, err
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		result, err := client.JobDeleteManyTx(ctx, tx, params)
		if err != nil {
			return nil, err
		}
		return map[string]any{"jobs": normalizeJobs(result.Jobs)}, nil

	case "tx_queue_get":
		var params struct {
			Handle string `json:"handle"`
			Name   string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		queue, err := client.QueueGetTx(ctx, tx, params.Name)
		if err != nil {
			return nil, err
		}
		return normalizeQueue(queue), nil

	case "tx_queue_list":
		var params struct {
			Handle string `json:"handle"`
			Limit  int    `json:"limit"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.Limit == 0 {
			params.Limit = 100
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		result, err := client.QueueListTx(ctx, tx, river.NewQueueListParams().First(params.Limit))
		if err != nil {
			return nil, err
		}
		queues := make([]any, len(result.Queues))
		for i, queue := range result.Queues {
			queues[i] = normalizeQueue(queue)
		}
		return map[string]any{"queues": queues}, nil

	case "tx_queue_pause", "tx_queue_resume":
		var params struct {
			Handle string `json:"handle"`
			Name   string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		if req.Method == "tx_queue_pause" {
			err = client.QueuePauseTx(ctx, tx, params.Name, nil)
		} else {
			err = client.QueueResumeTx(ctx, tx, params.Name, nil)
		}
		return map[string]any{}, err

	case "tx_queue_update":
		var params struct {
			Handle   string          `json:"handle"`
			Metadata json.RawMessage `json:"metadata"`
			Name     string          `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		client, err := s.client()
		if err != nil {
			return nil, err
		}
		queue, err := client.QueueUpdateTx(ctx, tx, params.Name, &river.QueueUpdateParams{Metadata: params.Metadata})
		if err != nil {
			return nil, err
		}
		return normalizeQueue(queue), nil

	case "tx_fail":
		handle, err := requestHandle(req.Params)
		if err != nil {
			return nil, err
		}
		tx, ok := s.transactions[handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", handle)
		}
		_, err = tx.Exec(ctx, "SELECT 1 / 0")
		return nil, err

	case "tx_commit", "tx_rollback":
		handle, err := requestHandle(req.Params)
		if err != nil {
			return nil, err
		}
		tx, ok := s.transactions[handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", handle)
		}
		delete(s.transactions, handle)
		if req.Method == "tx_commit" {
			return map[string]any{}, tx.Commit(ctx)
		}
		return map[string]any{}, tx.Rollback(ctx)
	}

	return nil, fmt.Errorf("method not found: %s", req.Method)
}

//nolint:cyclop,funlen,gocognit,maintidx
func (s *sqliteAdapterState) handle(ctx context.Context, req *request) (any, error) {
	switch req.Method {
	case "handshake":
		methods, profileCapabilities := sqliteAdapterMethods, sqliteCapabilities
		if s.profile == "sqlite-runtime-v1" {
			methods, profileCapabilities = sqliteRuntimeMethods, sqliteRuntimeCapabilities
		}
		return map[string]any{
			"adapter_version":        adapterVersion,
			"backend":                "sqlite",
			"capabilities":           profileCapabilities,
			"implementation":         "go",
			"implementation_version": "0.44.0-development",
			"methods":                methods,
			"migration_lines":        map[string]int{"main": 7},
			"profile":                s.profile,
			"protocol_revision":      protocolRevision,
		}, nil

	case "migrate":
		var params struct {
			Direction     string `json:"direction"`
			DryRun        bool   `json:"dry_run"`
			MaxSteps      *int   `json:"max_steps"`
			Schema        string `json:"schema"`
			TargetVersion *int   `json:"target_version"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.Schema != "" {
			return nil, errors.New("SQLite conformance does not support custom schemas")
		}
		migrator, err := rivermigrate.New(riversqlite.New(s.pool), &rivermigrate.Config{Logger: adapterLogger()})
		if err != nil {
			return nil, err
		}
		direction := rivermigrate.DirectionUp
		if params.Direction != "" {
			direction = rivermigrate.Direction(params.Direction)
		}
		var opts *rivermigrate.MigrateOpts
		if params.DryRun || params.MaxSteps != nil || params.TargetVersion != nil {
			opts = &rivermigrate.MigrateOpts{DryRun: params.DryRun}
			if params.MaxSteps != nil {
				opts.MaxSteps = *params.MaxSteps
			}
			if params.TargetVersion != nil {
				opts.TargetVersion = *params.TargetVersion
			}
		}
		result, err := migrator.Migrate(ctx, direction, opts)
		if err != nil {
			return nil, err
		}
		versions := make([]int, len(result.Versions))
		for i, version := range result.Versions {
			versions[i] = version.Version
		}
		existingMigrations, err := migrator.ExistingVersions(ctx)
		if err != nil {
			return nil, err
		}
		existing := make([]int, len(existingMigrations))
		for i, migration := range existingMigrations {
			existing[i] = migration.Version
		}
		validation, err := migrator.Validate(ctx, nil)
		if err != nil {
			return nil, err
		}
		return map[string]any{"applied": versions, "existing": existing, "valid": validation.OK}, nil

	case "reset":
		if len(s.transactions) > 0 {
			return nil, errors.New("reset requires no open transaction")
		}
		for _, table := range []string{
			"river_notification", "river_job", "river_queue", "river_leader",
		} {
			// #nosec G202 -- Table names are fixed constants above, never request input.
			if _, err := s.pool.ExecContext(ctx, "DELETE FROM "+table); err != nil {
				return nil, err
			}
		}
		return map[string]any{}, nil

	case "clock_set":
		var params struct {
			Now time.Time `json:"now"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		s.clock = &params.Now
		return map[string]any{}, nil

	case "rng_seed":
		var params struct {
			Seed uint64 `json:"seed"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		s.rngSeed = params.Seed
		return map[string]any{}, nil

	case "retry_delay":
		if s.clock == nil {
			return nil, errors.New("clock_set is required before retry_delay")
		}
		var params struct {
			ErrorCount uint32 `json:"error_count"`
			JobID      int64  `json:"job_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.ErrorCount < 1 {
			return nil, errors.New("error_count must be positive")
		}
		delay := deterministicRetryDelay(*s.clock, params.JobID, params.ErrorCount, s.rngSeed)
		return map[string]any{"delay_ns": delay.Nanoseconds()}, nil

	case "unique_key":
		return handleUniqueKey(req.Params)

	case "insert":
		var params insertParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.Schema != "" {
			return nil, errors.New("SQLite conformance does not support custom schemas")
		}
		opts, err := params.Opts.opts()
		if err != nil {
			return nil, err
		}
		result, err := s.client().Insert(ctx, params.args(), opts)
		if err != nil {
			return nil, err
		}
		return normalizeJob(result.Job), nil

	case "insert_many", "insert_many_fast":
		jobs, err := decodeInsertManyParams(req.Params)
		if err != nil {
			return nil, err
		}
		if req.Method == "insert_many_fast" {
			count, err := s.client().InsertManyFast(ctx, jobs)
			return map[string]any{"count": count}, err
		}
		results, err := s.client().InsertMany(ctx, jobs)
		if err != nil {
			return nil, err
		}
		return normalizeInsertManyResults(results), nil

	case "raw_insert_no_notify":
		var params insertParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		encodedArgs, err := json.Marshal(params.args())
		if err != nil {
			return nil, err
		}
		kind := params.Kind
		if kind == "" {
			kind = "conformance_echo"
		}
		maxAttempts := 25
		if params.Opts.MaxAttempts != nil {
			maxAttempts = *params.Opts.MaxAttempts
		}
		var id int64
		err = s.pool.QueryRowContext(ctx,
			"INSERT INTO river_job (args, kind, max_attempts) VALUES (jsonb(?), ?, ?) RETURNING id",
			string(encodedArgs), kind, maxAttempts,
		).Scan(&id)
		if err != nil {
			return nil, err
		}
		job, err := s.client().JobGet(ctx, id)
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "get": //nolint:usestdlibvars // JSON-RPC method names are lowercase protocol values.
		var params struct {
			ID     int64  `json:"id"`
			Schema string `json:"schema"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.Schema != "" {
			return nil, errors.New("SQLite conformance does not support custom schemas")
		}
		job, err := s.client().JobGet(ctx, params.ID)
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "list":
		params, err := makeJobListParams(req.Params)
		if err != nil {
			return nil, err
		}
		result, err := s.client().JobList(ctx, params)
		if err != nil {
			return nil, err
		}
		return normalizeJobListResult(result)

	case "cancel", "delete", "retry": //nolint:usestdlibvars // JSON-RPC method names are lowercase protocol values.
		id, err := requestID(req.Params)
		if err != nil {
			return nil, err
		}
		client := s.client()
		var job *rivertype.JobRow
		switch req.Method {
		case "cancel":
			job, err = client.JobCancel(ctx, id)
		case "delete": //nolint:usestdlibvars // JSON-RPC method names are lowercase protocol values.
			job, err = client.JobDelete(ctx, id)
		case "retry":
			job, err = client.JobRetry(ctx, id)
		}
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "delete_many":
		params, err := makeJobDeleteManyParams(req.Params)
		if err != nil {
			return nil, err
		}
		result, err := s.client().JobDeleteMany(ctx, params)
		if err != nil {
			return nil, err
		}
		return map[string]any{"jobs": normalizeJobs(result.Jobs)}, nil

	case "update":
		var params struct {
			ID     int64 `json:"id"`
			Output any   `json:"output"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		job, err := s.client().JobUpdate(ctx, params.ID, &river.JobUpdateParams{Output: params.Output})
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "raw_finalize":
		var params struct {
			ID       int64          `json:"id"`
			Metadata map[string]any `json:"metadata"`
			State    string         `json:"state"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.State != "completed" && params.State != "discarded" {
			return nil, errors.New("state must be completed or discarded")
		}
		metadata, err := json.Marshal(params.Metadata)
		if err != nil {
			return nil, err
		}
		attemptError, err := json.Marshal(map[string]any{
			"at":      "2026-02-03T04:05:06.789Z",
			"attempt": 1,
			"error":   "external discard",
			"trace":   "external trace",
		})
		if err != nil {
			return nil, err
		}
		result, err := s.pool.ExecContext(ctx, `
			UPDATE river_job
			SET errors = CASE WHEN ? = 'discarded'
					THEN jsonb(json_insert(json(coalesce(errors, jsonb('[]'))), '$[#]', json(?)))
					ELSE errors END,
				finalized_at = ?,
				metadata = jsonb_patch(json(metadata), json(?)),
				state = ?
			WHERE id = ? AND state = 'running'`,
			params.State, string(attemptError), "2026-02-03 04:05:06.789",
			string(metadata), params.State, params.ID,
		)
		if err != nil {
			return nil, err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if rowsAffected != 1 {
			return nil, errors.New("running job not found")
		}
		job, err := s.client().JobGet(ctx, params.ID)
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "raw_job_timestamps":
		id, err := requestID(req.Params)
		if err != nil {
			return nil, err
		}
		var createdAt, scheduledAt string
		err = s.pool.QueryRowContext(ctx, `
			SELECT CAST(created_at AS TEXT), CAST(scheduled_at AS TEXT)
			FROM river_job
			WHERE id = ?`, id).Scan(&createdAt, &scheduledAt)
		return map[string]any{"created_at": createdAt, "scheduled_at": scheduledAt}, err

	case "barrier_create", "barrier_release":
		var params struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if req.Method == "barrier_create" {
			return map[string]any{}, s.barriers.create(params.Name)
		}
		return map[string]any{}, s.barriers.release(params.Name)

	case "queue_add":
		if s.running == nil {
			return nil, errors.New("queue_add requires a running client")
		}
		return handleQueueAdd(ctx, req.Params, s.running.client.Queues().Add, s.running.client.Queues().Remove)

	case "queue_get":
		var params struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		queue, err := s.client().QueueGet(ctx, params.Name)
		if err != nil {
			return nil, err
		}
		return normalizeQueue(queue), nil

	case "queue_list":
		var params struct {
			Limit int `json:"limit"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.Limit == 0 {
			params.Limit = 100
		}
		result, err := s.client().QueueList(ctx, river.NewQueueListParams().First(params.Limit))
		if err != nil {
			return nil, err
		}
		queues := make([]any, len(result.Queues))
		for i, queue := range result.Queues {
			queues[i] = normalizeQueue(queue)
		}
		return map[string]any{"queues": queues}, nil

	case "queue_pause", "queue_resume":
		var params struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if req.Method == "queue_pause" {
			return map[string]any{}, s.client().QueuePause(ctx, params.Name, nil)
		}
		return map[string]any{}, s.client().QueueResume(ctx, params.Name, nil)

	case "queue_remove":
		if s.running == nil {
			return nil, errors.New("queue_remove requires a running client")
		}
		var params struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		return map[string]any{}, s.running.client.Queues().Remove(ctx, params.Name)

	case "queue_update":
		var params struct {
			Metadata json.RawMessage `json:"metadata"`
			Name     string          `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		queue, err := s.client().QueueUpdate(ctx, params.Name, &river.QueueUpdateParams{Metadata: params.Metadata})
		if err != nil {
			return nil, err
		}
		return normalizeQueue(queue), nil

	case "leader":
		var leaderID string
		var electedAt time.Time
		err := s.pool.QueryRowContext(ctx,
			"SELECT leader_id, elected_at FROM river_leader WHERE name = 'default' AND expires_at >= CURRENT_TIMESTAMP",
		).Scan(&leaderID, &electedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]any{"elected_at": nil, "leader_id": nil}, nil
		}
		return map[string]any{"elected_at": formatTime(electedAt), "leader_id": leaderID}, err

	case "request_resign":
		return map[string]any{}, s.client().Notify().RequestResign(ctx)

	case "start":
		if s.running != nil {
			return nil, errors.New("client already running")
		}
		var params struct {
			ClientID            string  `json:"client_id"`
			ErrorHandlerCancel  bool    `json:"error_handler_cancel"`
			FetchPollIntervalMS *uint64 `json:"fetch_poll_interval_ms"`
			Instrumented        bool    `json:"instrumented"`
			JobStuckThresholdMS *uint64 `json:"job_stuck_threshold_ms"`
			JobTimeoutMS        *uint64 `json:"job_timeout_ms"`
			MaxWorkers          int     `json:"max_workers"`
			PeriodicRunOnStart  bool    `json:"periodic_run_on_start"`
			PollOnly            bool    `json:"poll_only"`
			Queue               string  `json:"queue"`
			RetryDelayMS        *uint64 `json:"retry_delay_ms"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.MaxWorkers == 0 {
			params.MaxWorkers = 4
		}
		if params.Queue == "" {
			params.Queue = river.QueueDefault
		}
		probe := &runtimeProbe{}
		client, err := newSQLiteWorkerClient(s.pool, s.barriers, workerClientConfig{
			errorHandlerCancel: params.ErrorHandlerCancel, fetchPollIntervalMS: params.FetchPollIntervalMS,
			id: params.ClientID, instrumented: params.Instrumented,
			jobStuckThresholdMS: params.JobStuckThresholdMS, jobTimeoutMS: params.JobTimeoutMS,
			maxWorkers: params.MaxWorkers, periodicRunOnStart: params.PeriodicRunOnStart,
			pollOnly: params.PollOnly, probe: probe, queue: params.Queue, retryDelayMS: params.RetryDelayMS,
		})
		if err != nil {
			return nil, err
		}
		subscription, subscriptionCancel := client.Subscribe(
			river.EventKindJobCancelled, river.EventKindJobCompleted, river.EventKindJobFailed,
			river.EventKindJobInterrupted, river.EventKindJobSnoozed,
			river.EventKindQueuePaused, river.EventKindQueueResumed,
		)
		if err := client.Start(ctx); err != nil {
			subscriptionCancel()
			return nil, err
		}
		s.running = &sqliteRunningClient{client: client, probe: probe, subscription: subscription, subscriptionCancel: subscriptionCancel}
		return map[string]any{}, nil

	case "stop":
		if s.running == nil {
			return nil, errors.New("client is not running")
		}
		var params struct {
			Cancel bool `json:"cancel"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		var err error
		if params.Cancel {
			err = s.running.client.StopAndCancel(stopCtx)
		} else {
			err = s.running.client.Stop(stopCtx)
		}
		s.running.subscriptionCancel()
		s.running = nil
		return map[string]any{}, err

	case "runtime_stats":
		if s.running == nil {
			return nil, errors.New("runtime_stats requires a running client")
		}
		for {
			select {
			case event := <-s.running.subscription:
				if event != nil {
					s.running.probe.addEvent(event.Kind)
				}
			default:
				return s.running.probe.snapshot(), nil
			}
		}

	case "wait":
		var params struct {
			ID     int64                `json:"id"`
			States []rivertype.JobState `json:"states"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		job, err := waitForStates(ctx, s.client(), params.ID, params.States)
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "work":
		var params struct {
			ClientID string `json:"client_id"`
			ID       int64  `json:"id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.ClientID == "" {
			params.ClientID = "go-conformance-adapter"
		}
		probe := &runtimeProbe{}
		client, err := newSQLiteWorkerClient(s.pool, s.barriers, workerClientConfig{
			id: params.ClientID, maxWorkers: 1, probe: probe, queue: river.QueueDefault,
		})
		if err != nil {
			return nil, err
		}
		if err := client.Start(ctx); err != nil {
			return nil, err
		}
		job, waitErr := waitForStates(ctx, client, params.ID, nil)
		stopCtx, stopCancel := context.WithTimeout(ctx, 10*time.Second)
		defer stopCancel()
		stopErr := client.Stop(stopCtx)
		if waitErr != nil {
			return nil, waitErr
		}
		if stopErr != nil {
			return nil, stopErr
		}
		return normalizeJob(job), nil

	case "tx_begin":
		handle, err := requestHandle(req.Params)
		if err != nil {
			return nil, err
		}
		if _, ok := s.transactions[handle]; ok {
			return nil, fmt.Errorf("transaction %q already exists", handle)
		}
		tx, err := s.pool.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		s.transactions[handle] = tx
		return map[string]any{}, nil

	case "tx_insert":
		var params struct {
			Handle string       `json:"handle"`
			Job    insertParams `json:"job"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		opts, err := params.Job.Opts.opts()
		if err != nil {
			return nil, err
		}
		result, err := s.client().InsertTx(ctx, tx, params.Job.args(), opts)
		if err != nil {
			return nil, err
		}
		return normalizeJob(result.Job), nil

	case "tx_insert_many", "tx_insert_many_fast":
		var params struct {
			Handle string          `json:"handle"`
			Jobs   json.RawMessage `json:"jobs"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		jobs, err := decodeInsertManyParams(params.Jobs)
		if err != nil {
			return nil, err
		}
		if req.Method == "tx_insert_many_fast" {
			count, err := s.client().InsertManyFastTx(ctx, tx, jobs)
			return map[string]any{"count": count}, err
		}
		results, err := s.client().InsertManyTx(ctx, tx, jobs)
		if err != nil {
			return nil, err
		}
		return normalizeInsertManyResults(results), nil

	case "tx_get", "tx_cancel", "tx_delete", "tx_retry":
		var params struct {
			Handle string `json:"handle"`
			ID     int64  `json:"id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		client := s.client()
		var job *rivertype.JobRow
		var err error
		switch req.Method {
		case "tx_cancel":
			job, err = client.JobCancelTx(ctx, tx, params.ID)
		case "tx_delete":
			job, err = client.JobDeleteTx(ctx, tx, params.ID)
		case "tx_get":
			job, err = client.JobGetTx(ctx, tx, params.ID)
		case "tx_retry":
			job, err = client.JobRetryTx(ctx, tx, params.ID)
		}
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "tx_update":
		var params struct {
			Handle string `json:"handle"`
			ID     int64  `json:"id"`
			Output any    `json:"output"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		job, err := s.client().JobUpdateTx(ctx, tx, params.ID, &river.JobUpdateParams{Output: params.Output})
		if err != nil {
			return nil, err
		}
		return normalizeJob(job), nil

	case "tx_list":
		handle, err := requestHandle(req.Params)
		if err != nil {
			return nil, err
		}
		tx, ok := s.transactions[handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", handle)
		}
		params, err := makeJobListParams(req.Params)
		if err != nil {
			return nil, err
		}
		result, err := s.client().JobListTx(ctx, tx, params)
		if err != nil {
			return nil, err
		}
		return normalizeJobListResult(result)

	case "tx_delete_many":
		handle, err := requestHandle(req.Params)
		if err != nil {
			return nil, err
		}
		tx, ok := s.transactions[handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", handle)
		}
		params, err := makeJobDeleteManyParams(req.Params)
		if err != nil {
			return nil, err
		}
		result, err := s.client().JobDeleteManyTx(ctx, tx, params)
		if err != nil {
			return nil, err
		}
		return map[string]any{"jobs": normalizeJobs(result.Jobs)}, nil

	case "tx_queue_get":
		var params struct {
			Handle string `json:"handle"`
			Name   string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		queue, err := s.client().QueueGetTx(ctx, tx, params.Name)
		if err != nil {
			return nil, err
		}
		return normalizeQueue(queue), nil

	case "tx_queue_list":
		var params struct {
			Handle string `json:"handle"`
			Limit  int    `json:"limit"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		if params.Limit == 0 {
			params.Limit = 100
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		result, err := s.client().QueueListTx(ctx, tx, river.NewQueueListParams().First(params.Limit))
		if err != nil {
			return nil, err
		}
		queues := make([]any, len(result.Queues))
		for i, queue := range result.Queues {
			queues[i] = normalizeQueue(queue)
		}
		return map[string]any{"queues": queues}, nil

	case "tx_queue_pause", "tx_queue_resume":
		var params struct {
			Handle string `json:"handle"`
			Name   string `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		var err error
		if req.Method == "tx_queue_pause" {
			err = s.client().QueuePauseTx(ctx, tx, params.Name, nil)
		} else {
			err = s.client().QueueResumeTx(ctx, tx, params.Name, nil)
		}
		return map[string]any{}, err

	case "tx_queue_update":
		var params struct {
			Handle   string          `json:"handle"`
			Metadata json.RawMessage `json:"metadata"`
			Name     string          `json:"name"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, err
		}
		tx, ok := s.transactions[params.Handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", params.Handle)
		}
		queue, err := s.client().QueueUpdateTx(ctx, tx, params.Name, &river.QueueUpdateParams{Metadata: params.Metadata})
		if err != nil {
			return nil, err
		}
		return normalizeQueue(queue), nil

	case "tx_commit", "tx_rollback":
		handle, err := requestHandle(req.Params)
		if err != nil {
			return nil, err
		}
		tx, ok := s.transactions[handle]
		if !ok {
			return nil, fmt.Errorf("transaction %q not found", handle)
		}
		delete(s.transactions, handle)
		if req.Method == "tx_commit" {
			return map[string]any{}, tx.Commit()
		}
		return map[string]any{}, tx.Rollback()
	}

	return nil, fmt.Errorf("method not found for SQLite profile: %s", req.Method)
}

func (s *sqliteAdapterState) client() *river.Client[*sql.Tx] {
	if s.running != nil {
		return s.running.client
	}
	client, err := river.NewClient(riversqlite.New(s.pool), &river.Config{Logger: adapterLogger()})
	if err != nil {
		panic(fmt.Sprintf("build SQLite conformance client: %v", err))
	}
	return client
}

func deterministicRetryDelay(now time.Time, jobID int64, errorCount uint32, seed uint64) time.Duration {
	const maxRetryNanos = int64(math.MaxInt64)
	baseSeconds := math.Pow(float64(errorCount), 4)
	if baseSeconds*float64(time.Second) >= float64(maxRetryNanos) {
		return time.Duration(maxRetryNanos)
	}
	base := time.Duration(baseSeconds * float64(time.Second))
	var seedBytes [8]byte
	var jobIDBytes [8]byte
	var errorCountBytes [4]byte
	var nowBytes [8]byte
	binary.BigEndian.PutUint64(seedBytes[:], seed)
	jobIDUint, _ := strconv.ParseUint(strconv.FormatInt(jobID, 10), 10, 64)
	binary.BigEndian.PutUint64(jobIDBytes[:], jobIDUint)
	binary.BigEndian.PutUint32(errorCountBytes[:], errorCount)
	binary.BigEndian.PutUint64(nowBytes[:], uint64(now.UnixNano()))
	hash := sha256.New()
	_, _ = hash.Write(seedBytes[:])
	_, _ = hash.Write(jobIDBytes[:])
	_, _ = hash.Write(errorCountBytes[:])
	_, _ = hash.Write(nowBytes[:])
	sum := hash.Sum(nil)
	sample := binary.BigEndian.Uint32(sum[:4])
	ratio := float64(sample) / float64(math.MaxUint32)
	return time.Duration(math.Round(float64(base) * (0.9 + ratio*0.2)))
}

func durationFromMilliseconds(milliseconds uint64) (time.Duration, error) {
	duration, err := time.ParseDuration(strconv.FormatUint(milliseconds, 10) + "ms")
	if err != nil {
		return 0, fmt.Errorf("milliseconds out of range: %w", err)
	}
	return duration, nil
}

func (s *adapterState) client() (*river.Client[pgx.Tx], error) {
	return s.clientForSchema("")
}

func (s *adapterState) clientForSchema(schema string) (*river.Client[pgx.Tx], error) {
	if s.running != nil {
		if s.running.client.Schema() != schema {
			return nil, fmt.Errorf("running client schema %q does not match requested schema %q", s.running.client.Schema(), schema)
		}
		return s.running.client, nil
	}
	return river.NewClient(riverpgxv5.New(s.pool), &river.Config{Logger: adapterLogger(), Schema: schema})
}

type workerClientConfig struct {
	errorHandlerCancel  bool
	fetchPollIntervalMS *uint64
	id                  string
	instrumented        bool
	jobStuckThresholdMS *uint64
	jobTimeoutMS        *uint64
	maxWorkers          int
	periodicRunOnStart  bool
	pollOnly            bool
	probe               *runtimeProbe
	queue               string
	retryDelayMS        *uint64
	schema              string
}

func newWorkerClient(pool *pgxpool.Pool, barriers *barrierRegistry, config workerClientConfig) (*river.Client[pgx.Tx], error) {
	riverConfig, err := newWorkerConfig(pool, barriers, config)
	if err != nil {
		return nil, err
	}
	return river.NewClient(riverpgxv5.New(pool), riverConfig)
}

func newSQLiteWorkerClient(pool *sql.DB, barriers *barrierRegistry, config workerClientConfig) (*river.Client[*sql.Tx], error) {
	riverConfig, err := newWorkerConfig(nil, barriers, config)
	if err != nil {
		return nil, err
	}
	return river.NewClient(riversqlite.New(pool), riverConfig)
}

func newWorkerConfig(pool *pgxpool.Pool, barriers *barrierRegistry, config workerClientConfig) (*river.Config, error) {
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, &conformanceWorker{barriers: barriers, pool: pool, probe: config.probe}); err != nil {
		return nil, err
	}
	riverConfig := &river.Config{
		ErrorHandler:      nil,
		FetchCooldown:     time.Millisecond,
		FetchPollInterval: 10 * time.Millisecond,
		ID:                config.id,
		Logger:            adapterLogger(),
		PollOnly:          config.pollOnly,
		Queues: map[string]river.QueueConfig{
			config.queue: {MaxWorkers: config.maxWorkers},
		},
		Schema:   config.schema,
		TestOnly: true,
		Workers:  workers,
	}
	if config.fetchPollIntervalMS != nil {
		duration, err := durationFromMilliseconds(*config.fetchPollIntervalMS)
		if err != nil {
			return nil, err
		}
		riverConfig.FetchPollInterval = duration
	}
	if config.errorHandlerCancel {
		riverConfig.ErrorHandler = &conformanceErrorHandler{probe: config.probe}
	}
	if config.instrumented {
		riverConfig.Plugins = []rivertype.Plugin{&conformancePlugin{probe: config.probe}}
	}
	if config.jobStuckThresholdMS != nil {
		duration, err := durationFromMilliseconds(*config.jobStuckThresholdMS)
		if err != nil {
			return nil, err
		}
		riverConfig.JobStuckThreshold = duration
	}
	if config.jobTimeoutMS != nil {
		duration, err := durationFromMilliseconds(*config.jobTimeoutMS)
		if err != nil {
			return nil, err
		}
		riverConfig.JobTimeout = duration
	}
	if config.periodicRunOnStart {
		riverConfig.PeriodicJobs = []*river.PeriodicJob{river.NewPeriodicJob(
			river.PeriodicInterval(time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return conformanceArgs{Message: "periodic run on start"}, &river.InsertOpts{
					Metadata: []byte(`{"periodic":true}`),
				}
			},
			&river.PeriodicJobOpts{ID: "conformance-periodic", RunOnStart: true},
		)}
	}
	if config.retryDelayMS != nil {
		duration, err := durationFromMilliseconds(*config.retryDelayMS)
		if err != nil {
			return nil, err
		}
		riverConfig.RetryPolicy = fixedRetryPolicy{delay: duration}
	}
	return riverConfig, nil
}

func adapterLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func errorResponse(id any, code int, err error) response {
	return response{
		Error:   &responseError{Code: code, Message: err.Error()},
		ID:      id,
		JSONRPC: "2.0",
	}
}

func normalizeJob(job *rivertype.JobRow) map[string]any {
	var args any
	if err := json.Unmarshal(job.EncodedArgs, &args); err != nil {
		args = nil
	}
	var metadata any
	if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
		metadata = nil
	}
	if metadataObject, ok := metadata.(map[string]any); ok {
		delete(metadataObject, "river:unique_nonce")
	}
	attemptedBy := job.AttemptedBy
	if attemptedBy == nil {
		attemptedBy = []string{}
	}
	errorsNormalized := make([]any, len(job.Errors))
	for i, attemptErr := range job.Errors {
		errorsNormalized[i] = map[string]any{
			"at":      formatTime(attemptErr.At),
			"attempt": attemptErr.Attempt,
			"error":   attemptErr.Error,
			"trace":   attemptErr.Trace,
		}
	}
	var uniqueKey any
	if job.UniqueKey != nil {
		uniqueKey = hex.EncodeToString(job.UniqueKey)
	}
	return map[string]any{
		"args":          args,
		"attempt":       job.Attempt,
		"attempted_at":  formatOptionalTime(job.AttemptedAt),
		"attempted_by":  attemptedBy,
		"created_at":    formatTime(job.CreatedAt),
		"errors":        errorsNormalized,
		"finalized_at":  formatOptionalTime(job.FinalizedAt),
		"id":            job.ID,
		"kind":          job.Kind,
		"max_attempts":  job.MaxAttempts,
		"metadata":      metadata,
		"priority":      job.Priority,
		"queue":         job.Queue,
		"scheduled_at":  formatTime(job.ScheduledAt),
		"state":         job.State,
		"tags":          valueOrEmpty(job.Tags),
		"unique_key":    uniqueKey,
		"unique_states": job.UniqueStates,
	}
}

func normalizeJobs(jobs []*rivertype.JobRow) []any {
	normalized := make([]any, len(jobs))
	for i, job := range jobs {
		normalized[i] = normalizeJob(job)
	}
	return normalized
}

func decodeInsertManyParams(encoded json.RawMessage) ([]river.InsertManyParams, error) {
	var envelope struct {
		Jobs []insertParams `json:"jobs"`
	}
	if len(encoded) > 0 && encoded[0] == '[' {
		if err := json.Unmarshal(encoded, &envelope.Jobs); err != nil {
			return nil, err
		}
	} else if err := json.Unmarshal(encoded, &envelope); err != nil {
		return nil, err
	}
	jobs := make([]river.InsertManyParams, len(envelope.Jobs))
	for i, job := range envelope.Jobs {
		opts, err := job.Opts.opts()
		if err != nil {
			return nil, err
		}
		jobs[i] = river.InsertManyParams{Args: job.args(), InsertOpts: opts}
	}
	return jobs, nil
}

func normalizeInsertManyResults(results []*rivertype.JobInsertResult) map[string]any {
	normalized := make([]any, len(results))
	for i, result := range results {
		normalized[i] = map[string]any{
			"job":                         normalizeJob(result.Job),
			"unique_skipped_as_duplicate": result.UniqueSkippedAsDuplicate,
		}
	}
	return map[string]any{"results": normalized}
}

func normalizeJobListResult(result *river.JobListResult) (map[string]any, error) {
	var cursor any
	if result.LastCursor != nil {
		encoded, err := result.LastCursor.MarshalText()
		if err != nil {
			return nil, err
		}
		cursor = string(encoded)
	}
	return map[string]any{"cursor": cursor, "jobs": normalizeJobs(result.Jobs)}, nil
}

func normalizeQueue(queue *rivertype.Queue) map[string]any {
	var metadata any
	if err := json.Unmarshal(queue.Metadata, &metadata); err != nil {
		metadata = nil
	}
	return map[string]any{
		"created_at": formatTime(queue.CreatedAt),
		"metadata":   metadata,
		"name":       queue.Name,
		"paused_at":  formatOptionalTime(queue.PausedAt),
		"updated_at": formatTime(queue.UpdatedAt),
	}
}

func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func formatOptionalTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return formatTime(*value)
}

func requestID(paramsJSON json.RawMessage) (int64, error) {
	var params struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(paramsJSON, &params); err != nil {
		return 0, err
	}
	if params.ID < 1 {
		return 0, errors.New("id must be positive")
	}
	return params.ID, nil
}

func requestHandle(paramsJSON json.RawMessage) (string, error) {
	var params struct {
		Handle string `json:"handle"`
	}
	if err := json.Unmarshal(paramsJSON, &params); err != nil {
		return "", err
	}
	if params.Handle == "" {
		return "", errors.New("handle is required")
	}
	return params.Handle, nil
}

func waitForStates[TTx any](ctx context.Context, client *river.Client[TTx], id int64, states []rivertype.JobState) (*rivertype.JobRow, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if len(states) == 0 {
		states = []rivertype.JobState{
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
		}
	}
	for {
		job, err := client.JobGet(ctx, id)
		if err != nil {
			return nil, err
		}
		if slices.Contains(states, job.State) {
			return job, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("job %d did not reach %v from state %s: %w", id, states, job.State, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func makeJobListParams(raw json.RawMessage) (*river.JobListParams, error) {
	var params struct {
		After      string               `json:"after"`
		Direction  string               `json:"direction"`
		IDs        []int64              `json:"ids"`
		Kinds      []string             `json:"kinds"`
		Limit      int                  `json:"limit"`
		Metadata   json.RawMessage      `json:"metadata"`
		OrderBy    string               `json:"order_by"`
		Priorities []int16              `json:"priorities"`
		Queues     []string             `json:"queues"`
		States     []rivertype.JobState `json:"states"`
		TagsAll    []string             `json:"tags_all"`
		TagsAny    []string             `json:"tags_any"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, err
	}
	if params.Limit == 0 {
		params.Limit = 100
	}
	result := river.NewJobListParams().First(params.Limit)
	if params.IDs != nil {
		result = result.IDs(params.IDs...)
	}
	if params.Kinds != nil {
		result = result.Kinds(params.Kinds...)
	}
	if params.Metadata != nil {
		result = result.Metadata(string(params.Metadata))
	}
	if params.OrderBy != "" || params.Direction != "" {
		field := river.JobListOrderByID
		switch params.OrderBy {
		case "", string(river.JobListOrderByID):
		case string(river.JobListOrderByFinalizedAt):
			field = river.JobListOrderByFinalizedAt
		case string(river.JobListOrderByScheduledAt):
			field = river.JobListOrderByScheduledAt
		case string(river.JobListOrderByTime):
			field = river.JobListOrderByTime
		default:
			return nil, fmt.Errorf("unsupported order_by %q", params.OrderBy)
		}
		direction := river.SortOrderAsc
		switch params.Direction {
		case "", "asc":
		case "desc":
			direction = river.SortOrderDesc
		default:
			return nil, fmt.Errorf("unsupported direction %q", params.Direction)
		}
		result = result.OrderBy(field, direction)
	}
	if params.Priorities != nil {
		result = result.Priorities(params.Priorities...)
	}
	if params.Queues != nil {
		result = result.Queues(params.Queues...)
	}
	if params.States != nil {
		result = result.States(params.States...)
	}
	if params.TagsAll != nil {
		result = result.TagsAll(params.TagsAll...)
	}
	if params.TagsAny != nil {
		result = result.TagsAny(params.TagsAny...)
	}
	if params.After != "" {
		var cursor river.JobListCursor
		if err := cursor.UnmarshalText([]byte(params.After)); err != nil {
			return nil, err
		}
		result = result.After(&cursor)
	}
	return result, nil
}

func makeJobDeleteManyParams(raw json.RawMessage) (*river.JobDeleteManyParams, error) {
	var params struct {
		All    bool                 `json:"all"`
		IDs    []int64              `json:"ids"`
		Kinds  []string             `json:"kinds"`
		Limit  int                  `json:"limit"`
		Queues []string             `json:"queues"`
		States []rivertype.JobState `json:"states"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, err
	}
	if params.Limit == 0 {
		params.Limit = 100
	}
	result := river.NewJobDeleteManyParams().First(params.Limit)
	if params.All {
		return result.UnsafeAll(), nil
	}
	if params.IDs != nil {
		result = result.IDs(params.IDs...)
	}
	if params.Kinds != nil {
		result = result.Kinds(params.Kinds...)
	}
	if params.Queues != nil {
		result = result.Queues(params.Queues...)
	}
	if params.States != nil {
		result = result.States(params.States...)
	}
	return result, nil
}

func valueOrZero[T any](value *T) T {
	if value == nil {
		var zero T
		return zero
	}
	return *value
}

func valueOrEmpty[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}
