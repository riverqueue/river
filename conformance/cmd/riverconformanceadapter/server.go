package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/conformance/protocol"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

// version is River Go's version, which the handshake reports.
const version = "0.49.0"

// server handles requests for one database driver.
type server[TTx any] struct {
	barriers *barrierRegistry
	// clients are insert-only clients by schema, used for every operation
	// other than working jobs.
	clients    map[string]*river.Client[TTx]
	driver     riverdriver.Driver[TTx]
	driverName string
	logger     *slog.Logger
	running    *runningClient[TTx]
	txFuncs    *txFuncs[TTx]
	txs        map[string]TTx
}

func newServer[TTx any](driverName string, driver riverdriver.Driver[TTx], logger *slog.Logger, txFuncs *txFuncs[TTx]) *server[TTx] {
	return &server[TTx]{
		barriers:   newBarrierRegistry(),
		clients:    make(map[string]*river.Client[TTx]),
		driver:     driver,
		driverName: driverName,
		logger:     logger,
		txFuncs:    txFuncs,
		txs:        make(map[string]TTx),
	}
}

// runningClient is the worker client started by `start`.
type runningClient[TTx any] struct {
	claimBarrier string
	client       *river.Client[TTx]
	stats        *stats
	unsubscribe  func()
}

// txFuncs begin and end a driver's transactions.
type txFuncs[TTx any] struct {
	begin    func(ctx context.Context) (TTx, error)
	commit   func(ctx context.Context, tx TTx) error
	rollback func(ctx context.Context, tx TTx) error
}

func (s *server[TTx]) handle(ctx context.Context, method string, rawParams json.RawMessage) (any, error) {
	switch method {
	case protocol.MethodCancel, protocol.MethodRetry:
		return s.handleJob(ctx, method, rawParams)
	case protocol.MethodHandshake:
		if err := decodeParams(rawParams, &struct{}{}); err != nil {
			return nil, err
		}
		return &protocol.HandshakeResult{Driver: s.driverName, Implementation: "go", Version: version}, nil
	case protocol.MethodInsert:
		return s.handleInsert(ctx, rawParams)
	case protocol.MethodList:
		return s.handleList(ctx, rawParams)
	case protocol.MethodMigrate:
		return s.handleMigrate(ctx, rawParams)
	case protocol.MethodQueue:
		return nil, s.handleQueue(ctx, rawParams)
	case protocol.MethodRelease:
		var params protocol.ReleaseParams
		if err := decodeParams(rawParams, &params); err != nil {
			return nil, err
		}
		s.barriers.release(params.Name)
		return nil, nil //nolint:nilnil // empty result
	case protocol.MethodRequestResign:
		return nil, s.handleRequestResign(ctx, rawParams)
	case protocol.MethodStart:
		return nil, s.handleStart(ctx, rawParams)
	case protocol.MethodStats:
		if err := decodeParams(rawParams, &struct{}{}); err != nil {
			return nil, err
		}
		if s.running == nil {
			return nil, errors.New("no client is running")
		}
		return s.running.stats.snapshot(), nil
	case protocol.MethodStop:
		return nil, s.handleStop(ctx, rawParams)
	case protocol.MethodTxBegin:
		return nil, s.handleTxBegin(ctx, rawParams)
	case protocol.MethodTxEnd:
		return nil, s.handleTxEnd(ctx, rawParams)
	}
	return nil, &codedError{code: protocol.CodeMethodNotFound, err: fmt.Errorf("unknown method %q", method)}
}

// client returns the insert-only client for schema.
func (s *server[TTx]) client(schema string) (*river.Client[TTx], error) {
	if client, ok := s.clients[schema]; ok {
		return client, nil
	}
	client, err := river.NewClient(s.driver, &river.Config{Logger: s.logger, Schema: schema})
	if err != nil {
		return nil, err
	}
	s.clients[schema] = client
	return client, nil
}

// tx returns the open transaction named name, and whether name is set.
func (s *server[TTx]) tx(name string) (TTx, bool, error) {
	var zero TTx
	if name == "" {
		return zero, false, nil
	}
	tx, ok := s.txs[name]
	if !ok {
		return zero, false, notFound(fmt.Errorf("transaction %q is not open", name))
	}
	return tx, true, nil
}

func (s *server[TTx]) handleInsert(ctx context.Context, rawParams json.RawMessage) (any, error) {
	var params protocol.InsertParams
	if err := decodeParams(rawParams, &params); err != nil {
		return nil, err
	}
	client, err := s.client(params.Schema)
	if err != nil {
		return nil, err
	}
	tx, inTx, err := s.tx(params.Tx)
	if err != nil {
		return nil, err
	}

	insertParams := make([]river.InsertManyParams, len(params.Jobs))
	for i, job := range params.Jobs {
		insertParams[i] = river.InsertManyParams{Args: echoArgs(job.Args), InsertOpts: insertOpts(job.Opts)}
	}

	var results []*rivertype.JobInsertResult
	if inTx {
		results, err = client.InsertManyTx(ctx, tx, insertParams)
	} else {
		results, err = client.InsertMany(ctx, insertParams)
	}
	if err != nil {
		return nil, err
	}

	result := &protocol.InsertResult{Results: make([]protocol.JobInsertResult, len(results))}
	for i, inserted := range results {
		job, err := toProtocolJob(inserted.Job)
		if err != nil {
			return nil, err
		}
		result.Results[i] = protocol.JobInsertResult{Job: *job, UniqueSkippedAsDuplicate: inserted.UniqueSkippedAsDuplicate}
	}
	return result, nil
}

func (s *server[TTx]) handleJob(ctx context.Context, method string, rawParams json.RawMessage) (any, error) {
	var params protocol.JobParams
	if err := decodeParams(rawParams, &params); err != nil {
		return nil, err
	}
	client, err := s.client(params.Schema)
	if err != nil {
		return nil, err
	}
	tx, inTx, err := s.tx(params.Tx)
	if err != nil {
		return nil, err
	}

	var job *rivertype.JobRow
	switch {
	case method == protocol.MethodCancel && inTx:
		job, err = client.JobCancelTx(ctx, tx, params.ID)
	case method == protocol.MethodCancel:
		job, err = client.JobCancel(ctx, params.ID)
	case inTx:
		job, err = client.JobRetryTx(ctx, tx, params.ID)
	default:
		job, err = client.JobRetry(ctx, params.ID)
	}
	if errors.Is(err, river.ErrNotFound) {
		return nil, notFound(err)
	}
	if err != nil {
		return nil, err
	}
	return toProtocolJob(job)
}

func (s *server[TTx]) handleList(ctx context.Context, rawParams json.RawMessage) (any, error) {
	var params protocol.ListParams
	if err := decodeParams(rawParams, &params); err != nil {
		return nil, err
	}
	client, err := s.client(params.Schema)
	if err != nil {
		return nil, err
	}
	tx, inTx, err := s.tx(params.Tx)
	if err != nil {
		return nil, err
	}
	listParams, err := jobListParams(&params)
	if err != nil {
		return nil, err
	}

	var listed *river.JobListResult
	if inTx {
		listed, err = client.JobListTx(ctx, tx, listParams)
	} else {
		listed, err = client.JobList(ctx, listParams)
	}
	if err != nil {
		return nil, err
	}

	result := &protocol.ListResult{Jobs: make([]protocol.Job, len(listed.Jobs))}
	for i, row := range listed.Jobs {
		job, err := toProtocolJob(row)
		if err != nil {
			return nil, err
		}
		result.Jobs[i] = *job
	}
	if listed.LastCursor != nil {
		text, err := listed.LastCursor.MarshalText()
		if err != nil {
			return nil, err
		}
		cursor := string(text)
		result.Cursor = &cursor
	}
	return result, nil
}

func (s *server[TTx]) handleMigrate(ctx context.Context, rawParams json.RawMessage) (any, error) {
	var params protocol.MigrateParams
	if err := decodeParams(rawParams, &params); err != nil {
		return nil, err
	}
	migrator, err := rivermigrate.New(s.driver, &rivermigrate.Config{Logger: s.logger, Schema: params.Schema})
	if err != nil {
		return nil, err
	}

	direction := rivermigrate.DirectionUp
	switch params.Direction {
	case "", "up":
	case "down":
		direction = rivermigrate.DirectionDown
	default:
		return nil, invalidParams(fmt.Errorf("unknown direction %q", params.Direction))
	}
	opts := &rivermigrate.MigrateOpts{}
	if params.TargetVersion != nil {
		opts.TargetVersion = *params.TargetVersion
	}

	migrated, err := migrator.Migrate(ctx, direction, opts)
	if err != nil {
		return nil, err
	}
	result := &protocol.MigrateResult{Versions: make([]int, len(migrated.Versions))}
	for i, version := range migrated.Versions {
		result.Versions[i] = version.Version
	}
	return result, nil
}

func (s *server[TTx]) handleQueue(ctx context.Context, rawParams json.RawMessage) error {
	var params protocol.QueueParams
	if err := decodeParams(rawParams, &params); err != nil {
		return err
	}
	client, err := s.client(params.Schema)
	if err != nil {
		return err
	}
	tx, inTx, err := s.tx(params.Tx)
	if err != nil {
		return err
	}

	switch params.Action {
	case protocol.QueueActionPause:
		if inTx {
			err = client.QueuePauseTx(ctx, tx, params.Name, nil)
		} else {
			err = client.QueuePause(ctx, params.Name, nil)
		}
	case protocol.QueueActionResume:
		if inTx {
			err = client.QueueResumeTx(ctx, tx, params.Name, nil)
		} else {
			err = client.QueueResume(ctx, params.Name, nil)
		}
	case protocol.QueueActionUpdate:
		updateParams := &river.QueueUpdateParams{Metadata: params.Metadata}
		if inTx {
			_, err = client.QueueUpdateTx(ctx, tx, params.Name, updateParams)
		} else {
			_, err = client.QueueUpdate(ctx, params.Name, updateParams)
		}
	default:
		return invalidParams(fmt.Errorf("unknown queue action %q", params.Action))
	}
	if errors.Is(err, river.ErrNotFound) {
		return notFound(err)
	}
	return err
}

func (s *server[TTx]) handleRequestResign(ctx context.Context, rawParams json.RawMessage) error {
	var params protocol.RequestResignParams
	if err := decodeParams(rawParams, &params); err != nil {
		return err
	}
	client, err := s.client(params.Schema)
	if err != nil {
		return err
	}
	tx, inTx, err := s.tx(params.Tx)
	if err != nil {
		return err
	}
	if inTx {
		return client.Notify().RequestResignTx(ctx, tx)
	}
	return client.Notify().RequestResign(ctx)
}

func (s *server[TTx]) handleStart(ctx context.Context, rawParams json.RawMessage) error {
	var params protocol.StartParams
	if err := decodeParams(rawParams, &params); err != nil {
		return err
	}
	if s.running != nil {
		return errors.New("a client is already running")
	}

	stats := &stats{}
	config, err := workerConfig(&params, s.barriers, stats, s.logger)
	if err != nil {
		return err
	}
	client, err := river.NewClient(withClaimBarrier(s.driver, s.barriers, params.ClaimBarrier), config)
	if err != nil {
		return err
	}

	events, unsubscribe := client.Subscribe(
		river.EventKindJobCancelled,
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
		river.EventKindJobSnoozed,
		river.EventKindQueuePaused,
		river.EventKindQueueResumed,
	)
	go stats.consume(events)

	if err := client.Start(ctx); err != nil {
		unsubscribe()
		return err
	}
	s.running = &runningClient[TTx]{claimBarrier: params.ClaimBarrier, client: client, stats: stats, unsubscribe: unsubscribe}
	return nil
}

func (s *server[TTx]) handleStop(ctx context.Context, rawParams json.RawMessage) error {
	var params protocol.StopParams
	if err := decodeParams(rawParams, &params); err != nil {
		return err
	}
	if s.running == nil {
		return errors.New("no client is running")
	}
	return s.stop(ctx, params.Cancel)
}

func (s *server[TTx]) stop(ctx context.Context, cancel bool) error {
	running := s.running
	s.running = nil
	defer running.unsubscribe()

	// A claim held on its barrier would keep the client from stopping.
	if running.claimBarrier != "" {
		s.barriers.release(running.claimBarrier)
	}
	ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFunc()
	if cancel {
		return running.client.StopAndCancel(ctx)
	}
	return running.client.Stop(ctx)
}

func (s *server[TTx]) handleTxBegin(ctx context.Context, rawParams json.RawMessage) error {
	var params protocol.TxParams
	if err := decodeParams(rawParams, &params); err != nil {
		return err
	}
	if params.Tx == "" {
		return invalidParams(errors.New("tx is required"))
	}
	if _, ok := s.txs[params.Tx]; ok {
		return fmt.Errorf("transaction %q is already open", params.Tx)
	}
	tx, err := s.txFuncs.begin(ctx)
	if err != nil {
		return err
	}
	s.txs[params.Tx] = tx
	return nil
}

func (s *server[TTx]) handleTxEnd(ctx context.Context, rawParams json.RawMessage) error {
	var params protocol.TxEndParams
	if err := decodeParams(rawParams, &params); err != nil {
		return err
	}
	tx, _, err := s.tx(params.Tx)
	if err != nil {
		return err
	}
	delete(s.txs, params.Tx)
	if params.Commit {
		return s.txFuncs.commit(ctx, tx)
	}
	return s.txFuncs.rollback(ctx, tx)
}

func (s *server[TTx]) shutdown(ctx context.Context) {
	if s.running != nil {
		_ = s.stop(ctx, true)
	}
	for name, tx := range s.txs {
		_ = s.txFuncs.rollback(ctx, tx)
		delete(s.txs, name)
	}
}

// echoArgs are the args of every job the adapter inserts.
type echoArgs protocol.Args

func (echoArgs) Kind() string { return protocol.KindEcho }

func insertOpts(opts *protocol.InsertOpts) *river.InsertOpts {
	if opts == nil {
		return nil
	}
	insertOpts := &river.InsertOpts{
		MaxAttempts: opts.MaxAttempts,
		Metadata:    opts.Metadata,
		Pending:     opts.Pending,
		Priority:    opts.Priority,
		Queue:       opts.Queue,
		Tags:        opts.Tags,
	}
	if opts.ScheduledAt != nil {
		insertOpts.ScheduledAt = *opts.ScheduledAt
	}
	if unique := opts.Unique; unique != nil {
		insertOpts.UniqueOpts = river.UniqueOpts{
			ByArgs:      unique.ByArgs,
			ByPeriod:    time.Duration(unique.ByPeriodMS) * time.Millisecond,
			ByQueue:     unique.ByQueue,
			ExcludeKind: unique.ExcludeKind,
		}
		for _, state := range unique.ByState {
			insertOpts.UniqueOpts.ByState = append(insertOpts.UniqueOpts.ByState, rivertype.JobState(state))
		}
	}
	return insertOpts
}

func jobListParams(listParams *protocol.ListParams) (*river.JobListParams, error) {
	params := river.NewJobListParams()
	if listParams.After != "" {
		cursor := &river.JobListCursor{}
		if err := cursor.UnmarshalText([]byte(listParams.After)); err != nil {
			return nil, invalidParams(fmt.Errorf("invalid cursor: %w", err))
		}
		params = params.After(cursor)
	}
	if len(listParams.IDs) > 0 {
		params = params.IDs(listParams.IDs...)
	}
	if len(listParams.Kinds) > 0 {
		params = params.Kinds(listParams.Kinds...)
	}
	if listParams.Limit > 0 {
		params = params.First(listParams.Limit)
	}
	if len(listParams.Metadata) > 0 {
		params = params.Metadata(string(listParams.Metadata))
	}

	direction := river.SortOrderAsc
	switch listParams.Direction {
	case "", "asc":
	case "desc":
		direction = river.SortOrderDesc
	default:
		return nil, invalidParams(fmt.Errorf("unknown direction %q", listParams.Direction))
	}
	orderBy := river.JobListOrderByID
	if listParams.OrderBy != "" {
		orderBy = river.JobListOrderByField(listParams.OrderBy)
	}
	params = params.OrderBy(orderBy, direction)

	if len(listParams.Priorities) > 0 {
		priorities := make([]int16, len(listParams.Priorities))
		for i, priority := range listParams.Priorities {
			priorities[i] = int16(priority) //nolint:gosec // job priorities are small
		}
		params = params.Priorities(priorities...)
	}
	if len(listParams.Queues) > 0 {
		params = params.Queues(listParams.Queues...)
	}
	if len(listParams.States) > 0 {
		states := make([]rivertype.JobState, len(listParams.States))
		for i, state := range listParams.States {
			states[i] = rivertype.JobState(state)
		}
		params = params.States(states...)
	}
	if len(listParams.TagsAll) > 0 {
		params = params.TagsAll(listParams.TagsAll...)
	}
	return params, nil
}

// toProtocolJob reports a job row in the contract's form.
func toProtocolJob(row *rivertype.JobRow) (*protocol.Job, error) {
	job := &protocol.Job{
		Attempt:     row.Attempt,
		AttemptedAt: utc(row.AttemptedAt),
		AttemptedBy: row.AttemptedBy,
		CreatedAt:   row.CreatedAt.UTC(),
		Errors:      make([]protocol.AttemptError, len(row.Errors)),
		FinalizedAt: utc(row.FinalizedAt),
		ID:          row.ID,
		Kind:        row.Kind,
		MaxAttempts: row.MaxAttempts,
		Priority:    row.Priority,
		Queue:       row.Queue,
		ScheduledAt: row.ScheduledAt.UTC(),
		State:       string(row.State),
		Tags:        row.Tags,
	}
	if job.AttemptedBy == nil {
		job.AttemptedBy = []string{}
	}
	if job.Tags == nil {
		job.Tags = []string{}
	}
	for i, attemptErr := range row.Errors {
		job.Errors[i] = protocol.AttemptError{At: attemptErr.At.UTC(), Attempt: attemptErr.Attempt, Error: attemptErr.Error, Trace: attemptErr.Trace}
	}
	if err := json.Unmarshal(row.EncodedArgs, &job.Args); err != nil {
		return nil, fmt.Errorf("error decoding args of job %d: %w", row.ID, err)
	}
	// Numbers decode exactly, so they're reported as stored.
	decoder := json.NewDecoder(bytes.NewReader(row.Metadata))
	decoder.UseNumber()
	if err := decoder.Decode(&job.Metadata); err != nil {
		return nil, fmt.Errorf("error decoding metadata of job %d: %w", row.ID, err)
	}
	delete(job.Metadata, "river:unique_nonce")
	if row.UniqueKey != nil {
		uniqueKey := hex.EncodeToString(row.UniqueKey)
		job.UniqueKey = &uniqueKey
	}
	if row.UniqueStates != nil {
		job.UniqueStates = make([]string, len(row.UniqueStates))
		for i, state := range row.UniqueStates {
			job.UniqueStates[i] = string(state)
		}
		slices.Sort(job.UniqueStates)
	}
	return job, nil
}

func utc(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	converted := value.UTC()
	return &converted
}
