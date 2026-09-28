//go:build foundationdb

package riverfdb

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivertype"
)

func (e *executor) JobCancel(ctx context.Context, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	return e.updateJob(ctx, params.Schema, params.ID, func(tx fdb.Transaction, job *rivertype.JobRow) error {
		if job.FinalizedAt != nil {
			return nil
		}
		if job.State != rivertype.JobStateRunning {
			job.State = rivertype.JobStateCancelled
			job.FinalizedAt = new(timeOrNow(params.Now))
		}
		updates, err := json.Marshal(map[string]time.Time{"cancel_attempted_at": params.CancelAttemptedAt})
		if err != nil {
			return err
		}
		job.Metadata, err = mergeMetadata(job.Metadata, updates)
		if err != nil {
			return err
		}
		payload, err := json.Marshal(map[string]any{"action": "cancel", "job_id": job.ID, "queue": job.Queue})
		if err != nil {
			return err
		}
		return e.notifyMany(tx, &riverdriver.NotifyManyParams{Payload: []string{string(payload)}, Schema: params.Schema, Topic: params.ControlTopic})
	})
}

func (e *executor) JobCountByAllStates(ctx context.Context, params *riverdriver.JobCountByAllStatesParams) (map[rivertype.JobState]int, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (map[rivertype.JobState]int, error) {
		jobs, err := e.jobs(tx, params.Schema)
		if err != nil {
			return nil, err
		}
		counts := make(map[rivertype.JobState]int)
		for _, state := range rivertype.JobStates() {
			counts[state] = 0
		}
		for _, job := range jobs {
			counts[job.State]++
		}
		return counts, nil
	})
}

func (e *executor) JobCountByQueueAndState(ctx context.Context, params *riverdriver.JobCountByQueueAndStateParams) ([]*riverdriver.JobCountByQueueAndStateResult, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]*riverdriver.JobCountByQueueAndStateResult, error) {
		jobs, err := e.jobs(tx, params.Schema)
		if err != nil {
			return nil, err
		}
		names := slices.Clone(params.QueueNames)
		slices.Sort(names)
		results := make([]*riverdriver.JobCountByQueueAndStateResult, 0, len(names))
		for _, name := range slices.Compact(names) {
			result := &riverdriver.JobCountByQueueAndStateResult{Queue: name}
			for _, job := range jobs {
				if job.Queue != name {
					continue
				}
				if job.State == rivertype.JobStateAvailable {
					result.CountAvailable++
				}
				if job.State == rivertype.JobStateRunning {
					result.CountRunning++
				}
			}
			results = append(results, result)
		}
		return results, nil
	})
}

func (e *executor) JobCountByState(ctx context.Context, params *riverdriver.JobCountByStateParams) (int, error) {
	counts, err := e.JobCountByAllStates(ctx, &riverdriver.JobCountByAllStatesParams{Schema: params.Schema})
	return counts[params.State], err
}

func (e *executor) JobDelete(ctx context.Context, params *riverdriver.JobDeleteParams) (*rivertype.JobRow, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (*rivertype.JobRow, error) {
		job, err := e.job(tx, params.Schema, params.ID)
		if err != nil {
			return nil, err
		}
		if job.State == rivertype.JobStateRunning {
			return nil, rivertype.ErrJobRunning
		}
		e.clearJob(tx, params.Schema, job)
		return job, nil
	})
}

func (e *executor) JobDeleteBefore(ctx context.Context, params *riverdriver.JobDeleteBeforeParams) (int, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (int, error) {
		jobs, err := e.jobs(tx, params.Schema)
		if err != nil {
			return 0, err
		}
		count := 0
		for _, job := range jobs {
			if count >= params.Max {
				break
			}
			if job.FinalizedAt == nil || slices.Contains(params.QueuesExcluded, job.Queue) ||
				(params.QueuesIncluded != nil && !slices.Contains(params.QueuesIncluded, job.Queue)) {
				continue
			}
			deleteJob := (job.State == rivertype.JobStateCancelled && params.CancelledDoDelete && job.FinalizedAt.Before(params.CancelledFinalizedAtHorizon)) ||
				(job.State == rivertype.JobStateCompleted && params.CompletedDoDelete && job.FinalizedAt.Before(params.CompletedFinalizedAtHorizon)) ||
				(job.State == rivertype.JobStateDiscarded && params.DiscardedDoDelete && job.FinalizedAt.Before(params.DiscardedFinalizedAtHorizon))
			if deleteJob {
				e.clearJob(tx, params.Schema, job)
				count++
			}
		}
		return count, nil
	})
}

func (e *executor) JobGetAvailable(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]*rivertype.JobRow, error) {
		result := make([]*rivertype.JobRow, 0)
		if params.MaxToLock <= 0 || (params.Kind != nil && len(params.Kind) == 0) {
			return result, nil
		}
		now := timeOrNow(params.Now)
		// The ready index avoids scanning completed jobs. Normal (not snapshot)
		// range reads conflict with other claimers, which retry after commit.
		iter := tx.GetRange(prefixRange(e.driver.key(params.Schema, "ready", params.Queue)), fdb.RangeOptions{}).Iterator()
		for len(result) < params.MaxToLock && iter.Advance() {
			entry, err := iter.Get()
			if err != nil {
				return nil, err
			}
			id, err := strconv.ParseInt(string(entry.Value), 10, 64)
			if err != nil {
				return nil, err
			}
			job, err := e.job(tx, params.Schema, id)
			if err != nil {
				return nil, err
			}
			if job.ScheduledAt.After(now) || (params.Kind != nil && !slices.Contains(params.Kind, job.Kind)) {
				continue
			}
			before := *job
			job.Attempt++
			job.AttemptedAt = &now
			job.AttemptedBy = append(job.AttemptedBy, params.ClientID)
			if params.MaxAttemptedBy > 0 && len(job.AttemptedBy) > params.MaxAttemptedBy {
				job.AttemptedBy = job.AttemptedBy[len(job.AttemptedBy)-params.MaxAttemptedBy:]
			}
			job.State = rivertype.JobStateRunning
			if err := e.saveJob(tx, params.Schema, &before, job); err != nil {
				return nil, err
			}
			result = append(result, job)
		}
		return result, nil
	})
}

func (e *executor) JobGetByID(ctx context.Context, params *riverdriver.JobGetByIDParams) (*rivertype.JobRow, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (*rivertype.JobRow, error) { return e.job(tx, params.Schema, params.ID) })
}

func (e *executor) JobGetByIDMany(ctx context.Context, params *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]*rivertype.JobRow, error) {
		ids := slices.Clone(params.ID)
		slices.Sort(ids)
		jobs := make([]*rivertype.JobRow, 0, len(ids))
		for _, id := range slices.Compact(ids) {
			job, err := e.job(tx, params.Schema, id)
			if errors.Is(err, rivertype.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, job)
		}
		return jobs, nil
	})
}

func (e *executor) JobGetByKindMany(ctx context.Context, params *riverdriver.JobGetByKindManyParams) ([]*rivertype.JobRow, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]*rivertype.JobRow, error) {
		jobs, err := e.jobs(tx, params.Schema)
		if err != nil {
			return nil, err
		}
		return slices.DeleteFunc(jobs, func(job *rivertype.JobRow) bool { return !slices.Contains(params.Kind, job.Kind) }), nil
	})
}

func (e *executor) JobGetCancelRequested(ctx context.Context, params *riverdriver.JobGetCancelRequestedParams) ([]int64, error) {
	jobs, err := e.JobGetByIDMany(ctx, &riverdriver.JobGetByIDManyParams{ID: params.ID, Schema: params.Schema})
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0)
	for _, job := range jobs {
		if job.State == rivertype.JobStateRunning && cancelRequested(job) {
			ids = append(ids, job.ID)
		}
	}
	return ids, nil
}

func (e *executor) JobGetStuck(ctx context.Context, params *riverdriver.JobGetStuckParams) ([]*rivertype.JobRow, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]*rivertype.JobRow, error) {
		jobs, err := e.jobs(tx, params.Schema)
		if err != nil {
			return nil, err
		}
		jobs = slices.DeleteFunc(jobs, func(job *rivertype.JobRow) bool {
			return job.ID <= params.AfterID || job.State != rivertype.JobStateRunning || job.AttemptedAt == nil || !job.AttemptedAt.Before(params.StuckHorizon)
		})
		return jobs[:min(len(jobs), max(0, params.Max))], nil
	})
}

func (e *executor) JobInsertFastMany(ctx context.Context, params *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]*riverdriver.JobInsertFastResult, error) {
		results := make([]*riverdriver.JobInsertFastResult, 0, len(params.Jobs))
		for _, param := range params.Jobs {
			job := &rivertype.JobRow{
				CreatedAt: timeOrNow(param.CreatedAt), EncodedArgs: bytes.Clone(param.EncodedArgs),
				Kind: param.Kind, MaxAttempts: param.MaxAttempts, Metadata: defaultMetadata(param.Metadata),
				Priority: param.Priority, Queue: param.Queue, ScheduledAt: timeOrNow(param.ScheduledAt),
				State: param.State, Tags: slices.Clone(param.Tags), UniqueKey: bytes.Clone(param.UniqueKey),
				UniqueStates: uniquestates.UniqueBitmaskToStates(param.UniqueStates),
			}
			if err := validateJob(job); err != nil {
				return nil, err
			}
			if uniqueActive(job) {
				otherID, err := e.uniqueOwner(tx, params.Schema, job.UniqueKey)
				if err != nil {
					return nil, err
				}
				if otherID != 0 {
					other, err := e.job(tx, params.Schema, otherID)
					if err != nil {
						return nil, err
					}
					results = append(results, &riverdriver.JobInsertFastResult{Job: other, UniqueSkippedAsDuplicate: true})
					continue
				}
			}
			id, err := e.nextID(tx, params.Schema, param.ID)
			if err != nil {
				return nil, err
			}
			job.ID = id
			if err := e.saveJob(tx, params.Schema, nil, job); err != nil {
				return nil, err
			}
			results = append(results, &riverdriver.JobInsertFastResult{Job: job})
		}
		return results, nil
	})
}

func (e *executor) JobInsertFastManyNoReturning(ctx context.Context, params *riverdriver.JobInsertFastManyParams) (int, error) {
	results, err := e.JobInsertFastMany(ctx, params)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, result := range results {
		if !result.UniqueSkippedAsDuplicate {
			count++
		}
	}
	return count, nil
}

func (e *executor) JobInsertFull(ctx context.Context, params *riverdriver.JobInsertFullParams) (*rivertype.JobRow, error) {
	jobs, err := e.JobInsertFullMany(ctx, &riverdriver.JobInsertFullManyParams{Jobs: []*riverdriver.JobInsertFullParams{params}, Schema: params.Schema})
	if err != nil {
		return nil, err
	}
	return jobs[0], nil
}

func (e *executor) JobInsertFullMany(ctx context.Context, params *riverdriver.JobInsertFullManyParams) ([]*rivertype.JobRow, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]*rivertype.JobRow, error) {
		jobs := make([]*rivertype.JobRow, 0, len(params.Jobs))
		for _, param := range params.Jobs {
			id, err := e.nextID(tx, params.Schema, nil)
			if err != nil {
				return nil, err
			}
			job := &rivertype.JobRow{
				ID: id, Attempt: param.Attempt, AttemptedAt: param.AttemptedAt, AttemptedBy: slices.Clone(param.AttemptedBy),
				CreatedAt: timeOrNow(param.CreatedAt), EncodedArgs: bytes.Clone(param.EncodedArgs),
				FinalizedAt: param.FinalizedAt, Kind: param.Kind, MaxAttempts: param.MaxAttempts, Metadata: defaultMetadata(param.Metadata),
				Priority: param.Priority, Queue: param.Queue, ScheduledAt: timeOrNow(param.ScheduledAt),
				State: param.State, Tags: slices.Clone(param.Tags), UniqueKey: bytes.Clone(param.UniqueKey),
				UniqueStates: uniquestates.UniqueBitmaskToStates(param.UniqueStates),
			}
			for _, data := range param.Errors {
				if err := appendError(job, data); err != nil {
					return nil, err
				}
			}
			if err := e.saveJob(tx, params.Schema, nil, job); err != nil {
				return nil, err
			}
			jobs = append(jobs, job)
		}
		return jobs, nil
	})
}

func (e *executor) JobKindList(ctx context.Context, params *riverdriver.JobKindListParams) ([]string, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]string, error) {
		jobs, err := e.jobs(tx, params.Schema)
		if err != nil {
			return nil, err
		}
		kinds := make([]string, 0)
		for _, job := range jobs {
			kinds = append(kinds, job.Kind)
		}
		return filterNames(kinds, params.After, params.Match, params.Exclude, params.Max), nil
	})
}

func (e *executor) JobRescueMany(ctx context.Context, params *riverdriver.JobRescueManyParams) (*struct{}, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (*struct{}, error) {
		for i, id := range params.ID {
			job, err := e.job(tx, params.Schema, id)
			if errors.Is(err, rivertype.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if job.State != rivertype.JobStateRunning || job.AttemptedAt == nil || !job.AttemptedAt.Before(params.StuckHorizon) {
				continue
			}
			before := *job
			job.FinalizedAt = params.FinalizedAt[i]
			job.ScheduledAt = params.ScheduledAt[i]
			job.State = rivertype.JobState(params.State[i])
			if err := appendError(job, params.Error[i]); err != nil {
				return nil, err
			}
			var metadata map[string]json.RawMessage
			if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
				return nil, err
			}
			var count int
			_ = json.Unmarshal(metadata["river:rescue_count"], &count)
			job.Metadata, err = mergeMetadata(job.Metadata, []byte(fmt.Sprintf(`{"river:rescue_count":%d}`, count+1)))
			if err != nil {
				return nil, err
			}
			if err := e.saveJob(tx, params.Schema, &before, job); err != nil {
				return nil, err
			}
		}
		return &struct{}{}, nil
	})
}

func (e *executor) JobRetry(ctx context.Context, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error) {
	return e.updateJob(ctx, params.Schema, params.ID, func(tx fdb.Transaction, job *rivertype.JobRow) error {
		now := timeOrNow(params.Now)
		if job.State == rivertype.JobStateRunning || (job.State == rivertype.JobStateAvailable && job.ScheduledAt.Before(now)) {
			return nil
		}
		job.FinalizedAt = nil
		if job.Attempt == job.MaxAttempts {
			job.MaxAttempts++
		}
		job.ScheduledAt = now
		job.State = rivertype.JobStateAvailable
		return nil
	})
}

func (e *executor) JobSchedule(ctx context.Context, params *riverdriver.JobScheduleParams) ([]*riverdriver.JobScheduleResult, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]*riverdriver.JobScheduleResult, error) {
		jobs, err := e.jobs(tx, params.Schema)
		if err != nil {
			return nil, err
		}
		now := timeOrNow(params.Now)
		jobs = slices.DeleteFunc(jobs, func(job *rivertype.JobRow) bool {
			return (job.State != rivertype.JobStateScheduled && job.State != rivertype.JobStateRetryable) || job.ScheduledAt.After(now)
		})
		slices.SortFunc(jobs, compareJobs)
		results := make([]*riverdriver.JobScheduleResult, 0)
		for _, job := range jobs[:min(len(jobs), max(0, params.Max))] {
			before := *job
			job.State = rivertype.JobStateAvailable
			conflict := false
			if len(job.UniqueKey) > 0 && len(job.UniqueStates) > 0 {
				owner, err := e.uniqueOwner(tx, params.Schema, job.UniqueKey)
				if err != nil {
					return nil, err
				}
				conflict = owner != 0 && owner != job.ID
				if conflict {
					job.State = rivertype.JobStateDiscarded
					job.FinalizedAt = &now
					job.Metadata, err = mergeMetadata(job.Metadata, []byte(`{"unique_key_conflict":"scheduler_discarded"}`))
					if err != nil {
						return nil, err
					}
				}
			}
			if err := e.saveJob(tx, params.Schema, &before, job); err != nil {
				return nil, err
			}
			results = append(results, &riverdriver.JobScheduleResult{ConflictDiscarded: conflict, Job: *job})
		}
		return results, nil
	})
}

func (e *executor) JobSetStateIfRunningMany(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	return transact(ctx, e, func(tx fdb.Transaction) ([]*rivertype.JobRow, error) {
		jobs := make([]*rivertype.JobRow, 0, len(params.ID))
		for i, id := range params.ID {
			job, err := e.job(tx, params.Schema, id)
			if errors.Is(err, rivertype.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			before := *job
			if job.State == rivertype.JobStateRunning {
				if err := appendError(job, params.ErrData[i]); err != nil {
					return nil, err
				}
				state := params.State[i]
				if cancelRequested(job) && (state == rivertype.JobStateAvailable || state == rivertype.JobStateRetryable || state == rivertype.JobStateScheduled) {
					job.FinalizedAt = new(timeOrNow(params.Now))
					job.State = rivertype.JobStateCancelled
				} else {
					if params.Attempt[i] != nil {
						job.Attempt = *params.Attempt[i]
					}
					if params.FinalizedAt[i] != nil {
						job.FinalizedAt = params.FinalizedAt[i]
					}
					if params.ScheduledAt[i] != nil {
						job.ScheduledAt = *params.ScheduledAt[i]
					}
					job.State = state
				}
			}
			if params.MetadataDoMerge[i] {
				job.Metadata, err = mergeMetadata(job.Metadata, params.MetadataUpdates[i])
				if err != nil {
					return nil, err
				}
			}
			if err := e.saveJob(tx, params.Schema, &before, job); err != nil {
				return nil, err
			}
			jobs = append(jobs, job)
		}
		slices.SortFunc(jobs, func(a, b *rivertype.JobRow) int { return cmp.Compare(a.ID, b.ID) })
		return jobs, nil
	})
}

func (e *executor) JobUpdate(ctx context.Context, params *riverdriver.JobUpdateParams) (*rivertype.JobRow, error) {
	return e.updateJob(ctx, params.Schema, params.ID, func(tx fdb.Transaction, job *rivertype.JobRow) error {
		if !params.MetadataDoMerge {
			return nil
		}
		var err error
		job.Metadata, err = mergeMetadata(job.Metadata, params.Metadata)
		return err
	})
}

func (e *executor) JobUpdateFull(ctx context.Context, params *riverdriver.JobUpdateFullParams) (*rivertype.JobRow, error) {
	return e.updateJob(ctx, params.Schema, params.ID, func(tx fdb.Transaction, job *rivertype.JobRow) error {
		if params.AttemptDoUpdate {
			job.Attempt = params.Attempt
		}
		if params.AttemptedAtDoUpdate {
			job.AttemptedAt = params.AttemptedAt
		}
		if params.AttemptedByDoUpdate {
			job.AttemptedBy = slices.Clone(params.AttemptedBy)
		}
		if params.ErrorsDoUpdate {
			job.Errors = nil
			for _, data := range params.Errors {
				if err := appendError(job, data); err != nil {
					return err
				}
			}
		}
		if params.FinalizedAtDoUpdate {
			job.FinalizedAt = params.FinalizedAt
		}
		if params.MaxAttemptsDoUpdate {
			job.MaxAttempts = params.MaxAttempts
		}
		if params.MetadataDoUpdate {
			job.Metadata = bytes.Clone(params.Metadata)
		}
		if params.StateDoUpdate {
			job.State = params.State
		}
		if params.UniqueKeyDoUpdate {
			job.UniqueKey = bytes.Clone(params.UniqueKey)
		}
		return nil
	})
}

func (e *executor) clearJob(tx fdb.Transaction, schema string, job *rivertype.JobRow) {
	tx.Clear(e.driver.key(schema, "job", job.ID))
	if job.State == rivertype.JobStateAvailable {
		tx.Clear(e.readyKey(schema, job))
	}
	if uniqueActive(job) {
		tx.Clear(e.driver.key(schema, "unique", job.UniqueKey))
	}
}

func (e *executor) job(tx fdb.Transaction, schema string, id int64) (*rivertype.JobRow, error) {
	return readJSON[rivertype.JobRow](tx, e.driver.key(schema, "job", id))
}

func (e *executor) jobs(tx fdb.Transaction, schema string) ([]*rivertype.JobRow, error) {
	return scanJSON[rivertype.JobRow](tx, e.driver.key(schema, "job"))
}

func (e *executor) nextID(tx fdb.Transaction, schema string, requested *int64) (int64, error) {
	if requested != nil {
		if *requested <= 0 {
			return 0, errors.New("riverfdb: job ID must be positive")
		}
		data, err := tx.Get(e.driver.key(schema, "job", *requested)).Get()
		if err != nil {
			return 0, err
		}
		if data != nil {
			return 0, errors.New("riverfdb: duplicate job ID")
		}
		return *requested, nil
	}
	key := e.driver.key(schema, "sequence")
	data, err := tx.Get(key).Get()
	if err != nil {
		return 0, err
	}
	var id int64
	if data != nil {
		id, err = strconv.ParseInt(string(data), 10, 64)
		if err != nil {
			return 0, err
		}
	}
	for {
		if id == math.MaxInt64 {
			return 0, errors.New("riverfdb: job ID sequence exhausted")
		}
		id++
		data, err := tx.Get(e.driver.key(schema, "job", id)).Get()
		if err != nil {
			return 0, err
		}
		if data == nil {
			break
		}
	}
	tx.Set(key, []byte(strconv.FormatInt(id, 10)))
	return id, nil
}

func (e *executor) readyKey(schema string, job *rivertype.JobRow) fdb.Key {
	return e.driver.key(schema, "ready", job.Queue, job.Priority, job.ScheduledAt.Unix(), job.ScheduledAt.Nanosecond(), job.ID)
}

func (e *executor) saveJob(tx fdb.Transaction, schema string, before, job *rivertype.JobRow) error {
	if err := validateJob(job); err != nil {
		return err
	}
	if uniqueActive(job) {
		owner, err := e.uniqueOwner(tx, schema, job.UniqueKey)
		if err != nil {
			return err
		}
		if owner != 0 && owner != job.ID {
			return errors.New("riverfdb: unique job conflict")
		}
	}
	// Marshal and validate before applying mutations to the transaction.
	data, err := encodeJSON(job)
	if err != nil {
		return err
	}
	if before != nil {
		e.clearJob(tx, schema, before)
	}
	tx.Set(e.driver.key(schema, "job", job.ID), data)
	if job.State == rivertype.JobStateAvailable {
		tx.Set(e.readyKey(schema, job), []byte(strconv.FormatInt(job.ID, 10)))
	}
	if uniqueActive(job) {
		tx.Set(e.driver.key(schema, "unique", job.UniqueKey), []byte(strconv.FormatInt(job.ID, 10)))
	}
	return nil
}

func (e *executor) uniqueOwner(tx fdb.Transaction, schema string, uniqueKey []byte) (int64, error) {
	data, err := tx.Get(e.driver.key(schema, "unique", uniqueKey)).Get()
	if err != nil || data == nil {
		return 0, err
	}
	return strconv.ParseInt(string(data), 10, 64)
}

func (e *executor) updateJob(ctx context.Context, schema string, id int64, updateFunc func(fdb.Transaction, *rivertype.JobRow) error) (*rivertype.JobRow, error) {
	return transact(ctx, e, func(tx fdb.Transaction) (*rivertype.JobRow, error) {
		job, err := e.job(tx, schema, id)
		if err != nil {
			return nil, err
		}
		before := *job
		if err := updateFunc(tx, job); err != nil {
			return nil, err
		}
		if err := e.saveJob(tx, schema, &before, job); err != nil {
			return nil, err
		}
		return job, nil
	})
}

func appendError(job *rivertype.JobRow, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var attemptError rivertype.AttemptError
	if err := json.Unmarshal(data, &attemptError); err != nil {
		return fmt.Errorf("riverfdb: decode attempt error: %w", err)
	}
	job.Errors = append(job.Errors, attemptError)
	return nil
}

func cancelRequested(job *rivertype.JobRow) bool {
	var metadata map[string]json.RawMessage
	if json.Unmarshal(job.Metadata, &metadata) != nil {
		return false
	}
	_, exists := metadata["cancel_attempted_at"]
	return exists
}

func compareJobs(jobA, jobB *rivertype.JobRow) int {
	if result := cmp.Compare(jobA.Priority, jobB.Priority); result != 0 {
		return result
	}
	if result := jobA.ScheduledAt.Compare(jobB.ScheduledAt); result != 0 {
		return result
	}
	return cmp.Compare(jobA.ID, jobB.ID)
}

func defaultMetadata(data []byte) []byte {
	if len(data) == 0 {
		return []byte("{}")
	}
	return bytes.Clone(data)
}

func filterNames(names []string, after, match string, exclude []string, limit int) []string {
	slices.Sort(names)
	names = slices.Compact(names)
	names = slices.DeleteFunc(names, func(name string) bool {
		return name <= after || !strings.Contains(strings.ToLower(name), strings.ToLower(match)) || slices.Contains(exclude, name)
	})
	return names[:min(len(names), max(0, limit))]
}

func mergeMetadata(data, updates []byte) ([]byte, error) {
	var existing, extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &existing); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(updates, &extra); err != nil {
		return nil, err
	}
	if existing == nil {
		existing = make(map[string]json.RawMessage)
	}
	maps.Copy(existing, extra)
	return json.Marshal(existing)
}

func timeOrNow(value *time.Time) time.Time {
	if value != nil {
		return value.UTC()
	}
	return time.Now().UTC()
}

func uniqueActive(job *rivertype.JobRow) bool {
	return len(job.UniqueKey) > 0 && slices.Contains(job.UniqueStates, job.State)
}

func validateJob(job *rivertype.JobRow) error {
	if !slices.Contains(rivertype.JobStates(), job.State) {
		return errors.New("riverfdb: invalid job state")
	}
	finalized := job.State == rivertype.JobStateCancelled || job.State == rivertype.JobStateCompleted || job.State == rivertype.JobStateDiscarded
	if finalized != (job.FinalizedAt != nil) {
		return errors.New("riverfdb: finalized state and finalized_at must agree")
	}
	if job.Priority < 1 || job.Priority > 4 {
		return errors.New("riverfdb: priority must be between 1 and 4")
	}
	if len(job.Kind) == 0 || len(job.Kind) > 127 || len(job.Queue) == 0 || len(job.Queue) > 127 {
		return errors.New("riverfdb: kind and queue must contain 1–127 bytes")
	}
	if job.Attempt < 0 || job.Attempt > math.MaxInt16 || job.MaxAttempts < 0 || job.MaxAttempts > math.MaxInt16 {
		return errors.New("riverfdb: attempts must fit a nonnegative int16")
	}
	if !json.Valid(job.EncodedArgs) {
		return errors.New("riverfdb: arguments must be valid JSON")
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(job.Metadata, &metadata); err != nil || metadata == nil {
		return errors.New("riverfdb: metadata must be a JSON object")
	}
	if len(job.UniqueKey) > 1024 {
		return errors.New("riverfdb: unique key exceeds 1024 bytes")
	}
	return nil
}
