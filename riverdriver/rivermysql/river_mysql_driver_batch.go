package rivermysql

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

// sqlc's MySQL parser doesn't support JSON_TABLE. Keep these batch queries
// outside its input directory and use the same schema replacement and executor
// as generated queries. Single-row operations continue to use sqlc.
var (
	//go:embed internal/dbbatch/job_insert.sql
	jobInsertBatchSQL string
	//go:embed internal/dbbatch/job_rescue.sql
	jobRescueBatchSQL string
	//go:embed internal/dbbatch/job_set_state.sql
	jobSetStateBatchSQL string
	//go:embed internal/dbbatch/notification_insert.sql
	notificationInsertBatchSQL string
)

// A joined UPDATE only updates each matching row once. Use the sequential path
// for repeated IDs to retain the ordering of state changes and metadata merges.
func batchIDsUnique(ids []int64) bool {
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			return false
		}
		seen[id] = struct{}{}
	}
	return true
}

func batchTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	return new(t.UTC().Format("2006-01-02 15:04:05.000000"))
}

func execJSONBatch(ctx context.Context, dbtx templateReplaceWrapper, query string, rows any, args ...any) error {
	encoded, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	_, err = dbtx.ExecContext(ctx, query, append([]any{encoded}, args...)...)
	return interpretError(err)
}

// MySQL requires a default database for joined updates containing derived
// tables, even with fully qualified table names. Retain the sequential path
// for connections without a default database; don't change pooled session state.
func isNoDatabaseSelected(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1046
}

func jobInsertBatch(ctx context.Context, dbtx templateReplaceWrapper, params *riverdriver.JobInsertFastManyParams) error {
	// JSON documents travel as strings, then CAST back to JSON in SQL. This
	// preserves JSON null (distinct from SQL NULL), large integers, and the
	// database's validation of malformed JSON without decoding job arguments.
	type row struct {
		ID           *int64             `json:"id"`
		Args         string             `json:"args"`
		CreatedAt    *string            `json:"created_at"`
		Kind         string             `json:"kind"`
		MaxAttempts  int                `json:"max_attempts"`
		Metadata     string             `json:"metadata"`
		Priority     int                `json:"priority"`
		Queue        string             `json:"queue"`
		ScheduledAt  *string            `json:"scheduled_at"`
		State        rivertype.JobState `json:"state"`
		Tags         string             `json:"tags"`
		UniqueStates byte               `json:"unique_states"`
	}
	rows := make([]row, len(params.Jobs))
	for i, job := range params.Jobs {
		tags, err := json.Marshal(job.Tags)
		if err != nil {
			return err
		}
		rows[i] = row{
			ID:           job.ID,
			Args:         string(job.EncodedArgs),
			CreatedAt:    batchTime(job.CreatedAt),
			Kind:         job.Kind,
			MaxAttempts:  job.MaxAttempts,
			Metadata:     string(sliceutil.FirstNonEmpty(job.Metadata, []byte("{}"))),
			Priority:     job.Priority,
			Queue:        job.Queue,
			ScheduledAt:  batchTime(job.ScheduledAt),
			State:        job.State,
			Tags:         string(tags),
			UniqueStates: job.UniqueStates,
		}
	}
	return execJSONBatch(ctx, dbtx, jobInsertBatchSQL, rows)
}

func jobRescueBatch(ctx context.Context, dbtx templateReplaceWrapper, params *riverdriver.JobRescueManyParams) error {
	type row struct {
		ID          int64   `json:"id"`
		Error       *string `json:"error"`
		FinalizedAt *string `json:"finalized_at"`
		ScheduledAt *string `json:"scheduled_at"`
		State       string  `json:"state"`
	}
	rows := make([]row, len(params.ID))
	for i, id := range params.ID {
		rows[i] = row{
			ID:          id,
			FinalizedAt: batchTime(params.FinalizedAt[i]),
			ScheduledAt: batchTime(&params.ScheduledAt[i]),
			State:       params.State[i],
		}
		if params.Error[i] != nil {
			rows[i].Error = new(string(params.Error[i]))
		}
	}
	return execJSONBatch(ctx, dbtx, jobRescueBatchSQL, rows, params.StuckHorizon.UTC())
}

func jobSetStateBatch(ctx context.Context, dbtx templateReplaceWrapper, params *riverdriver.JobSetStateIfRunningManyParams) error {
	type row struct {
		ID          int64              `json:"id"`
		Attempt     *int               `json:"attempt"`
		Error       *string            `json:"error"`
		FinalizedAt *string            `json:"finalized_at"`
		Metadata    *string            `json:"metadata"`
		ScheduledAt *string            `json:"scheduled_at"`
		State       rivertype.JobState `json:"state"`
	}
	rows := make([]row, len(params.ID))
	for i, id := range params.ID {
		rows[i] = row{
			ID:          id,
			Attempt:     params.Attempt[i],
			FinalizedAt: batchTime(params.FinalizedAt[i]),
			ScheduledAt: batchTime(params.ScheduledAt[i]),
			State:       params.State[i],
		}
		if params.ErrData[i] != nil {
			rows[i].Error = new(string(params.ErrData[i]))
		}
		if params.MetadataDoMerge[i] {
			rows[i].Metadata = new(string(params.MetadataUpdates[i]))
		}
	}
	return execJSONBatch(ctx, dbtx, jobSetStateBatchSQL, rows, nullTimeFromPtr(params.Now))
}
