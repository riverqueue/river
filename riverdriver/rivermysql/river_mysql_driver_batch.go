package rivermysql

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/rivermysql/internal/dbsqlc"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

// sqlc's MySQL parser doesn't support JSON_TABLE. Keep these batch queries
// outside its input directory and use the same schema replacement and executor
// as generated queries. Single-row operations continue to use sqlc.
var (
	//go:embed internal/dbbatch/job_insert.sql
	jobInsertBatchSQL string
	//go:embed internal/dbbatch/job_insert_upsert.sql
	jobInsertBatchUpsertSQL string
	//go:embed internal/dbbatch/job_rescue.sql
	jobRescueBatchSQL string
	//go:embed internal/dbbatch/job_set_state.sql
	jobSetStateBatchSQL string
	//go:embed internal/dbbatch/job_unique_key.sql
	jobUniqueKeySQL string
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

func jobInsertBatch(ctx context.Context, dbtx templateReplaceWrapper, params *riverdriver.JobInsertFastManyParams, nonces []string) (sql.Result, error) {
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
		UniqueActive bool               `json:"unique_active"`
		UniqueKey    string             `json:"unique_key"`
		UniqueStates byte               `json:"unique_states"`
	}
	rows := make([]row, len(params.Jobs))
	upsert := ""
	for i, job := range params.Jobs {
		tags, err := json.Marshal(job.Tags)
		if err != nil {
			return nil, err
		}
		metadata := sliceutil.FirstNonEmpty(job.Metadata, []byte("{}"))
		if nonces != nil {
			metadata, err = sjson.SetBytes(metadata, riverdriver.UniqueInsertMetadataKey, nonces[i])
			if err != nil {
				return nil, err
			}
		}
		rows[i] = row{
			ID:           job.ID,
			Args:         string(job.EncodedArgs),
			CreatedAt:    batchTime(job.CreatedAt),
			Kind:         job.Kind,
			MaxAttempts:  job.MaxAttempts,
			Metadata:     string(metadata),
			Priority:     job.Priority,
			Queue:        job.Queue,
			ScheduledAt:  batchTime(job.ScheduledAt),
			State:        job.State,
			Tags:         string(tags),
			UniqueActive: job.UniqueStates&uniquestates.UniqueStatesToBitmask([]rivertype.JobState{job.State}) != 0,
			UniqueKey:    hex.EncodeToString(job.UniqueKey),
			UniqueStates: job.UniqueStates,
		}
		if len(job.UniqueKey) > 0 {
			upsert = jobInsertBatchUpsertSQL
		}
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}
	ctx = sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"on_duplicate_key": {Stable: true, Value: upsert},
	}, nil)
	result, err := dbtx.ExecContext(ctx, jobInsertBatchSQL, encoded)
	return result, interpretError(err)
}

func jobInsertBatchReturning(ctx context.Context, dbtx templateReplaceWrapper, params *riverdriver.JobInsertFastManyParams, nonces []string) ([]*dbsqlc.RiverJob, error) {
	// Use the first generated ID only as a lower bound, never to infer the
	// remaining IDs: concurrent INSERT ... SELECT statements may interleave them.
	// A per-input nonce preserves order and distinguishes newly inserted jobs.
	// The caller removes these temporary nonces before committing.
	prefix := randutil.Hex(16) + ":"
	for i := range nonces {
		nonces[i] = prefix + strconv.Itoa(i)
	}
	result, err := jobInsertBatch(schemaTemplateParam(ctx, params.Schema), dbtx, params, nonces)
	if err != nil {
		return nil, err
	}
	firstID, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	jobsByNonce := make(map[string]*dbsqlc.RiverJob, len(nonces))
	if firstID != 0 {
		jobs, err := dbsqlc.New().JobGetByInsertNonce(schemaTemplateParam(ctx, params.Schema), dbtx, &dbsqlc.JobGetByInsertNonceParams{
			FirstID: firstID, NoncePrefix: []byte(prefix + "%"),
		})
		if err != nil {
			return nil, interpretError(err)
		}
		for _, job := range jobs {
			jobsByNonce[gjson.GetBytes(job.Metadata, riverdriver.UniqueInsertMetadataKey).String()] = job
		}
	}

	inserted := make([]*dbsqlc.RiverJob, len(nonces))
	var missingKeys []sql.NullString
	for i, nonce := range nonces {
		inserted[i] = jobsByNonce[nonce]
		if inserted[i] == nil && len(params.Jobs[i].UniqueKey) > 0 {
			missingKeys = append(missingKeys, sql.NullString{String: string(params.Jobs[i].UniqueKey), Valid: true})
		}
	}
	if len(missingKeys) > 0 {
		lookupCtx := sqlctemplate.WithReplacements(schemaTemplateParam(ctx, params.Schema), map[string]sqlctemplate.Replacement{
			"unique_key": {Stable: true, Value: "(" + jobUniqueKeySQL + ")"},
		}, nil)
		jobs, err := dbsqlc.New().JobGetByUniqueKeyManyForUpdate(lookupCtx, dbtx, missingKeys)
		if err != nil {
			return nil, interpretError(err)
		}
		jobsByKey := make(map[string]*dbsqlc.RiverJob, len(jobs))
		for _, job := range jobs {
			jobsByKey[job.UniqueKey.String] = job
		}
		for i, job := range params.Jobs {
			if inserted[i] == nil {
				inserted[i] = jobsByKey[string(job.UniqueKey)]
			}
		}
	}
	for i, job := range inserted {
		if job == nil {
			return nil, fmt.Errorf("could not find job inserted at index %d", i)
		}
	}
	return inserted, nil
}

func jobInsertBatchSupported(params *riverdriver.JobInsertFastManyParams) bool {
	if len(params.Jobs) < 2 {
		return false
	}
	for _, job := range params.Jobs {
		// Explicit IDs may precede the first generated ID. Preserve non-object
		// metadata and caller-supplied reserved keys through the sequential path.
		if job.ID != nil || (len(job.Metadata) > 0 && (!json.Valid(job.Metadata) || !gjson.ParseBytes(job.Metadata).IsObject())) ||
			gjson.GetBytes(job.Metadata, riverdriver.UniqueInsertMetadataKey).Exists() {
			return false
		}
	}
	return true
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
