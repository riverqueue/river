package harness

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/riverqueue/river/conformance/protocol"
	"github.com/riverqueue/river/rivershared/uniquestates"
)

// Drivers.
const (
	DriverPostgres = "postgres"
	DriverSQLite   = "sqlite"
)

// harnessApplicationName identifies the harness's own connections, which
// fault injection never targets.
const harnessApplicationName = "river-conformance-harness"

// sqliteTimeLayout is how River stores times in SQLite. SQLite compares
// times as text, so every implementation must write this layout.
const sqliteTimeLayout = "2006-01-02 15:04:05.000"

// Database is the database of one scenario, which the harness reads and
// writes directly: on Postgres a schema of its own, which adapters use
// through their search path, and on SQLite a file of its own.
type Database struct {
	// Driver is DriverPostgres or DriverSQLite.
	Driver string

	// Schema is the scenario's Postgres schema.
	Schema string

	adapterURL string
	baseURL    string
	pool       *pgxpool.Pool
	sqlite     *sql.DB
}

func newDatabase(t *testing.T, driver string, searchPath []string) *Database {
	t.Helper()

	ctx := context.Background()
	switch driver {
	case DriverPostgres:
		baseURL := postgresURL()
		config, err := pgxpool.ParseConfig(baseURL)
		require.NoError(t, err)
		config.ConnConfig.RuntimeParams["application_name"] = harnessApplicationName
		config.MaxConns = 4

		schema := "river_conformance_" + randomHex(t, 6)
		config.ConnConfig.RuntimeParams["search_path"] = schema
		pool, err := pgxpool.NewWithConfig(ctx, config)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := pool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
			pool.Close()
			require.NoError(t, err)
		})

		adapterURL, err := searchPathURL(baseURL, strings.Join(append([]string{schema}, searchPath...), ","))
		require.NoError(t, err)
		return &Database{Driver: driver, Schema: schema, adapterURL: adapterURL, baseURL: baseURL, pool: pool}

	case DriverSQLite:
		path := filepath.Join(t.TempDir(), "river.sqlite3")
		db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return &Database{Driver: driver, adapterURL: path, sqlite: db}
	}
	require.FailNow(t, "unknown driver "+driver)
	return nil
}

// searchPathURL returns url, without pgx's pool parameters, with options
// that set the search path. Spaces are escaped as %20, which
// every driver's URL parser decodes, rather than as +.
func searchPathURL(databaseURL, searchPath string) (string, error) {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "", fmt.Errorf("error parsing database URL: %w", err)
	}
	query := parsed.Query()
	for key := range query {
		if strings.HasPrefix(key, "pool_") {
			query.Del(key)
		}
	}
	query.Set("options", "-c search_path="+searchPath)
	parsed.RawQuery = strings.ReplaceAll(query.Encode(), "+", "%20")
	return parsed.String(), nil
}

func randomHex(t *testing.T, bytes int) string {
	t.Helper()

	buf := make([]byte, bytes)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	return hex.EncodeToString(buf)
}

// Exec runs a statement written for the scenario's driver.
func (d *Database) Exec(t *testing.T, query string, args ...any) {
	t.Helper()

	var err error
	if d.pool != nil {
		_, err = d.pool.Exec(context.Background(), query, args...)
	} else {
		_, err = d.sqlite.ExecContext(context.Background(), query, args...)
	}
	require.NoError(t, err, "query: %s", query)
}

// QueryRow runs a query written for the scenario's driver and scans its one
// row into dest.
func (d *Database) QueryRow(t *testing.T, query string, args []any, dest ...any) {
	t.Helper()

	var err error
	if d.pool != nil {
		err = d.pool.QueryRow(context.Background(), query, args...).Scan(dest...)
	} else {
		err = d.sqlite.QueryRowContext(context.Background(), query, args...).Scan(dest...)
	}
	require.NoError(t, err, "query: %s", query)
}

// Pool returns the Postgres pool, for observations only Postgres has.
func (d *Database) Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	require.NotNil(t, d.pool, "the scenario's database isn't Postgres")
	return d.pool
}

// SQLite returns the SQLite database.
func (d *Database) SQLite(t *testing.T) *sql.DB {
	t.Helper()

	require.NotNil(t, d.sqlite, "the scenario's database isn't SQLite")
	return d.sqlite
}

// rowColumns selects a job row's columns as text the harness decodes itself.
func (d *Database) rowColumns() string {
	if d.pool != nil {
		return `id, args::text, attempt, attempted_at, coalesce(attempted_by, '{}'), created_at,
			coalesce(to_json(errors)::text, '[]'), finalized_at, kind, max_attempts, metadata::text,
			priority, queue, scheduled_at, state::text, tags, unique_key, unique_states::int`
	}
	return `id, json(args), attempt, CAST(attempted_at AS TEXT), coalesce(json(attempted_by), '[]'),
		CAST(created_at AS TEXT), coalesce(json(errors), '[]'), CAST(finalized_at AS TEXT), kind,
		max_attempts, json(metadata), priority, queue, CAST(scheduled_at AS TEXT), state, json(tags),
		unique_key, unique_states`
}

// Job reads a job the way adapters report it, or returns nil if it doesn't
// exist.
func (d *Database) Job(t *testing.T, id int64) *protocol.Job {
	t.Helper()

	jobs := d.Jobs(t, "id = $1", id)
	if len(jobs) == 0 {
		return nil
	}
	return jobs[0]
}

// MustJob reads a job that must exist.
func (d *Database) MustJob(t *testing.T, id int64) *protocol.Job {
	t.Helper()

	job := d.Job(t, id)
	require.NotNil(t, job, "job %d doesn't exist", id)
	return job
}

// Jobs reads the jobs matching where, a condition written for the
// scenario's driver, in ID order.
func (d *Database) Jobs(t *testing.T, where string, args ...any) []*protocol.Job {
	t.Helper()

	query := "SELECT " + d.rowColumns() + " FROM river_job WHERE " + where + " ORDER BY id" //nolint:gosec // conditions are the scenarios' own
	var jobs []*protocol.Job
	if d.pool != nil {
		rows, err := d.pool.Query(context.Background(), query, args...)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var (
				job                        protocol.Job
				args, errorsJSON, metadata string
				uniqueKey                  []byte
				uniqueStates               *int
				attemptedAt, finalizedAt   *time.Time
				createdAt, scheduledAt     time.Time
			)
			require.NoError(t, rows.Scan(&job.ID, &args, &job.Attempt, &attemptedAt, &job.AttemptedBy, &createdAt,
				&errorsJSON, &finalizedAt, &job.Kind, &job.MaxAttempts, &metadata, &job.Priority, &job.Queue,
				&scheduledAt, &job.State, &job.Tags, &uniqueKey, &uniqueStates))
			job.AttemptedAt, job.FinalizedAt = utc(attemptedAt), utc(finalizedAt)
			job.CreatedAt, job.ScheduledAt = createdAt.UTC(), scheduledAt.UTC()
			finishJob(t, &job, args, errorsJSON, metadata, "", uniqueKey, uniqueStates)
			jobs = append(jobs, &job)
		}
		require.NoError(t, rows.Err())
		return jobs
	}

	rows, err := d.sqlite.QueryContext(context.Background(), query, args...)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var (
			job                                           protocol.Job
			args, attemptedBy, errorsJSON, metadata, tags string
			createdAt, scheduledAt                        string
			attemptedAt, finalizedAt                      *string
			uniqueKey                                     []byte
			uniqueStates                                  *int
		)
		require.NoError(t, rows.Scan(&job.ID, &args, &job.Attempt, &attemptedAt, &attemptedBy, &createdAt,
			&errorsJSON, &finalizedAt, &job.Kind, &job.MaxAttempts, &metadata, &job.Priority, &job.Queue,
			&scheduledAt, &job.State, &tags, &uniqueKey, &uniqueStates))
		job.AttemptedAt, job.FinalizedAt = parseOptionalSQLiteTime(t, attemptedAt), parseOptionalSQLiteTime(t, finalizedAt)
		job.CreatedAt, job.ScheduledAt = parseSQLiteTime(t, createdAt), parseSQLiteTime(t, scheduledAt)
		require.NoError(t, json.Unmarshal([]byte(attemptedBy), &job.AttemptedBy))
		require.NoError(t, json.Unmarshal([]byte(tags), &job.Tags))
		finishJob(t, &job, args, errorsJSON, metadata, "", uniqueKey, uniqueStates)
		jobs = append(jobs, &job)
	}
	require.NoError(t, rows.Err())
	return jobs
}

// finishJob decodes a row's JSON and unique columns into job, normalized the
// way adapters report jobs.
func finishJob(t *testing.T, job *protocol.Job, args, errorsJSON, metadata, _ string, uniqueKey []byte, uniqueStates *int) {
	t.Helper()

	require.NoError(t, json.Unmarshal([]byte(args), &job.Args), "job %d args", job.ID)
	job.Errors = decodeAttemptErrors(t, job.ID, errorsJSON)
	if err := json.Unmarshal([]byte(metadata), &job.Metadata); err != nil {
		// Numbers beyond a float64's range, which some scenarios store on
		// purpose, decode exactly instead.
		decoder := json.NewDecoder(strings.NewReader(metadata))
		decoder.UseNumber()
		require.NoError(t, decoder.Decode(&job.Metadata), "job %d metadata", job.ID)
	}
	delete(job.Metadata, "river:unique_nonce")
	if job.AttemptedBy == nil {
		job.AttemptedBy = []string{}
	}
	if job.Tags == nil {
		job.Tags = []string{}
	}
	if uniqueKey != nil {
		key := hex.EncodeToString(uniqueKey)
		job.UniqueKey = &key
	}
	if uniqueStates != nil {
		job.UniqueStates = []string{}
		for _, state := range uniquestates.UniqueBitmaskToStates(byte(*uniqueStates)) { //nolint:gosec // an 8-bit mask
			job.UniqueStates = append(job.UniqueStates, string(state))
		}
		slices.Sort(job.UniqueStates)
	}
}

// decodeAttemptErrors decodes a job's attempt errors as leniently as River Go
// does: an `at` that isn't RFC 3339 is left zero, a numeric string attempt is
// a number, and an error or trace that isn't a string is its JSON text.
func decodeAttemptErrors(t *testing.T, id int64, errorsJSON string) []protocol.AttemptError {
	t.Helper()

	var elements []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(errorsJSON), &elements), "job %d errors", id)
	attemptErrors := make([]protocol.AttemptError, len(elements))
	text := func(raw json.RawMessage) string {
		var value string
		if json.Unmarshal(raw, &value) == nil {
			return value
		}
		return string(raw)
	}
	for i, element := range elements {
		var fields map[string]json.RawMessage
		if json.Unmarshal(element, &fields) != nil {
			attemptErrors[i].Error = string(element)
			continue
		}
		if raw, ok := fields["at"]; ok {
			if at, err := time.Parse(time.RFC3339Nano, text(raw)); err == nil {
				attemptErrors[i].At = at.UTC()
			}
		}
		if raw, ok := fields["attempt"]; ok {
			attemptErrors[i].Attempt, _ = strconv.Atoi(text(raw))
		}
		if raw, ok := fields["error"]; ok {
			attemptErrors[i].Error = text(raw)
		}
		if raw, ok := fields["trace"]; ok {
			attemptErrors[i].Trace = text(raw)
		}
	}
	return attemptErrors
}

func utc(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	converted := value.UTC()
	return &converted
}

func parseSQLiteTime(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(sqliteTimeLayout, value)
	require.NoError(t, err, "SQLite time %q isn't in River's layout", value)
	return parsed
}

func parseOptionalSQLiteTime(t *testing.T, value *string) *time.Time {
	t.Helper()

	if value == nil {
		return nil
	}
	parsed := parseSQLiteTime(t, *value)
	return &parsed
}

// WaitJob polls a job until it reaches one of states, which default to the
// finalized states, and returns it.
func (d *Database) WaitJob(t *testing.T, id int64, timeout time.Duration, states ...string) *protocol.Job {
	t.Helper()

	if len(states) == 0 {
		states = []string{"cancelled", "completed", "discarded"}
	}
	var job *protocol.Job
	deadline := time.Now().Add(timeout)
	for {
		job = d.Job(t, id)
		if job != nil && slices.Contains(states, job.State) {
			return job
		}
		if time.Now().After(deadline) {
			state := "<missing>"
			if job != nil {
				state = job.State
			}
			require.FailNowf(t, "timed out", "job %d didn't reach %v within %s; it's %s: %+v", id, states, timeout, state, job)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// WaitJobCount polls until exactly count jobs match where, and returns them.
func (d *Database) WaitJobCount(t *testing.T, count int, timeout time.Duration, where string, args ...any) []*protocol.Job {
	t.Helper()

	var jobs []*protocol.Job
	WaitFor(t, fmt.Sprintf("%d jobs where %s", count, where), timeout, func() bool {
		jobs = d.Jobs(t, where, args...)
		return len(jobs) == count
	})
	return jobs
}

// RawJob is a job row the harness inserts itself, without an implementation
// and without a notification. Zero values take the column defaults, except
// Args, which default to KindEcho args with the message "raw".
type RawJob struct {
	Args        *protocol.Args
	Attempt     int
	AttemptedAt *time.Time
	AttemptedBy []string
	FinalizedAt *time.Time
	ID          int64
	Kind        string
	MaxAttempts int
	Metadata    string
	Queue       string
	ScheduledAt *time.Time
	State       string
	Tags        []string
}

// InsertRaw inserts job and returns its ID.
func (d *Database) InsertRaw(t *testing.T, job RawJob) int64 {
	t.Helper()

	args := job.Args
	if args == nil {
		args = &protocol.Args{Message: "raw"}
	}
	encodedArgs, err := json.Marshal(args)
	require.NoError(t, err)
	kind := cmpOr(job.Kind, protocol.KindEcho)
	maxAttempts := cmpOr(job.MaxAttempts, 25)
	metadata := cmpOr(job.Metadata, "{}")
	queue := cmpOr(job.Queue, "default")
	state := cmpOr(job.State, "available")
	attemptedBy, err := json.Marshal(job.AttemptedBy)
	require.NoError(t, err)
	tags := job.Tags
	if tags == nil {
		tags = []string{}
	}
	encodedTags, err := json.Marshal(tags)
	require.NoError(t, err)
	scheduledAt := time.Now().UTC()
	if job.ScheduledAt != nil {
		scheduledAt = job.ScheduledAt.UTC()
	}

	var id int64
	if d.pool != nil {
		var attemptedByArray []string
		if job.AttemptedBy != nil {
			attemptedByArray = job.AttemptedBy
		}
		require.NoError(t, d.pool.QueryRow(context.Background(), `
			INSERT INTO river_job (id, args, attempt, attempted_at, attempted_by, finalized_at, kind, max_attempts, metadata, queue, scheduled_at, state, tags)
			VALUES (coalesce($1, nextval('river_job_id_seq')), $2::jsonb, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12::river_job_state, $13)
			RETURNING id`,
			optionalID(job.ID), string(encodedArgs), job.Attempt, job.AttemptedAt, attemptedByArray, job.FinalizedAt, kind, maxAttempts, metadata, queue,
			scheduledAt, state, tags,
		).Scan(&id))
		return id
	}

	var attemptedByJSON *string
	if job.AttemptedBy != nil {
		attemptedByJSON = new(string(attemptedBy))
	}
	require.NoError(t, d.sqlite.QueryRowContext(context.Background(), `
		INSERT INTO river_job (id, args, attempt, attempted_at, attempted_by, created_at, finalized_at, kind, max_attempts, metadata, queue, scheduled_at, state, tags)
		VALUES (?, jsonb(?), ?, ?, jsonb(?), ?, ?, ?, ?, jsonb(?), ?, ?, ?, jsonb(?))
		RETURNING id`,
		optionalID(job.ID), string(encodedArgs), job.Attempt, sqliteTime(job.AttemptedAt), attemptedByJSON, time.Now().UTC().Format(sqliteTimeLayout),
		sqliteTime(job.FinalizedAt), kind, maxAttempts,
		metadata, queue, scheduledAt.Format(sqliteTimeLayout), state, string(encodedTags),
	).Scan(&id))
	return id
}

func optionalID(id int64) *int64 {
	if id == 0 {
		return nil
	}
	return &id
}

func sqliteTime(value *time.Time) *string {
	if value == nil {
		return nil
	}
	return new(value.UTC().Format(sqliteTimeLayout))
}

func cmpOr[T comparable](value, fallback T) T {
	var zero T
	if value == zero {
		return fallback
	}
	return value
}

// SetKind changes a job's kind out of band.
func (d *Database) SetKind(t *testing.T, id int64, kind string) {
	t.Helper()

	d.Exec(t, "UPDATE river_job SET kind = $1 WHERE id = $2", kind, id)
}

// Leader is the leadership row.
type Leader struct {
	ElectedAt time.Time
	ExpiresAt time.Time
	LeaderID  string
}

// Leader returns the current leader, if there is one.
func (d *Database) Leader(t *testing.T) (Leader, bool) {
	t.Helper()

	var leader Leader
	var err error
	if d.pool != nil {
		err = d.pool.QueryRow(context.Background(), "SELECT elected_at, expires_at, leader_id FROM river_leader").
			Scan(&leader.ElectedAt, &leader.ExpiresAt, &leader.LeaderID)
	} else {
		var electedAt, expiresAt string
		err = d.sqlite.QueryRowContext(context.Background(),
			"SELECT CAST(elected_at AS TEXT), CAST(expires_at AS TEXT), leader_id FROM river_leader").
			Scan(&electedAt, &expiresAt, &leader.LeaderID)
		if err == nil {
			leader.ElectedAt, leader.ExpiresAt = parseLooseSQLiteTime(t, electedAt), parseLooseSQLiteTime(t, expiresAt)
		}
	}
	if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
		return Leader{}, false
	}
	require.NoError(t, err)
	return leader, true
}

// parseLooseSQLiteTime parses a SQLite time that SQL wrote, which may have
// any precision.
func parseLooseSQLiteTime(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse("2006-01-02 15:04:05.999999999", value)
	require.NoError(t, err, "SQLite time %q", value)
	return parsed
}

// WaitLeader waits for a leader other than previous, by client ID, and
// returns it.
func (d *Database) WaitLeader(t *testing.T, previous string) Leader {
	t.Helper()

	var leader Leader
	WaitFor(t, "a leader other than "+previous, 30*time.Second, func() bool {
		var ok bool
		leader, ok = d.Leader(t)
		return ok && leader.LeaderID != previous
	})
	return leader
}

// WaitNewTerm waits for a leadership term elected at a time other than
// previous, and returns it.
func (d *Database) WaitNewTerm(t *testing.T, previous time.Time) Leader {
	t.Helper()

	var leader Leader
	WaitFor(t, "a new leadership term", 30*time.Second, func() bool {
		var ok bool
		leader, ok = d.Leader(t)
		return ok && !leader.ElectedAt.Equal(previous)
	})
	return leader
}

// ExpireLeader expires the leader's lease, standing in for the lease of a
// killed leader running out.
func (d *Database) ExpireLeader(t *testing.T) {
	t.Helper()

	if d.pool != nil {
		d.Exec(t, "UPDATE river_leader SET expires_at = now() - interval '1 second'")
		return
	}
	d.Exec(t, "UPDATE river_leader SET expires_at = datetime('now', '-1 second')")
}

// QueueRow is a river_queue row.
type QueueRow struct {
	Metadata  map[string]any
	Name      string
	PausedAt  *time.Time
	UpdatedAt time.Time
}

// Queue reads a queue row.
func (d *Database) Queue(t *testing.T, name string) *QueueRow {
	t.Helper()

	var queue QueueRow
	var metadata string
	if d.pool != nil {
		require.NoError(t, d.pool.QueryRow(context.Background(),
			"SELECT metadata::text, name, paused_at, updated_at FROM river_queue WHERE name = $1", name).
			Scan(&metadata, &queue.Name, &queue.PausedAt, &queue.UpdatedAt))
		queue.PausedAt = utc(queue.PausedAt)
		queue.UpdatedAt = queue.UpdatedAt.UTC()
	} else {
		var pausedAt *string
		var updatedAt string
		require.NoError(t, d.sqlite.QueryRowContext(context.Background(),
			"SELECT json(metadata), name, CAST(paused_at AS TEXT), CAST(updated_at AS TEXT) FROM river_queue WHERE name = ?", name).
			Scan(&metadata, &queue.Name, &pausedAt, &updatedAt))
		if pausedAt != nil {
			queue.PausedAt = new(parseLooseSQLiteTime(t, *pausedAt))
		}
		queue.UpdatedAt = parseLooseSQLiteTime(t, updatedAt)
	}
	require.NoError(t, json.Unmarshal([]byte(metadata), &queue.Metadata))
	return &queue
}

// MigrationVersions returns the applied versions of the main migration line
// in schema, or in the scenario's database if schema is empty.
func (d *Database) MigrationVersions(t *testing.T, schema string) []int {
	t.Helper()

	var tableExists, hasLine bool
	if d.pool != nil {
		schemaName := schema
		if schemaName == "" {
			schemaName = d.Schema
		}
		d.QueryRow(t, `SELECT
				EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'river_migration'),
				EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'river_migration' AND column_name = 'line')`,
			[]any{schemaName}, &tableExists, &hasLine)
	} else {
		d.QueryRow(t, `SELECT
				EXISTS (SELECT 1 FROM sqlite_master WHERE name = 'river_migration'),
				EXISTS (SELECT 1 FROM pragma_table_info('river_migration') WHERE name = 'line')`, nil, &tableExists, &hasLine)
	}
	versions := []int{}
	if !tableExists {
		return versions
	}

	table := "river_migration"
	if schema != "" {
		table = pgx.Identifier{schema, "river_migration"}.Sanitize()
	}
	query := "SELECT version FROM " + table
	if hasLine {
		query += " WHERE line = 'main'"
	}
	query += " ORDER BY version"
	if d.pool != nil {
		rows, err := d.pool.Query(context.Background(), query)
		require.NoError(t, err)
		versions, err = pgx.CollectRows(rows, pgx.RowTo[int])
		require.NoError(t, err)
		return versions
	}
	rows, err := d.sqlite.QueryContext(context.Background(), query)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var version int
		require.NoError(t, rows.Scan(&version))
		versions = append(versions, version)
	}
	require.NoError(t, rows.Err())
	return versions
}
