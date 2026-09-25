//go:build riverconformance

package harness_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// rawJobRow is a job's JSON and timestamp columns exactly as the database
// renders them (the raw_job_row method).
type rawJobRow struct {
	Args        string  `json:"args"`
	AttemptedAt *string `json:"attempted_at"`
	AttemptedBy *string `json:"attempted_by"`
	CreatedAt   string  `json:"created_at"`
	Errors      *string `json:"errors"`
	FinalizedAt *string `json:"finalized_at"`
	Metadata    string  `json:"metadata"`
	ScheduledAt string  `json:"scheduled_at"`
	Tags        string  `json:"tags"`
}

var (
	// goTimeJSONPattern matches a JSON string holding a time the way Go's
	// encoding/json writes a time.Time: RFC 3339 with the shortest fractional
	// seconds, so a fraction never ends in zero.
	goTimeJSONPattern = regexp.MustCompile(`"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d*[1-9])?(Z|[+-]\d{2}:\d{2})"`)

	// rfc3339JSONPattern matches any RFC 3339 time in a JSON string, so
	// times that don't match goTimeJSONPattern can be reported.
	rfc3339JSONPattern = regexp.MustCompile(`"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})"`)

	// sqliteTimePattern is Go's SQLite time format, `2006-01-02 15:04:05.000`.
	sqliteTimePattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$`)

	// uniqueNoncePattern matches the random `river:unique_nonce` member
	// Go's SQLite driver writes as eight lowercase hex bytes into rows it
	// inserts and returns.
	uniqueNoncePattern = regexp.MustCompile(`"river:unique_nonce":"[0-9a-f]{16}",?|,"river:unique_nonce":"[0-9a-f]{16}"`)
)

// rowBytesText is text written into every JSON column the byte scenarios
// compare. Go escapes `<`, `>`, `&`, U+2028, and U+2029 in JSON strings, and
// SQLite JSONB stores each string's escapes as written.
const rowBytesText = "a<b>&c\u2028d\u2029\u00e9"

// comparableJobRow checks the raw timestamp formats in row and returns its
// columns with values that legitimately differ between two writers (times
// and unique nonces) replaced by placeholders, so the rest can be compared
// byte for byte. Null columns are nil.
func comparableJobRow(t *testing.T, writer string, row rawJobRow) map[string]any {
	t.Helper()

	sqliteTime := func(name string, value *string) any {
		if value == nil {
			return nil
		}
		require.Regexp(t, sqliteTimePattern, *value, "%s wrote %s in a non-Go format", writer, name)
		return "<time>"
	}
	jsonTimes := func(name string, value *string) any {
		if value == nil {
			return nil
		}
		for _, match := range rfc3339JSONPattern.FindAllString(*value, -1) {
			require.Regexp(t, goTimeJSONPattern, match, "%s wrote a %s time in a non-Go format: %s", writer, name, *value)
		}
		return rfc3339JSONPattern.ReplaceAllString(*value, `"<time>"`)
	}
	metadata := uniqueNoncePattern.ReplaceAllString(row.Metadata, "")
	var attemptedBy any
	if row.AttemptedBy != nil {
		attemptedBy = *row.AttemptedBy
	}

	return map[string]any{
		"args":         row.Args,
		"attempted_at": sqliteTime("attempted_at", row.AttemptedAt),
		"attempted_by": attemptedBy,
		"created_at":   sqliteTime("created_at", &row.CreatedAt),
		"errors":       jsonTimes("errors", row.Errors),
		"finalized_at": sqliteTime("finalized_at", row.FinalizedAt),
		"metadata":     jsonTimes("metadata", &metadata),
		"scheduled_at": sqliteTime("scheduled_at", &row.ScheduledAt),
		"tags":         row.Tags,
	}
}

// requireSameJobRowBytes requires that the rows reference and candidate
// wrote for the same operation are identical once times and nonces are
// normalized, and that both or neither carry a unique nonce.
func requireSameJobRowBytes(t *testing.T, operation string, reference, candidate *adapter, referenceID, candidateID int64) {
	t.Helper()

	var referenceRow, candidateRow rawJobRow
	reference.call(t, "raw_job_row", map[string]any{"id": referenceID}, &referenceRow)
	candidate.call(t, "raw_job_row", map[string]any{"id": candidateID}, &candidateRow)
	require.Equal(t,
		strings.Contains(referenceRow.Metadata, `"river:unique_nonce"`),
		strings.Contains(candidateRow.Metadata, `"river:unique_nonce"`),
		"%s: unique nonce presence differs:\n%s: %s\n%s: %s",
		operation, reference.name, referenceRow.Metadata, candidate.name, candidateRow.Metadata)
	require.Equal(t,
		comparableJobRow(t, reference.name, referenceRow),
		comparableJobRow(t, candidate.name, candidateRow),
		"%s: %s and %s wrote different bytes", operation, reference.name, candidate.name)
}

// verifySQLiteJobRowBytes has each implementation write the same jobs
// through insert, batch insert, update, cancel, and retry, then compares the
// SQLite bytes of every JSON and timestamp column with Go's.
func verifySQLiteJobRowBytes(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	opts := map[string]any{
		"max_attempts": 7,
		"metadata": map[string]any{
			"note":   rowBytesText,
			"number": 1.5,
			"nested": map[string]any{"zeta": rowBytesText, "alpha": []any{1, "<&>", nil}},
		},
		"priority":     2,
		"scheduled_at": "2031-02-03T04:05:06.789Z",
		"tags":         []string{"row-bytes", "tag_2"},
	}
	type writtenJobs struct {
		batch, cancelled, inserted, retried, updated int64
	}
	write := func(writer *adapter) writtenJobs {
		var written writtenJobs
		var inserted normalizedJob
		writer.call(t, "insert", map[string]any{"message": rowBytesText, "opts": opts}, &inserted)
		written.inserted = inserted.ID

		var batch struct {
			Results []normalizedInsertResult `json:"results"`
		}
		writer.call(t, "insert_many", map[string]any{"jobs": []map[string]any{
			{"message": rowBytesText + " batch", "opts": opts},
			{"message": rowBytesText + " update", "opts": opts},
			{"message": rowBytesText + " cancel", "opts": opts},
			{"message": rowBytesText + " retry", "opts": opts},
		}}, &batch)
		require.Len(t, batch.Results, 4)
		written.batch = batch.Results[0].Job.ID
		written.updated = batch.Results[1].Job.ID
		written.cancelled = batch.Results[2].Job.ID
		written.retried = batch.Results[3].Job.ID

		writer.call(t, "update", map[string]any{
			"id":     written.updated,
			"output": map[string]any{"text": rowBytesText, "values": []any{2.5, "<&>"}},
		}, nil)
		writer.call(t, "cancel", map[string]any{"id": written.cancelled}, nil)
		writer.call(t, "cancel", map[string]any{"id": written.retried}, nil)
		writer.call(t, "retry", map[string]any{"id": written.retried}, nil)
		return written
	}

	goAdapter.call(t, "reset", map[string]any{}, nil)
	reference := write(goAdapter)
	candidate := write(candidateAdapter)
	for _, operation := range []struct {
		name                 string
		reference, candidate int64
	}{
		{"insert", reference.inserted, candidate.inserted},
		{"insert_many", reference.batch, candidate.batch},
		{"update", reference.updated, candidate.updated},
		{"cancel", reference.cancelled, candidate.cancelled},
		{"retry", reference.retried, candidate.retried},
	} {
		requireSameJobRowBytes(t, operation.name, goAdapter, candidateAdapter, operation.reference, operation.candidate)
	}
}

// verifySQLiteWorkedJobRowBytes has each implementation work the same Go
// inserted jobs to completion, discard, and recorded output, then compares
// the SQLite bytes written by each worker with Go's.
func verifySQLiteWorkedJobRowBytes(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	const clientID = "sqlite-row-bytes-worker"
	opts := map[string]any{
		"max_attempts": 1,
		"metadata":     map[string]any{"note": rowBytesText},
		"tags":         []string{"row-bytes", "tag_2"},
	}
	behaviors := []string{"", "error", "output"}
	insert := func() []int64 {
		ids := make([]int64, len(behaviors))
		for index, behavior := range behaviors {
			var inserted normalizedJob
			goAdapter.call(t, "insert", map[string]any{
				"behavior": behavior, "message": rowBytesText, "opts": opts,
			}, &inserted)
			ids[index] = inserted.ID
		}
		return ids
	}

	goAdapter.call(t, "reset", map[string]any{}, nil)
	referenceIDs := insert()
	for _, id := range referenceIDs {
		goAdapter.call(t, "work", map[string]any{"client_id": clientID, "id": id}, nil)
	}
	candidateIDs := insert()
	for _, id := range candidateIDs {
		candidateAdapter.call(t, "work", map[string]any{"client_id": clientID, "id": id}, nil)
	}
	for index, behavior := range behaviors {
		requireSameJobRowBytes(t, "work "+behavior, goAdapter, candidateAdapter, referenceIDs[index], candidateIDs[index])
	}
}
