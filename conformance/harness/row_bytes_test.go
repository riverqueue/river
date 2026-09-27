//go:build riverconformance

package harness_test

import (
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
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
	// JSONB holds SQLite's stored JSONB bytes as hex, and is nil on
	// PostgreSQL.
	JSONB *struct {
		Args        string  `json:"args"`
		AttemptedBy *string `json:"attempted_by"`
		Errors      *string `json:"errors"`
		Metadata    string  `json:"metadata"`
		Tags        string  `json:"tags"`
	} `json:"jsonb"`
	Metadata    string `json:"metadata"`
	ScheduledAt string `json:"scheduled_at"`
	Tags        string `json:"tags"`
}

// jsonbTypeNames names SQLite's JSONB element types by their header code.
var jsonbTypeNames = [...]string{ //nolint:gochecknoglobals // fixed lookup table
	"null", "true", "false", "int", "int5", "float", "float5",
	"text", "textj", "text5", "textraw", "array", "object",
}

// jsonbNode is one decoded SQLite JSONB element.
type jsonbNode struct {
	children []jsonbNode
	payload  []byte
	typ      byte
}

// decodeJSONB decodes the first JSONB element of data and returns it with
// the bytes that follow it.
func decodeJSONB(data []byte) (jsonbNode, []byte, error) {
	if len(data) == 0 {
		return jsonbNode{}, nil, errors.New("empty JSONB element")
	}
	node := jsonbNode{typ: data[0] & 0x0f}
	if int(node.typ) >= len(jsonbTypeNames) {
		return jsonbNode{}, nil, fmt.Errorf("reserved JSONB type %d", node.typ)
	}
	size, header := uint64(data[0]>>4), 1
	if size > 11 {
		width := 1 << (size - 12)
		if len(data) < 1+width {
			return jsonbNode{}, nil, errors.New("truncated JSONB header")
		}
		header += width
		// Wider sizes follow the header byte, big-endian.
		size = 0
		for _, b := range data[1:header] {
			size = size<<8 | uint64(b)
		}
	}
	if size > uint64(len(data)-header) { //nolint:gosec // len is never negative
		return jsonbNode{}, nil, errors.New("truncated JSONB payload")
	}
	node.payload = data[header : header+int(size)]
	rest := data[header+int(size):]
	if node.typ == 11 || node.typ == 12 {
		for remaining := node.payload; len(remaining) > 0; {
			var child jsonbNode
			var err error
			child, remaining, err = decodeJSONB(remaining)
			if err != nil {
				return jsonbNode{}, nil, err
			}
			node.children = append(node.children, child)
		}
		node.payload = nil
	}
	return node, rest, nil
}

// renderJSONB renders a SQLite JSONB column's element types and payloads,
// with the values that legitimately differ between two writers normalized
// after a format check: the unique nonce becomes `<nonce>` and times become
// `<time>`. Header size widths aren't rendered, since normalizing a value can
// change them.
func renderJSONB(t *testing.T, writer, column, hexBytes string) string {
	t.Helper()

	data, err := hex.DecodeString(hexBytes)
	require.NoError(t, err, "%s wrote %s JSONB that isn't hex", writer, column)
	node, rest, err := decodeJSONB(data)
	require.NoError(t, err, "%s wrote invalid %s JSONB: %s", writer, column, hexBytes)
	require.Empty(t, rest, "%s wrote trailing bytes after %s JSONB: %s", writer, column, hexBytes)

	var render func(node jsonbNode) string
	render = func(node jsonbNode) string {
		name := jsonbTypeNames[node.typ]
		switch node.typ {
		case 11:
			parts := make([]string, len(node.children))
			for index, child := range node.children {
				parts[index] = render(child)
			}
			return name + "[" + strings.Join(parts, ",") + "]"
		case 12:
			var parts []string
			for index := 0; index+1 < len(node.children); index += 2 {
				key, value := node.children[index], node.children[index+1]
				rendered := render(value)
				if string(key.payload) == "river:unique_nonce" {
					require.Regexp(t, `^[0-9a-f]{16}$`, string(value.payload),
						"%s wrote a unique nonce in a non-Go format", writer)
					rendered = jsonbTypeNames[value.typ] + "(<nonce>)"
				}
				parts = append(parts, render(key)+":"+rendered)
			}
			return name + "{" + strings.Join(parts, ",") + "}"
		}
		payload := string(node.payload)
		if rfc3339TextPattern.MatchString(payload) {
			require.Regexp(t, goTimeTextPattern, payload,
				"%s wrote a %s time in a non-Go format: %s", writer, column, payload)
			payload = "<time>"
		}
		return name + "(" + strconv.Quote(payload) + ")"
	}
	return render(node)
}

var (
	// goTimeJSONPattern matches a JSON string holding a time the way Go's
	// encoding/json writes a time.Time: RFC 3339 with the shortest fractional
	// seconds, so a fraction never ends in zero.
	goTimeJSONPattern = regexp.MustCompile(`"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d*[1-9])?(Z|[+-]\d{2}:\d{2})"`)

	// rfc3339JSONPattern matches any RFC 3339 time in a JSON string, so
	// times that don't match goTimeJSONPattern can be reported.
	rfc3339JSONPattern = regexp.MustCompile(`"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})"`)

	// goTimeTextPattern and rfc3339TextPattern match the same times as
	// goTimeJSONPattern and rfc3339JSONPattern, as an unquoted JSONB string
	// payload.
	goTimeTextPattern  = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d*[1-9])?(Z|[+-]\d{2}:\d{2})$`)
	rfc3339TextPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`)

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
	require.NotNil(t, row.JSONB, "%s returned no SQLite JSONB bytes", writer)
	jsonb := func(column string, hexBytes *string) any {
		if hexBytes == nil {
			return nil
		}
		return renderJSONB(t, writer, column, *hexBytes)
	}

	return map[string]any{
		"jsonb_args":         jsonb("args", &row.JSONB.Args),
		"jsonb_attempted_by": jsonb("attempted_by", row.JSONB.AttemptedBy),
		"jsonb_errors":       jsonb("errors", row.JSONB.Errors),
		"jsonb_metadata":     jsonb("metadata", &row.JSONB.Metadata),
		"jsonb_tags":         jsonb("tags", &row.JSONB.Tags),
		"args":               row.Args,
		"attempted_at":       sqliteTime("attempted_at", row.AttemptedAt),
		"attempted_by":       attemptedBy,
		"created_at":         sqliteTime("created_at", &row.CreatedAt),
		"errors":             jsonTimes("errors", row.Errors),
		"finalized_at":       sqliteTime("finalized_at", row.FinalizedAt),
		"metadata":           jsonTimes("metadata", &metadata),
		"scheduled_at":       sqliteTime("scheduled_at", &row.ScheduledAt),
		"tags":               row.Tags,
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
