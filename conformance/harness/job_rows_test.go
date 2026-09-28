//go:build riverconformance

package harness_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rawJobRow is a job's JSON and timestamp columns as the database renders
// them (the raw_job_row method).
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
	// UniqueKey is the stored unique key as uppercase hex.
	UniqueKey *string `json:"unique_key"`
	// UniqueKeyType is SQLite's typeof(unique_key), and nil on PostgreSQL.
	UniqueKeyType *string `json:"unique_key_type"`
	// UniqueStates is the stored state mask rendered as text.
	UniqueStates *string `json:"unique_states"`
	// UniqueStatesType is SQLite's typeof(unique_states), and nil on
	// PostgreSQL.
	UniqueStatesType *string `json:"unique_states_type"`
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

// jsonbValue decodes a JSONB element into the value comparableJSON returns
// for the same JSON. Every string and number encoding SQLite may store
// decodes, so two writers only need to store the same value.
func jsonbValue(node jsonbNode) (any, error) {
	payload := string(node.payload)
	switch jsonbTypeNames[node.typ] {
	case "null":
		return nil, nil //nolint:nilnil // JSON null
	case "true":
		return true, nil
	case "false":
		return false, nil
	case "int", "float":
		return comparableJSON(json.Number(payload))
	case "int5":
		integer, ok := new(big.Int).SetString(strings.TrimPrefix(payload, "+"), 0)
		if !ok {
			return nil, fmt.Errorf("invalid JSON5 integer %q", payload)
		}
		return comparableJSON(json.Number(integer.String()))
	case "float5":
		number := strings.TrimPrefix(payload, "+")
		number = strings.Replace(number, "-.", "-0.", 1)
		if strings.HasPrefix(number, ".") {
			number = "0" + number
		}
		number = strings.Replace(strings.Replace(number, ".e", ".0e", 1), ".E", ".0E", 1)
		number = strings.TrimSuffix(number, ".")
		return comparableJSON(json.Number(number))
	case "text", "textraw":
		return payload, nil
	case "textj", "text5":
		return unescapeJSON5(payload)
	case "array":
		values := make([]any, len(node.children))
		for index, child := range node.children {
			value, err := jsonbValue(child)
			if err != nil {
				return nil, err
			}
			values[index] = value
		}
		return values, nil
	case "object":
		if len(node.children)%2 != 0 {
			return nil, errors.New("JSONB object without a value for its last key")
		}
		object := make(map[string]any, len(node.children)/2)
		for index := 0; index < len(node.children); index += 2 {
			key, err := jsonbValue(node.children[index])
			if err != nil {
				return nil, err
			}
			keyText, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("JSONB object key %v isn't a string", key)
			}
			value, err := jsonbValue(node.children[index+1])
			if err != nil {
				return nil, err
			}
			object[keyText] = value
		}
		return object, nil
	}
	return nil, fmt.Errorf("unexpected JSONB type %d", node.typ)
}

// unescapeJSON5 decodes the escapes in a JSONB TEXTJ or TEXT5 string
// payload: JSON's, plus JSON5's `\'`, `\v`, `\0`, `\xHH`, and escaped line
// breaks.
func unescapeJSON5(payload string) (string, error) {
	var output strings.Builder
	for index := 0; index < len(payload); index++ {
		if payload[index] != '\\' {
			output.WriteByte(payload[index])
			continue
		}
		index++
		if index >= len(payload) {
			return "", fmt.Errorf("trailing backslash in %q", payload)
		}
		switch escape := payload[index]; escape {
		case '0':
			output.WriteByte(0)
		case '\'', '"', '\\', '/':
			output.WriteByte(escape)
		case 'b':
			output.WriteByte('\b')
		case 'f':
			output.WriteByte('\f')
		case 'n':
			output.WriteByte('\n')
		case 'r':
			output.WriteByte('\r')
		case 't':
			output.WriteByte('\t')
		case 'v':
			output.WriteByte('\v')
		case '\n':
		case '\r':
			if index+1 < len(payload) && payload[index+1] == '\n' {
				index++
			}
		case 'x':
			if index+2 >= len(payload) {
				return "", fmt.Errorf("truncated \\x escape in %q", payload)
			}
			code, err := strconv.ParseUint(payload[index+1:index+3], 16, 8)
			if err != nil {
				return "", fmt.Errorf("invalid \\x escape in %q: %w", payload, err)
			}
			output.WriteRune(rune(code))
			index += 2
		case 'u':
			// JSON decodes surrogate pairs, so hand it every consecutive
			// `\u` escape at once.
			end := index - 1
			for end+6 <= len(payload) && payload[end] == '\\' && payload[end+1] == 'u' {
				end += 6
			}
			var decoded string
			if err := json.Unmarshal([]byte(`"`+payload[index-1:end]+`"`), &decoded); err != nil {
				return "", fmt.Errorf("invalid \\u escape in %q: %w", payload, err)
			}
			output.WriteString(decoded)
			index = end - 1
		default:
			// JSON5 allows escaping any other character as itself; the
			// U+2028 and U+2029 line continuations are multi-byte, so
			// they're written whole.
			if escape >= 0x80 {
				rest := payload[index:]
				character := []rune(rest)[0]
				if character != ' ' && character != ' ' {
					output.WriteRune(character)
				}
				index += len(string(character)) - 1
				continue
			}
			output.WriteByte(escape)
		}
	}
	return output.String(), nil
}

// comparableNumber is a JSON number reduced to its exact value, so `1.50`,
// `1.5`, and `15e-1` compare equal while remaining distinct from a string.
type comparableNumber string

// comparableJSON returns value, as decoded with json.Number, with numbers
// replaced by their exact value, so two JSON texts compare equal whenever
// they hold the same values with the same JSON types.
func comparableJSON(value any) (any, error) {
	switch value := value.(type) {
	case json.Number:
		exact, ok := new(big.Rat).SetString(string(value))
		if !ok {
			return nil, fmt.Errorf("invalid JSON number %q", value)
		}
		return comparableNumber(exact.RatString()), nil
	case []any:
		for index, element := range value {
			converted, err := comparableJSON(element)
			if err != nil {
				return nil, err
			}
			value[index] = converted
		}
		return value, nil
	case map[string]any:
		for key, element := range value {
			converted, err := comparableJSON(element)
			if err != nil {
				return nil, err
			}
			value[key] = converted
		}
		return value, nil
	}
	return value, nil
}

// storedJSONValue decodes a SQLite JSON column's text and its stored JSONB
// bytes, requires the column to be stored as JSONB holding the same value
// as the text, and returns the value.
func storedJSONValue(t *testing.T, writer, column string, text, hexBytes *string) any {
	t.Helper()

	if text == nil {
		require.Nil(t, hexBytes, "%s's %s is null as JSON but not as JSONB", writer, column)
		return nil
	}
	decoded, err := decodeJSONWithNumbers([]byte(*text))
	require.NoError(t, err, "%s wrote invalid %s JSON: %s", writer, column, *text)
	value, err := comparableJSON(decoded)
	require.NoError(t, err, "%s wrote invalid %s JSON: %s", writer, column, *text)

	require.NotNil(t, hexBytes, "%s stored %s without JSONB bytes", writer, column)
	data, err := hex.DecodeString(*hexBytes)
	require.NoError(t, err, "%s wrote %s JSONB that isn't hex", writer, column)
	node, rest, err := decodeJSONB(data)
	require.NoError(t, err, "%s didn't store %s as JSONB: %s", writer, column, *hexBytes)
	require.Empty(t, rest, "%s wrote trailing bytes after %s JSONB: %s", writer, column, *hexBytes)
	stored, err := jsonbValue(node)
	require.NoError(t, err, "%s stored undecodable %s JSONB: %s", writer, column, *hexBytes)
	require.Equal(t, value, stored, "%s's stored %s JSONB and JSON text differ", writer, column)
	return value
}

// normalizeJSONTimes checks every RFC 3339 time in value against Go's
// `time.Time` JSON format and replaces it with `<time>`, since times
// legitimately differ between two writers.
func normalizeJSONTimes(t *testing.T, writer, column string, value any) any {
	t.Helper()

	switch value := value.(type) {
	case string:
		if rfc3339TextPattern.MatchString(value) {
			require.Regexp(t, goTimeTextPattern, value,
				"%s wrote a %s time in a non-Go format: %s", writer, column, value)
			return "<time>"
		}
	case []any:
		for index, element := range value {
			value[index] = normalizeJSONTimes(t, writer, column, element)
		}
	case map[string]any:
		for key, element := range value {
			value[key] = normalizeJSONTimes(t, writer, column, element)
		}
	}
	return value
}

var (
	// goTimeTextPattern matches a time the way Go's encoding/json writes a
	// time.Time, without quotes: RFC 3339 with the shortest fractional
	// seconds, so a fraction never ends in zero.
	goTimeTextPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d*[1-9])?(Z|[+-]\d{2}:\d{2})$`)

	// rfc3339TextPattern matches any RFC 3339 time, so times that don't
	// match goTimeTextPattern can be reported.
	rfc3339TextPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`)

	// sqliteTimePattern is Go's SQLite time format, `2006-01-02 15:04:05.000`.
	// SQLite compares times as text, so every writer must use it.
	sqliteTimePattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$`)

	// uniqueNoncePattern matches the random `river:unique_nonce` Go's SQLite
	// driver writes as eight lowercase hex bytes into rows it inserts and
	// returns.
	uniqueNoncePattern = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

// jobRowText is text written into every JSON column the row scenarios
// compare. It holds characters JSON encoders escape differently (Go escapes
// `<`, `>`, `&`, U+2028, and U+2029), which is fine as long as every writer
// stores the same string.
const jobRowText = "a<b>&c d é"

// comparableJobRow checks the timestamp formats and JSONB storage in row and
// returns its columns as comparable values, with values that legitimately
// differ between two writers (times and unique nonces) replaced by
// placeholders. JSON columns compare as decoded values, so escaping, member
// order, and number spelling don't matter. Null columns are nil.
func comparableJobRow(t *testing.T, writer string, row rawJobRow) map[string]any {
	t.Helper()

	sqliteTime := func(name string, value *string) any {
		if value == nil {
			return nil
		}
		require.Regexp(t, sqliteTimePattern, *value, "%s wrote %s in a non-Go format", writer, name)
		return "<time>"
	}
	require.NotNil(t, row.JSONB, "%s returned no SQLite JSONB bytes", writer)
	jsonColumn := func(column string, text, hexBytes *string) any {
		return normalizeJSONTimes(t, writer, column, storedJSONValue(t, writer, column, text, hexBytes))
	}
	metadata := jsonColumn("metadata", &row.Metadata, &row.JSONB.Metadata)
	if object, ok := metadata.(map[string]any); ok {
		if nonce, ok := object["river:unique_nonce"]; ok {
			require.IsType(t, "", nonce, "%s wrote a non-string unique nonce", writer)
			require.Regexp(t, uniqueNoncePattern, nonce, "%s wrote a unique nonce in a non-Go format", writer)
			object["river:unique_nonce"] = "<nonce>"
		}
	}

	return map[string]any{
		"args":         jsonColumn("args", &row.Args, &row.JSONB.Args),
		"attempted_at": sqliteTime("attempted_at", row.AttemptedAt),
		"attempted_by": jsonColumn("attempted_by", row.AttemptedBy, row.JSONB.AttemptedBy),
		"created_at":   sqliteTime("created_at", &row.CreatedAt),
		"errors":       jsonColumn("errors", row.Errors, row.JSONB.Errors),
		"finalized_at": sqliteTime("finalized_at", row.FinalizedAt),
		"metadata":     metadata,
		"scheduled_at": sqliteTime("scheduled_at", &row.ScheduledAt),
		"tags":         jsonColumn("tags", &row.Tags, &row.JSONB.Tags),
		"unique": uniqueColumns{
			Key: row.UniqueKey, KeyType: row.UniqueKeyType, States: row.UniqueStates, StatesType: row.UniqueStatesType,
		},
	}
}

// requireSameJobRows requires that the rows reference and candidate wrote
// for the same operation hold the same values once times and nonces are
// normalized.
func requireSameJobRows(t *testing.T, operation string, reference, candidate *adapter, referenceID, candidateID int64) {
	t.Helper()

	var referenceRow, candidateRow rawJobRow
	reference.call(t, "raw_job_row", map[string]any{"id": referenceID}, &referenceRow)
	candidate.call(t, "raw_job_row", map[string]any{"id": candidateID}, &candidateRow)
	require.Equal(t,
		comparableJobRow(t, reference.name, referenceRow),
		comparableJobRow(t, candidate.name, candidateRow),
		"%s: %s and %s wrote different rows", operation, reference.name, candidate.name)
}

// verifySQLiteJobRows has each implementation write the same jobs through
// insert, batch insert, update, cancel, and retry, then compares every JSON
// and timestamp column it stored in SQLite with Go's.
func verifySQLiteJobRows(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	opts := map[string]any{
		"max_attempts": 7,
		"metadata": map[string]any{
			"note":   jobRowText,
			"number": 1.5,
			"nested": map[string]any{"zeta": jobRowText, "alpha": []any{1, "<&>", nil}},
		},
		"priority":     2,
		"scheduled_at": "2031-02-03T04:05:06.789Z",
		"tags":         []string{"job-rows", "tag_2"},
	}
	type writtenJobs struct {
		batch, cancelled, inserted, retried, updated int64
	}
	write := func(writer *adapter) writtenJobs {
		var written writtenJobs
		var inserted normalizedJob
		writer.call(t, "insert", map[string]any{"message": jobRowText, "opts": opts}, &inserted)
		written.inserted = inserted.ID

		var batch struct {
			Results []normalizedInsertResult `json:"results"`
		}
		writer.call(t, "insert_many", map[string]any{"jobs": []map[string]any{
			{"message": jobRowText + " batch", "opts": opts},
			{"message": jobRowText + " update", "opts": opts},
			{"message": jobRowText + " cancel", "opts": opts},
			{"message": jobRowText + " retry", "opts": opts},
		}}, &batch)
		require.Len(t, batch.Results, 4)
		written.batch = batch.Results[0].Job.ID
		written.updated = batch.Results[1].Job.ID
		written.cancelled = batch.Results[2].Job.ID
		written.retried = batch.Results[3].Job.ID

		writer.call(t, "update", map[string]any{
			"id":     written.updated,
			"output": map[string]any{"text": jobRowText, "values": []any{2.5, "<&>"}},
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
		requireSameJobRows(t, operation.name, goAdapter, candidateAdapter, operation.reference, operation.candidate)
	}
}

// verifySQLiteWorkedJobRows has each implementation work the same Go
// inserted jobs to completion, discard, and recorded output, then compares
// the SQLite columns written by each worker with Go's.
func verifySQLiteWorkedJobRows(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	const clientID = "sqlite-job-rows-worker"
	opts := map[string]any{
		"max_attempts": 1,
		"metadata":     map[string]any{"note": jobRowText},
		"tags":         []string{"job-rows", "tag_2"},
	}
	behaviors := []string{"", "error", "output"}
	insert := func() []int64 {
		ids := make([]int64, len(behaviors))
		for index, behavior := range behaviors {
			var inserted normalizedJob
			goAdapter.call(t, "insert", map[string]any{
				"behavior": behavior, "message": jobRowText, "opts": opts,
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
		requireSameJobRows(t, "work "+behavior, goAdapter, candidateAdapter, referenceIDs[index], candidateIDs[index])
	}
}

// verifySQLiteRuntimeJobRows compares the SQLite columns each
// implementation's client writes when it claims a job, snoozes one, discards
// a retry that conflicts with a unique job, and rescues an abandoned job.
// Go sets up the same jobs for both, so every other column matches too.
func verifySQLiteRuntimeJobRows(t *testing.T, repositoryRoot, databaseURL, profile string, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	operations := []string{"claim", "snooze", "scheduler discard", "rescue"}
	crashes := 0
	write := func(actor *adapter) map[string]rawJobRow {
		t.Helper()

		rows := make(map[string]rawJobRow, len(operations))
		read := func(operation string, id int64) {
			t.Helper()

			var row rawJobRow
			goAdapter.call(t, "raw_job_row", map[string]any{"id": id}, &row)
			rows[operation] = row
		}
		goAdapter.call(t, "reset", map[string]any{}, nil)
		opts := map[string]any{"metadata": map[string]any{"note": jobRowText}, "tags": []string{"job-rows"}}

		// Claim and snooze: the actor works jobs Go inserts, one held on a
		// barrier while running and one snoozed well beyond the scheduler's
		// threshold so it stays scheduled.
		const barrier = "job-rows-claim"
		actor.call(t, "barrier_create", map[string]any{"name": barrier}, nil)
		actor.call(t, "start", map[string]any{"client_id": "job-rows-runtime", "max_workers": 2}, nil)
		var claimed, snoozed normalizedJob
		goAdapter.call(t, "insert", map[string]any{"behavior": "barrier_wait", "message": barrier, "opts": opts}, &claimed)
		goAdapter.call(t, "wait", map[string]any{"id": claimed.ID, "states": []string{"running"}}, &claimed)
		read("claim", claimed.ID)
		actor.call(t, "barrier_release", map[string]any{"name": barrier}, nil)
		goAdapter.call(t, "insert", map[string]any{
			"behavior": "snooze_once", "duration_ms": 60_000, "message": jobRowText, "opts": opts,
		}, &snoozed)
		goAdapter.call(t, "wait", map[string]any{"id": snoozed.ID, "states": []string{"scheduled"}}, &snoozed)
		read("snooze", snoozed.ID)
		goAdapter.call(t, "wait", map[string]any{"id": claimed.ID}, &claimed)
		actor.call(t, "stop", map[string]any{}, nil)

		// Scheduler discard: a retryable unique job whose unique states
		// exclude retryable becomes due while another job holds its key, so
		// the leader's scheduler discards it. The retry delay exceeds Go's
		// default scheduler interval, so the retry stays retryable until then.
		uniqueOpts := map[string]any{
			"max_attempts": 3, "queue": "job_rows_discard",
			"unique": map[string]any{"by_args": true, "by_state": []string{"available", "pending", "running", "scheduled"}},
		}
		goAdapter.call(t, "start", map[string]any{
			"client_id": "job-rows-setup", "leader_election_disabled": true, "max_workers": 1,
			"queue": "job_rows_discard", "retry_delay_ms": 5_500,
		}, nil)
		var discarded, holder normalizedJob
		goAdapter.call(t, "insert", map[string]any{"behavior": "error", "message": jobRowText, "opts": uniqueOpts}, &discarded)
		goAdapter.call(t, "wait", map[string]any{"id": discarded.ID, "states": []string{"retryable"}}, &discarded)
		goAdapter.call(t, "stop", map[string]any{}, nil)
		goAdapter.call(t, "insert", map[string]any{"behavior": "error", "message": jobRowText, "opts": uniqueOpts}, &holder)
		require.NotEqual(t, discarded.ID, holder.ID, "a retryable job outside its unique states blocked insertion")
		time.Sleep(time.Until(parseTime(t, discarded.ScheduledAt).Add(100 * time.Millisecond)))
		actor.startWithTuning(t, map[string]any{"client_id": "job-rows-scheduler", "max_workers": 1},
			map[string]any{"elect_interval_ms": 20, "scheduler_interval_ms": 20})
		goAdapter.call(t, "wait", map[string]any{"id": discarded.ID, "states": []string{"discarded"}}, &discarded)
		read("scheduler discard", discarded.ID)
		actor.call(t, "stop", map[string]any{}, nil)

		// Rescue: a process holding a running attempt dies, and the actor's
		// leader rescues the abandoned attempt.
		crashes++
		const rescueAfter = time.Second
		crasher := startReferenceAdapterForProfile(t, repositoryRoot, databaseURL, "sqlite", profile,
			fmt.Sprintf("go-job-rows-crasher-%d", crashes))
		crasher.call(t, "start", map[string]any{
			"client_id": "job-rows-crasher", "leader_election_disabled": true, "max_workers": 1, "queue": "job_rows_rescue",
		}, nil)
		var rescued normalizedJob
		goAdapter.call(t, "insert", map[string]any{
			"behavior": "sleep", "duration_ms": 60_000, "message": jobRowText,
			"opts": map[string]any{"max_attempts": 3, "queue": "job_rows_rescue", "tags": []string{"job-rows"}},
		}, &rescued)
		goAdapter.call(t, "wait", map[string]any{"id": rescued.ID, "states": []string{"running"}}, &rescued)
		crasher.kill(t)
		waitUntilRescuable(t, rescued, rescueAfter)
		actor.startWithTuning(t, map[string]any{
			"client_id": "job-rows-rescuer", "job_timeout_ms": rescueAfter.Milliseconds(), "max_workers": 1,
			"rescue_after_ms": rescueAfter.Milliseconds(),
		}, map[string]any{"elect_interval_ms": 20, "rescuer_interval_ms": 20})
		goAdapter.call(t, "wait", map[string]any{"id": rescued.ID, "states": []string{"available", "retryable"}}, &rescued)
		read("rescue", rescued.ID)
		actor.call(t, "stop", map[string]any{}, nil)
		return rows
	}

	reference := write(goAdapter)
	candidate := write(candidateAdapter)
	for _, operation := range operations {
		require.Equal(t,
			comparableJobRow(t, goAdapter.name, reference[operation]),
			comparableJobRow(t, candidateAdapter.name, candidate[operation]),
			"%s: %s and %s wrote different rows", operation, goAdapter.name, candidateAdapter.name)
	}
}
