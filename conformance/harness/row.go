package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rowTimeTolerance bounds how far apart the same time may be in two rows the
// same steps wrote at different moments, relative to each row's created_at.
// It absorbs scheduling differences between implementations while catching a
// time taken at the wrong step or computed with a wrong delay.
const rowTimeTolerance = 2 * time.Second

var (
	// goTimeTextPattern matches a time the way Go's encoding/json writes a
	// time.Time: RFC 3339 with the shortest fractional seconds.
	goTimeTextPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d*[1-9])?(Z|[+-]\d{2}:\d{2})$`)

	// rfc3339TextPattern matches any RFC 3339 time.
	rfc3339TextPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`)

	// sqliteTimePattern is River's SQLite time layout.
	sqliteTimePattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$`)

	// uniqueNoncePattern matches the `river:unique_nonce` River writes as
	// eight lowercase hex bytes.
	uniqueNoncePattern = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

// StoredRow is a job row as the database stores it, reduced to values two
// implementations must agree on: JSON columns as decoded values with exact
// numbers, so escaping, member order, and number spelling don't matter;
// times, including RFC 3339 times inside JSON, as rowTimes; and the unique
// columns as stored bytes and, on SQLite, storage types. Reading it checks
// the storage format: on SQLite, JSON columns must be stored as JSONB and
// times in River's layout, and everywhere times inside JSON must be in Go's
// format.
type StoredRow map[string]any

// rowTime is a time stored in a row.
type rowTime struct {
	at time.Time
}

// StoredRow reads a job's stored row. writer names who wrote it, for
// failure messages.
func (d *Database) StoredRow(t *testing.T, writer string, id int64) StoredRow {
	t.Helper()

	ctx := context.Background()
	jsonColumns := []string{"args", "attempted_by", "errors", "metadata", "tags"}
	timeColumns := []string{"attempted_at", "created_at", "finalized_at", "scheduled_at"}
	row := StoredRow{}

	if d.pool != nil {
		var (
			jsonTexts    [5]*string
			times        [4]*time.Time
			uniqueKey    []byte
			uniqueStates *string
			attempt      int
			state        string
		)
		require.NoError(t, d.pool.QueryRow(ctx, `
			SELECT args::text, to_json(attempted_by)::text, to_json(errors)::text, metadata::text, to_json(tags)::text,
				attempted_at, created_at, finalized_at, scheduled_at, unique_key, unique_states::text, attempt, state::text
			FROM river_job WHERE id = $1`, id).Scan(
			&jsonTexts[0], &jsonTexts[1], &jsonTexts[2], &jsonTexts[3], &jsonTexts[4],
			&times[0], &times[1], &times[2], &times[3], &uniqueKey, &uniqueStates, &attempt, &state))
		for i, column := range jsonColumns {
			row[column] = jsonColumnValue(t, writer, column, jsonTexts[i])
		}
		for i, column := range timeColumns {
			if times[i] != nil {
				row[column] = rowTime{at: *times[i]}
			} else {
				row[column] = nil
			}
		}
		row["unique_key"] = uniqueKey
		row["unique_states"] = uniqueStates
		row["attempt"] = attempt
		row["state"] = state
	} else {
		var (
			jsonTexts           [5]*string
			jsonTypes           [5]*string
			jsonValid           [5]*bool
			times               [4]*string
			uniqueKey           []byte
			uniqueStates        *int64
			keyType, statesType string
			attempt             int
			state               string
		)
		selects := make([]string, 0, 3*len(jsonColumns)+len(timeColumns)+6)
		for _, column := range jsonColumns {
			selects = append(selects, "json("+column+")", "typeof("+column+")", column+" IS NULL OR json_valid("+column+", 8)")
		}
		for _, column := range timeColumns {
			selects = append(selects, "CAST("+column+" AS TEXT)")
		}
		selects = append(selects, "unique_key", "unique_states", "typeof(unique_key)", "typeof(unique_states)", "attempt", "state")
		dest := make([]any, 0, 3*len(jsonColumns)+len(timeColumns)+6)
		for i := range jsonColumns {
			dest = append(dest, &jsonTexts[i], &jsonTypes[i], &jsonValid[i])
		}
		for i := range timeColumns {
			dest = append(dest, &times[i])
		}
		dest = append(dest, &uniqueKey, &uniqueStates, &keyType, &statesType, &attempt, &state)
		require.NoError(t, d.sqlite.QueryRowContext(ctx,
			"SELECT "+strings.Join(selects, ", ")+" FROM river_job WHERE id = ?", id).Scan(dest...))
		for i, column := range jsonColumns {
			if jsonTexts[i] != nil {
				require.Equal(t, "blob", *jsonTypes[i], "%s stored %s as %s rather than JSONB", writer, column, *jsonTypes[i])
				require.True(t, *jsonValid[i], "%s stored %s as invalid JSONB", writer, column)
			}
			row[column] = jsonColumnValue(t, writer, column, jsonTexts[i])
		}
		for i, column := range timeColumns {
			if times[i] == nil {
				row[column] = nil
				continue
			}
			require.Regexp(t, sqliteTimePattern, *times[i], "%s wrote %s in a layout other than River's", writer, column)
			row[column] = rowTime{at: parseSQLiteTime(t, *times[i])}
		}
		row["unique_key"] = uniqueKey
		row["unique_states"] = uniqueStates
		row["unique_key_type"] = keyType
		row["unique_states_type"] = statesType
		row["attempt"] = attempt
		row["state"] = state
	}

	if metadata, ok := row["metadata"].(map[string]any); ok {
		if nonce, ok := metadata["river:unique_nonce"]; ok {
			require.IsType(t, "", nonce, "%s wrote a non-string unique nonce", writer)
			require.Regexp(t, uniqueNoncePattern, nonce, "%s wrote a unique nonce in a format other than Go's", writer)
			metadata["river:unique_nonce"] = "<nonce>"
		}
	}
	return row
}

// jsonColumnValue decodes a JSON column with exact numbers and checks the
// times inside it.
func jsonColumnValue(t *testing.T, writer, column string, text *string) any {
	t.Helper()

	if text == nil {
		return nil
	}
	decoder := json.NewDecoder(strings.NewReader(*text))
	decoder.UseNumber()
	var value any
	require.NoError(t, decoder.Decode(&value), "%s wrote invalid %s JSON: %s", writer, column, *text)
	return comparableJSON(t, writer, column, value)
}

// comparableJSON replaces numbers with their exact values and RFC 3339 times
// with rowTimes, checking that the times are in Go's format.
func comparableJSON(t *testing.T, writer, column string, value any) any {
	t.Helper()

	switch value := value.(type) {
	case json.Number:
		exact, ok := new(big.Rat).SetString(string(value))
		require.True(t, ok, "%s wrote an invalid number in %s: %s", writer, column, value)
		return exactNumber(exact.RatString())
	case string:
		if rfc3339TextPattern.MatchString(value) {
			require.Regexp(t, goTimeTextPattern, value, "%s wrote a time in %s in a format other than Go's: %s", writer, column, value)
			at, err := time.Parse(time.RFC3339Nano, value)
			require.NoError(t, err)
			return rowTime{at: at}
		}
		return value
	case []any:
		for i, element := range value {
			value[i] = comparableJSON(t, writer, column, element)
		}
		return value
	case map[string]any:
		for key, element := range value {
			value[key] = comparableJSON(t, writer, column, element)
		}
		return value
	}
	return value
}

// exactNumber is a JSON number reduced to its exact value, so 1.50, 1.5,
// and 15e-1 are equal.
type exactNumber string

// RequireEquivalentRows requires two rows the same steps wrote to hold the
// same values. Times are equivalent when they're the same instant, as for a
// time given in a request, or when their offsets from their own row's
// created_at differ by at most rowTimeTolerance. Times at the paths in
// unpinned, like ".finalized_at", only need to be present in both.
func RequireEquivalentRows(t *testing.T, operation string, expected, actual StoredRow, unpinned ...string) {
	t.Helper()

	expectedCreated, ok := expected["created_at"].(rowTime)
	require.True(t, ok)
	actualCreated, ok := actual["created_at"].(rowTime)
	require.True(t, ok)
	sameTime := func(path string, expectedTime, actualTime rowTime) bool {
		if expectedTime.at.Equal(actualTime.at) || slices.Contains(unpinned, path) {
			return true
		}
		difference := expectedTime.at.Sub(expectedCreated.at) - actualTime.at.Sub(actualCreated.at)
		return difference.Abs() <= rowTimeTolerance
	}
	require.Empty(t, rowDifferences("", map[string]any(expected), map[string]any(actual), sameTime),
		"%s: the rows differ", operation)
}

func rowDifferences(path string, expected, actual any, sameTime func(string, rowTime, rowTime) bool) []string {
	switch expected := expected.(type) {
	case rowTime:
		actual, ok := actual.(rowTime)
		if !ok || !sameTime(path, expected, actual) {
			return []string{fmt.Sprintf("%s: %v != %v", path, describe(expected), describe(actual))}
		}
		return nil
	case map[string]any:
		actual, ok := actual.(map[string]any)
		if !ok {
			return []string{fmt.Sprintf("%s: %v != %v", path, describe(expected), describe(actual))}
		}
		var differences []string
		for key := range expected {
			if _, ok := actual[key]; !ok {
				differences = append(differences, path+"."+key+": missing")
			}
		}
		for key := range actual {
			if _, ok := expected[key]; !ok {
				differences = append(differences, fmt.Sprintf("%s.%s: unexpected %v", path, key, describe(actual[key])))
				continue
			}
			differences = append(differences, rowDifferences(path+"."+key, expected[key], actual[key], sameTime)...)
		}
		slices.Sort(differences)
		return differences
	case []any:
		actual, ok := actual.([]any)
		if !ok || len(actual) != len(expected) {
			return []string{fmt.Sprintf("%s: %v != %v", path, describe(expected), describe(actual))}
		}
		var differences []string
		for i := range expected {
			differences = append(differences, rowDifferences(fmt.Sprintf("%s[%d]", path, i), expected[i], actual[i], sameTime)...)
		}
		return differences
	}
	if !reflect.DeepEqual(expected, actual) {
		return []string{fmt.Sprintf("%s: %v != %v", path, describe(expected), describe(actual))}
	}
	return nil
}

func describe(value any) string {
	switch value := value.(type) {
	case rowTime:
		return value.at.Format(time.RFC3339Nano)
	case *string:
		if value == nil {
			return "<nil>"
		}
		return strconv.Quote(*value)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%#v", value)
	}
	return string(encoded)
}
