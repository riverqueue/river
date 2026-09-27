package dbunique

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/riverqueue/river/rivershared/structtag"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

// Default job states for UniqueOpts.ByState. Stored here to a variable so we
// don't have to reallocate a slice over and over again.
var uniqueOptsByStateDefault = rivertype.UniqueOptsByStateDefault() //nolint:gochecknoglobals

type UniqueOpts struct {
	ByArgs      bool
	ByPeriod    time.Duration
	ByQueue     bool
	ByState     []rivertype.JobState
	ExcludeKind bool
}

func (o *UniqueOpts) IsEmpty() bool {
	return !o.ByArgs &&
		o.ByPeriod == time.Duration(0) &&
		!o.ByQueue &&
		o.ByState == nil &&
		!o.ExcludeKind
}

func (o *UniqueOpts) StateBitmask() byte {
	states := uniqueOptsByStateDefault
	if len(o.ByState) > 0 {
		states = o.ByState
	}
	return uniquestates.UniqueStatesToBitmask(states)
}

func UniqueKey(timeGen rivertype.TimeGenerator, uniqueOpts *UniqueOpts, params *rivertype.JobInsertParams) ([]byte, error) {
	uniqueKeyString, err := buildUniqueKeyString(timeGen, uniqueOpts, params)
	if err != nil {
		return nil, err
	}
	uniqueKeyHash := sha256.Sum256([]byte(uniqueKeyString))
	return uniqueKeyHash[:], nil
}

// Builds a unique key made up of the unique options in place. The key is hashed
// to become a value for `unique_key`.
func buildUniqueKeyString(timeGen rivertype.TimeGenerator, uniqueOpts *UniqueOpts, params *rivertype.JobInsertParams) (string, error) {
	var sb strings.Builder

	if !uniqueOpts.ExcludeKind {
		sb.WriteString("&kind=" + params.Kind)
	}

	if uniqueOpts.ByArgs {
		var encodedArgsForUnique []byte
		// Get unique JSON keys from the JobArgs struct:
		uniqueFields, err := structtag.SortedFieldsWithTag(params.Args, "unique")
		if err != nil {
			return "", err
		}

		if len(uniqueFields) > 0 {
			// Extract unique values from the EncodedArgs JSON
			uniqueValues := structtag.ExtractValues(params.EncodedArgs, uniqueFields)

			// Assemble the JSON object using bytes.Buffer
			// Better to overallocate a bit than to allocate multiple times, so just
			// assume we'll cap out at the length of the full encoded args.
			sortedJSONWithOnlyUniqueValues := make([]byte, 0, len(params.EncodedArgs))

			sjsonOpts := &sjson.Options{ReplaceInPlace: true}
			for i, key := range uniqueFields {
				if uniqueValues[i] == "undefined" {
					continue
				}
				sortedJSONWithOnlyUniqueValues, err = sjson.SetRawBytesOptions(sortedJSONWithOnlyUniqueValues, key, []byte(uniqueValues[i]), sjsonOpts)
				if err != nil {
					// Should not happen unless key was invalid
					return "", err
				}
			}
			encodedArgsForUnique = sortedJSONWithOnlyUniqueValues
		} else {
			// Use all keys from EncodedArgs sorted alphabetically
			encodedArgsForUnique, err = appendSortedObject(make([]byte, 0, len(params.EncodedArgs)), params.EncodedArgs)
			if err != nil {
				return "", err
			}
		}

		sb.WriteString("&args=")
		sb.Write(encodedArgsForUnique)
	}

	if uniqueOpts.ByPeriod != time.Duration(0) {
		// Format the period bound in UTC. RFC 3339 formatting includes the
		// time's location, so without normalization the same period produces
		// different keys for processes or callers in different time zones.
		// Truncation operates on absolute time, so the bound itself is
		// independent of location.
		lowerPeriodBound := ptrutil.ValOrDefaultFunc(params.ScheduledAt, timeGen.Now).Truncate(uniqueOpts.ByPeriod).UTC()
		sb.WriteString("&period=" + lowerPeriodBound.Format(time.RFC3339))
	}

	if uniqueOpts.ByQueue {
		sb.WriteString("&queue=" + params.Queue)
	}

	return sb.String(), nil
}

// appendJSONKey appends key to buf as a JSON string, encoded the same way sjson
// encodes object keys: verbatim between quotes, unless the key contains a byte
// below 0x20 or above 0x7f, `"`, or `\`, in which case it's encoded with
// encoding/json (which also escapes `<`, `>`, `&`, U+2028, and U+2029, and
// replaces invalid UTF-8 with U+FFFD). Matching sjson keeps unique keys
// identical to those of earlier versions, which built unique args with sjson.
func appendJSONKey(buf []byte, key string) []byte {
	for i := range len(key) {
		if key[i] < ' ' || key[i] > 0x7f || key[i] == '"' || key[i] == '\\' {
			encodedKey, _ := json.Marshal(key) //nolint:errchkjson // marshaling a string can't fail
			return append(buf, encodedKey...)
		}
	}

	buf = append(buf, '"')
	buf = append(buf, key...)
	return append(buf, '"')
}

// appendSortedObject appends to buf a compact JSON object containing each
// top-level key of encodedObject along with its raw value, sorted by key. If
// a key appears more than once, its first value is used. As in the previous
// path-based implementation, empty input and an empty array produce `{}`;
// other non-object input is rejected.
//
// Keys are walked directly rather than addressed as gjson/sjson paths so that
// keys containing path syntax (like `.`, `@`, or a leading `:`) and empty
// keys are included literally, and distinct values of such keys produce
// distinct output. For all other keys, output is byte-identical to that
// produced by setting each key onto `{}` with sjson.
func appendSortedObject(buf, encodedObject []byte) ([]byte, error) {
	type keyValue struct {
		key      string
		rawValue string
	}

	var (
		keyValues []keyValue
		keysSeen  = make(map[string]struct{})
		err       error
	)
	gjson.ParseBytes(encodedObject).ForEach(func(key, value gjson.Result) bool {
		if key.Type != gjson.String {
			err = errors.New("unique args must encode a JSON object")
			return false
		}
		if _, ok := keysSeen[key.Str]; !ok {
			keysSeen[key.Str] = struct{}{}
			keyValues = append(keyValues, keyValue{key: key.Str, rawValue: value.Raw})
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(keyValues, func(a, b keyValue) int { return strings.Compare(a.key, b.key) })

	buf = append(buf, '{')
	for i, keyValue := range keyValues {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendJSONKey(buf, keyValue.key)
		buf = append(buf, ':')
		buf = append(buf, keyValue.rawValue...)
	}
	return append(buf, '}'), nil
}
