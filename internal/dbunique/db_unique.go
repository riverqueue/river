package dbunique

import (
	"crypto/sha256"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
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
		uniqueFields, err := getSortedUniqueFieldsCached(params.Args)
		if err != nil {
			return "", err
		}

		if len(uniqueFields) > 0 {
			// Extract unique values from the EncodedArgs JSON
			uniqueValues := extractUniqueValues(params.EncodedArgs, uniqueFields)

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
			keys := sliceutil.Map(gjson.GetBytes(params.EncodedArgs, "@keys").Array(), func(v gjson.Result) string { return v.String() })
			slices.Sort(keys)

			sortedJSON := make([]byte, 0, len(params.EncodedArgs))
			sortedJSON = append(sortedJSON, "{}"...)
			sjsonOpts := &sjson.Options{ReplaceInPlace: true}
			for _, key := range keys {
				sortedJSON, err = sjson.SetRawBytesOptions(sortedJSON, key, []byte(gjson.GetBytes(params.EncodedArgs, key).Raw), sjsonOpts)
				if err != nil {
					// Should not happen unless key was invalid
					return "", err
				}
			}
			encodedArgsForUnique = sortedJSON
		}

		sb.WriteString("&args=")
		sb.Write(encodedArgsForUnique)
	}

	if uniqueOpts.ByPeriod != time.Duration(0) {
		lowerPeriodBound := ptrutil.ValOrDefaultFunc(params.ScheduledAt, timeGen.NowUTC).Truncate(uniqueOpts.ByPeriod)
		sb.WriteString("&period=" + lowerPeriodBound.Format(time.RFC3339))
	}

	if uniqueOpts.ByQueue {
		sb.WriteString("&queue=" + params.Queue)
	}

	return sb.String(), nil
}
