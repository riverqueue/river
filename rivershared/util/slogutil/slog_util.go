package slogutil

import (
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/riverqueue/river/rivershared/util/sliceutil"
)

// Standard functions suitable for use with slog.HandlerOptions.ReplaceAttr that
// remove certain log keys for better test output stability. Prefer NoLevelTime
// unless you also have `job_id` in output, then use the latter.
var (
	NoLevelTime      = removeReplaceAttrFunc(slog.LevelKey, slog.TimeKey)           //nolint:gochecknoglobals
	NoLevelTimeJobID = removeReplaceAttrFunc(slog.LevelKey, slog.TimeKey, "job_id") //nolint:gochecknoglobals
)

// Produces a function suitable for use with slog.HandlerOptions.ReplaceAttr
// that removes the given attrKeys from all log entries. Generally used to
// remove keys like level and time for more stable logging for tests.
func removeReplaceAttrFunc(attrKeys ...string) func(groups []string, attr slog.Attr) slog.Attr {
	return func(groups []string, attr slog.Attr) slog.Attr {
		if len(groups) < 1 {
			if slices.Contains(attrKeys, attr.Key) {
				return slog.Attr{}
			}
		}
		return attr
	}
}

// SliceInt64 is a type that implements slog.LogValue and which will format a
// slice for inclusion in logging, but lazily so that no work is done unless a
// log line is actually emitted.
type SliceInt64 []int64

func (s SliceInt64) LogValue() slog.Value {
	return slog.StringValue(strings.Join(
		sliceutil.Map(s, func(i int64) string { return strconv.FormatInt(i, 10) }),
		",",
	))
}

// SliceString is a type that implements slog.LogValue and which will format a
// slice for inclusion in logging, but lazily so that no work is done unless a
// log line is actually emitted.
type SliceString []string

func (s SliceString) LogValue() slog.Value {
	return slog.StringValue(strings.Join(s, ","))
}
