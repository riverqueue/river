package slogutil

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/riverqueue/river/rivershared/util/sliceutil"
)

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

// SlogMessageOnlyHandler is a trivial slog handler that prints only messages.
// All attributes and groups are ignored. It's useful in example tests where it
// produces output that's normalized so we match against it (normally, all log
// lines include timestamps so it's not possible to have reproducible output).
type SlogMessageOnlyHandler struct {
	Level slog.Level
	Out   io.Writer
}

func (h *SlogMessageOnlyHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.Level
}

func (h *SlogMessageOnlyHandler) Handle(ctx context.Context, record slog.Record) error {
	w := h.Out
	if w == nil {
		w = os.Stdout
	}

	fmt.Fprintf(w, "%s\n", record.Message)
	return nil
}

func (h *SlogMessageOnlyHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *SlogMessageOnlyHandler) WithGroup(name string) slog.Handler       { return h }
