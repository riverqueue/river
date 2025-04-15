package slogutil

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
)

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
