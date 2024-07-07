package slogutil

import (
	"context"
	"fmt"
	"log/slog"
)

// SlogMessageOnlyHandler is a trivial slog handler that prints only messages.
// All attributes and groups are ignored. It's useful in example tests where it
// produces output that's normalized so we match against it (normally, all log
// lines include timestamps so it's not possible to have reproducible output).
type SlogMessageOnlyHandler struct {
	Level slog.Level
}

func (h *SlogMessageOnlyHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.Level
}

func (h *SlogMessageOnlyHandler) Handle(ctx context.Context, record slog.Record) error {
	fmt.Printf("%s\n", record.Message)
	return nil
}

func (h *SlogMessageOnlyHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *SlogMessageOnlyHandler) WithGroup(name string) slog.Handler       { return h }
