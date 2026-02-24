// Package riverlog provides a context logging middleware for workers that
// collates output and stores it to job records.
package riverlog

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"

	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

const (
	maxSizeMB      = 2
	maxSizeBytes   = maxSizeMB * 1024 * 1024
	maxTotalSizeMB = 8
	maxTotalBytes  = maxTotalSizeMB * 1024 * 1024
	// Hard ceiling to prevent pathological allocations from extreme configs.
	maxTotalSizeMaxMB = 64
	maxTotalBytesMax  = maxTotalSizeMaxMB * 1024 * 1024
	metadataKey    = "river:log"
)

type contextKey struct{}

// Logger extracts a logger from context from within the Work body of a worker.
// Middleware must be installed on either the worker or client for this function
// to be usable.
//
// This variant panics if no logger was available in context.
func Logger(ctx context.Context) *slog.Logger {
	logger, ok := LoggerSafely(ctx)
	if !ok {
		panic("no logger in context; do you have riverlog.Middleware configured?")
	}
	return logger
}

// LoggerSafely extracts a logger from context from within the Work body of a
// worker.  Middleware must be installed on either the worker or client for this
// function to be usable.
//
// This variant returns a boolean that's true if a logger was available in
// context and false otherwise.
func LoggerSafely(ctx context.Context) (*slog.Logger, bool) {
	logger, ok := ctx.Value(contextKey{}).(*slog.Logger)
	return logger, ok
}

// Middleware injects a context logger into the Work function of workers it's
// installed on (or workers of the client it's installed on) which is accessible
// with Logger, and which collates all log output to store it to metadata after
// the job finishes execution. This output is then viewable from River UI.
type Middleware struct {
	baseservice.BaseService
	rivertype.Middleware

	config           *MiddlewareConfig
	newCustomContext func(ctx context.Context, w io.Writer) context.Context
	newSlogHandler   func(w io.Writer) slog.Handler
}

// MiddlewareConfig is configuration for Middleware.
type MiddlewareConfig struct {
	// MaxSizeBytes is the maximum size of log data that'll be persisted in
	// bytes per job attempt. Anything larger will be truncated will be
	// truncated down to MaxSizeBytes.
	//
	// Be careful with this number because the maximum total log size is equal
	// to maximum number of attempts multiplied by this number (each attempt's
	// logs are kept separately). For example, 25 * 2 MB = 50 MB maximum
	// theoretical log size. Log data goes into metadata which is a JSONB field,
	// and JSONB fields have a maximum size of 255 MB, so any number larger than
	// 255 divided by maximum number of attempts may cause serious operational
	// problems.
	//
	// Defaults to 2 MB (which is per job attempt).
	MaxSizeBytes int

	// MaxTotalBytes is the maximum total size of all persisted river logs for a
	// job attempt history. If appending the latest attempt would exceed this
	// size, oldest log entries are dropped first.
	//
	// The latest entry is always retained, even if doing so means the resulting
	// payload exceeds MaxTotalBytes.
	//
	// Defaults to 8 MB. Values larger than 64 MB are clamped to 64 MB.
	MaxTotalBytes int
}

// NewMiddleware initializes a new Middleware with the given slog handler
// initialization function and configuration.
//
// newHandler is a function which is invoked on every Work execution to generate
// a new slog.Handler for a work-specific slog.Logger. It should take an
// io.Writer and return a slog.Handler of choice that's configured to suit the
// caller.
//
// For example:
//
//	riverlog.NewMiddleware(func(w io.Writer) slog.Handler {
//		return slog.NewJSONHandler(w, nil)
//	}, nil)
//
// With the middleware in place, the logger is available in a work function's
// context:
//
//	func (w *MyWorker) Work(ctx context.Context, job *river.Job[MyArgs]) error {
//		Logger(ctx).InfoContext(ctx, "Hello from work")
func NewMiddleware(newSlogHandler func(w io.Writer) slog.Handler, config *MiddlewareConfig) *Middleware {
	return &Middleware{
		config:         defaultConfig(config),
		newSlogHandler: newSlogHandler,
	}
}

// NewMiddlewareCustomContext initializes a new Middleware with the given arbitrary
// context initialization function and configuration.
//
// newContext is a function which is invoked on every Work execution to generate
// a new context for the worker. It's generally used to initialize a logger with
// the given writer and put it in context under a user-defined context key for
// later use.
//
// This variant is meant to provide callers with a version of the middleware
// that's not tied to slog. A non-slog standard library logger, Logrus, or Zap
// logger could all be placed in context according to preferred convention.
//
// For example:
//
//	riverlog.NewMiddlewareCustomContext(func(ctx context.Context, w io.Writer) context.Context {
//		logger := log.New(w, "", 0)
//		return context.WithValue(ctx, ArbitraryContextKey{}, logger)
//	}, nil),
func NewMiddlewareCustomContext(newCustomContext func(ctx context.Context, w io.Writer) context.Context, config *MiddlewareConfig) *Middleware {
	return &Middleware{
		config:           defaultConfig(config),
		newCustomContext: newCustomContext,
	}
}

func defaultConfig(config *MiddlewareConfig) *MiddlewareConfig {
	if config == nil {
		config = &MiddlewareConfig{}
	}

	config.MaxSizeBytes = cmp.Or(config.MaxSizeBytes, maxSizeBytes)
	config.MaxTotalBytes = cmp.Or(config.MaxTotalBytes, maxTotalBytes)
	if config.MaxTotalBytes > maxTotalBytesMax {
		config.MaxTotalBytes = maxTotalBytesMax
	}

	return config
}

type logAttempt struct {
	Attempt int    `json:"attempt"`
	Log     string `json:"log"`
}

type metadataWithLog struct {
	RiverLog []logAttempt `json:"river:log"`
}

type metadataWithRawLog struct {
	RiverLog []json.RawMessage `json:"river:log"`
}

func (m *Middleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) error {
	var (
		existingRawLogData metadataWithRawLog
		logBuf             bytes.Buffer
	)

	switch {
	case m.newCustomContext != nil:
		ctx = m.newCustomContext(ctx, &logBuf)
	case m.newSlogHandler != nil:
		logger := slog.New(m.newSlogHandler(&logBuf))
		ctx = context.WithValue(ctx, contextKey{}, logger)
	default:
		return errors.New("expected either newContextLogger or newSlogHandler to be set")
	}

	if err := json.Unmarshal(job.Metadata, &existingRawLogData); err != nil {
		return err
	}

	metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if !hasMetadataUpdates {
		return errors.New("expected to find metadata updates in context, but didn't")
	}

	// This all runs invariant of whether the job panics or returns an error.
	defer func() {
		logData := logBuf.String()

		// Return early if nothing ended up getting logged.
		if len(logData) < 1 {
			return
		}

		// Postgres JSONB is limited to 255MB, but it would be a bad idea to get
		// anywhere close to that limit here.
		if len(logData) > m.config.MaxSizeBytes {
			m.Logger.WarnContext(ctx, m.Name+": Logs size exceeded maximum; truncating",
				slog.Int("logs_size", len(logData)),
				slog.Int("max_size", m.config.MaxSizeBytes),
			)
			logData = logData[0:m.config.MaxSizeBytes]
		}

		newLogEntryBytes, err := json.Marshal(logAttempt{
			Attempt: job.Attempt,
			Log:     logData,
		})
		if err != nil {
			m.Logger.ErrorContext(ctx, m.Name+": Error marshaling log data",
				slog.Any("error", err),
			)
			return
		}

		allLogDataBytes, numDroppedEntries, err := marshalRawLogDataWithCap(append(existingRawLogData.RiverLog, json.RawMessage(newLogEntryBytes)), m.config.MaxTotalBytes)
		if err != nil {
			m.Logger.ErrorContext(ctx, m.Name+": Error marshaling log data",
				slog.Any("error", err),
			)
			return
		}

		if numDroppedEntries > 0 {
			m.Logger.WarnContext(ctx, m.Name+": Logs size exceeded total maximum; dropping oldest entries",
				slog.Int("max_total_size", m.config.MaxTotalBytes),
				slog.Int("num_entries_dropped", numDroppedEntries),
			)
		}

		metadataUpdates[metadataKey] = json.RawMessage(allLogDataBytes)
	}()

	return doInner(ctx)
}

func marshalRawLogDataWithCap(allLogData []json.RawMessage, maxTotalBytes int) ([]byte, int, error) {
	allLogDataBytes, err := json.Marshal(allLogData)
	if err != nil {
		return nil, 0, err
	}

	if maxTotalBytes <= 0 || len(allLogDataBytes) <= maxTotalBytes {
		return allLogDataBytes, 0, nil
	}

	// Drop oldest entries first, while always retaining the latest one.
	var numDroppedEntries int
	for numDroppedEntries < len(allLogData)-1 && len(allLogDataBytes) > maxTotalBytes {
		numDroppedEntries++

		allLogDataBytes, err = json.Marshal(allLogData[numDroppedEntries:])
		if err != nil {
			return nil, numDroppedEntries, err
		}
	}

	return allLogDataBytes, numDroppedEntries, nil
}
