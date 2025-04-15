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
	maxSizeMB    = 2
	maxSizeBytes = maxSizeMB * 1024 * 1024
	metadataKey  = "river:log"
)

type contextKey struct{}

// Logger extracts a logger from context from within the Work body of a worker.
// Middleware must be installed on either the worker or client for this function
// to be usable.
func Logger(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(contextKey{}).(*slog.Logger)
	if !ok {
		panic("no logger in context; do you have riverlog.Middleware configured?")
	}
	return logger
}

// Middleware injects a context logger into the Work function of workers it's
// installed on (or workers of the client it's installed on) which is accessible
// with Logger, and which collates all log output to store it to metadata after
// the job finishes execution. This output is then viewable from River UI.
type Middleware struct {
	baseservice.BaseService
	rivertype.Middleware
	config     *MiddlewareConfig
	newHandler func(w io.Writer) slog.Handler
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
}

// NewMiddleware initializes a new Middleware with the given handler function
// and configuration.
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
func NewMiddleware(newHandler func(w io.Writer) slog.Handler, config *MiddlewareConfig) *Middleware {
	if config == nil {
		config = &MiddlewareConfig{}
	}

	// Assign defaults.
	config = &MiddlewareConfig{
		MaxSizeBytes: cmp.Or(config.MaxSizeBytes, maxSizeBytes),
	}

	return &Middleware{
		config:     config,
		newHandler: newHandler,
	}
}

type logAttempt struct {
	Attempt int    `json:"attempt"`
	Log     string `json:"log"`
}

type metadataWithLog struct {
	RiverLog []logAttempt `json:"river:log"`
}

func (m *Middleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) error {
	var (
		existingLogData metadataWithLog
		logBuf          bytes.Buffer
		logger          = slog.New(m.newHandler(&logBuf))
	)

	if err := json.Unmarshal(job.Metadata, &existingLogData); err != nil {
		return err
	}

	metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if !hasMetadataUpdates {
		return errors.New("expected to find metadata updates in context, but didn't")
	}

	// This all runs invariant of whether the job panics or returns an error.
	defer func() {
		logData := logBuf.String()

		// Postgres JSONB is limited to 255MB, but it would be a bad idea to get
		// anywhere close to that limit here.
		if len(logData) > m.config.MaxSizeBytes {
			m.Logger.WarnContext(ctx, m.Name+": Logs size exceeded maximum; truncating",
				slog.Int("logs_size", len(logData)),
				slog.Int("max_size", m.config.MaxSizeBytes),
			)
			logData = logData[0:m.config.MaxSizeBytes]
		}

		allLogDataBytes, err := json.Marshal(append(existingLogData.RiverLog, logAttempt{
			Attempt: job.Attempt,
			Log:     logData,
		}))
		if err != nil {
			m.Logger.ErrorContext(ctx, m.Name+": Error marshaling log data",
				slog.String("error", err.Error()),
			)
		}

		metadataUpdates[metadataKey] = json.RawMessage(allLogDataBytes)
	}()

	return doInner(context.WithValue(ctx, contextKey{}, logger))
}
