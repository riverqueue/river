// Package riverlog provides a context logging middleware for workers that
// collates output and stores it to job records.
package riverlog

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/tidwall/gjson"

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
	metadataKey       = "river:log"
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
	config.MaxTotalBytes = min(cmp.Or(config.MaxTotalBytes, maxTotalBytes), maxTotalBytesMax)

	return config
}

type logAttempt struct {
	Attempt int    `json:"attempt"`
	Log     string `json:"log"`
}

type metadataWithLog struct {
	RiverLog []logAttempt `json:"river:log"`
}

func (m *Middleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) error {
	var logBuf bytes.Buffer

	switch {
	case m.newCustomContext != nil:
		ctx = m.newCustomContext(ctx, &logBuf)
	case m.newSlogHandler != nil:
		logger := slog.New(m.newSlogHandler(&logBuf))
		ctx = context.WithValue(ctx, contextKey{}, logger)
	default:
		return errors.New("expected either newContextLogger or newSlogHandler to be set")
	}

	metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if !hasMetadataUpdates {
		return errors.New("expected to find metadata updates in context, but didn't")
	}

	// This all runs invariant of whether the job panics or returns an error.
	defer func() {
		logBytes := logBuf.Bytes()

		// Return early if nothing ended up getting logged.
		if len(logBytes) < 1 {
			return
		}

		// Postgres JSONB is limited to 255MB, but it would be a bad idea to get
		// anywhere close to that limit here.
		if len(logBytes) > m.config.MaxSizeBytes {
			m.Logger.WarnContext(ctx, m.Name+": Logs size exceeded maximum; truncating",
				slog.Int("logs_size", len(logBytes)),
				slog.Int("max_size", m.config.MaxSizeBytes),
			)
			logBytes = logBytes[0:m.config.MaxSizeBytes]
		}

		newLogEntryBytes, err := json.Marshal(logAttempt{
			Attempt: job.Attempt,
			Log:     string(logBytes),
		})
		if err != nil {
			m.Logger.ErrorContext(ctx, m.Name+": Error marshaling log data",
				slog.Any("error", err),
			)
			return
		}

		allLogDataBytes, numDroppedEntries, err := appendLogDataWithCap(job.Metadata, newLogEntryBytes, m.config.MaxTotalBytes)
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

func appendLogDataWithCap(metadataBytes, newLogEntryBytes []byte, maxTotalBytes int) ([]byte, int, error) {
	existingLogData := gjson.GetBytes(metadataBytes, metadataKey)
	var existingLogArrayBytes []byte
	switch {
	case !existingLogData.Exists():
		existingLogArrayBytes = []byte("[]")
	case existingLogData.IsArray():
		// Slice raw JSON straight from metadata bytes to avoid an extra copy.
		existingLogArrayBytes = metadataBytes[existingLogData.Index : existingLogData.Index+len(existingLogData.Raw)]
	default:
		return nil, 0, fmt.Errorf("%q value is not an array", metadataKey)
	}

	existingElementBounds, err := getArrayElementBounds(existingLogArrayBytes)
	if err != nil {
		return nil, 0, err
	}

	// Determine the smallest suffix to keep that still fits with the new entry.
	// This keeps pruning oldest-first while avoiding repeated full rewrites.
	keepStart := getKeepStart(existingElementBounds, len(newLogEntryBytes), maxTotalBytes)

	// Build the final array once from the kept suffix plus the new entry.
	appendedLogDataBytes := buildAppendedArray(existingLogArrayBytes, existingElementBounds, keepStart, newLogEntryBytes)
	numDroppedEntries := min(keepStart, len(existingElementBounds))

	return appendedLogDataBytes, numDroppedEntries, nil
}

type arrayElementBounds struct {
	Start int
	End   int
}

func getArrayElementBounds(arrayBytes []byte) ([]arrayElementBounds, error) {
	arrResult := gjson.ParseBytes(arrayBytes)
	if !arrResult.IsArray() {
		return nil, errors.New("expected a JSON array")
	}

	elements := arrResult.Array()
	bounds := make([]arrayElementBounds, len(elements))
	for i, elem := range elements {
		if elem.Index < 0 {
			return nil, errors.New("failed to determine array element index")
		}
		bounds[i] = arrayElementBounds{
			Start: elem.Index,
			End:   elem.Index + len(elem.Raw),
		}
	}
	return bounds, nil
}

func getKeepStart(bounds []arrayElementBounds, newEntryLen, maxTotalBytes int) int {
	if maxTotalBytes <= 0 {
		return 0
	}

	// Keep newest entry even if it's larger than the configured cap.
	newOnlyLen := 2 + newEntryLen // `[` + entry + `]`
	if newOnlyLen > maxTotalBytes {
		return len(bounds)
	}

	if len(bounds) == 0 {
		return 0
	}

	lastEnd := bounds[len(bounds)-1].End

	// Iterate from oldest to newest so we drop the minimum number of entries
	// necessary to fit the configured cap.
	for keepStart := 0; keepStart <= len(bounds); keepStart++ {
		contentLen := newEntryLen
		if keepStart < len(bounds) {
			// Use actual byte offsets from parsed elements so separator
			// whitespace is fully counted toward the cap.
			keptContentLen := lastEnd - bounds[keepStart].Start
			contentLen += 1 + keptContentLen // comma between kept suffix and new entry
		}
		totalLen := 2 + contentLen // `[` + content + `]`
		if totalLen <= maxTotalBytes {
			return keepStart
		}
	}

	return len(bounds)
}

func buildAppendedArray(existingArrayBytes []byte, bounds []arrayElementBounds, keepStart int, newEntryBytes []byte) []byte {
	totalLen := 0
	maxInt := int(^uint(0) >> 1)

	// Preallocation is an optimization only. If length math would overflow,
	// fall back to zero-capacity and let append grow as needed.
	if keepStart >= len(bounds) {
		if len(newEntryBytes) <= maxInt-2 {
			totalLen = 2 + len(newEntryBytes)
		}
	} else {
		// Kept suffix is contiguous in the original array bytes, so copy once.
		keptContentLen := bounds[len(bounds)-1].End - bounds[keepStart].Start
		if keptContentLen >= 0 && len(newEntryBytes) <= maxInt-3 && keptContentLen <= maxInt-3-len(newEntryBytes) {
			totalLen = 3 + keptContentLen + len(newEntryBytes) // [] + comma + new entry
		}
	}

	result := make([]byte, 0, totalLen)
	result = append(result, '[')

	if keepStart < len(bounds) {
		result = append(result, existingArrayBytes[bounds[keepStart].Start:bounds[len(bounds)-1].End]...)
		result = append(result, ',')
	}

	result = append(result, newEntryBytes...)
	result = append(result, ']')

	return result
}
