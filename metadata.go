package river

import (
	"context"
	"errors"
	"strings"

	"github.com/riverqueue/river/internal/jobexecutor"
)

var errMetadataNotSettable = errors.New("MetadataSet must be called within a worker, worker middleware, or work hook")

// MetadataSet records a metadata value to be merged into the job's metadata
// when the current work attempt finishes.
//
// This function is only valid from a worker, worker middleware, or work hook
// like rivertype.HookWorkBegin or rivertype.HookWorkEnd.
//
// Metadata updates are stored on the work context and merged into the job row
// when the current work attempt finishes, whether the attempt succeeds or
// errors. Values must be JSON marshalable because metadata is stored in a
// jsonb column, and setting a key replaces any existing value at that key.
//
// Keys prefixed with `river:` are reserved for internal use and may not be set
// by user code.
func MetadataSet(ctx context.Context, key string, value any) error {
	if strings.HasPrefix(key, "river:") {
		return errors.New("MetadataSet cannot be used with keys prefixed with `river:`")
	}

	metadataUpdates, ok := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if !ok {
		return errMetadataNotSettable
	}

	metadataUpdates[key] = value
	return nil
}
