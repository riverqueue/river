package river

import (
	"context"
	"errors"
	"strings"

	"github.com/riverqueue/river/internal/jobexecutor"
)

var errMetadataNotSettable = errors.New("SetMetadata must be called within a Worker, worker middleware, or work hook")

// SetMetadata records a metadata value to be merged into the job's metadata
// when the current work attempt finishes.
//
// This function is only valid from a worker, worker middleware, or work hook
// like rivertype.HookWorkEnd. Keys prefixed with `river:` are reserved for
// internal use and may not be set by user code.
func SetMetadata(ctx context.Context, key string, value any) error {
	if strings.HasPrefix(key, "river:") {
		return errors.New("SetMetadata cannot be used with keys prefixed with `river:`")
	}

	metadataUpdates, ok := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if !ok {
		return errMetadataNotSettable
	}

	metadataUpdates[key] = value
	return nil
}
