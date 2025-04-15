package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/rivertype"
)

const (
	maxOutputSizeMB    = 32
	maxOutputSizeBytes = maxOutputSizeMB * 1024 * 1024
)

// RecordOutput records output JSON from a job. The "output" can be any
// JSON-encodable value and will be stored in the database on the job row after
// the current execution attempt completes. Output may be useful for debugging,
// or for storing the result of a job temporarily without needing to create a
// dedicated table to keep it in.
//
// For example, with workflows, it's common for subsequent task to depend on
// something done in an earlier dependency task. Consider the creation of an
// external resource in another API or in an database—it will typically have a
// unique ID that must be used to reference the resource later. A later step
// may require that info in order to complete its work, and the output can be
// a convenient way to store that info.
//
// Output is stored in the job's metadata under the `"output"` key
// ([github.com/riverqueue/river/rivertype.MetadataKeyOutput]).
// This function must be called within an Worker's Work function. It returns an
// error if called anywhere else. As with any stored value, care should be taken
// to ensure that the payload size is not too large. Output is limited to 32MB
// in size for safety, but should be kept much smaller than this.
//
// Only one output can be stored per job. If this function is called more than
// once, the output will be overwritten with the latest value. The output also
// must be recorded _before_ the job finishes executing so that it can be stored
// when the job's row is updated.
//
// Once recorded, the output is stored regardless of the outcome of the
// execution attempt (success, error, panic, etc.).
//
// The output is marshalled to JSON as part of this function and it will return
// an error if the output is not JSON-encodable.
func RecordOutput(ctx context.Context, output any) error {
	metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if !hasMetadataUpdates {
		return errors.New("RecordOutput must be called within a Worker")
	}

	metadataUpdatesBytes, err := json.Marshal(output)
	if err != nil {
		return err
	}

	// Postgres JSONB is limited to 255MB, but it would be a bad idea to get
	// anywhere close to that limit here.
	if len(metadataUpdatesBytes) > maxOutputSizeBytes {
		return fmt.Errorf("output is too large: %d bytes (max %d MB)", len(metadataUpdatesBytes), maxOutputSizeMB)
	}

	metadataUpdates[rivertype.MetadataKeyOutput] = json.RawMessage(metadataUpdatesBytes)
	return nil
}
