package river

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/riverqueue/river/rivertype"
)

func RecordOutput(ctx context.Context, output any) error {
	metadataUpdates, hasMetadataUpdates := metadataUpdatesFromContext(ctx)
	if !hasMetadataUpdates {
		return errors.New("RecordOutput must be called within a Worker")
	}

	metadataUpdatesBytes, err := json.Marshal(output)
	if err != nil {
		return err
	}

	metadataUpdates[rivertype.MetadataKeyOutput] = json.RawMessage(metadataUpdatesBytes)
	return nil
}
