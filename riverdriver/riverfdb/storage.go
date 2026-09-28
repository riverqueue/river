//go:build foundationdb

package riverfdb

import (
	"encoding/json"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/riverqueue/river/rivertype"
)

func encodeJSON(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("riverfdb: encode record: %w", err)
	}
	if len(data) > 100_000 {
		return nil, fmt.Errorf("riverfdb: encoded record is %d bytes; FoundationDB permits 100000", len(data))
	}
	return data, nil
}

func prefixRange(prefix fdb.Key) fdb.KeyRange {
	// Driver keys always end with tuple data, so they cannot consist solely
	// of 0xff bytes (the only case where PrefixRange returns an error).
	keyRange, _ := fdb.PrefixRange(prefix)
	return keyRange
}

func readJSON[T any](tx fdb.Transaction, key fdb.Key) (*T, error) {
	data, err := tx.Get(key).Get()
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, rivertype.ErrNotFound
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, fmt.Errorf("riverfdb: decode record: %w", err)
	}
	return &value, nil
}

func scanJSON[T any](tx fdb.Transaction, prefix fdb.Key) ([]*T, error) {
	entries, err := tx.GetRange(prefixRange(prefix), fdb.RangeOptions{}).GetSliceWithError()
	if err != nil {
		return nil, err
	}
	values := make([]*T, 0, len(entries))
	for _, entry := range entries {
		var value T
		if err := json.Unmarshal(entry.Value, &value); err != nil {
			return nil, fmt.Errorf("riverfdb: decode record: %w", err)
		}
		values = append(values, &value)
	}
	return values, nil
}

func writeJSON(tx fdb.Transaction, key fdb.Key, value any) error {
	data, err := encodeJSON(value)
	if err != nil {
		return err
	}
	tx.Set(key, data)
	return nil
}
