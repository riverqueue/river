package riverdriver

import (
	"encoding/json"
	"fmt"
)

// UniqueInsertMetadataKey is a reserved job metadata key used to detect unique
// insert conflicts on databases that don't expose Postgres system columns.
const UniqueInsertMetadataKey = "river:unique_nonce"

// UniqueInsertMode is a database-specific strategy for detecting whether a
// unique insert returned a newly inserted job or an existing one.
type UniqueInsertMode uint32

const (
	// UniqueInsertModeUnknown indicates that a database's mode hasn't been
	// detected yet.
	UniqueInsertModeUnknown UniqueInsertMode = iota

	// UniqueInsertModeMetadataNonce detects conflicts by putting a nonce in the
	// metadata of the proposed job and checking whether the returned job
	// contains it.
	UniqueInsertModeMetadataNonce

	// UniqueInsertModeReturningOld uses Postgres 18's OLD row support in
	// RETURNING.
	UniqueInsertModeReturningOld

	// UniqueInsertModeXmax uses Postgres's xmax system column.
	UniqueInsertModeXmax
)

// SQL returns the SQL expression for the mode. UniqueInsertModeMetadataNonce
// always returns false because duplicate detection is performed in Go instead.
func (m UniqueInsertMode) SQL() string {
	switch m {
	case UniqueInsertModeMetadataNonce:
		return "false"

	case UniqueInsertModeReturningOld:
		return "(OLD.id IS NOT NULL)"

	case UniqueInsertModeXmax:
		return "(xmax != 0)"

	case UniqueInsertModeUnknown:
		panic("unique insert mode has not been detected")

	default:
		panic(fmt.Sprintf("invalid unique insert mode: %d", m))
	}
}

// UniqueInsertMetadataIsDuplicate returns whether metadata lacks the nonce
// from a proposed insert, indicating that an existing row was returned
// instead.
func UniqueInsertMetadataIsDuplicate(metadata []byte, nonce string) bool {
	metadataNonce, ok := UniqueInsertMetadataNonce(metadata)
	return !ok || metadataNonce != nonce
}

// UniqueInsertMetadataNonce returns the nonce in metadata, or false if the
// metadata has no valid nonce.
func UniqueInsertMetadataNonce(metadata []byte) (string, bool) {
	var metadataMap map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &metadataMap); err != nil {
		return "", false
	}

	var metadataNonce string
	if err := json.Unmarshal(metadataMap[UniqueInsertMetadataKey], &metadataNonce); err != nil {
		return "", false
	}
	return metadataNonce, true
}

// UniqueInsertMetadataWithNonce returns metadata with nonce set under
// UniqueInsertMetadataKey.
func UniqueInsertMetadataWithNonce(metadata []byte, nonce string) ([]byte, error) {
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	var metadataMap map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &metadataMap); err != nil {
		return nil, fmt.Errorf("error unmarshaling job metadata: %w", err)
	}
	if metadataMap == nil {
		metadataMap = make(map[string]json.RawMessage)
	}

	nonceJSON, err := json.Marshal(nonce)
	if err != nil {
		return nil, fmt.Errorf("error marshaling unique insert nonce: %w", err)
	}
	metadataMap[UniqueInsertMetadataKey] = nonceJSON

	metadata, err = json.Marshal(metadataMap)
	if err != nil {
		return nil, fmt.Errorf("error marshaling job metadata: %w", err)
	}
	return metadata, nil
}

// UniqueInsertModeFromProductAndVersion returns the unique insert mode
// appropriate for a database product and its Postgres-compatible server
// version number.
func UniqueInsertModeFromProductAndVersion(product string, version int32) UniqueInsertMode {
	if postgresProductIsYugabyte(product) {
		return UniqueInsertModeMetadataNonce
	}
	if version >= 180_000 {
		return UniqueInsertModeReturningOld
	}
	return UniqueInsertModeXmax
}
