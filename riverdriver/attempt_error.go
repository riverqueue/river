package riverdriver

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/riverqueue/river/rivertype"
)

// UnmarshalAttemptError decodes a stored attempt error, using ordinary JSON
// decoding first. If its shape doesn't match what River writes, valid JSON is
// decoded on a best effort basis so historical errors can't make a job unreadable.
// Timestamps outside RFC 3339 are left zero, numeric attempt strings are accepted,
// and non-string error and trace values are preserved as JSON text.
//
// This tolerance applies to database reads, not rivertype.AttemptError's normal
// JSON decoding. Only invalid JSON returns an error.
func UnmarshalAttemptError(data []byte, attemptError *rivertype.AttemptError) error {
	if err := json.Unmarshal(data, attemptError); err == nil {
		return nil
	}

	if !json.Valid(data) {
		return errors.New("attempt error is not valid JSON")
	}

	data = bytes.TrimSpace(data)
	if data[0] != '{' {
		*attemptError = rivertype.AttemptError{Error: attemptErrorLenientString(data)}
		return nil
	}

	var fields struct {
		At      json.RawMessage `json:"at"`
		Attempt json.RawMessage `json:"attempt"`
		Error   json.RawMessage `json:"error"`
		Trace   json.RawMessage `json:"trace"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}

	*attemptError = rivertype.AttemptError{
		At:      attemptErrorLenientTime(fields.At),
		Attempt: attemptErrorLenientInt(fields.Attempt),
		Error:   attemptErrorLenientString(fields.Error),
		Trace:   attemptErrorLenientString(fields.Trace),
	}
	return nil
}

// UnmarshalAttemptErrors decodes a stored JSON array of attempt errors. Ordinary
// arrays are decoded in one pass. If an element can't be decoded, the array is
// retried with UnmarshalAttemptError for each element. Invalid JSON and non-array
// values still return errors so the driver can mark the job as undecodable.
func UnmarshalAttemptErrors(data []byte, attemptErrors *[]rivertype.AttemptError) error {
	if err := json.Unmarshal(data, attemptErrors); err == nil {
		return nil
	}

	var rawErrors []json.RawMessage
	if err := json.Unmarshal(data, &rawErrors); err != nil {
		return err
	}

	// Start fresh: the first decode may have partially populated the slice.
	decoded := make([]rivertype.AttemptError, len(rawErrors))
	for i, rawError := range rawErrors {
		if err := UnmarshalAttemptError(rawError, &decoded[i]); err != nil {
			return err
		}
	}
	*attemptErrors = decoded
	return nil
}

func attemptErrorLenientInt(data json.RawMessage) int {
	var num json.Number
	if err := json.Unmarshal(data, &num); err != nil {
		var str string
		if err := json.Unmarshal(data, &str); err != nil {
			return 0
		}
		num = json.Number(strings.TrimSpace(str))
	}

	if i, err := num.Int64(); err == nil {
		return int(i)
	}
	// Floats are only accepted when they represent an integer exactly.
	if f, err := num.Float64(); err == nil && f == math.Trunc(f) && math.Abs(f) <= 1<<53 {
		return int(f)
	}
	return 0
}

func attemptErrorLenientString(data json.RawMessage) string {
	if len(data) == 0 {
		return ""
	}

	var str *string
	if err := json.Unmarshal(data, &str); err == nil {
		if str == nil {
			return ""
		}
		return *str
	}

	var compacted bytes.Buffer
	if err := json.Compact(&compacted, data); err != nil {
		return string(data)
	}
	return compacted.String()
}

func attemptErrorLenientTime(data json.RawMessage) time.Time {
	var attemptAt time.Time
	if err := json.Unmarshal(data, &attemptAt); err != nil {
		return time.Time{}
	}
	return attemptAt
}
