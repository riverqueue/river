package riverdriver

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestUnmarshalAttemptError(t *testing.T) {
	t.Parallel()

	attemptAt := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)

	t.Run("InvalidJSON", func(t *testing.T) {
		t.Parallel()

		var attemptErr rivertype.AttemptError
		require.EqualError(t, UnmarshalAttemptError([]byte(`{"at":`), &attemptErr), "attempt error is not valid JSON")
	})

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		attemptErr := rivertype.AttemptError{
			At:      attemptAt,
			Attempt: 3,
			Error:   "job failed",
			Trace:   "goroutine 1 [running]:",
		}
		data, err := json.Marshal(attemptErr)
		require.NoError(t, err)
		require.JSONEq(t, `{"at":"2024-01-02T03:04:05.123456Z","attempt":3,"error":"job failed","trace":"goroutine 1 [running]:"}`, string(data))

		var decoded rivertype.AttemptError
		require.NoError(t, UnmarshalAttemptError(data, &decoded))
		require.Equal(t, attemptErr, decoded)
	})

	t.Run("StrictShapeUnchanged", func(t *testing.T) {
		t.Parallel()

		// Missing fields and nulls decode the same as with encoding/json's
		// defaults.
		var attemptErr rivertype.AttemptError
		require.NoError(t, UnmarshalAttemptError([]byte(`{"attempt":2,"error":null}`), &attemptErr))
		require.Equal(t, rivertype.AttemptError{Attempt: 2}, attemptErr)

		attemptErr = rivertype.AttemptError{Error: "previous"}
		require.NoError(t, UnmarshalAttemptError([]byte(`null`), &attemptErr))
		require.Equal(t, rivertype.AttemptError{Error: "previous"}, attemptErr)
	})

	// Older or externally edited attempt errors can contain valid JSON with
	// unexpected shapes. Keep every field that can still be interpreted so
	// one historical error cannot make the whole job unreadable.
	t.Run("UnexpectedShapesPreserveUsableFields", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			expected rivertype.AttemptError
			json     string
			name     string
		}{
			{name: "AtInvalid", json: `{"at":"not a time","attempt":2,"error":"err"}`, expected: rivertype.AttemptError{Attempt: 2, Error: "err"}},
			{name: "AtNoOffset", json: `{"at":"2024-01-02T03:04:05.123456","attempt":2}`, expected: rivertype.AttemptError{Attempt: 2}},
			{name: "AtNumber", json: `{"at":1704164645,"attempt":2}`, expected: rivertype.AttemptError{Attempt: 2}},
			{name: "AtPostgresText", json: `{"at":"2024-01-02 03:04:05.123456+00","attempt":2}`, expected: rivertype.AttemptError{Attempt: 2}},
			{name: "AtRFC3339WithOtherInvalidField", json: `{"at":"2024-01-02T03:04:05.123456Z","attempt":"2"}`, expected: rivertype.AttemptError{At: attemptAt, Attempt: 2}},
			{name: "AtSpaceNoOffset", json: `{"at":"2024-01-02 03:04:05.123456","attempt":2}`, expected: rivertype.AttemptError{Attempt: 2}},
			{name: "AttemptFloat", json: `{"attempt":3.0,"error":"err"}`, expected: rivertype.AttemptError{Attempt: 3, Error: "err"}},
			{name: "AttemptFractional", json: `{"attempt":3.5,"error":"err"}`, expected: rivertype.AttemptError{Error: "err"}},
			{name: "AttemptObject", json: `{"attempt":{},"error":"err"}`, expected: rivertype.AttemptError{Error: "err"}},
			{name: "AttemptString", json: `{"attempt":" 3 ","error":"err"}`, expected: rivertype.AttemptError{Attempt: 3, Error: "err"}},
			{name: "AttemptStringInvalid", json: `{"attempt":"three","error":"err"}`, expected: rivertype.AttemptError{Error: "err"}},
			{name: "ElementArray", json: `[1, "two"]`, expected: rivertype.AttemptError{Error: `[1,"two"]`}},
			{name: "ElementNumber", json: `123`, expected: rivertype.AttemptError{Error: "123"}},
			{name: "ElementString", json: `"job failed"`, expected: rivertype.AttemptError{Error: "job failed"}},
			{name: "ErrorObject", json: `{"attempt":1,"error":{"message": "boom", "code": 7}}`, expected: rivertype.AttemptError{Attempt: 1, Error: `{"message":"boom","code":7}`}},
			{name: "TraceArray", json: `{"attempt":1,"error":"err","trace":["frame1", "frame2"]}`, expected: rivertype.AttemptError{Attempt: 1, Error: "err", Trace: `["frame1","frame2"]`}},
			{name: "TraceNullWithInvalidField", json: `{"attempt":"x","error":null,"trace":null}`, expected: rivertype.AttemptError{}},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()

				var attemptErr rivertype.AttemptError
				require.NoError(t, UnmarshalAttemptError([]byte(test.json), &attemptErr))
				require.True(t, test.expected.At.Equal(attemptErr.At), "expected at %s, got %s", test.expected.At, attemptErr.At)
				attemptErr.At = test.expected.At
				require.Equal(t, test.expected, attemptErr)
			})
		}
	})
}

func TestUnmarshalAttemptErrors(t *testing.T) {
	t.Parallel()

	attemptAt := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)

	t.Run("EmptyArray", func(t *testing.T) {
		t.Parallel()

		var attemptErrors []rivertype.AttemptError
		require.NoError(t, UnmarshalAttemptErrors([]byte(`[]`), &attemptErrors))
		require.Equal(t, []rivertype.AttemptError{}, attemptErrors)
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		t.Parallel()

		var attemptErrors []rivertype.AttemptError
		require.Error(t, UnmarshalAttemptErrors([]byte(`[{"at":`), &attemptErrors))
	})

	t.Run("MixedShapes", func(t *testing.T) {
		t.Parallel()

		// One unexpected element doesn't prevent decoding the others.
		var attemptErrs []rivertype.AttemptError
		require.NoError(t, UnmarshalAttemptErrors([]byte(`[{"at":"2024-01-02T03:04:05.123456Z","attempt":1,"error":"err1","trace":""},"err2"]`), &attemptErrs))
		require.Equal(t, []rivertype.AttemptError{
			{At: attemptAt, Attempt: 1, Error: "err1"},
			{Error: "err2"},
		}, attemptErrs)
	})

	t.Run("NonArray", func(t *testing.T) {
		t.Parallel()

		var attemptErrors []rivertype.AttemptError
		require.Error(t, UnmarshalAttemptErrors([]byte(`{"error":"not an array"}`), &attemptErrors))
	})

	t.Run("Null", func(t *testing.T) {
		t.Parallel()

		attemptErrors := []rivertype.AttemptError{{Error: "previous"}}
		require.NoError(t, UnmarshalAttemptErrors([]byte(`null`), &attemptErrors))
		require.Nil(t, attemptErrors)
	})

	t.Run("PartialDecodeDoesNotLeakFields", func(t *testing.T) {
		t.Parallel()

		attemptErrors := []rivertype.AttemptError{{At: attemptAt, Trace: "previous"}, {Trace: "previous"}}
		require.NoError(t, UnmarshalAttemptErrors([]byte(`[{"at":"invalid","attempt":"2","error":"err"},{"error":"next"}]`), &attemptErrors))
		require.Equal(t, []rivertype.AttemptError{
			{Attempt: 2, Error: "err"},
			{Error: "next"},
		}, attemptErrors)
	})

	t.Run("StrictShapeUnchanged", func(t *testing.T) {
		t.Parallel()

		data := []byte(`[{"at":"2024-01-02T03:04:05.123456Z","attempt":1,"error":"err","trace":""}]`)
		var expected, attemptErrors []rivertype.AttemptError
		require.NoError(t, json.Unmarshal(data, &expected))
		require.NoError(t, UnmarshalAttemptErrors(data, &attemptErrors))
		require.Equal(t, expected, attemptErrors)
	})
}
