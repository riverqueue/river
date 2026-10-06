package harness

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestProtocol(t *testing.T) {
	t.Parallel()

	// The barrier the harness holds jobs on keeps a job running with its
	// attempt until released, then lets it complete in that attempt.
	t.Run("Barrier", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			for _, adapter := range []*Adapter{env.Reference, env.Candidate} {
				adapter.Start(t, protocol.StartParams{ClientID: "barrier", MaxWorkers: 2})
				inserted := adapter.InsertJob(t, echo("barrier "+adapter.Label, protocol.BehaviorBarrierWait))
				running := env.DB.WaitJob(t, inserted.ID, workWait, "running")
				require.Equal(t, 1, running.Attempt)
				adapter.Release(t, "barrier "+adapter.Label)
				completed := env.DB.WaitJob(t, inserted.ID, workWait)
				require.Equal(t, "completed", completed.State)
				require.Equal(t, running.AttemptedAt, completed.AttemptedAt)
				adapter.Stop(t, protocol.StopParams{})
			}
		})
	})

	t.Run("Handshake", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{NoMigrate: true}, func(t *testing.T, env *Env) {
			for _, adapter := range []*Adapter{env.Reference, env.Candidate} {
				handshake := adapter.Handshake(t)
				require.Equal(t, adapter.Implementation.Name, handshake.Implementation, adapter.Label)
				require.Equal(t, env.Driver, handshake.Driver, adapter.Label)
				require.NotEmpty(t, handshake.Version, adapter.Label)
			}
		})
	})

	// Adapters reject what they don't understand rather than ignoring it, so
	// a scenario can't pass by an adapter silently dropping a parameter.
	t.Run("StrictRequests", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{NoMigrate: true}, func(t *testing.T, env *Env) {
			for _, adapter := range []*Adapter{env.Reference, env.Candidate} {
				RequireErrorCode(t, adapter.Call("not_a_method", struct{}{}, nil), protocol.CodeMethodNotFound)
				RequireErrorCode(t, adapter.Call(protocol.MethodHandshake, map[string]any{"unexpected": true}, nil), protocol.CodeInvalidParams)
				RequireErrorCode(t, adapter.Call(protocol.MethodInsert, map[string]any{
					"jobs": []any{map[string]any{"message": "unknown option", "opts": map[string]any{"not_an_option": true}}},
				}, nil), protocol.CodeInvalidParams)
				RequireErrorCode(t, adapter.Call(protocol.MethodStart, map[string]any{
					"client_id": "unknown", "not_an_option": json.RawMessage("1"),
				}, nil), protocol.CodeInvalidParams)
			}
		})
	})
}
