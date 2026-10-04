//go:build riverconformance

package harness_test

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// simulatedYugabyteSchema holds River's tables and the functions that make
// PostgreSQL look like YugabyteDB to connections that search it first.
const simulatedYugabyteSchema = "river_conformance_yugabyte"

// verifySimulatedYugabyte runs both implementations on PostgreSQL made to
// look like YugabyteDB without LISTEN/NOTIFY, the way River Go's own tests
// simulate it. A schema ahead of pg_catalog on the adapters' search_path
// shadows version() and current_setting(text, boolean) with a Yugabyte
// version whose yb_enable_listen_notify setting is absent, and shadows
// pg_notify with a function that raises, so any notification fails the
// operation that sends it.
//
// Each implementation must detect the server by itself: write unique jobs
// with a nonce rather than rely on xmax, which Yugabyte lacks, so the other
// implementation's duplicate insert returns the same job; send no
// notification when it inserts or cancels; and, without being configured
// as poll-only, notice the other implementation's cancellation of its
// running job by polling. The simulation doesn't emulate Yugabyte's storage
// or transaction semantics.
func verifySimulatedYugabyte(t *testing.T, observer *postgresObserver, root, databaseURL string, candidateSpec adapterSpec) {
	t.Helper()

	ctx := context.Background()
	schema := pgx.Identifier{simulatedYugabyteSchema}.Sanitize()
	_, err := observer.pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE;
CREATE SCHEMA `+schema+`;
CREATE FUNCTION `+schema+`.version() RETURNS text LANGUAGE sql AS $$
    SELECT 'PostgreSQL 15.12-YB-2025.2.1.0-b1'::text
$$;
CREATE FUNCTION `+schema+`.current_setting(setting_name text, missing_ok boolean) RETURNS text LANGUAGE sql AS $$
    SELECT CASE WHEN setting_name = 'yb_enable_listen_notify' THEN NULL::text
    ELSE pg_catalog.current_setting(setting_name, missing_ok) END
$$;
CREATE FUNCTION `+schema+`.pg_notify(text, text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'LISTEN/NOTIFY is unavailable'; END
$$;`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := observer.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		require.NoError(t, err)
	})

	// Spaces are escaped as %20 rather than +, which not every driver's URL
	// parser decodes as a space.
	parsed, err := url.Parse(databaseURL)
	require.NoError(t, err)
	options := "options=" + strings.ReplaceAll(url.QueryEscape("-c search_path="+simulatedYugabyteSchema+",pg_catalog"), "+", "%20")
	if parsed.RawQuery != "" {
		options = parsed.RawQuery + "&" + options
	}
	parsed.RawQuery = options
	yugabyteURL := parsed.String()

	goAdapter := startReferenceAdapter(t, root, yugabyteURL, "go-yugabyte")
	candidateAdapter := startCandidateAdapter(t, root, yugabyteURL, candidateSpec.Implementation+"-yugabyte", candidateSpec, candidateSpec.Command)
	// Without a schema, River uses the connection's current schema, the
	// simulated one.
	goAdapter.call(t, "migrate", map[string]any{}, nil)

	for _, pair := range []struct {
		controller *adapter
		worker     *adapter
	}{
		{controller: goAdapter, worker: candidateAdapter},
		{controller: candidateAdapter, worker: goAdapter},
	} {
		pair.worker.call(t, "reset", map[string]any{}, nil)

		unique := map[string]any{
			"message": "simulated yugabyte unique " + pair.controller.name,
			"opts":    map[string]any{"unique": map[string]any{"by_args": true}},
		}
		var inserted, duplicate normalizedJob
		pair.controller.call(t, "insert", unique, &inserted)
		// Adapters leave the nonce out of the jobs they report.
		var hasNonce bool
		require.NoError(t, observer.pool.QueryRow(ctx,
			`SELECT metadata ? 'river:unique_nonce' FROM `+schema+`.river_job WHERE id = $1`, inserted.ID,
		).Scan(&hasNonce))
		require.True(t, hasNonce, "%s inserted a unique job without a nonce", pair.controller.name)
		pair.worker.call(t, "insert", unique, &duplicate)
		require.Equal(t, inserted.ID, duplicate.ID, "%s inserted a duplicate of %s's unique job", pair.worker.name, pair.controller.name)

		pair.worker.call(t, "start", map[string]any{
			"client_id": pair.worker.name + "-yugabyte", "fetch_poll_interval_ms": 100, "max_workers": 1,
		}, nil)
		var cancellable normalizedJob
		pair.controller.call(t, "insert", map[string]any{
			"behavior": "cooperative_cancel", "message": "simulated yugabyte cancel",
		}, &cancellable)
		pair.worker.call(t, "wait", map[string]any{
			"id": cancellable.ID, "states": []string{"running"},
		}, &cancellable)
		startedAt := time.Now()
		pair.controller.call(t, "cancel", map[string]any{"id": cancellable.ID}, nil)
		pair.worker.call(t, "wait", map[string]any{"id": cancellable.ID}, &cancellable)
		require.Equal(t, "cancelled", cancellable.State)
		require.Less(t, time.Since(startedAt), 6*time.Second)
		pair.worker.call(t, "stop", map[string]any{}, nil)
	}
}
