package riversharedtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// DBPoolWithYugabyteVersion returns a Postgres pool that reports a Yugabyte
// version and LISTEN/NOTIFY setting. A nil setting simulates versions where it
// doesn't exist. This exercises detection on ordinary Postgres; it does not
// emulate Yugabyte's storage or transaction semantics.
//
// The schema must be isolated to this test. When notifications are disabled,
// pg_notify raises an exception to catch accidental attempts to broadcast.
func DBPoolWithYugabyteVersion(ctx context.Context, t *testing.T, schema string, listenNotifyEnabled *bool) *pgxpool.Pool {
	t.Helper()

	pool := DBPool(ctx, t)
	setting := "NULL::text"
	version := "2025.2.1.0"
	if listenNotifyEnabled != nil {
		version = "2025.2.3.0"
		setting = "'off'::text"
		if *listenNotifyEnabled {
			setting = "'on'::text"
		}
	}
	safeSchema := pgx.Identifier{schema}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
CREATE FUNCTION %s.version() RETURNS text LANGUAGE sql AS $$
    SELECT 'PostgreSQL 15.12-YB-%s-b1'::text
$$;
CREATE FUNCTION %s.current_setting(setting_name text, missing_ok boolean) RETURNS text LANGUAGE sql AS $$
    SELECT CASE WHEN setting_name = 'yb_enable_listen_notify' THEN %s
    ELSE pg_catalog.current_setting(setting_name, missing_ok) END
$$;`, safeSchema, version, safeSchema, setting))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := pool.Exec(ctx, "DROP FUNCTION "+safeSchema+".version(), "+safeSchema+".current_setting(text, boolean)")
		require.NoError(t, err)
	})

	if listenNotifyEnabled == nil || !*listenNotifyEnabled {
		_, err := pool.Exec(ctx, "CREATE FUNCTION "+safeSchema+`.pg_notify(text, text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'LISTEN/NOTIFY is unavailable'; END
$$;`)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := pool.Exec(ctx, "DROP FUNCTION "+safeSchema+".pg_notify(text, text)")
			require.NoError(t, err)
		})
	}

	config := pool.Config().Copy()
	config.AfterConnect = nil // DBPool normally clears the search path.
	config.MaxConns = 2
	config.ConnConfig.RuntimeParams["search_path"] = safeSchema + ", pg_catalog"
	yugabytePool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(yugabytePool.Close)
	return yugabytePool
}
