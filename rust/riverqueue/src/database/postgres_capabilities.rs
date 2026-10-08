//! Features of a Postgres-compatible server that River adapts to, like
//! River Go's `riverdriver.PostgresCapabilities`.
//!
//! YugabyteDB speaks Postgres's protocol but has no `xmax` system column
//! and, unless configured for it, no `LISTEN`/`NOTIFY`. River detects the
//! server once per database and caches the result, so enabling Yugabyte's
//! notifications takes effect only for a new database value, such as after
//! a restart.

use std::sync::{Arc, OnceLock};

use sqlx::{PgExecutor, Row};

/// Reads the server's product, version, and Yugabyte notification setting.
/// Functions are unqualified, as in River Go, so they resolve through the
/// connection's `search_path`.
const DETECT_SQL: &str = "SELECT \
    version()::text AS product, \
    current_setting('server_version_num')::int AS version_num, \
    coalesce(current_setting('yb_enable_listen_notify', true), 'off')::boolean AS yb_listen_notify_enabled";

/// How an insert that may conflict on its unique key tells a new row from
/// an existing one it returned instead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UniqueInsertMode {
    /// The proposed row's metadata carries a random nonce, and a returned
    /// row without it is an existing one. Used where `xmax` is unavailable.
    MetadataNonce,
    /// Postgres 18's `OLD` row in `RETURNING`.
    ReturningOld,
    /// Postgres's `xmax` system column, nonzero for an updated row.
    Xmax,
}

impl UniqueInsertMode {
    /// Returns the SQL expression that is true for a returned existing row.
    /// It's always false for [`MetadataNonce`](Self::MetadataNonce), which
    /// compares nonces after the insert instead.
    pub(crate) const fn sql(self) -> &'static str {
        match self {
            Self::MetadataNonce => "false",
            Self::ReturningOld => "(OLD.id IS NOT NULL)",
            Self::Xmax => "(xmax != 0)",
        }
    }
}

/// Features detected from a Postgres-compatible server.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PostgresCapabilities {
    /// Whether `pg_notify` delivers notifications to listeners. Without it,
    /// River skips notifications and clients poll instead.
    pub(crate) supports_listen_notify: bool,
    pub(crate) unique_insert_mode: UniqueInsertMode,
}

impl PostgresCapabilities {
    /// Derives capabilities from the server's `version()` text, its
    /// `server_version_num`, and Yugabyte's `yb_enable_listen_notify`
    /// setting, which is off when absent.
    pub(crate) fn new(product: &str, version: i32, yb_listen_notify_enabled: bool) -> Self {
        let yugabyte = is_yugabyte(product);
        Self {
            // Yugabyte's notifications need 2025.2.3 or later with
            // `ysql_yb_enable_listen_notify=true` on masters and tservers.
            supports_listen_notify: !yugabyte || yb_listen_notify_enabled,
            unique_insert_mode: if yugabyte {
                UniqueInsertMode::MetadataNonce
            } else if version >= 180_000 {
                UniqueInsertMode::ReturningOld
            } else {
                UniqueInsertMode::Xmax
            },
        }
    }

    /// Detects the capabilities of the server `executor` is connected to.
    pub(crate) async fn detect<'e>(executor: impl PgExecutor<'e>) -> Result<Self, sqlx::Error> {
        let row = sqlx::query(DETECT_SQL).fetch_one(executor).await?;
        Ok(Self::new(
            row.try_get("product")?,
            row.try_get("version_num")?,
            row.try_get("yb_listen_notify_enabled")?,
        ))
    }
}

fn is_yugabyte(product: &str) -> bool {
    let product = product.to_lowercase();
    product.contains("-yb") || product.contains("yugabyte")
}

/// Capabilities detected for one database, shared by its clones.
#[derive(Clone, Debug, Default)]
pub(crate) struct CapabilitiesCache(Arc<OnceLock<PostgresCapabilities>>);

impl CapabilitiesCache {
    /// Returns the cached capabilities, detecting them with `executor` the
    /// first time. Concurrent first callers may each detect; the first result
    /// stored wins. No lock is held while detecting, since the caller may hold
    /// the pool's only connection.
    pub(crate) async fn load<'e>(
        &self,
        executor: impl PgExecutor<'e>,
    ) -> Result<PostgresCapabilities, sqlx::Error> {
        if let Some(capabilities) = self.0.get() {
            return Ok(*capabilities);
        }
        let detected = PostgresCapabilities::detect(executor).await?;
        Ok(*self.0.get_or_init(|| detected))
    }

    /// Returns the capabilities from `cache`, or detects them with
    /// `executor` each time without one.
    pub(crate) async fn load_or_detect<'e>(
        cache: Option<&Self>,
        executor: impl PgExecutor<'e>,
    ) -> Result<PostgresCapabilities, sqlx::Error> {
        match cache {
            Some(cache) => cache.load(executor).await,
            None => PostgresCapabilities::detect(executor).await,
        }
    }

    /// Whether notifications are delivered, assuming they are until the
    /// server has been detected, like River Go before `InitDriver`.
    pub(crate) fn supports_listen_notify(&self) -> bool {
        self.0
            .get()
            .is_none_or(|capabilities| capabilities.supports_listen_notify)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_yugabyte_and_postgres_versions() {
        let postgres_17 = PostgresCapabilities::new(
            "PostgreSQL 17.4 on aarch64-apple-darwin, compiled by clang",
            170_004,
            false,
        );
        assert!(postgres_17.supports_listen_notify);
        assert_eq!(postgres_17.unique_insert_mode, UniqueInsertMode::Xmax);

        let postgres_18 = PostgresCapabilities::new("PostgreSQL 18.1", 180_001, false);
        assert!(postgres_18.supports_listen_notify);
        assert_eq!(
            postgres_18.unique_insert_mode,
            UniqueInsertMode::ReturningOld
        );

        for product in [
            "PostgreSQL 15.12-YB-2025.2.1.0-b1 on x86_64",
            "YugabyteDB 2025.2.3.0",
        ] {
            let without = PostgresCapabilities::new(product, 150_012, false);
            assert!(!without.supports_listen_notify, "{product}");
            assert_eq!(
                without.unique_insert_mode,
                UniqueInsertMode::MetadataNonce,
                "{product}"
            );
            assert!(
                PostgresCapabilities::new(product, 150_012, true).supports_listen_notify,
                "{product}"
            );
        }
    }

    #[test]
    fn unknown_capabilities_assume_notifications() {
        let cache = CapabilitiesCache::default();
        assert!(cache.supports_listen_notify());
        cache
            .0
            .set(PostgresCapabilities::new(
                "PostgreSQL 15.12-YB-2025.2.1.0",
                150_012,
                false,
            ))
            .unwrap();
        assert!(!cache.supports_listen_notify());
        assert!(cache.clone().0.get().is_some());
    }
}
