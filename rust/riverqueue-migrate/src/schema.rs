//! PostgreSQL schema names.

use std::fmt;

use thiserror::Error;

/// PostgreSQL's maximum identifier length.
const POSTGRES_IDENTIFIER_MAX: usize = 63;

/// Longest River notification topic, which a schema name must leave room for.
const NOTIFICATION_TOPIC_LONGEST: &str = "river_leadership";

/// Maximum schema length after reserving `<schema>.river_leadership` for
/// notification channels.
pub const SCHEMA_MAX_LEN: usize = POSTGRES_IDENTIFIER_MAX - NOTIFICATION_TOPIC_LONGEST.len() - 1;

/// The PostgreSQL schema River's tables live in.
///
/// [`SchemaName::current`] uses the connection's current schema (normally
/// `public`, following `search_path`). An explicit schema is quoted wherever
/// River renders it, exactly like River Go's `SafeIdentifier`, so names such
/// as `river-prod` or `MyRiver` work as written. Pass the same schema to the
/// migrator and the client.
///
/// ```
/// use riverqueue_migrate::SchemaName;
///
/// let schema = SchemaName::new("river")?;
/// assert_eq!(schema.qualify("river_job"), r#""river"."river_job""#);
/// # Ok::<(), riverqueue_migrate::SchemaNameError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaName(Option<String>);

impl SchemaName {
    /// Uses PostgreSQL's current schema.
    #[must_use]
    pub const fn current() -> Self {
        Self(None)
    }

    /// Validates an optional explicit schema.
    ///
    /// Like Go's `SafeIdentifier` quoting, any name is accepted and quoted
    /// when rendered, including mixed case and punctuation such as
    /// `river-prod`. Names containing NUL are rejected, as are names too long
    /// to prefix River's notification topics within PostgreSQL's identifier
    /// limit.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema is too long or contains NUL.
    pub fn new(schema: impl Into<String>) -> Result<Self, SchemaNameError> {
        let schema = schema.into();
        if schema.is_empty() {
            return Ok(Self::current());
        }
        if schema.len() > SCHEMA_MAX_LEN {
            return Err(SchemaNameError::TooLong {
                length: schema.len(),
                maximum: SCHEMA_MAX_LEN,
            });
        }
        if schema.contains('\0') {
            return Err(SchemaNameError::Invalid(schema));
        }

        Ok(Self(Some(schema)))
    }

    /// Returns the unquoted explicit schema, if configured.
    #[must_use]
    pub fn as_deref(&self) -> Option<&str> {
        self.0.as_deref()
    }

    /// Qualifies and quotes a database object name in this schema, for use in
    /// SQL that refers to River's tables.
    #[must_use]
    pub fn qualify(&self, object: &str) -> String {
        match &self.0 {
            Some(schema) => format!("{}.{}", quote_identifier(schema), quote_identifier(object)),
            None => quote_identifier(object),
        }
    }

    /// Prefix used by River's canonical migration templates.
    #[doc(hidden)]
    #[must_use]
    pub fn migration_prefix(&self) -> String {
        self.0.as_ref().map_or_else(String::new, |schema| {
            format!("{}.", quote_identifier(schema))
        })
    }

    /// Fully qualified PostgreSQL notification channel.
    #[doc(hidden)]
    #[must_use]
    pub fn notification_topic(&self, topic: &str) -> String {
        match &self.0 {
            Some(schema) => format!("{schema}.{topic}"),
            None => format!("public.{topic}"),
        }
    }
}

/// Quotes a PostgreSQL identifier, doubling embedded quotes like Go's
/// `dbutil.SafeIdentifier`.
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

impl Default for SchemaName {
    fn default() -> Self {
        Self::current()
    }
}

impl fmt::Display for SchemaName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_deref().unwrap_or("<current>"))
    }
}

/// Invalid River schema name.
#[derive(Debug, Error)]
pub enum SchemaNameError {
    /// Schema contains a NUL character, which PostgreSQL identifiers cannot.
    #[error("schema name cannot contain NUL: {0:?}")]
    Invalid(String),

    /// Schema is too long to prefix River's notification topics.
    #[error("schema length {length} exceeds maximum {maximum}")]
    TooLong {
        /// Observed byte length.
        length: usize,
        /// Maximum byte length.
        maximum: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_name_validates_and_qualifies() {
        let schema = SchemaName::new("river_test").unwrap();
        assert_eq!(schema.qualify("river_job"), "\"river_test\".\"river_job\"");
        assert_eq!(
            schema.notification_topic("river_insert"),
            "river_test.river_insert"
        );

        // Go quotes any schema with `SafeIdentifier`, so Rust accepts the
        // same names and escapes embedded quotes.
        let hyphenated = SchemaName::new("river-prod").unwrap();
        assert_eq!(
            hyphenated.qualify("river_job"),
            "\"river-prod\".\"river_job\""
        );
        assert_eq!(
            hyphenated.notification_topic("river_insert"),
            "river-prod.river_insert"
        );
        assert_eq!(
            SchemaName::new("MyRiver").unwrap().migration_prefix(),
            "\"MyRiver\"."
        );
        assert_eq!(
            SchemaName::new("odd\"name").unwrap().qualify("river_job"),
            "\"odd\"\"name\".\"river_job\""
        );
        assert!(SchemaName::new("1leading_digit").is_ok());
        assert!(SchemaName::new("nul\0byte").is_err());
        assert!(SchemaName::new("a".repeat(SCHEMA_MAX_LEN + 1)).is_err());
    }
}
