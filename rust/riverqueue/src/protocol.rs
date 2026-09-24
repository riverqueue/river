//! Wire-protocol values shared with River Go and other River clients.
//!
//! Applications don't need these to insert or work jobs. They're useful for
//! tools that interoperate with River's tables directly, such as computing
//! the unique key River would assign to a job or listening for River's
//! notifications.

use chrono::{DateTime, Utc};
use serde_json::value::RawValue;

use crate::{Error, JobState, UniqueOpts};

/// Notification topic for queue and job control messages.
pub const NOTIFICATION_TOPIC_CONTROL: &str = "river_control";

/// Notification topic for newly available jobs.
pub const NOTIFICATION_TOPIC_INSERT: &str = "river_insert";

/// Notification topic for leadership changes.
pub const NOTIFICATION_TOPIC_LEADERSHIP: &str = "river_leadership";

/// Inputs used to compute a job's unique key.
#[derive(Clone, Copy, Debug)]
pub struct UniqueKeyInput<'a> {
    /// Encoded arguments exactly as they will be stored, for example from
    /// [`encode_args`](crate::encoding::encode_args).
    pub encoded_args: &'a RawValue,
    /// Job kind.
    pub kind: &'a str,
    /// Current time, used for period-scoped uniqueness when `scheduled_at` is
    /// absent.
    pub now: DateTime<Utc>,
    /// Uniqueness options.
    pub opts: &'a UniqueOpts,
    /// Queue name.
    pub queue: &'a str,
    /// Scheduled time, if the job is scheduled.
    pub scheduled_at: Option<DateTime<Utc>>,
    /// Dotted argument paths selected for argument-scoped uniqueness, such as
    /// [`JobArgs::unique_fields`](crate::JobArgs::unique_fields). When empty,
    /// every top-level argument participates.
    pub unique_fields: &'a [&'a str],
}

/// Computes the SHA-256 unique key River Go stores in `river_job.unique_key`
/// for the same inputs. Returns `None` when `opts` enables no uniqueness
/// dimension.
///
/// # Errors
///
/// Returns an error when the options are invalid, when the arguments are not
/// a JSON object, or when a participating argument key contains JSON path
/// syntax that River Go cannot hash deterministically.
pub fn unique_key(input: &UniqueKeyInput<'_>) -> Result<Option<[u8; 32]>, Error> {
    crate::unique::build_unique_key_parts(
        input.kind,
        input.unique_fields,
        input.encoded_args,
        input.now,
        input.opts,
        input.queue,
        input.scheduled_at,
    )
}

/// Bit representing `state` in `river_job.unique_states`.
#[must_use]
pub const fn unique_state_bit(state: JobState) -> u8 {
    state.unique_bit()
}

/// Value River stores in `river_job.unique_states` for `opts`.
#[must_use]
pub fn unique_states_bitmask(opts: &UniqueOpts) -> u8 {
    opts.state_bitmask()
}
