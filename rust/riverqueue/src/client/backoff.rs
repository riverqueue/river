//! Backoff shared by runtime services that retry database operations.

use std::{
    hash::{BuildHasher, Hasher},
    time::Duration,
};

/// Attempts after which the exponential sequence starts over, matching River
/// Go's `serviceutil.MaxAttemptsBeforeResetDefault`.
const MAX_ATTEMPTS_BEFORE_RESET: u32 = 7;

/// Returns River's service backoff for a one-based attempt: `2^(attempt - 1)`
/// seconds with ±10% jitter, restarting the sequence every seven attempts so a
/// long outage never sleeps for more than about a minute.
///
/// This mirrors River Go's `serviceutil.ExponentialBackoff`, which the
/// notifier and completer use. It is intentionally distinct from the job retry
/// policy: services should recover promptly once the database returns.
pub(super) fn exponential_backoff(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1) % MAX_ATTEMPTS_BEFORE_RESET;
    let seconds = f64::from(1_u32 << exponent);
    Duration::from_secs_f64(seconds + seconds * (jitter_unit() * 0.2 - 0.1))
}

/// Returns a uniformly distributed value in `[0, 1)` for jitter.
///
/// Jitter only needs to decorrelate clients, so the standard library's
/// randomly keyed hasher avoids a dedicated random number dependency.
fn jitter_unit() -> f64 {
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    #[allow(
        clippy::cast_precision_loss,
        reason = "53 random bits are plenty for jitter"
    )]
    let unit = (hasher.finish() >> 11) as f64 / (1_u64 << 53) as f64;
    unit
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_doubles_with_jitter_and_resets() {
        for (attempt, base_seconds) in [(0, 1.0), (1, 1.0), (2, 2.0), (3, 4.0), (7, 64.0), (8, 1.0)]
        {
            let backoff = exponential_backoff(attempt).as_secs_f64();
            assert!(
                (base_seconds * 0.9..=base_seconds * 1.1).contains(&backoff),
                "attempt {attempt} slept {backoff}s, expected about {base_seconds}s"
            );
        }
    }
}
