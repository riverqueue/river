//! Backoff shared by runtime services that retry database operations.

use std::{
    collections::HashMap,
    hash::{BuildHasher, Hash, Hasher},
    time::Duration,
};

/// Attempts after which the exponential sequence starts over, matching River
/// Go's `serviceutil.MaxAttemptsBeforeResetDefault`.
const MAX_ATTEMPTS_BEFORE_RESET: u32 = 7;

/// How long a restarted service or producer must run before a failure counts
/// as a new outage whose backoff starts over, rather than as another failure
/// in a row. It exceeds the longest restart backoff (about 70 seconds).
pub(crate) const SERVICE_RESTART_RESET_AFTER: Duration = Duration::from_mins(2);

/// Consecutive failures of each restartable service or queue producer.
#[derive(Debug)]
pub(super) struct RestartBackoff<K> {
    attempts: HashMap<K, u32>,
}

impl<K> Default for RestartBackoff<K> {
    fn default() -> Self {
        Self {
            attempts: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash> RestartBackoff<K> {
    /// Returns the one-based restart attempt recorded for `key`, if it has
    /// failed since it was last forgotten.
    pub(super) fn attempt(&self, key: &K) -> Option<u32> {
        self.attempts.get(key).copied()
    }

    /// Records a failure of `key` after it ran for `ran_for`, returning the
    /// one-based restart attempt and the backoff before it. Like River Go's
    /// services, which reset their error counts once they succeed, one that
    /// ran for a while before failing starts its backoff over.
    pub(super) fn failed(&mut self, key: K, ran_for: Duration) -> (u32, Duration) {
        let attempt = self.attempts.entry(key).or_default();
        if ran_for >= SERVICE_RESTART_RESET_AFTER {
            *attempt = 0;
        }
        *attempt += 1;
        (*attempt, exponential_backoff(*attempt))
    }

    /// Forgets the failures of `key`.
    pub(super) fn forget(&mut self, key: &K) {
        self.attempts.remove(key);
    }
}

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

    #[test]
    fn restart_backoff_starts_over_after_a_long_healthy_run() {
        let mut restarts = RestartBackoff::default();
        let quick = Duration::from_secs(1);
        assert_eq!(restarts.failed("notifier", quick).0, 1);
        assert_eq!(restarts.failed("notifier", quick).0, 2);
        assert_eq!(restarts.failed("maintenance", quick).0, 1);
        assert_eq!(restarts.failed("notifier", quick).0, 3);
        assert_eq!(restarts.attempt(&"notifier"), Some(3));
        // A failure after a healthy run is the start of a new outage.
        let (attempt, delay) = restarts.failed("notifier", SERVICE_RESTART_RESET_AFTER);
        assert_eq!(attempt, 1);
        assert!(delay <= Duration::from_millis(1_100), "{delay:?}");
        assert_eq!(restarts.failed("maintenance", quick).0, 2);
        restarts.forget(&"maintenance");
        assert_eq!(restarts.attempt(&"maintenance"), None);
    }
}
