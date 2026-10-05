//! Paces a client's insert notifications, a port of Go's
//! `notifylimiter.Limiter`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Allows at most one insert notification per queue within each cooldown.
///
/// Producers fetch at most once per fetch cooldown, so a burst of insertions
/// into one queue needs only its first notification. Like Go, a queue's
/// window starts when a notification is allowed, whether or not the
/// transaction carrying it commits.
#[derive(Debug)]
pub(crate) struct InsertNotifyLimiter {
    cooldown: Duration,
    last_sent: Mutex<HashMap<String, Instant>>,
}

impl InsertNotifyLimiter {
    pub(crate) fn new(cooldown: Duration) -> Self {
        Self {
            cooldown,
            last_sent: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the queues among `queues` that are due a notification,
    /// recording each as notified now.
    pub(crate) fn due<'q>(&self, queues: impl IntoIterator<Item = &'q str>) -> Vec<&'q str> {
        let now = Instant::now();
        queues
            .into_iter()
            .filter(|queue| self.should_trigger_at(queue, now))
            .collect()
    }

    /// Returns whether `queue` is due a notification at `now`, recording it
    /// as notified then if so. A queue is due once more than the cooldown
    /// has passed since its last notification.
    fn should_trigger_at(&self, queue: &str, now: Instant) -> bool {
        let mut last_sent = self
            .last_sent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if last_sent
            .get(queue)
            .is_some_and(|last| now.saturating_duration_since(*last) <= self.cooldown)
        {
            return false;
        }
        last_sent.insert(queue.to_owned(), now);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::InsertNotifyLimiter;

    #[test]
    fn allows_one_notification_per_queue_per_cooldown() {
        let limiter = InsertNotifyLimiter::new(Duration::from_millis(100));
        let start = Instant::now();

        assert!(limiter.should_trigger_at("a", start));
        for _ in 0..10 {
            assert!(!limiter.should_trigger_at("a", start));
        }
        assert!(!limiter.should_trigger_at("a", start + Duration::from_millis(100)));
        assert!(limiter.should_trigger_at("a", start + Duration::from_millis(101)));
        assert!(!limiter.should_trigger_at("a", start + Duration::from_millis(150)));
    }

    #[test]
    fn due_filters_and_records_queues() {
        let limiter = InsertNotifyLimiter::new(Duration::from_hours(1));

        assert_eq!(limiter.due(["a", "b"]), ["a", "b"]);
        assert_eq!(limiter.due(["a", "c"]), ["c"]);
        assert_eq!(limiter.due(["a", "b", "c"]), Vec::<&str>::new());
    }

    #[test]
    fn tracks_queues_independently() {
        let limiter = InsertNotifyLimiter::new(Duration::from_millis(100));
        let start = Instant::now();

        assert!(limiter.should_trigger_at("a", start));
        assert!(limiter.should_trigger_at("b", start));
        assert!(!limiter.should_trigger_at("a", start + Duration::from_millis(50)));
        assert!(limiter.should_trigger_at("c", start + Duration::from_millis(50)));
    }
}
