//! Tracking of running job attempts for cancellation delivery.

#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) fn remove_running_attempt(
    running: &Mutex<HashMap<i64, CancellationToken>>,
    job_id: i64,
    cancellation: &CancellationToken,
) {
    let mut running = running
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if running
        .get(&job_id)
        .is_some_and(|active| active == cancellation)
    {
        running.remove(&job_id);
    }
}

pub(super) fn register_running_attempt(
    running: &Mutex<HashMap<i64, CancellationToken>>,
    pending_cancellations: &Mutex<HashMap<i64, std::time::Instant>>,
    job_id: i64,
    cancellation: &CancellationToken,
) {
    // Keep the locks in this order here and in `signal_running_attempt` so a
    // cancellation cannot fall between checking the active map and recording
    // a just-fetched attempt.
    let mut running = running
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    running.insert(job_id, cancellation.clone());
    let should_cancel = pending_cancellations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&job_id)
        .is_some();
    drop(running);
    if should_cancel {
        cancellation.cancel();
    }
}

pub(super) fn signal_running_attempt(
    running: &Mutex<HashMap<i64, CancellationToken>>,
    pending_cancellations: &Mutex<HashMap<i64, std::time::Instant>>,
    fetch_registration_windows: &AtomicU64,
    job_id: i64,
) {
    let running = running
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(cancellation) = running.get(&job_id).cloned() {
        drop(running);
        cancellation.cancel();
        return;
    }
    if fetch_registration_windows.load(Ordering::SeqCst) == 0 {
        return;
    }

    let now = std::time::Instant::now();
    let mut pending = pending_cancellations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    pending.retain(|_, received_at| {
        now.saturating_duration_since(*received_at) <= PENDING_CANCELLATION_RETENTION
    });
    if pending.len() >= PENDING_CANCELLATION_LIMIT
        && let Some(oldest_job_id) = pending
            .iter()
            .min_by_key(|(_, received_at)| **received_at)
            .map(|(job_id, _)| *job_id)
    {
        pending.remove(&oldest_job_id);
    }
    pending.insert(job_id, now);
}

pub(super) struct FetchRegistrationGuard<'a> {
    pub(super) inner: &'a ClientInner,
}

impl<'a> FetchRegistrationGuard<'a> {
    pub(super) fn new(inner: &'a ClientInner) -> Self {
        inner
            .fetch_registration_windows
            .fetch_add(1, Ordering::SeqCst);
        Self { inner }
    }
}

impl Drop for FetchRegistrationGuard<'_> {
    fn drop(&mut self) {
        // Synchronize the last-window transition with
        // `signal_running_attempt`, which holds this lock while deciding
        // whether to retain an unmatched cancellation.
        let _running = self
            .inner
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self
            .inner
            .fetch_registration_windows
            .fetch_sub(1, Ordering::SeqCst)
            == 1
        {
            self.inner
                .pending_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
    }
}
