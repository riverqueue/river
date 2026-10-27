//! Tracking of running job attempts for cancellation delivery.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use tokio_util::sync::CancellationToken;

use crate::client::ClientInner;

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
    inner: &ClientInner,
    job_id: i64,
    cancellation: &CancellationToken,
    generation: u64,
) {
    inner
        .running
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(job_id, cancellation.clone());
    if inner.cancellation_generation.load(Ordering::Acquire) != generation {
        inner.cancellation_wakeup.notify_one();
    }
}
