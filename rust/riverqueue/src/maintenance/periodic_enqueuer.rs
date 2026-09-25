//! Inserts periodic jobs while this client leads, a port of Go's
//! `PeriodicJobEnqueuer` run loop.

use std::{sync::Arc, time::Duration};

use chrono::Utc;

use super::maintainer::ServiceContext;

/// Sleep used when no periodic job is scheduled (Go's "very long duration").
const IDLE_WAIT: Duration = Duration::from_hours(24);

/// Runs periodic jobs for one leadership term.
///
/// Every term starts from a fresh schedule computed from the time leadership
/// began, like Go's enqueuer `Start`, and inserts `run_on_start` jobs once per
/// gained term.
pub(super) async fn run(context: Arc<ServiceContext>) {
    let periodic_jobs = context.inner.periodic_jobs.clone();
    periodic_jobs.reset_for_leadership();
    let client = context.client();
    loop {
        // Subscribe before scheduling so a job added while this pass runs
        // still wakes the next wait.
        let changed = periodic_jobs.changed();
        tokio::pin!(changed);
        changed.as_mut().enable();

        periodic_jobs
            .run_due(&client, Utc::now(), &context.cancel)
            .await;
        let wait = periodic_jobs.next_run_at().map_or(IDLE_WAIT, |next| {
            (next - Utc::now()).to_std().unwrap_or_default()
        });
        tokio::select! {
            biased;
            () = context.cancel.cancelled() => return,
            () = &mut changed => {}
            () = tokio::time::sleep(wait) => {}
        }
    }
}
