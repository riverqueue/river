//! Bounded local client event subscriptions.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use thiserror::Error;
use tokio::sync::mpsc;

use crate::{Error, JobRow, Queue};

/// A client event kind. Callers must opt in to each kind explicitly.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum EventKind {
    /// A job reached the cancelled state.
    JobCancelled,
    /// A job completed successfully.
    JobCompleted,
    /// A job failed, whether retryable or terminal.
    JobFailed,
    /// A running job was interrupted during shutdown.
    JobInterrupted,
    /// A job was snoozed.
    JobSnoozed,
    /// A queue was paused.
    QueuePaused,
    /// A queue was resumed.
    QueueResumed,
}

/// An event emitted by this client instance.
#[derive(Clone, Debug)]
pub struct Event {
    /// Event discriminator.
    pub kind: EventKind,
    /// Job snapshot for job events.
    pub job: Option<JobRow>,
    /// Timing information for the corresponding job execution.
    pub job_statistics: Option<JobStatistics>,
    /// Queue snapshot for queue events.
    pub queue: Option<Queue>,
}

impl Event {
    pub(crate) fn job(kind: EventKind, job: JobRow) -> Self {
        Self {
            job: Some(job),
            job_statistics: None,
            kind,
            queue: None,
        }
    }

    pub(crate) fn queue(kind: EventKind, queue: Queue) -> Self {
        Self {
            job: None,
            job_statistics: None,
            kind,
            queue: Some(queue),
        }
    }
}

/// Timing information for one execution of a job.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JobStatistics {
    /// Time spent persisting the worker result.
    pub complete_duration: Duration,
    /// Time between the job becoming eligible and beginning work.
    pub queue_wait_duration: Duration,
    /// Time spent running the worker and its work extensions.
    pub run_duration: Duration,
}

/// Configuration for one event subscription.
#[derive(Clone, Debug)]
pub struct SubscribeConfig {
    /// Capacity of this subscription's bounded buffer.
    pub buffer_capacity: usize,
    /// Event kinds to receive.
    pub kinds: Vec<EventKind>,
}

impl Default for SubscribeConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 1_000,
            kinds: Vec::new(),
        }
    }
}

/// Error returned while receiving client events.
#[derive(Debug, Error)]
pub enum EventRecvError {
    /// The client dropped the contained number of events because the receiver
    /// lagged its bounded buffer. The next call resumes at the oldest retained
    /// event.
    #[error("event receiver lagged by {0} events")]
    Lagged(u64),
    /// The client event channel closed.
    #[error("event channel closed")]
    Closed,
}

/// A filtered receiver for locally generated client events.
///
/// Job events are emitted only after their state transition commits. Concurrent
/// jobs and completion batches have no global event-ordering guarantee; use the
/// job ID and persisted timestamps when an application needs stable ordering.
pub struct EventReceiver {
    dropped: Arc<AtomicU64>,
    receiver: mpsc::Receiver<Event>,
}

impl EventReceiver {
    pub(crate) fn new(dropped: Arc<AtomicU64>, receiver: mpsc::Receiver<Event>) -> Self {
        Self { dropped, receiver }
    }

    /// Receives the next requested event.
    pub async fn recv(&mut self) -> Result<Event, EventRecvError> {
        let dropped = self.dropped.swap(0, Ordering::AcqRel);
        if dropped > 0 {
            return Err(EventRecvError::Lagged(dropped));
        }
        self.receiver.recv().await.ok_or(EventRecvError::Closed)
    }
}

pub(crate) fn validate_kinds(kinds: &[EventKind]) -> Result<HashSet<EventKind>, Error> {
    if kinds.is_empty() {
        return Err(Error::InvalidJob(
            "event subscription requires at least one event kind".to_owned(),
        ));
    }
    Ok(kinds.iter().copied().collect())
}
