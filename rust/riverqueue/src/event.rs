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
///
/// The enum separates job and queue payloads so an event can never contain an
/// invalid combination such as a queue event with job statistics.
#[derive(Clone, Debug)]
#[non_exhaustive]
#[allow(
    clippy::large_enum_variant,
    reason = "job events dominate and boxing every event would add an allocation"
)]
pub enum Event {
    /// A job lifecycle event.
    Job(JobEvent),
    /// A queue lifecycle event.
    Queue(QueueEvent),
}

impl Event {
    pub(crate) fn job(kind: JobEventKind, job: JobRow) -> Self {
        Self::Job(JobEvent {
            job,
            kind,
            statistics: None,
        })
    }

    pub(crate) fn queue(kind: QueueEventKind, queue: Queue) -> Self {
        Self::Queue(QueueEvent { kind, queue })
    }

    /// Returns this event's subscription discriminator.
    #[must_use]
    pub const fn kind(&self) -> EventKind {
        match self {
            Self::Job(event) => event.kind.as_event_kind(),
            Self::Queue(event) => event.kind.as_event_kind(),
        }
    }

    /// Returns the job event payload, if this is a job event.
    #[must_use]
    pub const fn as_job(&self) -> Option<&JobEvent> {
        match self {
            Self::Job(event) => Some(event),
            Self::Queue(_) => None,
        }
    }

    /// Returns the queue event payload, if this is a queue event.
    #[must_use]
    pub const fn as_queue(&self) -> Option<&QueueEvent> {
        match self {
            Self::Job(_) => None,
            Self::Queue(event) => Some(event),
        }
    }

    pub(crate) fn job_with_statistics(
        kind: JobEventKind,
        job: JobRow,
        statistics: JobStatistics,
    ) -> Self {
        Self::Job(JobEvent {
            job,
            kind,
            statistics: Some(statistics),
        })
    }
}

/// A job lifecycle event and its valid payload.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct JobEvent {
    /// Job snapshot after its state transition committed.
    pub job: JobRow,
    /// Job event discriminator derived from the persisted job state.
    ///
    /// An `available` row keeps the worker's requested retry, snooze, or
    /// interruption reason because that state alone is ambiguous. Terminal,
    /// retryable, and scheduled rows always determine the emitted kind.
    pub kind: JobEventKind,
    /// Timing information for the corresponding execution, when applicable.
    pub statistics: Option<JobStatistics>,
}

/// A job event kind.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum JobEventKind {
    /// A job reached the cancelled state.
    Cancelled,
    /// A job completed successfully.
    Completed,
    /// A job failed, whether retryable or terminal.
    Failed,
    /// A running job was interrupted during shutdown.
    Interrupted,
    /// A job was snoozed.
    Snoozed,
}

impl JobEventKind {
    const fn as_event_kind(self) -> EventKind {
        match self {
            Self::Cancelled => EventKind::JobCancelled,
            Self::Completed => EventKind::JobCompleted,
            Self::Failed => EventKind::JobFailed,
            Self::Interrupted => EventKind::JobInterrupted,
            Self::Snoozed => EventKind::JobSnoozed,
        }
    }
}

impl TryFrom<EventKind> for JobEventKind {
    type Error = EventKindMismatch;

    fn try_from(kind: EventKind) -> Result<Self, Self::Error> {
        match kind {
            EventKind::JobCancelled => Ok(Self::Cancelled),
            EventKind::JobCompleted => Ok(Self::Completed),
            EventKind::JobFailed => Ok(Self::Failed),
            EventKind::JobInterrupted => Ok(Self::Interrupted),
            EventKind::JobSnoozed => Ok(Self::Snoozed),
            EventKind::QueuePaused | EventKind::QueueResumed => Err(EventKindMismatch {
                actual: kind,
                expected: "job",
            }),
        }
    }
}

impl From<JobEventKind> for EventKind {
    fn from(kind: JobEventKind) -> Self {
        kind.as_event_kind()
    }
}

/// A queue lifecycle event and its valid payload.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct QueueEvent {
    /// Queue event discriminator.
    pub kind: QueueEventKind,
    /// Queue snapshot after its observed state transition committed.
    ///
    /// Queue events are best-effort wakeups rather than a durable transition
    /// log. Rapid pause/resume transitions may coalesce before a client reads
    /// the persisted queue state; use storage operations when authoritative
    /// current state is required.
    pub queue: Queue,
}

/// A queue event kind.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum QueueEventKind {
    /// A queue was paused.
    Paused,
    /// A queue was resumed.
    Resumed,
}

impl QueueEventKind {
    const fn as_event_kind(self) -> EventKind {
        match self {
            Self::Paused => EventKind::QueuePaused,
            Self::Resumed => EventKind::QueueResumed,
        }
    }
}

impl TryFrom<EventKind> for QueueEventKind {
    type Error = EventKindMismatch;

    fn try_from(kind: EventKind) -> Result<Self, Self::Error> {
        match kind {
            EventKind::QueuePaused => Ok(Self::Paused),
            EventKind::QueueResumed => Ok(Self::Resumed),
            EventKind::JobCancelled
            | EventKind::JobCompleted
            | EventKind::JobFailed
            | EventKind::JobInterrupted
            | EventKind::JobSnoozed => Err(EventKindMismatch {
                actual: kind,
                expected: "queue",
            }),
        }
    }
}

impl From<QueueEventKind> for EventKind {
    fn from(kind: QueueEventKind) -> Self {
        kind.as_event_kind()
    }
}

/// Error converting a broad subscription kind to a typed event kind.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("expected a {expected} event kind, received {actual:?}")]
pub struct EventKindMismatch {
    actual: EventKind,
    expected: &'static str,
}

impl EventKindMismatch {
    /// Returns the received event kind.
    #[must_use]
    pub const fn actual(&self) -> EventKind {
        self.actual
    }

    /// Returns the expected event category.
    #[must_use]
    pub const fn expected(&self) -> &'static str {
        self.expected
    }
}

/// Timing information for one execution of a job.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
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
    buffer_capacity: usize,
    kinds: Vec<EventKind>,
}

impl SubscribeConfig {
    /// Creates a subscription for at least one event kind.
    ///
    /// # Errors
    ///
    /// Returns an error when `kinds` is empty.
    pub fn new(kinds: impl IntoIterator<Item = EventKind>) -> Result<Self, Error> {
        let kinds = kinds.into_iter().collect::<Vec<_>>();
        validate_kinds(&kinds)?;
        Ok(Self {
            buffer_capacity: 1_000,
            kinds,
        })
    }

    /// Sets the bounded receiver capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when `capacity` is zero.
    pub fn with_buffer_capacity(mut self, capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::configuration_context(
                "event subscription",
                "event subscription buffer capacity must be positive".to_owned(),
            ));
        }
        self.buffer_capacity = capacity;
        Ok(self)
    }

    /// Returns the bounded receiver capacity.
    #[must_use]
    pub const fn buffer_capacity(&self) -> usize {
        self.buffer_capacity
    }

    /// Returns the requested event kinds.
    #[must_use]
    pub fn kinds(&self) -> &[EventKind] {
        &self.kinds
    }

    pub(crate) fn into_parts(self) -> (usize, Vec<EventKind>) {
        (self.buffer_capacity, self.kinds)
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

impl std::fmt::Debug for EventReceiver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventReceiver")
            .field("dropped", &self.dropped.load(Ordering::Acquire))
            .field("closed", &self.receiver.is_closed())
            .finish_non_exhaustive()
    }
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
        return Err(Error::configuration_context(
            "event subscription",
            "event subscription requires at least one event kind".to_owned(),
        ));
    }
    Ok(kinds.iter().copied().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_kind_domains_do_not_overlap() {
        assert_eq!(
            JobEventKind::try_from(EventKind::JobCompleted).unwrap(),
            JobEventKind::Completed
        );
        assert!(JobEventKind::try_from(EventKind::QueuePaused).is_err());
        assert_eq!(
            QueueEventKind::try_from(EventKind::QueueResumed).unwrap(),
            QueueEventKind::Resumed
        );
        assert!(QueueEventKind::try_from(EventKind::JobFailed).is_err());
    }

    #[test]
    fn subscription_is_valid_by_construction() {
        assert!(SubscribeConfig::new([]).is_err());
        let config = SubscribeConfig::new([EventKind::JobCompleted])
            .unwrap()
            .with_buffer_capacity(42)
            .unwrap();
        assert_eq!(config.buffer_capacity(), 42);
        assert_eq!(config.kinds(), [EventKind::JobCompleted]);
        assert!(config.with_buffer_capacity(0).is_err());
    }
}
