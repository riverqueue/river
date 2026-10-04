//! Bounded local client event subscriptions.

use std::{
    collections::HashSet,
    num::NonZeroUsize,
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

impl From<QueueEventKind> for EventKind {
    fn from(kind: QueueEventKind) -> Self {
        kind.as_event_kind()
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

/// Receiver capacity of a subscription that doesn't set one.
const DEFAULT_BUFFER_CAPACITY: NonZeroUsize = NonZeroUsize::new(1_000).unwrap();

/// Configuration for one event subscription.
#[derive(Clone, Debug)]
pub struct SubscribeConfig {
    buffer_capacity: NonZeroUsize,
    kinds: Vec<EventKind>,
}

impl SubscribeConfig {
    /// Creates a subscription for at least one event kind, with a receiver
    /// buffer of 1,000 events.
    ///
    /// # Errors
    ///
    /// Returns an error when `kinds` is empty.
    pub fn new(kinds: impl IntoIterator<Item = EventKind>) -> Result<Self, Error> {
        let kinds = kinds.into_iter().collect::<Vec<_>>();
        validate_kinds(&kinds)?;
        Ok(Self {
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            kinds,
        })
    }

    /// Returns the configuration with a receiver buffer of `capacity`
    /// events. A receiver that falls further behind loses the oldest events
    /// and learns how many on its next receive.
    #[must_use]
    pub const fn with_buffer_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.buffer_capacity = capacity;
        self
    }

    /// Returns the bounded receiver capacity.
    #[must_use]
    pub const fn buffer_capacity(&self) -> NonZeroUsize {
        self.buffer_capacity
    }

    /// Returns the requested event kinds.
    #[must_use]
    pub fn kinds(&self) -> &[EventKind] {
        &self.kinds
    }

    pub(crate) fn into_parts(self) -> (NonZeroUsize, Vec<EventKind>) {
        (self.buffer_capacity, self.kinds)
    }
}

/// Error returned while receiving client events.
#[derive(Debug, Error)]
#[non_exhaustive]
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
    ///
    /// # Errors
    ///
    /// Returns [`EventRecvError::Lagged`] with the number of events dropped
    /// because the receiver fell behind, after which receiving resumes, and
    /// [`EventRecvError::Closed`] once the client is gone.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe: dropping its future before it completes
    /// loses no event, and the next call receives it.
    pub async fn recv(&mut self) -> Result<Event, EventRecvError> {
        let dropped = self.dropped.swap(0, Ordering::AcqRel);
        if dropped > 0 {
            return Err(EventRecvError::Lagged(dropped));
        }
        self.receiver.recv().await.ok_or(EventRecvError::Closed)
    }
}

/// Yields what [`EventReceiver::recv`] returns, including
/// [`EventRecvError::Lagged`], and ends once the client is gone instead of
/// yielding [`EventRecvError::Closed`].
impl futures_util::Stream for EventReceiver {
    type Item = Result<Event, EventRecvError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let dropped = self.dropped.swap(0, Ordering::AcqRel);
        if dropped > 0 {
            return std::task::Poll::Ready(Some(Err(EventRecvError::Lagged(dropped))));
        }
        self.receiver.poll_recv(context).map(|event| event.map(Ok))
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
    fn typed_event_kinds_map_to_subscription_kinds() {
        assert_eq!(
            EventKind::from(JobEventKind::Completed),
            EventKind::JobCompleted
        );
        assert_eq!(
            EventKind::from(QueueEventKind::Resumed),
            EventKind::QueueResumed
        );
    }

    #[tokio::test]
    async fn receiver_streams_lags_events_and_ends_when_closed() {
        use futures_util::StreamExt as _;

        let dropped = Arc::new(AtomicU64::new(2));
        let (sender, receiver) = mpsc::channel(1);
        let mut events = EventReceiver::new(Arc::clone(&dropped), receiver);
        let now = chrono::Utc::now();
        sender
            .send(Event::queue(
                QueueEventKind::Paused,
                Queue {
                    created_at: now,
                    metadata: serde_json::Map::new(),
                    metadata_text: "{}".to_owned(),
                    name: "default".to_owned(),
                    paused_at: Some(now),
                    updated_at: now,
                },
            ))
            .await
            .unwrap();
        drop(sender);

        assert!(matches!(
            events.next().await,
            Some(Err(EventRecvError::Lagged(2)))
        ));
        assert_eq!(
            events.next().await.unwrap().unwrap().kind(),
            EventKind::QueuePaused
        );
        assert!(events.next().await.is_none());
    }

    #[test]
    fn subscription_is_valid_by_construction() {
        assert!(SubscribeConfig::new([]).is_err());
        let capacity = NonZeroUsize::new(42).unwrap();
        let config = SubscribeConfig::new([EventKind::JobCompleted])
            .unwrap()
            .with_buffer_capacity(capacity);
        assert_eq!(config.buffer_capacity(), capacity);
        assert_eq!(config.kinds(), [EventKind::JobCompleted]);
    }
}
