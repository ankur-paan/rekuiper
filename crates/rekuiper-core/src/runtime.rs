use crate::model::StreamRecord;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Bounded per-subscriber queue depth. Backpressure (awaiting capacity)
/// applies beyond this; records are never silently overwritten.
pub const STREAM_QUEUE_CAPACITY: usize = 4096;

/// Upper bound on records accepted in one HTTP ingestion request.
pub const MAX_HTTP_BATCH_RECORDS: usize = 10_000;

/// Records reserved and committed per admission step of a batch publish.
/// Must not exceed [`STREAM_QUEUE_CAPACITY`]: tokio rejects reservations
/// larger than a channel's capacity.
pub const ADMISSION_CHUNK_RECORDS: usize = 512;

const _: () =
    assert!(ADMISSION_CHUNK_RECORDS > 0 && ADMISSION_CHUNK_RECORDS <= STREAM_QUEUE_CAPACITY);

type RecordTx = mpsc::Sender<StreamRecord>;

/// Send-side handle for one stream topic. Cloneable; never holds the global
/// topic lock across an await (senders are snapshot-cloned first).
#[derive(Clone, Debug)]
pub struct StreamSender {
    bus: StreamBus,
    topic: String,
}

/// Receive-side handle: one bounded mpsc queue per subscriber.
pub type StreamReceiver = mpsc::Receiver<StreamRecord>;

/// Publish failure. Every error is returned only when zero records of the
/// request were admitted to any subscriber, so a caller may retry safely.
#[derive(Debug)]
pub enum PublishError {
    /// No subscribers registered on the topic; nothing was enqueued.
    NoSubscribers,
    /// Admission could not complete without waiting (a subscriber queue is
    /// full, or another publisher is mid-admission on the topic); nothing
    /// was enqueued.
    Full,
    /// Every selected subscriber queue closed before any record was
    /// committed; nothing was enqueued.
    Closed,
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublishError::NoSubscribers => write!(f, "no subscribers"),
            PublishError::Full => write!(f, "subscriber queue full"),
            PublishError::Closed => write!(f, "subscriber queue closed"),
        }
    }
}

impl std::error::Error for PublishError {}

#[derive(Debug, Default)]
struct Topic {
    senders: Vec<RecordTx>,
    /// Serialises admission per topic: a request's records are reserved and
    /// committed without interleaving with other publishers of the topic.
    /// Separate from the global topic map lock, which is never held across
    /// an await.
    gate: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Clone, Default, Debug)]
pub struct StreamBus {
    topics: Arc<RwLock<HashMap<String, Topic>>>,
    /// Records (per subscriber) that were not delivered because the
    /// subscriber closed after its request had started committing.
    undelivered_after_commit: Arc<AtomicU64>,
}

impl StreamBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve (creating if absent) the send handle for a topic. Cheap:
    /// one short map read; no per-record global locking by callers that
    /// reuse the handle.
    pub fn get_or_create(&self, stream_name: &str) -> StreamSender {
        let exists = self.topics.read().contains_key(stream_name);
        if !exists {
            self.topics
                .write()
                .entry(stream_name.to_string())
                .or_default();
        }
        StreamSender {
            bus: self.clone(),
            topic: stream_name.to_string(),
        }
    }

    /// Subscribe a new independent bounded queue on a topic. Every subscriber
    /// receives every record (fan-out); a slow subscriber applies
    /// backpressure to publishers, never silent loss of another subscriber's
    /// data.
    pub fn subscribe(&self, stream_name: &str) -> StreamReceiver {
        let (tx, rx) = mpsc::channel(STREAM_QUEUE_CAPACITY);
        self.topics
            .write()
            .entry(stream_name.to_string())
            .or_default()
            .senders
            .push(tx);
        rx
    }

    /// Records not delivered because their subscriber closed after the
    /// request had already started committing (accept-once lifecycle
    /// losses; see [`StreamBus::publish_batch`]).
    pub fn undelivered_after_commit(&self) -> u64 {
        self.undelivered_after_commit.load(Ordering::Relaxed)
    }

    fn gate(&self, topic: &str) -> Option<Arc<tokio::sync::Mutex<()>>> {
        self.topics.read().get(topic).map(|t| t.gate.clone())
    }

    /// Returns (registered subscriber count, live senders).
    fn snapshot(&self, topic: &str) -> (usize, Vec<RecordTx>) {
        let map = self.topics.read();
        match map.get(topic) {
            Some(t) => (
                t.senders.len(),
                t.senders
                    .iter()
                    .filter(|s| !s.is_closed())
                    .cloned()
                    .collect(),
            ),
            None => (0, Vec::new()),
        }
    }

    fn purge_closed(&self, topic: &str) {
        let mut map = self.topics.write();
        if let Some(t) = map.get_mut(topic) {
            t.senders.retain(|s| !s.is_closed());
        }
    }

    /// Backpressured publish of one record to every subscriber. Awaits queue
    /// capacity; never overwrites buffered records.
    pub async fn publish_async(
        &self,
        stream_name: &str,
        record: StreamRecord,
    ) -> Result<usize, PublishError> {
        self.publish_batch(stream_name, vec![record]).await
    }

    /// Backpressured publish of a whole validated batch, in order, to every
    /// subscriber selected when admission starts.
    ///
    /// Contract (reserve-then-commit, accept-once):
    /// - The per-topic admission gate is held for the whole request, so the
    ///   request's records stay contiguous and in order on every subscriber.
    /// - Records are admitted in chunks of [`ADMISSION_CHUNK_RECORDS`]:
    ///   capacity for a chunk is reserved on every selected subscriber
    ///   before any record of the chunk becomes visible.
    /// - Before the first chunk commits, a subscriber that closed is dropped
    ///   from the selection; if every selected subscriber closed, the
    ///   request fails with [`PublishError::Closed`] and zero records were
    ///   admitted anywhere (safe to retry).
    /// - Once the first chunk commits, the request is accepted: it returns
    ///   `Ok` even if subscribers close later, and records those subscribers
    ///   miss are counted in [`StreamBus::undelivered_after_commit`]. It
    ///   never turns a mid-request closure into a retryable error that would
    ///   duplicate the committed prefix.
    ///
    /// No registered subscribers is a success with `Ok(0)`. Returns the number
    /// of subscribers that received the complete request.
    pub async fn publish_batch(
        &self,
        stream_name: &str,
        records: Vec<StreamRecord>,
    ) -> Result<usize, PublishError> {
        if records.is_empty() {
            return Ok(0);
        }
        let Some(gate) = self.gate(stream_name) else {
            return Ok(0);
        };
        // Uncontended fast path avoids the async acquire (and its coop yield).
        let _admission = match gate.try_lock() {
            Ok(guard) => guard,
            Err(_) => gate.lock().await,
        };
        let (registered, mut selected) = self.snapshot(stream_name);
        if registered == 0 {
            return Ok(0);
        }
        if selected.len() < registered {
            self.purge_closed(stream_name);
        }
        if selected.is_empty() {
            return Err(PublishError::Closed);
        }

        let mut remaining = records.len();
        let mut records = records.into_iter();
        let mut committed = false;
        while remaining > 0 {
            let chunk = remaining.min(ADMISSION_CHUNK_RECORDS);
            let mut permits = Vec::with_capacity(selected.len());
            let mut closed = Vec::new();
            for (idx, sender) in selected.iter().enumerate() {
                // Never await while holding the gate when capacity is already
                // free: awaiting (or a coop yield) inside the gate makes
                // concurrent publishers convoy on it, one task hand-off per
                // request. Only a genuinely full queue waits.
                let reserved = match sender.try_reserve_many(chunk) {
                    Ok(p) => Some(p),
                    Err(mpsc::error::TrySendError::Closed(())) => None,
                    Err(mpsc::error::TrySendError::Full(())) => {
                        sender.reserve_many(chunk).await.ok()
                    }
                };
                match reserved {
                    Some(p) => permits.push(p),
                    None => closed.push(idx),
                }
            }
            if committed && !closed.is_empty() {
                // Accepted request: those subscribers miss this chunk and
                // everything after it.
                self.undelivered_after_commit
                    .fetch_add((closed.len() * remaining) as u64, Ordering::Relaxed);
            }
            if permits.is_empty() {
                self.purge_closed(stream_name);
                return if committed {
                    Ok(0)
                } else {
                    Err(PublishError::Closed)
                };
            }
            for record in records.by_ref().take(chunk) {
                let (last, others) = permits.split_last_mut().expect("at least one reservation");
                for p in others {
                    p.next().expect("reserved slot").send(record.clone());
                }
                last.next().expect("reserved slot").send(record);
            }
            drop(permits);
            committed = true;
            remaining -= chunk;
            if !closed.is_empty() {
                for idx in closed.into_iter().rev() {
                    selected.remove(idx);
                }
                self.purge_closed(stream_name);
            }
        }
        Ok(selected.len())
    }

    /// Non-blocking all-or-nothing publish for cyclic feedback (memory sink).
    /// Never awaits, so a rule feeding its own input stream cannot deadlock
    /// its sink worker against its input queue. Capacity is reserved on
    /// every live subscriber before the record becomes visible, so a
    /// concurrent publisher can never cause a partial fan-out. On `Full`
    /// (a queue is full, or another publisher is mid-admission on the topic)
    /// nothing was enqueued and the caller must account the drop explicitly.
    pub fn try_publish(
        &self,
        stream_name: &str,
        record: StreamRecord,
    ) -> Result<usize, PublishError> {
        let Some(gate) = self.gate(stream_name) else {
            return Err(PublishError::NoSubscribers);
        };
        let Ok(_admission) = gate.try_lock() else {
            if self.snapshot(stream_name).0 == 0 {
                return Err(PublishError::NoSubscribers);
            }
            return Err(PublishError::Full);
        };
        let (registered, selected) = self.snapshot(stream_name);
        if registered == 0 {
            return Err(PublishError::NoSubscribers);
        }
        if selected.len() < registered {
            self.purge_closed(stream_name);
        }
        let mut permits = Vec::with_capacity(selected.len());
        for sender in &selected {
            match sender.try_reserve() {
                Ok(p) => permits.push(p),
                // Dropping the acquired permits releases their capacity:
                // nothing becomes visible anywhere.
                Err(mpsc::error::TrySendError::Full(())) => return Err(PublishError::Full),
                Err(mpsc::error::TrySendError::Closed(())) => {}
            }
        }
        if permits.is_empty() {
            self.purge_closed(stream_name);
            return Err(PublishError::Closed);
        }
        let delivered = permits.len();
        let mut permits = permits.into_iter();
        let last = permits.next_back().expect("at least one reservation");
        for p in permits {
            p.send(record.clone());
        }
        last.send(record);
        Ok(delivered)
    }

    /// Synchronous legacy-compatible publish used only where no subscriber
    /// backpressure context exists (tests, best-effort sources migrating).
    /// Tries to enqueue without awaiting; returns receiver count.
    pub fn publish(&self, stream_name: &str, record: StreamRecord) -> Result<usize, PublishError> {
        self.try_publish(stream_name, record).or_else(|e| match e {
            PublishError::NoSubscribers => Ok(0),
            other => Err(other),
        })
    }
}

impl StreamSender {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Backpressured send of one record to every subscriber of this topic.
    pub async fn send(&self, record: StreamRecord) -> Result<usize, PublishError> {
        self.bus.publish_async(&self.topic, record).await
    }

    /// Backpressured send of a validated batch, in order, to every subscriber.
    /// See [`StreamBus::publish_batch`] for the admission contract.
    pub async fn send_batch(&self, records: Vec<StreamRecord>) -> Result<usize, PublishError> {
        self.bus.publish_batch(&self.topic, records).await
    }

    /// Non-blocking feedback publish (all-or-nothing).
    pub fn try_send(&self, record: StreamRecord) -> Result<usize, PublishError> {
        self.bus.try_publish(&self.topic, record)
    }

    /// Queue depth high-water hint: deepest current subscriber backlog.
    pub fn backlog_hint(&self) -> usize {
        let map = self.bus.topics.read();
        map.get(&self.topic)
            .map(|t| {
                t.senders
                    .iter()
                    .map(|s| STREAM_QUEUE_CAPACITY.saturating_sub(s.capacity()))
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0)
    }
}
