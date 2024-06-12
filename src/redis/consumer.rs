use crate::consumer::{ConsumerProvider, ConsumerStream};
use crate::redis::data::StreamInfoGroupsReply;
use crate::{Message, MessageId};
use redis::AsyncCommands as _;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RedisConsumer {
    client: redis::Client,
    stream: Arc<str>,
    group: Arc<str>,
    idle_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RedisConsumerError {
    #[error("failed to connect to redis: {source}")]
    Connection {
        #[source]
        source: redis::RedisError,
    },
    #[error("invalid consumer name: {name}")]
    InvalidConsumerName { name: String },
    #[error("failed to create the stream group: {source}")]
    CreateStreamFailure {
        #[source]
        source: redis::RedisError,
    },
}

const NACK_CONSUMER: &str = "$$nack";

impl RedisConsumer {
    /// Creates a new redis stream consumer.
    ///
    /// The redis client should be set up to connect to the redis
    /// server; this does not directly connect to the server yet,
    /// but when a stream is requested, it will connect to the
    /// server.
    ///
    /// The stream is the redis key of the stream to consume from;
    /// this is different from the group, which indicates which
    /// group a consumer this spawns is a part of.  Each group
    /// consumes the entire stream independent of each other.
    /// You can think of a group as a single combined consumer,
    /// and each consumer in the group as a single thread, processing
    /// one task at a time.
    ///
    /// For more information on the redis streams and how they're used
    /// here, please see the [modul-level
    /// documentation](crate::redis).
    #[must_use = "consumer does nothing unless used"]
    pub fn new(
        client: redis::Client,
        stream: String,
        group: String,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            client,
            stream: stream.into(),
            group: group.into(),
            idle_timeout,
        }
    }

    /// Creates both the Stream itself, and the group within the
    /// stream, if neither exist.
    ///
    /// This initializes the stream and sets up the group to track
    /// from the start of the stream.  This is likely what you
    /// want; if you want to start from a specific point in the
    /// stream, you can use the
    /// [`RedisStreamConsumer::create_group`] method instead.
    ///
    /// This only needs to be called once, but it is safe to call it
    /// multiple times.
    #[tracing::instrument]
    pub async fn create(&mut self) -> Result<(), RedisConsumerError> {
        let mut connection = self.connection().await?;
        // first, checks if the stream exists.
        // `EXISTS {stream-key}`
        let exists = connection
            .exists::<_, i64>(&*self.stream)
            .await
            .map(|v| v > 0)
            .map_err(|source| RedisConsumerError::CreateStreamFailure { source })?;

        let has_group = if exists {
            let reply: StreamInfoGroupsReply = connection
                .xinfo_groups(&*self.stream)
                .await
                .map_err(|source| RedisConsumerError::CreateStreamFailure { source })?;

            reply.groups.iter().any(|group| group.name == *self.group)
        } else {
            false
        };

        if !has_group {
            connection
                .xgroup_create_mkstream(&*self.stream, &*self.group, "0")
                .await
                .or_else(|e| {
                    // We get this error because another consumer created the
                    // group before we did.  This is fine, so we'll just ignore
                    // it.
                    if e.code().is_some_and(|code| code == "BUSYGROUP") {
                        Ok(())
                    } else {
                        Err(e)
                    }
                })
                .map_err(|source| RedisConsumerError::CreateStreamFailure { source })?;
        }

        Ok(())
    }

    async fn connection(
        &mut self,
    ) -> Result<redis::aio::MultiplexedConnection, RedisConsumerError> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|source| RedisConsumerError::Connection { source })
    }
}

#[async_trait::async_trait]
impl<T: serde::de::DeserializeOwned + Send + Unpin + 'static> ConsumerProvider<T>
    for RedisConsumer
{
    type Error = RedisConsumerError;
    type Stream = RedisStream<T>;

    async fn stream(&mut self, consumer: &str) -> Result<Self::Stream, Self::Error> {
        if consumer == NACK_CONSUMER {
            return Err(RedisConsumerError::InvalidConsumerName {
                name: consumer.to_owned(),
            });
        }
        let connection = self.connection().await?;
        Ok(RedisStream {
            stream: Arc::clone(&self.stream),
            group: Arc::clone(&self.group),
            consumer: consumer.into(),
            connection,

            task_buffer: BTreeMap::new(),
            processing_set: BTreeSet::new(),

            idle_timeout: self.idle_timeout,

            _phantom: std::marker::PhantomData,
        })
    }
}

pub struct RedisStream<T: Unpin> {
    /// The redis key of the stream to interact with.
    ///
    /// This is the same as the stream that the producer is pushing
    /// into.
    stream: Arc<str>,
    /// The name of the group this consumer is a part of.
    ///
    /// A group, as a collective, processes the entire stream; within
    /// the group, each consumer processes a single unique message.
    /// As this is likely to be shared across multiple streams,
    /// without updating, this is behind a reference-counted
    /// pointer.
    group: Arc<str>,
    /// The name of the consumer that this stream is a part of.
    ///
    /// As we don't update this at all, boxing this saves some space,
    /// and decomposes to `&str` freely.  This is the name of the
    /// consumer within the group, and is used to identify which
    /// consumer is processing a given message.
    consumer: Box<str>,
    /// The redis connection to use for this consumer.
    ///
    /// This connection is used only for this consumer, and is not
    /// shared with other consumers; this simplifies the
    /// implementation.  However, because of this, we need to borrow
    /// `self` as mutable whenever connecting to redis, which is
    /// fine.
    connection: redis::aio::MultiplexedConnection,

    // Internal state.
    //
    // While these both combined would allow us to implement batching
    // and buffering, along with a consumer processing multiple items
    // at once, we don't currently support that - that's not the intended
    // use case for this library.  As such, we'll put it in asserts to
    // prevent that from happening.
    /// The task buffer.
    ///
    /// Keeps track of pending tasks for this consumer.  These are
    /// tasks we have received and claimed from redis, but have
    /// not begun processing; as such, it contains all of the task
    /// information.  The key is the stream ID; we can place this
    /// in a `BTreeMap` because the keys are ordered by default,
    /// in the order they were received.  (A design decision by redis
    /// streams.)
    task_buffer: BTreeMap<MessageId, StreamEntry>,

    /// The processing set.
    ///
    /// This contains all of the tasks that are currently being
    /// processed by this consumer.  We can generally assume there is
    /// only one item in the set, and treat more than one item in the
    /// set as a logic error.
    processing_set: BTreeSet<MessageId>,

    idle_timeout: Duration,

    _phantom: std::marker::PhantomData<T>,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RedisStreamError {
    #[error("could not deserialize the task data: {source}")]
    DeserializationFailure {
        #[source]
        source: serde_json::Error,
    },
    #[error("failure to nack message {id} for {consumer} in {group}: {source}")]
    Nack {
        consumer: Box<str>,
        id: MessageId,
        group: Arc<str>,
        #[source]
        source: redis::RedisError,
    },
    #[error("failure to ack message {id} for {consumer} in {group}: {source}")]
    Ack {
        consumer: Box<str>,
        id: MessageId,
        group: Arc<str>,
        #[source]
        source: redis::RedisError,
    },
    #[error("failed to send heartbeat for {consumer} in {group}: {source}")]
    Heartbeat {
        consumer: Box<str>,
        group: Arc<str>,
        #[source]
        source: redis::RedisError,
    },
    #[error("failed to load pending messages for {consumer} in {group}: {source}")]
    Pending {
        consumer: Box<str>,
        group: Arc<str>,
        #[source]
        source: redis::RedisError,
    },
    #[error("failed to load any pending messages under {group} for {consumer}: {source}")]
    PendingAll {
        consumer: Box<str>,
        group: Arc<str>,
        #[source]
        source: redis::RedisError,
    },
    #[error("failed to load any nacked messages under {group} for {consumer}: {source}")]
    PendingNack {
        consumer: Box<str>,
        group: Arc<str>,
        #[source]
        source: redis::RedisError,
    },
    #[error("failed to claim entries for {consumer} in {group}: {source}")]
    Claim {
        consumer: Box<str>,
        group: Arc<str>,
        #[source]
        source: redis::RedisError,
    },
    #[error("failed to read from the stream group {group} for {consumer}: {source}")]
    ReadGroup {
        consumer: Box<str>,
        group: Arc<str>,
        #[source]
        source: redis::RedisError,
    },

    #[error("failed to join consumer task: {source}")]
    Task {
        #[from]
        source: tokio::task::JoinError,
    },
}

impl<T: serde::de::DeserializeOwned + Unpin> RedisStream<T> {
    /// This name is a bit of a misnomer in the current async context
    /// of rust.
    ///
    /// `poll_next` tends to imply that it will return a
    /// `std::task::Poll`; instead, this returns a future that
    /// will take about as long as `timeout` to elapse.  This is
    /// just a wrapper around calling `poll_pending`,
    /// `poll_steal`, and `poll_group`, in order, and returning the
    /// first non-`None` value.  Keep in mind that any of these can
    /// return multiple values, which is then pushed into our task
    /// buffer.
    async fn poll_next(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<StreamEntry>, RedisStreamError> {
        debug_assert!(
            self.processing_set.is_empty(),
            "task in processing set! (try acking the task?)"
        );
        if let Some((id, entry)) = self.task_buffer.pop_first() {
            self.processing_set.insert(id);
            return Ok(Some(entry));
        }

        // we need to store this in a local because
        // `self.push(self.poll().await?)` borrows `self` mutably
        // twice.

        let got = self.poll_pending().await?;
        if let Some(v) = self.push(got) {
            return Ok(Some(v));
        }

        let got = self.poll_steal().await?;
        if let Some(v) = self.push(got) {
            return Ok(Some(v));
        }

        let got = self.poll_nack().await?;
        if let Some(v) = self.push(got) {
            return Ok(Some(v));
        }

        let got = self.poll_group(timeout).await?;
        if let Some(v) = self.push(got) {
            return Ok(Some(v));
        }

        Ok(None)
    }

    /// Finds the pending tasks for this current consumer.
    ///
    /// This is to check if we have any pending tasks that we don't
    /// know of. This can happen if the consumer is restarted, and
    /// we need to re-claim the tasks that we were processing
    /// before.  This normally should not return anything, as we would
    /// have processed all tasks before reaching this point.
    #[tracing::instrument]
    async fn poll_pending(&mut self) -> Result<Vec<StreamEntry>, RedisStreamError> {
        // Find all of the pending tasks for this consumer, in specific.
        // We want to re-claim these, if we, for some reason, rebooted.
        //
        // This calls `XPENDING {stream} {group} - + 10 {consumer}`, which
        // returns the top 1 items in _this consumer's_ pending entries list.
        // (The `-` and `+` represent the smallest and largest ID possible,
        // respectively.)
        let mut result: redis::streams::StreamPendingCountReply = self
            .connection
            .xpending_consumer_count(&*self.stream, &*self.group, "-", "+", 10, &*self.consumer)
            .await
            .map_err(|source| RedisStreamError::Pending {
                group: Arc::clone(&self.group),
                consumer: self.consumer.clone(),
                source,
            })?;

        // Only take in the IDs that we haven't already claimed or are already
        // processing.
        result.ids = result
            .ids
            .into_iter()
            .filter(|id| {
                let message_id = MessageId::new(&id.id);
                !self.processing_set.contains(&message_id)
                    && !self.task_buffer.contains_key(&message_id)
            })
            .collect::<Vec<_>>();
        let min_idle = result
            .ids
            .iter()
            .map(|item| item.last_delivered_ms)
            .min()
            .unwrap_or(0);

        self.claim_from(result, min_idle).await
    }

    /// Finds expired/idle tasks for this consumer.
    ///
    /// We're going to attempt to claim any tasks that have been idle
    /// for longer than the configured `idle_timeout`.  As of
    /// Redis 6.2, we can do this in only two calls - XPENDING
    /// with the `IDLE` option, and XCLAIM on the returned IDs.
    /// Redis 7.0 includes `AUTOCLAIM`, which does this in one
    /// call, but does not return any retry information - which we
    /// want.
    #[tracing::instrument]
    async fn poll_steal(&mut self) -> Result<Vec<StreamEntry>, RedisStreamError> {
        let idle_timeout = self.idle_timeout.as_millis() as usize;

        // XPENDING key group [[IDLE min-idle-time] start end count
        // [consumer]]
        //
        // Retrieves any one pending message from the group that exceed the
        // idle timeout time.
        let pending: redis::streams::StreamPendingCountReply = redis::cmd("XPENDING")
            .arg(&*self.stream)
            .arg(&*self.group)
            .arg("IDLE")
            .arg(idle_timeout)
            .arg("-")
            .arg("+")
            .arg(1)
            .query_async(&mut self.connection)
            .await
            .map_err(|source| RedisStreamError::PendingAll {
                group: Arc::clone(&self.group),
                consumer: self.consumer.clone(),
                source,
            })?;

        self.claim_from(pending, idle_timeout).await
    }

    /// Finds NACK'd tasks for this consumer.
    ///
    /// NACK'd messages are sent to a special consumer's PEL -
    /// `$$nack`. There will never be a consumer named `$$nack`
    /// (hopefully), so those messages are never processed.
    /// Instead, we can claim them, and re-process them, without
    /// having to wait for the idle timeout that `poll_steal`
    /// would require (though if the message is left in `$$nack`'s
    /// PEL for too long, `poll_steal` will steal it again).
    async fn poll_nack(&mut self) -> Result<Vec<StreamEntry>, RedisStreamError> {
        let pending: redis::streams::StreamPendingCountReply = self
            .connection
            .xpending_consumer_count(&*self.stream, &*self.group, "-", "+", 1, NACK_CONSUMER)
            .await
            .map_err(|source| RedisStreamError::PendingNack {
                group: Arc::clone(&self.group),
                consumer: self.consumer.clone(),
                source,
            })?;

        self.claim_from(pending, 0).await
    }

    #[tracing::instrument]
    async fn claim_from(
        &mut self,
        pending: redis::streams::StreamPendingCountReply,
        idle: usize,
    ) -> Result<Vec<StreamEntry>, RedisStreamError> {
        if pending.ids.is_empty() {
            return Ok(vec![]);
        }

        let ids = pending
            .ids
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>();

        // Now, we're using `XCLAIM` for its intended purpose - to claim an
        // item! Here, we're just going to try to claim the one item we
        // took earlier.
        //
        // XCLAIM stream group consumer min-idle-time ID [ID ...]
        let claims: redis::streams::StreamClaimReply = self
            .connection
            .xclaim(&*self.stream, &*self.group, &*self.consumer, idle, &ids)
            .await
            .map_err(|source| RedisStreamError::Claim {
                group: Arc::clone(&self.group),
                consumer: self.consumer.clone(),
                source,
            })?;

        // Failed to claim - return immediately.
        if claims.ids.is_empty() {
            return Ok(vec![]);
        };

        Ok(claims
            .ids
            .into_iter()
            .map(map_stream_id)
            .map(|entry| {
                let retries = pending
                    .ids
                    .iter()
                    .find(|item| item.id == entry.id.as_ref())
                    .map_or(u64::MAX, |item| item.times_delivered as u64);
                entry.with_retries(retries)
            })
            .collect())
    }

    /// The main polling function.
    ///
    /// This polls the stream for brand-new entries, and returns them,
    /// if possible. It will wait for some time for a response, and
    /// Redis guarantees a fair algorithm for polling, so we don't
    /// need to worry about starvation.
    #[tracing::instrument]
    async fn poll_group(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<StreamEntry>, RedisStreamError> {
        let options = redis::streams::StreamReadOptions::default()
            .block(timeout.as_millis() as usize)
            .count(1)
            .group(&*self.group, &*self.consumer);

        let tasks: redis::streams::StreamReadReply = self
            .connection
            .xread_options(&[&*self.stream], &[">"], &options)
            .await
            .map_err(|source| RedisStreamError::ReadGroup {
                group: self.group.clone(),
                consumer: self.consumer.clone(),
                source,
            })?;

        let Some(stream) = tasks.keys.into_iter().find(|x| x.key == *self.stream) else {
            return Ok(vec![]);
        };

        let entries = stream.ids.into_iter().map(map_stream_id).collect();

        Ok(entries)
    }

    fn push(&mut self, list: Vec<StreamEntry>) -> Option<StreamEntry> {
        self.task_buffer
            .extend(list.into_iter().map(|task| (task.id.clone(), task)));

        if let Some((id, next)) = self.task_buffer.pop_first() {
            self.processing_set.insert(id);
            Some(next)
        } else {
            None
        }
    }
}

impl<T: Unpin> std::fmt::Debug for RedisStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStreamConsumerStream")
            .field("stream", &self.stream)
            .field("group", &self.group)
            .field("consumer", &self.consumer)
            .field("task_buffer", &self.task_buffer)
            .field("processing_set", &self.processing_set)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl<T: serde::de::DeserializeOwned + Send + Unpin + 'static> ConsumerStream<T> for RedisStream<T> {
    type Error = RedisStreamError;

    async fn next(
        mut self: Pin<&mut Self>,
        timeout: Duration,
    ) -> Result<Option<Message<T>>, Self::Error> {
        let Some(message) = self.poll_next(timeout).await? else {
            return Ok(None);
        };

        // This does not nack because it does not need to nack; ideally, an
        // error here will cause the consumer to crash, and the message will
        // be re-delivered.
        message.compose_message().map(Some)
    }

    /// Acknowledges the given task.
    ///
    /// This will remove the task from the stream, acknowledging that
    /// it has been completed.
    ///
    /// # Panics
    ///
    /// Note that the given task _must_ belong to this consumer,
    /// otherwise this will cause issues; as such, if the task was
    /// not previously returned by a call to `next`, or was acked
    /// or nacked since that call, this will panic.
    #[tracing::instrument]
    async fn ack(mut self: Pin<&mut Self>, id: &MessageId) -> Result<(), Self::Error> {
        // Without this, rust complains because it can't do split borrows -
        // the type for self is `Pin<&mut T>` (with `T: Unpin`), not
        // `&mut Self`.  It can split borrows for the latter, not the former;
        // thus, we force it to decompose into the latter, since we can just
        // deref mut `self`.
        let this = &mut *self;
        assert!(
            this.processing_set.contains(id) || this.task_buffer.contains_key(id),
            "task does not belong to this consumer"
        );

        this.connection
            .xack(&*this.stream, &*this.group, &[&id])
            .await
            .map_err(|source| RedisStreamError::Ack {
                group: Arc::clone(&this.group),
                consumer: this.consumer.clone(),
                id: id.to_owned(),
                source,
            })?;

        this.processing_set.remove(id);
        this.task_buffer.remove(id);

        Ok(())
    }

    /// Rejects the given task.
    ///
    /// This will re-queue the task, allowing another consumer to
    /// pick it up.
    ///
    /// # Panics
    ///
    /// Note that the given task _must_ belong to this consumer,
    /// otherwise this will cause issues; as such, if the task was
    /// not previously returned by a call to `next`, or was acked
    /// or nacked since that call, this will panic.
    #[tracing::instrument]
    async fn nack(mut self: Pin<&mut Self>, id: &MessageId) -> Result<(), Self::Error> {
        // Without this, rust complains because it can't do split borrows -
        // the type for self is `Pin<&mut T>` (with `T: Unpin`), not
        // `&mut Self`.  It can split borrows for the latter, not the former;
        // thus, we force it to decompose into the latter, since we can just
        // deref mut `self`.
        let this = &mut *self;
        assert!(
            this.processing_set.contains(id) || this.task_buffer.contains_key(id),
            "task does not belong to this consumer"
        );

        let options = redis::streams::StreamClaimOptions::default().with_justid();

        let claims: Vec<String> = this
            .connection
            .xclaim_options(
                &*this.stream,
                &*this.group,
                NACK_CONSUMER,
                0,
                &[id.as_ref()],
                options,
            )
            .await
            .map_err(|source| RedisStreamError::Nack {
                group: Arc::clone(&this.group),
                consumer: this.consumer.clone(),
                id: id.to_owned(),
                source,
            })?;

        if claims.is_empty() {
            return Err(RedisStreamError::Nack {
                group: Arc::clone(&this.group),
                consumer: this.consumer.clone(),
                id: id.to_owned(),
                source: redis::RedisError::from((
                    redis::ErrorKind::ClientError,
                    "failed to nack message; the xclaim failed, so the message was not in the PEL?",
                )),
            });
        }

        this.processing_set.remove(id);
        this.task_buffer.remove(id);

        Ok(())
    }

    /// Performs a heartbeat.
    ///
    /// Since we keep track of all of the tasks we're processing, we
    /// can trivally create the list, and `XCLAIM` them again.
    async fn heartbeat(mut self: Pin<&mut Self>) -> Result<(), Self::Error> {
        // Without this, rust complains because it can't do split borrows -
        // the type for self is `Pin<&mut T>` (with `T: Unpin`), not
        // `&mut Self`.  It can split borrows for the latter, not the former;
        // thus, we force it to decompose into the latter, since we can just
        // deref mut `self`.
        let this = &mut *self;
        let ids = this
            .processing_set
            .iter()
            .chain(this.task_buffer.keys())
            .cloned()
            .collect::<Vec<_>>();

        // This is the same abuse of `XCLAIM` that we saw in `nack`.
        // However, instead of setting the idle time of the messages into
        // the future, this resets them to 0 - effectively extending the
        // amount of time that the message has been idle.
        let options = redis::streams::StreamClaimOptions::default()
            .idle(0)
            .with_justid();

        let _: redis::Value = this
            .connection
            .xclaim_options(
                &*this.stream,
                &*this.group,
                &*this.consumer,
                0,
                &ids[..],
                options,
            )
            .await
            .map_err(|source| RedisStreamError::Heartbeat {
                group: Arc::clone(&this.group),
                consumer: this.consumer.clone(),
                source,
            })?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
/// An entry in the stream.
struct StreamEntry {
    /// The entry ID.  Used to ack the entry.
    pub id: MessageId,
    pub data: HashMap<String, String>,
    /// The number of retries this entry has had.
    pub retry: u64,
}

impl StreamEntry {
    pub fn compose_message<T: serde::de::DeserializeOwned>(
        mut self,
    ) -> Result<Message<T>, RedisStreamError> {
        let data = self.data.remove("data");
        let data = data.as_deref().unwrap_or("null");
        let data = serde_json::from_str(data)
            .map_err(|source| RedisStreamError::DeserializationFailure { source })?;
        Ok(Message::new(self.id, data, self.retry))
    }

    fn with_retries(mut self, retries: u64) -> Self {
        self.retry = retries;
        self
    }
}

fn map_stream_id(s: redis::streams::StreamId) -> StreamEntry {
    StreamEntry {
        id: MessageId::new(s.id),
        data: s
            .map
            .into_iter()
            .filter_map(|(k, v)| {
                <String as redis::FromRedisValue>::from_redis_value(&v)
                    .ok()
                    .map(|v| (k, v))
            })
            .collect(),
        retry: 0,
    }
}
