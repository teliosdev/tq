//! # Redis Provider
//!
//! This is the redis provider for the task queue.  It uses Redis
//! streams to back the queue.
//!
//! **NOTE: THIS REQUIRES REDIS 7.0 OR LATER.**  While Redis Streams
//! is a 5.0 addition, this uses an option with the `XPENDING` command
//! that was only added in 6.2.  This also uses fields in `XINFO
//! GROUPS` that were added in 7.0.  *This will not work on versions
//! before 7.0.*
//!
//! ## Producer
//!
//! The producer for the redis provider is very simple - all it needs
//! to do is issue an `XADD` command to the stream, and it's done - we
//! only serialize on top of that, to pass arbitrary user data.
//!
//! We use `serde_json` to serialize the data, so you can use any type
//! that implements `serde::Serialize`/`serde::de::DeserializeOwned`.
//! This is stored under the `data` field for the stream object; no
//! other fields are used, as redis streams provides an ID and retry
//! count natively.
//!
//! The producer _does_ require you to specify a max stream length.
//! This is because redis streams are unbounded, and we don't want to
//! fill up your queue unbounded.  This limit is approximate, for
//! performance reasons (and implies the stream can have a few more
//! items in it than the limit).  However, we cannot know easily if
//! the stream is full - each group has its own position
//! in the stream, and without checking every group how many entries
//! have been read, compared to the stream length, we can't know if
//! the stream is full. Thus, the max length specification here can
//! truncate active data in the case that some consumers are extremely
//! slow, as we'd be producing faster than they can consume.
//!
//! Thus, the default method checks that the `lag` of all groups
//! within the stream is less than the max length.  If it is, we can
//! safely add to the queue, as everything past the lag is considered
//! "dead" data, and can be safely truncated.  If it is not, we return
//! an error.  However, redis's documentation notes that the lag can
//! sometimes be null, especially if the group is created with an
//! arbitrary ID (i.e., not the first entry, last entry, or zero ID).
//! Thus, we assume that if the lag is null, the group's
//! lag is equivalent to the stream's length (provided by `XLEN`)
//! minus the `entries-read`.  This is a conservative estimate, and
//! may cause the producer to fail to add to the queue when it could
//! have, but it is better than overrunning the queue.
//!
//! ## Consumer
//!
//! The consumer is far more complicated than the producer.  To break
//! down how streams work on Redis in a really short time: for a given
//! key, there can be a "Stream" data type.  Within that stream, there
//! can be "groups," which refers to a group of "consumers."  A group
//! can be considered a logical consumer, in that it has a position in
//! the stream, and can read from that position.  Each consumer within
//! that group, then, will generally receive different messages, as
//! they read from the group's position.  The benefit to this is error
//! recovery - when, and if, a consumer fails, the group can keep
//! track of who was working on what, and re-deliver the message to
//! another consumer.  This is done by tracking all of the messages
//! currently pending in the group, and the last ID that was
//! acknowledged by each consumer.  When a consumer fails, the group
//! can then re-deliver all of the messages that were pending to
//! another consumer, and continue on.
//!
//! This is, of course, a simplification of the process, but it
//! should be enough to understand how the consumer works.
//!
//! An important note is the configuration of the timeout passed in.
//! This is the timeout for the `XREADGROUP` command, which is used to
//! read from the stream.  This should be configured high - we don't
//! want to busy loop too much, and waiting for a notification from
//! redis is cheaper than looping - but not too high, as then we can't
//! check for expired messages.  This needs to be calculated with your
//! expected message processing time, and your `idle_timeout` (the
//! amount of time a message can be in the queue before it's
//! considered expired).  All of these things are configured - the
//! `idle_timeout` is configured on the redis consumer provider,
//! while the `keep_alive_interval` and `poll_timeout` are configured
//! on the [`Consumer`][crate::Consumer] itself.  A good default for
//! the `keep_alive_interval` is half of the `idle_timeout`.

#[cfg(feature = "producer")]
mod producer;

#[cfg(feature = "producer")]
pub use self::producer::{RedisProducer, RedisProducerError};
use crate::MessageId;

#[cfg(feature = "consumer")]
mod consumer;

#[cfg(feature = "consumer")]
pub use self::consumer::{RedisConsumer, RedisStreamConsumerError};

mod data;

impl redis::ToRedisArgs for MessageId {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        redis::ToRedisArgs::write_redis_args(&&*self.0, out);
    }
}
