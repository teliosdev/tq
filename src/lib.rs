//! # **T**ask **Q**ueue.
//!
//! A simple task queue system implementation for Rust.  Task queues
//! can be defined by the following traits:
//!
//! 1. Persistent storage of in-order objects, each containing data;
//! 2. Persistent storage of consumers, each containing information
//!    about which objects have been seen, which objects are being
//!    worked on, and which objects have been completed;
//!
//! Note that these both imply restrictions on the storage system that
//! is used. However, at no point does the data have to be discarded,
//! so it is possible to implement a task queue that is backed by a
//! database, for example.  (Just not recommended, for reasons we'll
//! go into later.)
//!
//! ## Usage
//!
//! This library is split in two parts: the consumer, and the
//! producer.  The producer is much more simple, as all it has to do
//! is push objects into the queue (serializing the object however it
//! wishes), and the consumer is responsible for pulling objects out
//! of the queue, and deserializing them.
//!
//! Thus, the consumer is gated behind the `consumer` feature, and the
//! producer is gated behind the `producer` feature.  This allows you
//! to use the library as-needed.
//!
//! The library is also split into providers, which allow you to
//! customize how the queue is stored.  Currently, there is only one
//! provider, which is `redis-streams`, which uses Redis streams to
//! store the queue.  However, it is possible to implement your own
//! provider, and use that instead.
//!
//! So you'll have the `redis-streams`'s producer, which pushes
//! objects into the queue, and the `redis-streams`'s consumer, which
//! pulls objects out of the queue.

#[cfg(feature = "consumer")]
mod consumer;
#[cfg(feature = "producer")]
mod producer;

#[cfg(feature = "redis")]
pub mod redis;

#[cfg(feature = "consumer")]
pub use self::consumer::{Consumer, ConsumerProvider, ConsumerStream, Message, MessageId, Spawn};
#[cfg(feature = "producer")]
pub use self::producer::ProducerProvider;
