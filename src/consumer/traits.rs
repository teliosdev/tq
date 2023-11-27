use super::{Message, MessageId};
use std::pin::Pin;

#[async_trait::async_trait]
pub trait ConsumerProvider<T: serde::de::DeserializeOwned> {
    type Error: std::error::Error;
    type Stream: ConsumerStream<T>;

    async fn stream(&mut self, consumer: &str) -> Result<Self::Stream, Self::Error>;
}

#[async_trait::async_trait]
pub trait ConsumerStream<T: serde::de::DeserializeOwned>: Send + 'static {
    type Error: std::error::Error;

    /// Retrieves the next item in this consumer stream.
    ///
    /// If there is no item available, this method will wait for
    /// `timeout` before returning `None`.
    async fn next(
        self: Pin<&mut Self>,
        timeout: std::time::Duration,
    ) -> Result<Option<Message<T>>, Self::Error>;

    /// Acknowledges the given message.
    ///
    /// This removes the message from the stream, and prevents
    /// it from being re-delivered.  Note that passing in the same
    /// ID twice (or to `nack` additionally) may result in a no-op,
    /// error, or panic, depending on the implementation.
    async fn ack(self: Pin<&mut Self>, id: &MessageId) -> Result<(), Self::Error>;
    /// Negatively acknowledges the given message.
    ///
    /// This can cause the message to be re-delivered, depending on
    /// the implementation.  Note that passing in the same ID twice
    /// (or to `ack` additionally) may result in a no-op, error, or
    /// panic, depending on the implementation.
    async fn nack(self: Pin<&mut Self>, id: &MessageId) -> Result<(), Self::Error>;
    /// Performs a heartbeat.
    ///
    /// This is, generally, only required when a message has been
    /// received by `next`, but has not been acknowledged or
    /// negatively acknowledged. This causes us to inform the
    /// provider that we're still working on the message, and that
    /// it should not be re-delivered.  This doesn't
    /// take a message ID because it's assumed that the last message
    /// received is the one that we're working on - or for all
    /// received messages.
    async fn heartbeat(self: Pin<&mut Self>) -> Result<(), Self::Error>;
}
