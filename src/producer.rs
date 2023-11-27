#[async_trait::async_trait]
/// A provider.
///
/// All a provider must do is provide a way to insert into a named
/// queue; this is the `push` method.  Ideally a provider would not
/// maintain a persistent connection to the queue, but instead would
/// open a connection (or use a connection pool) when `push` is
/// called, and close it when `push` returns.
pub trait ProducerProvider {
    /// The error for the provider.
    ///
    /// This is used to return errors from the `push` method.
    type Error: std::error::Error;

    /// Pushes a value into the queue.
    ///
    /// The name is the name of the queue to push into, and the value
    /// is the value to push into the queue.  The value will
    /// likely be serialized by the provider.
    async fn push<V: serde::Serialize + Sync>(
        &mut self,
        name: &str,
        value: &V,
    ) -> Result<(), Self::Error>;
}
