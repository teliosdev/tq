use crate::producer::ProducerProvider;
use crate::redis::data::StreamInfoGroupsReply;
use redis::AsyncCommands;

#[derive(Debug, Clone)]
pub struct RedisProducer {
    client: bb8::Pool<bb8_redis::RedisConnectionManager>,
    max_stream_length: u32,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RedisProducerError {
    #[error("failed to connect to redis: {source}")]
    Connection {
        #[source]
        source: bb8::RunError<redis::RedisError>,
    },
    #[error("failed to load the current lag from the redis server: {source}")]
    GetCurrentLag {
        #[source]
        source: redis::RedisError,
    },
    #[error("the current lag of the stream exceeds the limit: {source}")]
    LagExceedsLimit {
        #[source]
        source: eyre::Report,
    },
    #[error("failed to serialize the data: {source}")]
    SerializationFailure {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to append item to stream: {source}")]
    StreamAppend {
        #[source]
        source: redis::RedisError,
    },
}

impl RedisProducer {
    /// Creates a new producer.
    ///
    /// The client should be configured with the appropriate
    /// connection information.
    ///
    /// For more information on the max stream length, and redis
    /// streams and how they're used here, please see the
    /// [modul-level documentation](crate::redis).
    #[must_use = "a producer does nothing unless used"]
    pub fn new(
        client: bb8::Pool<bb8_redis::RedisConnectionManager>,
        max_stream_length: u32,
    ) -> Self {
        Self {
            client,
            max_stream_length,
        }
    }

    /// Retrieves the current lag from the redis server.
    ///
    /// This is calculated from the `XINFO GROUPS` command, which
    /// returns the lag of each group.  If the lag is null, we
    /// assume that the lag is equivalent to the stream length
    /// minus the entries read, as this is the most conservative
    /// estimate.
    pub async fn current_lag(&mut self, key: &str) -> Result<u64, RedisProducerError> {
        let mut connection = self.connection().await?;

        self.current_lag_from(&mut connection, key).await
    }

    /// Pushes unchecked into the stream.
    ///
    /// This similar to the [`ProducerProvider::push`] implementation,
    /// but the key difference is that it does not check if the
    /// target stream can hold the data.  This is, of course,
    /// faster, since it doesn't have to check the lag of the
    /// stream, but it also means that if the stream is full and
    /// the lag exceeds the limit, data will be lost.
    pub async fn push_unchecked<V: serde::Serialize + Sync>(
        &self,
        key: &str,
        value: &V,
    ) -> Result<(), RedisProducerError> {
        let serialized = serde_json::to_string(&value)
            .map_err(|source| RedisProducerError::SerializationFailure { source })?;
        let mut connection = self.connection().await?;

        self.push_unchecked_into(&mut connection, serialized, key)
            .await
    }

    async fn connection(
        &self,
    ) -> Result<bb8::PooledConnection<'_, bb8_redis::RedisConnectionManager>, RedisProducerError>
    {
        self.client
            .get()
            .await
            .map_err(|source| RedisProducerError::Connection { source })
    }

    /// Attempts to calculate the current total lag of the consumers.
    ///
    /// This iterates through all of the groups in the stream, and
    /// attempts to calculate the lag of each group.
    /// Unfortunately, lag was a feature added in 7.0.0, which may
    /// cause compatability issues.  Therefore, we gracefully
    /// degrade to nothing - however, that provides no protection.
    /// Lag can also be null, if an entry is deleted from the
    /// stream, or the group was started in the middle of the
    /// stream.  In this case, we attempt to calculate the lag by
    /// checking `entries-read` against the current stream length.
    /// Entries read, unfortunately, is not an actual count - it
    /// is instead calculated based off of the ID.  Which means that
    /// in the case of deletions - the exact thing that would cause
    /// the lag to be null - the entries read will be off.
    ///
    /// Ideally, we would be able to calculate the lag, but we can't
    /// always get what we want.  Also ideally, we would never lag
    /// behind enough to trigger the logic in [`push`].  These things
    /// are all meant as approximations, anyway, and not guarantees.
    async fn current_lag_from(
        &self,
        conn: &mut redis::aio::Connection,
        key: &str,
    ) -> Result<u64, RedisProducerError> {
        let stream_length: u64 = redis::cmd("XLEN")
            .arg(key)
            .query_async(conn)
            .await
            .map_err(|source| RedisProducerError::GetCurrentLag { source })?;

        // it can be zero if the stream doesn't exist, but more importantly,
        // if there are no items in the stream, there can be no lag.
        if stream_length == 0 {
            return Ok(0);
        }

        let stream_info: StreamInfoGroupsReply = conn
            .xinfo_groups(key)
            .await
            .map_err(|source| RedisProducerError::GetCurrentLag { source })?;

        // if there are no groups, there is no lag.  Magic!
        let mut current: u64 = 0;

        for group in stream_info.groups {
            if let Some(lag) = group.lag {
                current = current.max(lag);
            } else if let Some(read) = group.entries_read {
                current = current.max(stream_length - read);
            } else {
                tracing::warn!("group has no lag or entries read; this may cause issues!");
                return Ok(0);
            }
        }

        Ok(current)
    }

    async fn push_unchecked_into(
        &self,
        connection: &mut bb8::PooledConnection<'_, bb8_redis::RedisConnectionManager>,
        serialized: String,
        key: &str,
    ) -> Result<(), RedisProducerError> {
        (connection)
            .xadd_maxlen(
                key,
                redis::streams::StreamMaxlen::Approx(self.max_stream_length as usize),
                "*",
                &[("data", serialized)],
            )
            .await
            .map_err(|source| RedisProducerError::StreamAppend { source })
    }
}

#[async_trait::async_trait]
impl ProducerProvider for RedisProducer {
    type Error = RedisProducerError;

    async fn push<V: serde::Serialize + Sync>(
        &mut self,
        key: &str,
        value: &V,
    ) -> Result<(), Self::Error> {
        let serialized = serde_json::to_string(&value)
            .map_err(|source| RedisProducerError::SerializationFailure { source })?;
        let mut connection = self.connection().await?;
        let current_lag = self.current_lag_from(&mut connection, key).await?;

        if current_lag >= u64::from(self.max_stream_length) {
            return Err(RedisProducerError::LagExceedsLimit {
                source: eyre::eyre!(
                    "lag of {current_lag} exceeds the limit of {}",
                    self.max_stream_length
                ),
            });
        }

        self.push_unchecked_into(&mut connection, serialized, key)
            .await?;

        Ok(())
    }
}
