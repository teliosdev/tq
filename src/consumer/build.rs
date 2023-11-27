use super::{Config, Consumer, ConsumerProvider, Message};
use std::time::Duration;

pub struct ConsumerBuilder<T, P, S> {
    pub(super) provider: P,
    pub(super) service: S,
    pub(super) config: super::Config,
    pub(super) _phantom: std::marker::PhantomData<T>,
}

impl<T, P, S> ConsumerBuilder<T, P, S> {
    pub fn with_provider<NP>(self, provider: NP) -> ConsumerBuilder<T, NP, S>
    where
        T: serde::de::DeserializeOwned,
        NP: ConsumerProvider<T>,
    {
        ConsumerBuilder {
            provider,
            service: self.service,
            config: self.config,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn with_service<NS>(self, service: NS) -> ConsumerBuilder<T, P, NS>
    where
        NS: tower::MakeService<(), Message<T>, Response = (), Error = ()>,
    {
        ConsumerBuilder {
            provider: self.provider,
            config: self.config,
            service,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn with_keep_alive_interval(self, keep_alive_interval: Duration) -> Self {
        let config = Config {
            keep_alive_interval,
            ..self.config
        };

        Self { config, ..self }
    }

    pub fn with_poll_timeout(self, poll_timeout: Duration) -> Self {
        let config = Config {
            poll_timeout,
            ..self.config
        };

        Self { config, ..self }
    }
}

impl<
        T: serde::de::DeserializeOwned,
        P: ConsumerProvider<T>,
        S: tower::MakeService<(), Message<T>, Response = (), Error = ()>,
    > ConsumerBuilder<T, P, S>
{
    pub fn build(self) -> Consumer<T, P, S> {
        Consumer {
            provider: self.provider,
            service: self.service,
            config: self.config,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T, P: std::fmt::Debug, S: std::fmt::Debug> std::fmt::Debug for ConsumerBuilder<T, P, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerBuilder")
            .field("provider", &self.provider)
            .field("service", &self.service)
            .field("config", &self.config)
            .finish()
    }
}
