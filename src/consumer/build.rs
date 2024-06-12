use super::{Config, Consumer, ConsumerProvider, Message};
use crate::ConsumerStream;
use std::time::Duration;

pub struct ConsumerBuilder<T, P, S, E> {
    pub(super) provider: P,
    pub(super) service: S,
    pub(super) config: super::Config,
    pub(super) _message: std::marker::PhantomData<T>,
    pub(super) _error: std::marker::PhantomData<fn() -> E>,
}

impl<T, P, S, E> ConsumerBuilder<T, P, S, E> {
    pub fn with_provider<NP>(self, provider: NP) -> ConsumerBuilder<T, NP, S, E>
    where
        T: serde::de::DeserializeOwned,
        NP: ConsumerProvider<T>,
        E: From<NP::Error>,
    {
        ConsumerBuilder {
            provider,
            service: self.service,
            config: self.config,
            _message: std::marker::PhantomData,
            _error: std::marker::PhantomData,
        }
    }

    pub fn with_service<NS>(self, service: NS) -> ConsumerBuilder<T, P, NS, E>
    where
        NS: tower::MakeService<(), Message<T>, Response = (), Error = ()>,
    {
        ConsumerBuilder {
            provider: self.provider,
            config: self.config,
            service,
            _message: std::marker::PhantomData,
            _error: std::marker::PhantomData,
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
        E,
    > ConsumerBuilder<T, P, S, E>
where
    E: From<<<P as ConsumerProvider<T>>::Stream as ConsumerStream<T>>::Error> + 'static,
{
    pub fn build(self) -> Consumer<T, P, S, E> {
        Consumer {
            provider: self.provider,
            service: self.service,
            config: self.config,
            _message: std::marker::PhantomData,
            _error: std::marker::PhantomData,
        }
    }
}

impl<T, P: std::fmt::Debug, S: std::fmt::Debug, E> std::fmt::Debug for ConsumerBuilder<T, P, S, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerBuilder")
            .field("provider", &self.provider)
            .field("service", &self.service)
            .field("config", &self.config)
            .finish()
    }
}
