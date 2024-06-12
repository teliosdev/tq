mod build;
mod message;
mod traits;

pub use self::build::ConsumerBuilder;
pub use self::message::{Message, MessageId};
pub use self::traits::{ConsumerProvider, ConsumerStream};
use futures::TryStreamExt;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::watch;

pub struct Consumer<T, P, S, E> {
    provider: P,
    service: S,
    config: Config,
    _message: std::marker::PhantomData<T>,
    _error: std::marker::PhantomData<fn() -> E>,
}

impl Consumer<(), (), (), ()> {
    pub fn build<T, E, P>(provider: P) -> ConsumerBuilder<T, P, (), E>
    where
        T: serde::de::DeserializeOwned,
        P: ConsumerProvider<T>,
        E: From<P::Error>,
    {
        ConsumerBuilder {
            provider,
            service: (),
            config: Config::default(),
            _message: std::marker::PhantomData,
            _error: std::marker::PhantomData,
        }
    }
}

impl<
        T: Send + 'static,
        P: ConsumerProvider<T>,
        S: tower::MakeService<(), Message<T>, Response = (), Error = ()>,
        E,
    > Consumer<T, P, S, E>
where
    T: serde::de::DeserializeOwned,
    P::Stream: Send + 'static,
    S::Service: Send + 'static,
    S::Future: Send,
    <S::Service as tower::Service<Message<T>>>::Future: Send,
    P::Error: std::error::Error + Send + Sync + 'static,
    S::MakeError: std::error::Error + Send + Sync + 'static,
    <<P as ConsumerProvider<T>>::Stream as ConsumerStream<T>>::Error: Send + Sync + 'static,
    E: From<<<P as ConsumerProvider<T>>::Stream as ConsumerStream<T>>::Error> + 'static,
{
    pub async fn run(
        mut self,
        consumer: &str,
        spawn: usize,
    ) -> Result<Spawn<'static, E>, anyhow::Error> {
        let (tx, rx) = watch::channel(false);

        let mut spawns = Spawn {
            tasks: Vec::with_capacity(spawn),
            tx: Some(tx),
            stop: Box::pin(futures::future::pending()),
        };

        for i in 0..spawn {
            let stream = self.provider.stream(&format!("{consumer}-{i}")).await?;
            let service = self.service.make_service(()).await?;

            spawns.tasks.push(Box::pin(process_stream(
                stream,
                service,
                rx.clone(),
                self.config.clone(),
            )));
        }

        Ok(spawns)
    }
}

impl<T, P: std::fmt::Debug, S: std::fmt::Debug, E> std::fmt::Debug for Consumer<T, P, S, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("provider", &self.provider)
            .field("service", &self.service)
            .field("config", &self.config)
            .finish()
    }
}

#[tracing::instrument(skip_all, name = "queue.poll")]
async fn process_stream<T, C, S, E>(
    stream: C,
    mut service: S,
    mut rx: watch::Receiver<bool>,
    config: Config,
) -> Result<(), E>
where
    T: serde::de::DeserializeOwned,
    C: ConsumerStream<T>,
    S: tower::Service<Message<T>, Response = (), Error = ()>,
    S::Future: Send + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
    E: From<C::Error>,
{
    let mut stream = std::pin::pin!(stream);

    loop {
        let message = tokio::select! {
            _ = rx.changed() => {
                break;
            }
            message = stream.as_mut().next(config.poll_timeout) => { message? }
        };

        match message {
            Some(message) => {
                process_message(stream.as_mut(), &mut service, &config, message).await?;
            }
            None if config.poll_once => break,
            None => {}
        }
    }

    Ok(())
}

async fn process_message<T, C, S, E>(
    mut stream: Pin<&mut C>,
    service: &mut S,
    config: &Config,
    message: Message<T>,
) -> Result<(), E>
where
    C: ConsumerStream<T>,
    T: serde::de::DeserializeOwned,
    S: tower::Service<Message<T>, Response = (), Error = ()>,
    S::Future: Send + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
    E: From<C::Error>,
{
    let id = message.id.clone();
    let mut task = tokio::spawn(service.call(message));
    let mut keep_alive = tokio::time::interval_at(
        tokio::time::Instant::now() + config.keep_alive_interval,
        config.keep_alive_interval,
    );

    let task_result = loop {
        tokio::select! {
            result = &mut task => break result,
            _ = keep_alive.tick() => {
                stream.as_mut().heartbeat().await?;
            }
        }
    };

    match task_result {
        Ok(Ok(())) => {
            stream.ack(&id).await?;
            Ok(())
        }
        Ok(Err(())) => {
            stream.nack(&id).await?;
            Ok(())
        }
        Err(e) => Err(C::Error::from(e).into()),
    }
}

#[derive(Debug, Clone)]
struct Config {
    keep_alive_interval: std::time::Duration,
    poll_timeout: std::time::Duration,
    poll_once: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            keep_alive_interval: std::time::Duration::from_secs(30),
            poll_timeout: std::time::Duration::from_secs(30),
            poll_once: false,
        }
    }
}

type Task<E> = Pin<Box<dyn Future<Output = Result<(), E>>>>;

#[must_use]
pub struct Spawn<'shutdown, E> {
    tasks: Vec<Task<E>>,
    tx: Option<watch::Sender<bool>>,
    stop: Pin<Box<dyn Future<Output = ()> + 'shutdown>>,
}

impl<'shutdown, E> Spawn<'shutdown, E> {
    pub fn with_graceful_shutdown<F>(self, future: F) -> Self
    where
        F: Future<Output = ()> + 'shutdown,
    {
        if let Some(tx) = self.tx {
            let fut = Box::pin(async move {
                future.await;
                tx.send(true).ok();
            });

            Self {
                tasks: self.tasks,
                tx: None,
                stop: fut,
            }
        } else {
            self
        }
    }

    pub async fn wait(self) -> Result<(), E> {
        let tasks = futures::stream::FuturesUnordered::from_iter(self.tasks).try_collect::<()>();
        let stop = self.stop;
        let stop = async move {
            stop.await;
            Ok(())
        };
        futures::try_join!(tasks, stop)?;
        Ok(())
    }
}

impl<E> std::fmt::Debug for Spawn<'_, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spawn").finish_non_exhaustive()
    }
}
