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

pub struct Consumer<T, P, S> {
    provider: P,
    service: S,
    config: Config,
    _phantom: std::marker::PhantomData<T>,
}

impl Consumer<(), (), ()> {
    pub fn build<T, P>(provider: P) -> ConsumerBuilder<T, P, ()> {
        ConsumerBuilder {
            provider,
            service: (),
            config: Config::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<
        T: Send + 'static,
        P: ConsumerProvider<T>,
        S: tower::MakeService<(), Message<T>, Response = (), Error = ()>,
    > Consumer<T, P, S>
where
    T: serde::de::DeserializeOwned,
    P::Stream: Send + 'static,
    S::Service: Send + 'static,
    S::Future: Send,
    <S::Service as tower::Service<Message<T>>>::Future: Send,
    P::Error: std::error::Error + Send + Sync + 'static,
    S::MakeError: std::error::Error + Send + Sync + 'static,
    <<P as ConsumerProvider<T>>::Stream as ConsumerStream<T>>::Error: Send + Sync + 'static,
{
    pub async fn run(
        mut self,
        consumer: &str,
        spawn: usize,
    ) -> Result<Spawn<'static>, anyhow::Error> {
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

impl<T, P: std::fmt::Debug, S: std::fmt::Debug> std::fmt::Debug for Consumer<T, P, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("provider", &self.provider)
            .field("service", &self.service)
            .field("config", &self.config)
            .finish()
    }
}

async fn process_stream<T, C, S>(
    stream: C,
    mut service: S,
    mut rx: watch::Receiver<bool>,
    config: Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: serde::de::DeserializeOwned,
    C: ConsumerStream<T>,
    S: tower::Service<Message<T>, Response = (), Error = ()>,
    S::Future: Send + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
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

async fn process_message<T, C, S>(
    mut stream: Pin<&mut C>,
    service: &mut S,
    config: &Config,
    message: Message<T>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    C: ConsumerStream<T>,
    T: serde::de::DeserializeOwned,
    S: tower::Service<Message<T>, Response = (), Error = ()>,
    S::Future: Send + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
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
        Err(e) => Err(e.into()),
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

type TaskResult = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;

#[must_use]
pub struct Spawn<'shutdown> {
    tasks: Vec<Pin<Box<dyn Future<Output = TaskResult>>>>,
    tx: Option<watch::Sender<bool>>,
    stop: Pin<Box<dyn Future<Output = ()> + 'shutdown>>,
}

impl<'shutdown> Spawn<'shutdown> {
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

    pub async fn wait(self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
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

impl std::fmt::Debug for Spawn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spawn").finish_non_exhaustive()
    }
}
