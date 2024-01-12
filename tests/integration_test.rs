use bb8_redis::RedisConnectionManager;
use rand::Rng as _;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tower::service_fn;
use tq::{Consumer, Message, ProducerProvider as _};

async fn connect(key: &str) -> (redis::Client, bb8::Pool<RedisConnectionManager>) {
    let client = redis::Client::open("redis://127.0.0.1/0").expect("redis client");
    // ensure that we can actually connect to redis.  drop it immediately
    // after to disconnect.
    let mut conn = client
        .get_async_connection()
        .await
        .expect("redis connection");

    let _: () = redis::cmd("FLUSHDB")
        .arg("SYNC")
        .query_async(&mut conn)
        .await
        .expect("flushdb");

    let mut consumer = tq::redis::RedisConsumer::new(
        client.clone(),
        key.to_string(),
        "a".to_string(),
        Duration::from_secs(2),
    );

    // this needs to be run before we can push into the consumer, because
    // otherwise the lag will always be 0!
    consumer.create().await.expect("provider create");

    (
        client,
        bb8::Pool::builder()
            .build(
                bb8_redis::RedisConnectionManager::new("redis://127.0.0.1/0")
                    .expect("redis connection manager"),
            )
            .await
            .expect("redis pool"),
    )
}

fn queue_key() -> String {
    let rng = rand::thread_rng();
    let v: Vec<u8> = rng
        .sample_iter(rand::distributions::Alphanumeric)
        .take(16)
        .collect();
    let v = String::from_utf8(v).expect("from_utf8");
    format!("test-queue:{v}")
}

#[tokio::test]
async fn test_standard() {
    let key = queue_key();
    let (client, pool) = connect(&key).await;
    let mut producer = tq::redis::RedisProducer::new(pool.clone(), 128);

    for i in 0..64 {
        producer.push(&key, &Task { count: i }).await.expect("push");
    }

    let mut consumer =
        tq::redis::RedisConsumer::new(client, key.clone(), "a".to_string(), Duration::from_secs(2));

    consumer.create().await.expect("provider create");

    let (tx, mut rx) = tokio::sync::broadcast::channel(1);
    let mkservice = AtomicBool::new(false);

    let service = service_fn(|()| {
        let mkservice = &mkservice;
        let tx = tx.clone();
        async move {
            assert!(
                !mkservice.fetch_or(true, Ordering::SeqCst),
                "mkservice created another service?"
            );
            Ok::<_, core::convert::Infallible>(CounterService::new(Arc::new(AtomicU64::new(0)), tx))
        }
    });

    let consumer = Consumer::build::<Task, _>(consumer)
        .with_service(service)
        .with_keep_alive_interval(Duration::from_secs(1))
        .with_poll_timeout(Duration::from_secs(1))
        .build();

    let fut = async move { while !rx.recv().await.expect("recv") {} };

    consumer
        .run("main", 1)
        .await
        .expect("run")
        .with_graceful_shutdown(fut)
        .wait()
        .await
        .expect("failed to process tasks");

    assert_eq!(0, producer.current_lag(&key).await.expect("current-lag"));
}

#[tokio::test]
async fn test_overflow() {
    let key = queue_key();
    let (_, pool) = connect(&key).await;
    let mut provider = tq::redis::RedisProducer::new(pool.clone(), 64);

    for i in 0..64 {
        provider.push(&key, &Task { count: i }).await.expect("push");
    }

    let lag = provider.current_lag(&key).await.expect("current-lag");

    assert_eq!(lag, 64);

    provider
        .push(&key, &Task { count: 64 })
        .await
        .expect_err("should fail from overflow");
}

#[tokio::test]
async fn test_nack() {
    let key = queue_key();
    let (client, pool) = connect(&key).await;
    let mut producer = tq::redis::RedisProducer::new(pool.clone(), 128);

    for i in 0..64 {
        producer.push(&key, &Task { count: i }).await.expect("push");
    }

    let mut consumer =
        tq::redis::RedisConsumer::new(client, key.clone(), "a".to_string(), Duration::from_secs(2));

    consumer.create().await.expect("provider create");

    let (tx, mut rx) = tokio::sync::broadcast::channel(1);
    let mkservice = AtomicBool::new(false);

    let service = service_fn(|()| {
        let mkservice = &mkservice;
        let tx = tx.clone();
        async move {
            assert!(
                !mkservice.fetch_or(true, Ordering::SeqCst),
                "mkservice created another service?"
            );
            Ok::<_, core::convert::Infallible>(FailCounterService::new(
                Arc::new(AtomicU64::new(0)),
                tx.clone(),
            ))
        }
    });

    let consumer = Consumer::build::<Task, _>(consumer)
        .with_service(service)
        .with_keep_alive_interval(Duration::from_secs(1))
        .with_poll_timeout(Duration::from_secs(1))
        .build();

    let fut = async move { while !rx.recv().await.expect("recv") {} };

    consumer
        .run("main", 1)
        .await
        .expect("run")
        .with_graceful_shutdown(fut)
        .wait()
        .await
        .expect("failed to process tasks");

    assert_eq!(63, producer.current_lag(&key).await.expect("current-lag"));
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Task {
    count: u64,
}

// Extremely sub-optimal, as we have to clone for each message,
// despite the fact it won't be shared across messages.  But this is a
// test, so we don't have to worry to much about it.
struct CounterService {
    count: Arc<AtomicU64>,
    tx: tokio::sync::broadcast::Sender<bool>,
}

impl CounterService {
    fn new(count: Arc<AtomicU64>, tx: tokio::sync::broadcast::Sender<bool>) -> Self {
        Self { count, tx }
    }
}

impl tower::Service<Message<Task>> for CounterService {
    type Error = ();
    type Future = Pin<
        Box<
            dyn std::future::Future<Output = Result<Self::Response, Self::Error>>
                + Send
                + Sync
                + 'static,
        >,
    >;
    type Response = ();

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, message: Message<Task>) -> Self::Future {
        let count = self.count.clone();
        let tx = self.tx.clone();
        Box::pin(async move {
            let old = count.fetch_add(1, Ordering::SeqCst);
            assert_eq!(message.data.count, old);
            if old == 63 {
                tx.send(true).expect("send");
            }
            assert!(old < 64, "count exceeded 64");
            Ok(())
        })
    }
}

struct FailCounterService {
    count: Arc<AtomicU64>,
    tx: tokio::sync::broadcast::Sender<bool>,
}

impl FailCounterService {
    fn new(count: Arc<AtomicU64>, tx: tokio::sync::broadcast::Sender<bool>) -> Self {
        Self { count, tx }
    }
}

impl tower::Service<Message<Task>> for FailCounterService {
    type Error = ();
    type Future = Pin<
        Box<
            dyn std::future::Future<Output = Result<Self::Response, Self::Error>>
                + Send
                + Sync
                + 'static,
        >,
    >;
    type Response = ();

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, message: Message<Task>) -> Self::Future {
        let count = self.count.clone();
        let tx = self.tx.clone();
        Box::pin(async move {
            let old = count.fetch_add(1, Ordering::SeqCst);
            assert_eq!(message.retries, old);
            assert_eq!(message.data.count, 0);
            if old == 63 {
                tx.send(true).expect("send");
            }
            assert!(old < 64, "count exceeded 64");
            Err(())
        })
    }
}
