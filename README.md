# TQ

TQ is a Rust library to manage a task queue.

It is currently built for low-throughput queues, where the number of tasks
processed per second is low, but the number of tasks in the queue is high.

Right now, the only provider for TQ is Redis, but more providers can be added
in the future.

## Usage

The library is split into two parts: the producer, and the consumer.  To use
the former, the `producer` feature must be enabled; and the latter, `consumer`.
Each provide the opportunity to specify the name of the queue, with some
producer specific options available for both.  This readme will assume you're
using the `redis` provider (enabled by the `redis` feature).

### Producer

The producer is used to add tasks to the queue.  It can be used as follows:

```rust
use redis::Client;
use tq::producer::redis::RedisProducer;

// A redis client only contains the information to connect to Redis; it does
// not yet connect to redis.
let client = Client::open("redis://127.0.0.1/0")?;
// The maximum length of the stream.  If the stream is longer than this, the
// `push` operation may silently truncate the stream in some cases - in others
// it will error.
let stream_max_len = 1000;
// Create the producer.
let mut producer = RedisProducer::new(client, stream_max_len);

// The task must only be serializable/deserializable by serde (and
// Send + Sync + 'static).
#[derive(Serialize, Deserialize)]
struct Task {
    id: u64,
    /* ... */
}

// Push the task to the queue.  This connects to redis and creates the stream
// if it does not exist.  The connection only lasts for the length of the
// push.
producer.push("my_queue", &Task { id: 0, /* ... */ }).await?;
// Alternatively, you can bypass the lag check and push the task to the queue
// immediately.  This will still silently truncate the stream if it is longer
// than `stream_max_len`.
producer.push_unchecked("my_queue", &Task { id: 1, /* ... */ }).await?;
```

There's not much in the way of configuration here, apart from the
`stream_max_len`.  The only interesting thing we can check is the lag, via
`producer.current_lag()`, which returns the estimated maximum lag of all of
the groups, in messages.  This is an estimate, and may be inaccurate.

### Consumer

The consumer is used to consume tasks from the queue.  This portion is far
more complex than the producer, and has many more options.  It can be used as
follows:

```rust
use redis::Client;
use tq::consumer::redis::RedisConsumer;

let client = Client::open("redis://127.0.0.1/0")?;
let mut consumer = RedisConsumer::new(
    client,
    // The name of the queue.  This should match what was passed in to the
    // producer.
    "my_queue".to_string(),
    // The name of the consuemr group to use.
    "my_group".to_string(),
    // The idle time of a message.  If a message is not acknowledged within
    // this time, it will be requeued (notwithstanding heartbeats).
    Duration::from_secs(60 * 60)
);

// This creates the queue and the consumer group within the queue.  Note that
// the group doesn't begin tracking position until it is created, so lag will
// always be 0 until this is called. (It will then immediately jump to the
// length of the queue.)
consumer.create().await?;

// The task must only be serializable/deserializable by serde (and
// Send + Sync + 'static).
#[derive(Serialize, Deserialize)]
struct Task {
    id: u64,
    /* ... */
}

// To process messages, we must create a tower service.  This is a bit
// complicated, but it's not too bad - `tower::service_fn` makes this much
// easier.
let service = tower::service_fn(|()| async move {
    Ok::<_, core::convert::Infallible>(tower::service_fn(|message: tq::Message<Task>| async move {
        // Do something with the message.
        println!("Got message: {:?}", message);
        // `Ok` means the message should be ack'd, `Err` means it should be
        // nack'd.
        Ok::<(), ()>(())
    }))
});

let consumer = Consumer::build::<Task, _>(consumer)
    .with_service(service)
    // The keep alive interval between heartbeats.  A good default is half of
    // the idle timeout above.
    .with_keep_alive_interval(Duration::from_secs(60 * 30))
    // The amount of time to wait for a message from redis before looping
    // again.
    .with_poll_interval(Duration::from_secs(60))
    .build();

// "main" is the name of the consumer within the consuemr group to use.
// This will not be the actual name - it will instead be `"main-1"`, `"main-2"`,
// one for each of the consumer this spawns.  The second argument is the number
// of consumers to spawn, equivalent to the parallelism of the consumer.
//
// This does not start driving the future, it only creates it.  To drive it,
// you will need to call `wait` on spawn.
let spawn = consumer.run("main", 1).await?;
// Begin graceful shutdown on ctrl+c.
let spawn = spawn.with_graceful_shutdown(async move {
    tokio::signal::ctrl_c().await.ok();
});
let spawn = spawn.wait().await?;
```
