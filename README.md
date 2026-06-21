# room

`room` is a Rust SDK for Absurd durable workflows on PostgreSQL.

The library exposes Absurd's native model: tasks, runs, checkpoints, sleeps,
events, retries, cancellation, and queue operations. Workers are convenience
helpers over public claim streams, so applications can own their supervision,
backpressure, and shutdown logic.

Absurd execution is at least once. Code outside durable steps may run more than
once, and external side effects need application-level idempotency keys. A
completed checkpoint is a durable compatibility contract: reusing a checkpoint
name with different semantics can replay old data into new code.

Production applications should install `absurd.sql` with their normal migration
system. `Client::builder` only wraps an existing `sqlx::PgPool`; it does not run
schema migrations.

## Typed tasks

```rust,no_run
use room::{client::Client, task::{Router, Task}, types::CreateQueueOptions};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Deserialize, Serialize)]
struct Params {
    value: i32,
}

#[derive(Deserialize, Serialize)]
struct Output {
    value: i32,
}

# async fn example(pool: PgPool) -> room::error::Result<()> {
let mut builder = Client::builder(pool);
builder.default_queue("default")?;
let client = builder.build();
client.create_queue("default", CreateQueueOptions::default()).await?;

let task = Task::<Params, Output>::builder("double")?
    .queue("default")?
    .handler(|context, params| async move {
        let value = context
            .step("double-v1", || async move { Ok(params.value * 2) })
            .await?;
        Ok(Output { value })
    })
    .build();
let router = Router::new().task(task.clone())?;
let spawned = client.spawn(&task, Params { value: 21 }).send().await?;
# let _ = (router, spawned);
# Ok(())
# }
```

## Manual claim loop

```rust,no_run
use futures::StreamExt;
use room::{client::Client, task::Router, worker::ClaimOptions};

# async fn example(client: Client, router: Router) -> room::error::Result<()> {
let mut claims = client.claims("default", ClaimOptions::default());
while let Some(lease) = claims.next().await {
    router.dispatch(lease?).await?;
}
# Ok(())
# }
```

## Convenience worker

```rust,no_run
use room::{client::Client, task::Router};
use tokio_util::sync::CancellationToken;

# async fn example(client: Client, router: Router) -> room::error::Result<()> {
let shutdown = CancellationToken::new();
client.worker(router).concurrency(8).run(shutdown).await?;
# Ok(())
# }
```

The convenience worker automatically extends active claims. Use the manual claim
stream when the application needs custom supervision or backpressure.

## Sleeps and events

`TaskContext::sleep_for` and `TaskContext::sleep_until` use a default durable
sleep checkpoint. Use `sleep_for_named` or `sleep_until_named` when a workflow has
multiple sleeps or sleeps in a loop.

`TaskContext::await_event` derives a checkpoint name from the event name. Use
`await_event_named` when the same event can be awaited in multiple places.

Waiting for a task result from the same queue inside a task is rejected because it
can deadlock a worker pool. Cross-queue waits are available through
`TaskContext::await_task_result`.

## SQLx policy

Most Absurd calls target stored procedures and dynamic queue tables. `room` keeps
runtime-checked `sqlx` queries small, maps rows into typed structs immediately,
and covers those calls with `pgdb` integration tests against real PostgreSQL.
