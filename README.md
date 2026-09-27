# elephant

`elephant` is a Rust SDK for Absurd durable workflows on PostgreSQL.

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
use elephant::{client::Client, task::{Router, Task}, types::CreateQueueOptions};
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

# async fn example(pool: PgPool) -> elephant::error::Result<()> {
let mut builder = Client::builder(pool);
builder.default_queue("default")?;
let client = builder.build();
client.create_queue("default", CreateQueueOptions::default()).await?;

let task = Task::<Params, Output>::builder("double")?
    .queue("default")?
    .build();
let router = Router::new().task(task.handler(|context, params| async move {
        let value = context
            .step("double-v1", || async move { Ok(params.value * 2) })
            .await?;
        Ok(Output { value })
    }))?;
let spawned = client.spawn(&task, Params { value: 21 }).send().await?;
# let _ = (router, spawned);
# Ok(())
# }
```

`Task<P, R>` is a producer-facing contract: its builder does not require a
handler. Put contracts and JSON types in a shared application module or crate;
workers bind implementations with `task.handler(...)` to obtain a
`TaskRegistration<P, R>` for the router. The combined
`Task::builder(...).handler(...).build()` form remains available when producer
and worker live together.

Task IDs, run IDs, and `Spawned<R>` handles support `serde`. Handles serialize
only their queue and identifiers, not `R`, and can be stored in durable steps.
Deserialized names are validated. Use a stable idempotency key when spawning a
child inside a step: a crash can occur between spawning and checkpointing its
handle.

## Atomic enqueueing

`SpawnBuilder::send_on`, `Client::spawn_untyped_on`, and `Client::emit_event_on`
accept `&mut sqlx::PgConnection`, including a dereferenced caller-owned
transaction. They never commit. The pool-backed methods delegate to the same
implementations. Execution and administrative operations remain pool-backed.

```rust,no_run
use elephant::{client::Client, task::Task};

# async fn example(client: Client, task: Task<i32, i32>) -> elephant::error::Result<()> {
let mut transaction = client.pool().begin().await?;
sqlx::query("UPDATE orders SET status = 'queued' WHERE id = $1")
    .bind(42)
    .execute(&mut *transaction)
    .await?;
let spawned = client.spawn(&task, 42).send_on(&mut transaction).await?;
transaction.commit().await?;
# let _ = spawned;
# Ok(())
# }
```

The task becomes visible to workers only after commit. Do not await its result
before committing, and do not use a handle from a rolled-back transaction.

## Manual claim loop

```rust,no_run
use futures::StreamExt;
use elephant::{client::Client, task::Router, worker::ClaimOptions};

# async fn example(client: Client, router: Router) -> elephant::error::Result<()> {
let mut claims = client.claims("default", ClaimOptions::default());
while let Some(lease) = claims.next().await {
    router.dispatch(lease?).await?;
}
# Ok(())
# }
```

## Convenience worker

```rust,no_run
use elephant::{client::Client, task::Router};
use tokio_util::sync::CancellationToken;

# async fn example(client: Client, router: Router) -> elephant::error::Result<()> {
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

## Execution instrumentation

Every router dispatch has a `tracing` span carrying queue, task ID, run ID,
attempt, and task name. Parameters and headers are never logged automatically.
Handlers and execution wrappers can inspect `context.metadata().headers`.

`Router::wrap_execution` establishes application context around every handler,
including when using the convenience worker. For distributed tracing, extract
your tracing carrier from headers in this wrapper and instrument the returned
execution future with your application's span. Preserve errors unchanged:
suspension and owning-run cancellation are runtime control flow, not failures.
Use `SpawnBuilder::headers` from your application's enqueue wrapper to inject
carriers; this also works with `send_on` and caller-owned transactions.

```rust,no_run
use elephant::task::Router;
use tracing::Instrument;

let router = Router::new().wrap_execution(|context, execute| async move {
    let span = tracing::error_span!("application_task", task_id = %context.metadata().task_id);
    execute.instrument(span).await
});
# let _ = router;
```

## SQLx policy

Most Absurd calls target stored procedures and dynamic queue tables. `elephant` keeps
runtime-checked `sqlx` queries small, maps rows into typed structs immediately,
and covers those calls with `pgdb` integration tests against real PostgreSQL.
