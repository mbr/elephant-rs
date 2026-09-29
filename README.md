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

The convenience worker inherits the client's default queue (or `default`) unless
`.queue(...)` overrides it, and automatically extends active claims. Each claim batch is
bounded by free execution slots. Shutdown stops new claims and drains both
issued claim queries and active executions; it does not cancel handlers.
Infrastructure errors likewise stop claiming, drain active work, and return to
the application supervisor. Handler failures are recorded for database retry.
Use `.worker_id(instance_id)` to identify a process in persisted claims and
lease-expiry diagnostics; the default is `elephant-worker`. Choose identities
that distinguish worker instances across hosts and restarts.

## Database budgets and process supervision

Claim duration, execution deadlines, and cancellation grace do not bound a claim
query blocked inside PostgreSQL. Shutdown deliberately waits for issued claims,
including leases returned after shutdown was requested. Do not wrap the entire
worker in a timeout that drops its future: a claim could already have committed.

The application owns its SQLx pool and server-side execution limits. For example:

```rust,no_run
use elephant::{client::Client, task::Router};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

# async fn example(database_url: &str, instance_id: &str, router: Router, shutdown: CancellationToken) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let options = database_url.parse::<PgConnectOptions>()?
    .application_name(instance_id)
    .options([("statement_timeout", "5000"), ("lock_timeout", "2000")]);
let pool = PgPoolOptions::new()
    .max_connections(16)
    .acquire_timeout(Duration::from_secs(2))
    .connect_with(options)
    .await?;
let client = Client::builder(pool).build();
client.worker(router).worker_id(instance_id).run(shutdown).await?;
# Ok(())
# }
```

These are illustrative budgets, not SDK defaults. They apply to all queries on
this pool, including application SQL. Choose limits that accommodate legitimate
work and enough connections for claims, handlers, and renewal. Pool acquisition
timeouts do not bound SQL execution. PostgreSQL statement/lock timeouts cancel
and roll back a blocked claim statement; its error reaches the worker supervisor.
They do not bound every network failure, including a lost response after commit.

Translate termination signals into the worker's `CancellationToken`, await its
result, and let an external process supervisor enforce a final hard-stop budget.
For example, an application-managed systemd service can use:

```ini
[Service]
KillSignal=SIGTERM
TimeoutStopSec=20s
SendSIGKILL=yes
KillMode=control-group
Restart=on-failure
RestartSec=2s
```

The application must handle `SIGTERM` for graceful drain. After a hard stop,
unresolved leases recover through PostgreSQL; external effects still need
idempotency. These service settings illustrate supervision policy, not a complete
unit file or an in-process preemption guarantee.

## Execution supervision

`Router::dispatch_with` and `RunLease::run_supervised` expose the same supervision
used by workers. `run::ExecutionOptions` selects automatic, custom, or disabled
background renewal, stalled-task recovery, an optional total dispatch deadline,
and a cancellation grace period. Automatic renewal uses the actual claimed
lease duration and local request start, accounting for time buffered before
dispatch. Locally expired claims are rejected before the handler starts. The
deadline covers one dispatch, not the workflow across sleeps and retries.

`TaskContext::cancellation_token()` signals cooperative cleanup. Detected lease
cancellation/failure, renewal errors, and execution deadlines signal this token;
after the grace period, the handler future is dropped. Local deadlines and local
token cancellation fail the run normally. Known database terminal states are
not failed again. Renewal infrastructure errors abandon the lease for database
recovery and return an error. A renewal request is bounded by the locally known
lease deadline; it cannot hang indefinitely while work continues unprotected.

By default, `StallTimeout::ClaimDuration` cancels a dispatch that makes no
application progress within its lease window (initially the claimed duration,
30 seconds by default). Successful checkpoint writes and explicit
`TaskContext::heartbeat` calls reset this window to the requested effective
lease duration. Successful event checkpoint writes also count as progress;
child-result polling performs explicit heartbeats. Cached reads, failed writes,
raw client calls, and automatic background renewal do not reset it.

A stalled handler receives cancellation, then is dropped after the grace period.
Background renewal continues during cleanup unless ownership is lost. The run
is failed normally, allowing database retry and releasing worker capacity. A
late success or heartbeat cannot rescue an already interrupted dispatch.
Terminal persistence has a separate claim-duration budget, so a blocked
completion/failure query cannot hold worker capacity indefinitely. Its timeout
returns `Error::RunResolutionTimeout`; the database outcome may be uncertain,
so it does not imply that a write was rolled back.

Use `.stall_timeout(duration)` or `StallTimeout::After(duration)` for a fixed
inactivity window. `StallTimeout::Disabled` explicitly opts into potentially
unbounded renewal; an overall `timeout` can still bound the dispatch. The overall
timeout is never reset by progress. Raw `RunLease::run_supervised` futures without
a routed context have no context progress notifications and remain bounded by
their initial stall window unless configured otherwise.

Blocking code cannot be preempted by an async deadline, and dropping a future
does not undo external effects or stop detached tasks. Handlers must cooperate,
avoid detached work, and use idempotency keys. The SDK never terminates the
process; blocking or native-code hangs require an external process supervisor.

```rust,no_run
use elephant::{client::Client, run::{ExecutionOptions, StallTimeout}, task::Router, worker::ClaimOptions};
use futures::StreamExt;
use std::time::Duration;

# async fn example(client: Client, router: Router) -> elephant::error::Result<()> {
let options = ExecutionOptions {
    timeout: Some(Duration::from_secs(300)),
    stall_timeout: StallTimeout::After(Duration::from_secs(60)),
    cancellation_grace: Duration::from_secs(2),
    ..Default::default()
};
let mut claims = client.claims("default", ClaimOptions::default());
while let Some(lease) = claims.next().await {
    router.dispatch_with(lease?, options.clone()).await?;
}
# Ok(())
# }
```

For workers, use `.execution(options)`, or `.execution_timeout(duration)` and
`.cancellation_grace(duration)`. Bare `Router::dispatch` and `work_batch` enforce
the default stall policy but do not renew in the background. Checkpoint writes
and durable child-result polling still renew their claims. `RunLease::run` is the
explicitly unsupervised primitive. Manual streams can buffer an already-claimed
batch: keep the batch within your immediately available
capacity, and do not abandon in-flight claim queries during shutdown.

## Sleeps and events

`TaskContext::sleep_for` and `TaskContext::sleep_until` use the base checkpoint
name `sleep`. Named variants choose a different base name. Repeated sleeps are
numbered automatically, including within loops.

`TaskContext::await_event` derives its base checkpoint name from the event name;
`await_event_named` selects one explicitly. Repeated waits get separate
checkpoints, but an event name remains immutable: this is not an event stream.

The shared Absurd protocol does not checkpoint event timeout outcomes. After
catching a timeout and suspending again, replay can accept a late event. To
preserve a fallback decision, wrap the wait and its timeout handling in
`TaskContext::step`, returning the decision as a successful value before any
later suspension. A checkpoint before the wait alone does not preserve its
outcome. This mitigation protects a committed decision, not the crash window
between observing the timeout and checkpointing that decision.

Waiting for a task result from the same queue inside a task is rejected because it
can deadlock a worker pool. Cross-queue waits are available through
`TaskContext::await_task_result` and `await_task_result_named`. They checkpoint the
terminal snapshot before decoding, so replay survives child cleanup, including
failed or cancelled child results. A cancelled child produces `Error::TaskCancelled`,
not the owning-run control signal `Error::Cancelled`.

Child-result waits poll while occupying a worker slot; they do not durably
suspend the run. Cross-queue dependency cycles can still deadlock. The raw
`Client` result APIs are non-durable and intentionally do not enforce contextual
same-queue restrictions. Result polling timeouts cover pool acquisition, database
queries, and polling delays; a zero timeout expires immediately. Contextual waits
can still replay an already-checkpointed child result without polling.

## Checkpoint compatibility

Elephant follows the Go, Python, and TypeScript SDKs in Absurd `0.5.0`: repeated
names allocate `charge`, `charge#2`, `charge#3`, and so on. Counters start over on
each dispatch and are shared by context clones and all operation kinds,
including decomposed steps. Replaying the same call sequence reuses the same
checkpoints; repeating a call within one execution allocates another occurrence.

Use stable logical names such as `charge:{order_id}` for unordered or parallel
work; occurrence assignment follows invocation/poll order, not completion order.
Do not use generated suffixes or reserved operation prefixes for unrelated
steps. These APIs do not provide same-name single-flight execution. Version
names when payload shape or meaning changes.

| Operation | Base name and persisted value |
| --- | --- |
| Step | Caller name; serialized successful value |
| Sleep | Caller name, or `sleep`; RFC 3339 timestamp string |
| Event | Caller name, or `$awaitEvent:{event_name}`; raw event payload |
| Child result | Caller name, or `$awaitTaskResult:{task_id}`; terminal snapshot |

Go-generated PostgreSQL fixtures test both replay and Rust-written formats,
including repeated names, sleeps, events, and child snapshots. Cross-language
workflows must still agree on names, call order, and application JSON schemas.
For Rust's unnamed sleep helpers, use `sleep` as the name in other SDKs.

## Execution instrumentation

Every router dispatch has a `tracing` span carrying queue, task ID, run ID,
attempt, and task name. Parameters and headers are never logged automatically.
Handlers and execution wrappers can inspect `context.metadata().headers`.

`Router::wrap_execution` establishes application context around every handler,
including when using the convenience worker. For distributed tracing, extract
your tracing carrier from headers in this wrapper and instrument the returned
execution future with your application's span. Suspension and owning-run
cancellation are runtime control flow, not failures. Dispatch recognizes these
signals through `std::error::Error::source` chains, so domain errors may wrap them
without changing their meaning. Preserve the original error as a source rather
than replacing it with a formatted string. Child cancellation, event timeouts,
and local execution interruptions remain ordinary failures when wrapped.
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
