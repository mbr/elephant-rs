# Elephant specification

`elephant` is an idiomatic Rust SDK for Absurd, a Postgres-native durable
workflow system. The crate should expose Absurd's task, run, checkpoint, sleep,
event, retry, and cancellation model directly. It should not be a job queue
facade and should not hide Absurd behind a framework that owns the process.

The core design principle is inversion of control. Applications should be able
to own the claim loop, concurrency, supervision, shutdown, tracing, and
backpressure. A packaged worker is useful, but it should be a convenience layer
built on public primitives.

## Goals

`elephant` should make the following simple and type-safe:

- spawn typed tasks onto Absurd queues;
- claim runs and resolve them exactly once in Rust code paths;
- write typed durable steps backed by Absurd checkpoints;
- suspend tasks with durable sleeps and event waits;
- fetch and await typed task results;
- run workers with normal async Rust tools;
- manage queues, retry failed tasks, emit events, and cancel tasks.

Correctness should be encoded in types where Rust can express it. The database
remains the authority for distributed state, leases, retries, and crash
recovery. Rust should prevent local misuse such as double-completing a claimed
run, completing a checkpoint handle twice, or calling durable task operations
without an active task context.

The SDK must be honest about semantics. Absurd gives durable at-least-once
execution around crash windows, not magical exactly-once side effects. Public
APIs and documentation should push users toward idempotent external operations
and stable checkpoint names.

## Non-goals

`elephant` should not manage the Absurd schema as its main responsibility. The
recommended production path is to apply `absurd.sql` and its migrations through
the application's migration system. The crate may provide schema version checks
and test helpers, but migrations should not be hidden in `Client::connect`.

`elephant` should not require users to surrender their runtime to a framework. A
`run_worker` helper is acceptable only if the lower-level claim and dispatch
primitives are first-class.

`elephant` should not re-export everything from the crate root. Public items should
live at canonical module paths such as `elephant::client::Client` and
`elephant::task::Task`.

## Architecture

The crate is layered. Lower layers should be usable without higher layers.

1. Stored procedure client: thin typed wrappers around Absurd SQL functions.
2. Execution primitives: claimed runs, run leases, task contexts, checkpoints,
   sleeps, events, and heartbeats.
3. Registry and router: typed task definitions with erased internal dispatch.
4. Worker helpers: polling, concurrency, unknown-task deferral, and shutdown.
5. Operations API: queue management, retry, cancellation, task results, cleanup,
   and policy management.

The primitive type is a claimed run, not a worker. A worker is just a loop that
claims runs and dispatches them through a router.

```rust
let router = elephant::task::Router::new()
    .task(provision_user)
    .task(send_email);

client
    .claims("default", elephant::worker::ClaimOptions::default())
    .try_for_each_concurrent(8, |run| router.dispatch(run))
    .await?;
```

A convenience worker can be layered on top:

```rust
client
    .worker(router)
    .queue("default")
    .concurrency(8)
    .run(shutdown_token)
    .await?;
```

## Public module outline

`client` contains `Client`, `ClientBuilder`, stored procedure wrappers, and
high-level operations such as `spawn`, `emit_event`, `fetch_task_result`,
`await_task_result`, `retry_task`, and `cancel_task`.

`task` contains `Task`, `TaskOptions`, `Router`, typed task registration, and
internal type-erased dispatch.

`context` contains `TaskContext`, durable operations, step handles, sleep,
event wait, heartbeat, and task metadata accessors.

`run` contains `ClaimedRun`, `RunLease`, run outcome types, and terminal methods
such as `complete`, `fail`, `sleep_until`, and `forget`.

`worker` contains `ClaimOptions`, `WorkerOptions`, claim streams,
`work_batch`, and `run_worker` helpers.

`types` contains validated newtypes and data structures such as `QueueName`,
`TaskName`, `StepName`, `EventName`, `TaskId`, `RunId`, `RetryStrategy`,
`CancellationPolicy`, `SpawnOptions`, `SpawnResult`, `TaskResultSnapshot`, and
queue policy types.

`error` contains public error enums built with `thiserror`.

`schema` contains schema version inspection and optional helpers for tests or
operator checks.

## Stored procedure surface

The SDK should wrap these Absurd functions:

- `absurd.create_queue`, `absurd.drop_queue`, `absurd.list_queues`;
- `absurd.get_queue_policy`, `absurd.set_queue_policy`;
- `absurd.spawn_task`;
- `absurd.claim_task`;
- `absurd.complete_run`;
- `absurd.fail_run`;
- `absurd.schedule_run`;
- `absurd.set_task_checkpoint_state`;
- `absurd.get_task_checkpoint_state`;
- `absurd.get_task_checkpoint_states`;
- `absurd.await_event`;
- `absurd.emit_event`;
- `absurd.extend_claim`;
- `absurd.get_task_result`;
- `absurd.cancel_task`;
- `absurd.retry_task`;
- `absurd.cleanup_all_queues`, if an operations API is included early.

The wrappers should validate queue names before calling SQL. Queue names are
Postgres identifiers used in generated table names, so the SDK should mirror
Absurd's non-empty and maximum byte length checks.

## Correctness in types

`ClaimedRun` or `RunLease` should be `#[must_use]`. It represents an active
Absurd claim and owns the `run_id`, `task_id`, queue name, and lock context.
Terminal methods consume `self`:

```rust
impl RunLease {
    pub async fn complete<T: serde::Serialize>(self, result: T) -> Result<(), Error>;
    pub async fn fail(self, reason: FailureReason) -> Result<(), Error>;
    pub async fn sleep_until(self, wake_at: jiff::Timestamp) -> Result<(), Error>;
    pub fn forget(self);
}
```

Consuming methods prevent double resolution in local Rust code. `forget` is an
explicit escape hatch for advanced users who want the database lease to expire
or plan to resolve the run out of band.

`Drop` cannot perform async database work, so it must not pretend to resolve a
run. Dropping an unresolved lease should emit a `tracing` warning and rely on
Absurd's claim timeout and retry behavior. The database lease is the distributed
safety net; Rust's RAII guard is a local misuse detector.

`RunLease::run` should cover the common pattern:

```rust
lease
    .run(async {
        let result = handler(ctx).await?;
        Ok(result)
    })
    .await?;
```

On success it calls `complete_run`. On ordinary handler errors it calls
`fail_run`. Internal suspension and cancellation signals are handled by the
runtime and should not leak as normal user-facing errors.

`TaskContext` is a capability. Durable operations such as `step`, `sleep_for`,
`sleep_until`, `await_event`, `emit_event`, and `heartbeat` require a
`TaskContext` or a `Context` that carries one. This prevents checkpoint writes
and durable waits outside a claimed task.

`Task<P, R>` carries typed params and typed results. The registry erases task
handlers internally, but the user-facing task definition remains generic over
`P: DeserializeOwned` and `R: Serialize`.

`Spawned<R>` or a similar handle should preserve the expected result type for
callers that spawn through a typed `Task<P, R>`. Untyped spawn remains available
for cross-process or operational use.

`StepHandle<T, Pending>` and `StepHandle<T, Done>` can model decomposed steps.
The common API should still be ergonomic:

```rust
let value = ctx
    .step("render-email", || async { render_email(input).await })
    .await?;
```

The advanced API should prevent completing the same pending step handle twice:

```rust
let step = ctx.begin_step::<RenderedEmail>("render-email").await?;
match step {
    Step::Done(done) => done.into_value(),
    Step::Pending(pending) => pending.complete(value).await?,
}
```

Newtypes should reduce stringly APIs at the boundary. At minimum use validated
or parsed types for `QueueName`, `TaskName`, `StepName`, `EventName`, `TaskId`,
and `RunId`. Implement `FromStr` and `Display` next to the type definitions.

`RetryStrategy` should be an enum rather than a loose JSON map:

```rust
pub enum RetryStrategy {
    None,
    Fixed { base: std::time::Duration },
    Exponential {
        base: std::time::Duration,
        factor: f64,
        max: Option<std::time::Duration>,
    },
}
```

`CancellationPolicy` should be a typed struct with optional durations. Builders
are appropriate once there are multiple optional fields.

Waiting for another task in the same queue from inside a task can deadlock a
worker pool. The initial implementation should reject this at runtime with a
clear error. A later typed queue marker API may catch some cases statically, but
it should not complicate the first public API.

## Execution semantics

A task handler runs inside a `TaskContext`. Completed steps are read from
Absurd checkpoints and not re-executed. Code outside steps may run again after a
failure, process crash, lease timeout, or deployment. Documentation must state
this clearly.

Checkpoint names are durable contracts. Reusing a name means old stored JSON may
be returned forever. Users should version checkpoint names when the meaning or
shape changes, for example `process-payment:v2`. For loops over unordered or
parallel data, users should compose names from stable logical IDs instead of
encounter order.

`ctx.step` should store the successful return value by calling
`set_task_checkpoint_state`. If the closure returns an error, no checkpoint is
written and the task run fails through `fail_run`.

`ctx.sleep_until` should checkpoint the chosen wake time, then call
`schedule_run` if the wake time is still in the future. The task runtime handles
this as suspension, not as failure.

`ctx.await_event` should call `await_event`. If Absurd returns
`should_suspend = true`, the runtime treats this as suspension. If it returns a
payload, the payload is decoded and cached as a checkpoint. If it returns no
payload after a timeout, the API returns a timeout error.

`ctx.heartbeat` should call `extend_claim`. Checkpoint writes may also extend
the claim by passing the configured extension to
`set_task_checkpoint_state`.

External side effects inside steps are still at-least-once with respect to
crashes between the effect and checkpoint storage. APIs and docs should push
idempotency keys derived from `task_id`, business IDs, or event IDs.

## Worker behavior

The worker layer should implement the standard Absurd mechanics:

- poll `claim_task` with a worker ID, claim timeout, and batch size;
- build a `TaskContext` for each claimed run;
- load committed checkpoints visible to the current attempt;
- dispatch by `task_name` through a `Router`;
- call `complete_run` on success;
- call `fail_run` on ordinary handler errors and panics;
- call `schedule_run` for durable sleeps;
- suspend cleanly for event waits;
- call `extend_claim` for heartbeats;
- defer unknown task names with jitter instead of failing immediately;
- map database cancellation and already-failed states to internal control flow;
- expose hooks or spans around task execution.

Unknown task deferral matters for rolling deploys. If a worker claims a task
whose handler is not registered locally, it should schedule the run a short,
jittered delay into the future and return successfully. This avoids permanently
failing tasks while old and new binaries overlap.

Worker concurrency should use normal async Rust primitives. The implementation
can use a semaphore or `for_each_concurrent`, but the public API should make it
possible for callers to own concurrency themselves via the claim stream.

The continuous worker should accept a `tokio_util::sync::CancellationToken` or a
`Future`/context equivalent for graceful shutdown. It should stop claiming new
runs on shutdown and let in-flight tasks finish unless the caller cancels those
tasks explicitly.

A lease watchdog should warn when a task exceeds its claim timeout without a
heartbeat. A fatal process-exit watchdog should not be the default Rust
behavior; if provided, it must be explicit in `WorkerOptions`.

## API sketch

Task definition:

```rust
let provision_user = elephant::task::Task::builder("provision-user")
    .queue("default")
    .default_max_attempts(5)
    .handler(|ctx, params: ProvisionUserParams| async move {
        let user = ctx
            .step("create-user-record", || async {
                create_user(params).await
            })
            .await?;

        let activation: Activation = ctx
            .await_event(format!("user-activated:{}", user.id))
            .timeout(std::time::Duration::from_secs(3600))
            .await?;

        Ok(ProvisionUserResult { user, activation })
    })
    .build()?;
```

Client and router:

```rust
let client = elephant::client::Client::builder(pool)
    .default_queue("default")
    .default_max_attempts(5)
    .build()?;

let router = elephant::task::Router::new().task(provision_user)?;
```

Spawn:

```rust
let spawned = client
    .spawn(&provision_user, ProvisionUserParams { user_id, email })
    .idempotency_key(format!("provision-user:{user_id}"))
    .send()
    .await?;

let result: ProvisionUserResult = spawned.await_result(&client).await?;
```

Manual loop:

```rust
client
    .claims("default", elephant::worker::ClaimOptions::default())
    .try_for_each_concurrent(8, |run| router.dispatch(run))
    .await?;
```

Convenience worker:

```rust
client
    .worker(router)
    .queue("default")
    .concurrency(8)
    .claim_timeout(std::time::Duration::from_secs(120))
    .run(shutdown_token)
    .await?;
```

Operational use:

```rust
client.create_queue("default", CreateQueueOptions::default()).await?;
client.emit_event("default", event_name, payload).await?;
client.cancel_task("default", task_id).await?;
client.retry_task("default", task_id, RetryTaskOptions::default()).await?;
```

Exact builder names may change during implementation. The important points are
that typed task APIs exist, claim streams are public, and the convenience worker
is not the only way to run the system.

## Error handling

Use `thiserror` for public errors. Avoid `anyhow` in the library. Errors should
be specific enough for callers to decide whether to retry, inspect, or fail a
process.

Map Absurd SQL states explicitly:

- `AB001` means the task has been cancelled;
- `AB002` means the run has already failed.

Inside the runtime these are often control-flow conditions. Public APIs should
surface them as typed errors when the caller directly invokes an operation.

Task handler errors should be serialized into Absurd failure JSON with a stable
shape, for example `name`, `message`, and optional debug/backtrace data. The
implementation should not require handler errors to be `serde::Serialize`; a
`Display` message is enough for `fail_run`.

Panics in task handlers should be caught at the dispatch boundary, logged, and
converted into failed runs. The worker should continue unless configured
otherwise.

Timeouts from `await_event` and `await_task_result` should have a distinct
error variant and should implement enough structure for callers to detect them
without string matching.

## Dependencies

Use `tokio` as the runtime and `sqlx` for PostgreSQL access. The initial client
should accept an existing `sqlx::PgPool`; opening pools from URLs can be a
builder convenience later.

Use `serde` and `serde_json` for task params, results, headers, checkpoints,
and failure payloads. Use `uuid` for task and run IDs. Use `jiff` for absolute
time values and `std::time::Duration` for durations. Use `jiff-sqlx` if needed
for `timestamptz` integration.

Use `futures` for stream combinators, `tokio-util` for cancellation tokens,
`thiserror` for errors, and `tracing` for structured diagnostics.

Use `backon` or a small internal backoff helper for result polling and worker
polling. Keep polling behavior explicit and configurable.

## SQLx and migrations

The crate should use `sqlx` query APIs with clear typed row structs. Since
Absurd queue tables are dynamic and most access goes through stored functions,
compile-time checked `query_as!` may not always be practical. Prefer checked
macros where the shape is static and fall back to `query_as` with explicit row
structs where necessary.

The project should use `sqlx` offline mode once queries stabilize. Check in
`.sqlx` metadata if query macros are used.

Do not run Absurd migrations implicitly in `Client::builder`. Provide explicit
helpers such as `schema::get_version` or `schema::assert_installed` if useful.
Tests may apply a pinned `absurd.sql` fixture to ephemeral databases.

## Testing plan

Use `pgdb` for integration tests with ephemeral PostgreSQL. This means tests
run against real Postgres, not mocks or an in-memory substitute. Tests should
apply the Absurd schema fixture and create queues explicitly. Add the
`pgdb-rs` flake input to the development environment so the same fixture setup
works locally and in CI.

Initial meaningful tests:

- spawn a typed task and complete it through the router;
- checkpoint replay skips completed steps after an intentional failure;
- event wait suspends a task and resumes after `emit_event`;
- sleep schedules a future run without failing the task;
- idempotency keys return an existing task instead of creating duplicates;
- cancellation maps to the proper typed error or runtime control flow;
- unknown task names are deferred rather than failed;
- result fetching and awaiting decode typed results;
- queue creation and policy round-trip through the operations API.

Unit tests should cover validation, newtype parsing/display, retry strategy
serialization, cancellation policy serialization, and timeout/backoff logic.
Avoid testing behavior already guaranteed by the type system.

If typestate APIs become complex, consider `trybuild` tests for important
compile-fail cases. Keep those tests minimal.

## Documentation expectations

The crate docs should explain Absurd concepts in Rust terms: tasks, runs,
steps, checkpoints, events, sleeps, leases, retries, and cancellation.

Documentation must emphasize:

- completed steps return cached values forever;
- checkpoint names are durable compatibility contracts;
- code outside steps may execute more than once;
- external side effects need idempotency keys;
- dropping an unresolved `RunLease` does not perform async cleanup;
- workers are convenience helpers over claim streams;
- same-queue task waits can deadlock and are rejected.

Examples should show both the convenience worker and the manual claim loop.

## Initial implementation milestone

The first implementation should deliver a small but complete vertical slice:

1. project setup with the standard Rust flake, `check.sh`, and `format.sh`;
2. `Client` from `PgPool`;
3. queue creation;
4. typed `Task<P, R>` registration;
5. `Router` dispatch for claimed runs;
6. `spawn_task` and typed spawn handles;
7. `claim_task`, `complete_run`, and `fail_run`;
8. `TaskContext::step` backed by checkpoints;
9. `TaskContext::sleep_until` and `sleep_for`;
10. `TaskContext::await_event` and `Client::emit_event`;
11. `fetch_task_result` and `await_task_result`;
12. `claims` stream plus a convenience worker;
13. integration tests for completion, checkpoint replay, and event resume.

After that, add retry/cancel operations, queue policy APIs, cleanup helpers,
lease watchdogs, richer hooks, and more ergonomic builders.

## Release readiness

The release-ready SDK must keep the core correctness properties covered by code
and integration tests.

The current release-readiness review resolved these implementation issues:

- typed spawn handles retain their queue and await results without a separate
  queue argument;
- typed result awaiting treats failed, cancelled, and missing-result states as
  errors instead of returning `None`;
- task dispatch catches panics both while constructing and polling handler
  futures;
- router dispatch resolves runs through `RunLease` terminal methods and only
  forgets leases for database-controlled terminal or suspended states;
- queue-name validation uses Absurd's byte limit and does not rewrite accepted
  names by trimming them;
- `RetryStrategy::None` disables automatic retry by forcing a single attempt
  when explicitly selected;
- the decomposed step API does not expose impossible public typestate variants;
- failure payloads use stable category names instead of display strings as
  failure names;
- public event and checkpoint wrappers validate names before calling stored
  procedures;
- the single-checkpoint stored procedure wrapper is exposed;
- duration conversion to Absurd integer seconds rounds non-zero subsecond
  durations up instead of truncating to zero;
- unknown-task deferral jitter preserves subsecond delays.

Durable sleeps are replay-safe. `sleep_for` and `sleep_until` use a default
checkpoint, and `sleep_for_named` and `sleep_until_named` provide explicit stable
sleep identities for loops or multiple waits.

`RunLease::forget` consumes the lease and marks it abandoned locally without
leaking owned resources. The run remains unresolved in Absurd and is recovered by
normal lease expiry.

The convenience worker performs automatic lease management. A configurable
watchdog extends active claims while handlers run, warns when extension fails,
and treats Absurd cancellation or already-failed SQL states as runtime control
flow.

`await_event` supports explicit checkpoint names. A derived event-name checkpoint
remains available for simple waits, while `await_event_named` allows event names
and durable wait names to evolve independently.

The test matrix covers:

- sleep scheduling and replay after wakeup;
- idempotency key reuse;
- cancellation and `AB001` mapping;
- already-failed run and `AB002` mapping;
- unknown-task deferral;
- retry behavior and max-attempt exhaustion;
- queue policy round-trips;
- panic-to-failed-run conversion;
- failed task result snapshots;
- worker shutdown with in-flight tasks;
- same-queue result waits being rejected;
- explicit event wait names and repeated waits.

`sqlx` usage is runtime-checked by policy for Absurd calls. Stored procedure
calls and dynamic queue-table interactions stay small, map rows into typed
structs immediately, and are covered by `pgdb` integration tests.

Operational APIs avoid raw JSON where practical. Queue policy intervals use a
newtype, detach modes use an enum, retry and cancellation policies are typed, and
cleanup returns typed result rows.

Documentation must explain the manual claim loop, convenience worker, typed
spawning, durable steps, sleeps, events, retries, cancellation, and idempotent
side effects. It must clearly state Absurd's at-least-once execution semantics
and checkpoint compatibility rules.

The pinned `testdata/absurd.sql` fixture records its upstream revision and should
be updated deliberately. Test failures from schema changes are compatibility
signals, not incidental fixture churn.

Publishing readiness requires license files, crate metadata review, docs.rs
configuration if needed, and a successful dry-run publish check.
