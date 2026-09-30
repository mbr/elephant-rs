# elephant

[Absurd](https://earendil-works.github.io/absurd) is a Postgres-backend engine for [durable execution](https://earendil-works.github.io/absurd/concepts/) created by [Earendil Works](https://github.com/earendil-works). It has official SDKs for Go, Python and TypeScript, this crate aims to be the missing Rust SDK.

## Getting started

`elephant` uses [`sqlx`](https://docs.rs/sqlx/), thus it is recommended to integrate the installation of `absurd.sql` into your migration set:

```sh
curl -fL --create-dirs -o "migrations/$(date -u +%Y%m%d%H%M%S)_absurd_0.5.0.sql" \
  https://github.com/earendil-works/absurd/releases/download/0.5.0/absurd.sql
```

`absurd.sql` does not create queues. In a subsequent migration, create one for report jobs:

```sql
SELECT absurd.create_queue('reports');
```

Typically, use one queue per worker group, shared by all its instances. Once migrations have run, you can enqueue jobs.

## Example: generating a report

`elephant` adds additional typing over the Absurd primitives, usage is straightforward using `serde`. First, define an `enum` for all of your jobs:

```rust
# use serde::{Deserialize, Serialize};
// jobs.rs
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "task", content = "params")]
enum ReportJob {
    GenerateReport {
        customer_id: u64,
        year: u16,
    },
    DeleteReport {
        report_key: String,
    }
}
```

Setup a client, and use it to enqueue a job.

```rust
use elephant::client::Client;
use sqlx::PgPool;

let pool = PgPool::connect("postgresql://localhost/myapp").await?;
let client = Client::builder(pool).default_queue("reports")?.build();

client
    .spawn_job(ReportJob::GenerateReport {
        customer_id: 123,
        year: 2026,
    })
    .send()
    .await?;
```



---

A report job loads a dataset, calculates aggregates, and renders a PDF. Each
successful step stores its result in PostgreSQL. If rendering fails, a retry
reuses the stored dataset and aggregates instead of repeating that work.

Put the task contract in a shared `reports` module. Its input is an immutable
dataset snapshot key; its output is a durable storage key for the finished PDF.

```rust
use elephant::{error::Result, task::Task};

/// Defines the report task shared by workers and clients.
pub fn report_task() -> Result<Task<String, String>> {
    Ok(Task::builder("generate-report-v1")?
        .queue("reports")?
        .default_max_attempts(5)
        .build())
}
```

The worker binds an implementation to that contract. The `load_data`,
`calculate`, and `render_pdf` functions are application code in your `reports`
module, not SDK functions. They are async and return `elephant::error::Result<T>`;
their results must implement `serde::Serialize` and `serde::de::DeserializeOwned`.
`render_pdf` stores the PDF durably under a repeatable key derived from the
snapshot and report version, then returns its storage key. CPU-heavy calculations
or rendering should use `tokio::task::spawn_blocking` rather than block the async
executor.

```rust
use elephant::{
    client::Client,
    error::Result,
    run::{ExecutionOptions, StallTimeout},
    task::Router,
    types::CreateQueueOptions,
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::reports::{calculate, load_data, render_pdf, report_task};

/// Processes report jobs until shutdown is requested, then drains active work.
pub async fn run_worker(pool: PgPool, shutdown: CancellationToken) -> Result<()> {
    let client = Client::builder(pool).build();
    client
        .create_queue("reports", CreateQueueOptions::default())
        .await?;

    let task = report_task()?;
    let router = Router::new().task(task.handler(|context, snapshot_key| async move {
        let data = context
            .step("load-data-v1", || load_data(&snapshot_key))
            .await?;
        let aggregates = context
            .step("calculate-v1", || calculate(&data))
            .await?;
        context
            .step("render-pdf-v1", || render_pdf(&aggregates, &snapshot_key))
            .await
    }))?;

    client
        .worker(router)
        .queue("reports")
        .concurrency(2)
        .execution(ExecutionOptions {
            stall_timeout: StallTimeout::Disabled,
            ..ExecutionOptions::default()
        })
        .run(shutdown)
        .await
}
```

This worker creates the queue at startup; production deployments can provision
it separately. The example disables inactivity detection so an expensive step
can run longer than the default 30-second progress window. Lease renewal remains
automatic, but a hung operation may now run indefinitely. See [execution
supervision](DOCS.md#execution-supervision) for progress heartbeats and recovery
policies. The application supplies the pool and cancels `shutdown` when the
worker should stop claiming and drain.

Once the queue exists and a worker is running, a client in another process can
submit a report and wait for its storage key:

```rust
use elephant::{client::Client, error::Result};
use sqlx::PgPool;

use crate::reports::report_task;

/// Submits a report for a dataset snapshot and waits for its PDF storage key.
pub async fn generate_report(pool: PgPool, snapshot_key: String) -> Result<String> {
    let client = Client::builder(pool).build();
    let task = report_task()?;
    let idempotency_key = format!("generate-report-v1:{snapshot_key}");
    let spawned = client
        .spawn(&task, snapshot_key)
        .idempotency_key(idempotency_key)
        .send()
        .await?;

    client
        .await_typed_task_result(
            spawned.queue_name.as_str(),
            spawned.result.task_id.as_uuid(),
            None,
        )
        .await
}
```

The idempotency key makes repeated submissions reuse the same task while its
idempotency record is retained. Waiting is optional: the client can return the
spawned handle immediately instead. The worker continues independently.

A retry starts the handler from the beginning, but completed `step` calls return
their stored values without running their closures. Keep expensive work inside
those closures and keep step names and result formats stable. A crash after an
operation finishes but before its checkpoint commits can still repeat that
operation, so PDF storage must be idempotent.
