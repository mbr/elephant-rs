# elephant

[Absurd](https://earendil-works.github.io/absurd) is a Postgres-backend engine for [durable execution](https://earendil-works.github.io/absurd/concepts/) created by [Earendil Works](https://github.com/earendil-works). It has official SDKs for Go, Python and TypeScript, this crate aims to be the missing Rust SDK.

## Getting started

`elephant` uses [`sqlx`](https://docs.rs/sqlx/), thus it is recommended to integrate the installation of `absurd.sql` into your migration set:

```sh
curl -fL --create-dirs -o "migrations/$(date -u +%Y%m%d%H%M%S)_absurd_0.5.0.sql" \
  https://github.com/earendil-works/absurd/releases/download/0.5.0/absurd.sql
```

While queues can be created programmatically, it is usually simpler to create them in an additional migration:

```sh
sleep 1 # Avoid reusing the schema migration's timestamp.
echo "SELECT absurd.create_queue('reports');" \
  > "migrations/$(date -u +%Y%m%d%H%M%S)_reports_queue.sql"
```

`elephant` recommends one queue per worker type due to its enum abstraction (see below). Once migrations have run, you can start queueing jobs.

## Example: generating a report

`elephant` adds additional typing over the Absurd primitives using `serde`. First, define an `enum` for all jobs for a specific queue(-kind):

```rust
// jobs.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "task", content = "params")]
pub enum ReportJob {
    GenerateReport {
        customer_id: u64,
        year: u16,
    },
    DeleteReport {
        report_key: String,
    }
}
```

Now we can set up a client and use it to enqueue jobs:

```rust
// client.rs
use elephant::client::Client;
use sqlx::PgPool;

use jobs::ReportJob;

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

Now we can implement the worker code:

```rust
// worker.rs
use elephant::{client::Client, context::TaskContext, error::Result, task::Router};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use jobs::ReportJob;
use reports::{calculate, delete_report, load_data, render_pdf};

/// Executes a report job using durable steps.
async fn handle_job(context: TaskContext, job: ReportJob) -> Result<()> {
    match job {
        ReportJob::GenerateReport { customer_id, year } => {
            let data = context
                .step("load-data", || load_data(customer_id, year))
                .await?;
            let aggregates = context
                .step("calculate", || calculate(&data))
                .await?;
            let report_key = format!("reports/{customer_id}/{year}.pdf");
            context
                .step("render-pdf", || render_pdf(&aggregates, &report_key))
                .await
        }
        ReportJob::DeleteReport { report_key } => {
            context
                .step("delete-report", || delete_report(&report_key))
                .await
        }
    }
}

/// Runs the report worker process.
#[tokio::main]
async fn main() -> Result<()> {
    let pool = PgPool::connect("postgresql://localhost/myapp").await?;
    let client = Client::builder(pool).default_queue("reports")?.build();
    let router = Router::from_job_handler(handle_job);

    client
        .worker(router)
        .concurrency(2)
        .run(CancellationToken::new())
        .await
}
```

If a job is retried, it runs through the process, except previously successful `context.step`s will be skipped and substituted with the saved result. What `context.step` returns must thus be kept stable, at least while jobs using it are still running.

Be aware that jobs can be "rerun" even outside of errors (e.g. when using `context.sleep_for`) -- all side effects outside of `context.step` are executed again, and that code must be deterministic as well.
