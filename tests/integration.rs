//! Integration tests against ephemeral PostgreSQL.

use std::{
    error::Error as StdError,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use room::{
    client::Client,
    error::Error,
    task::{Router, Task},
    types::CreateQueueOptions,
    worker::work_batch,
};
use serde::{Deserialize, Serialize};
use sqlx::{Executor, PgPool};

/// Represents test task input.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Input {
    /// Carries a numeric value.
    value: i32,
}

/// Represents test task output.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Output {
    /// Carries a numeric value.
    value: i32,
}

/// Owns an ephemeral test database.
struct TestDb {
    /// Keeps the database fixture alive.
    _db: pgdb::DbInstance,
    /// Holds the Room client.
    client: Client,
}

/// Represents an intentional task failure.
#[derive(Debug)]
struct TestFailure;

impl fmt::Display for TestFailure {
    /// Formats the test failure.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("intentional test failure")
    }
}

impl StdError for TestFailure {}

/// Creates a test database with Absurd installed.
async fn setup() -> Result<TestDb, Box<dyn StdError + Send + Sync>> {
    let db = pgdb::db_fixture();
    let pool = PgPool::connect(db.as_str()).await?;
    pool.execute(sqlx::raw_sql(include_str!("../testdata/absurd.sql")))
        .await?;
    let mut builder = Client::builder(pool);
    builder.default_queue("default")?;
    let client = builder.build();
    client
        .create_queue("default", CreateQueueOptions::default())
        .await?;
    Ok(TestDb { _db: db, client })
}

/// Verifies that a typed task can be spawned and completed.
#[tokio::test]
async fn spawned_task_completes_through_router() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let task = Task::<Input, Output>::builder("echo")?
        .queue("default")?
        .handler(|_context, input| async move { Ok(Output { value: input.value }) })
        .build();
    let router = Router::new().task(task.clone())?;

    let spawned = test.client.spawn(&task, Input { value: 42 }).send().await?;
    work_batch(&test.client, &router, "default").await?;
    let result = spawned
        .await_result(&test.client, "default", Some(Duration::from_secs(5)))
        .await?
        .expect("completed task should have a result");

    assert_eq!(result, Output { value: 42 });
    Ok(())
}

/// Verifies that completed checkpoints are replayed after failure.
#[tokio::test]
async fn checkpoint_replay_skips_completed_step() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let step_calls = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));
    let task_step_calls = Arc::clone(&step_calls);
    let task_attempts = Arc::clone(&attempts);
    let task = Task::<Input, Output>::builder("checkpointed")?
        .queue("default")?
        .default_max_attempts(2)
        .handler(move |context, _input| {
            let step_calls = Arc::clone(&task_step_calls);
            let attempts = Arc::clone(&task_attempts);
            async move {
                let value = context
                    .step("once", || {
                        let step_calls = Arc::clone(&step_calls);
                        async move {
                            step_calls.fetch_add(1, Ordering::SeqCst);
                            Ok(7)
                        }
                    })
                    .await?;
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(Error::handler(Box::new(TestFailure)));
                }
                Ok(Output { value })
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;

    let spawned = test.client.spawn(&task, Input { value: 0 }).send().await?;
    work_batch(&test.client, &router, "default").await?;
    work_batch(&test.client, &router, "default").await?;
    let result = spawned
        .await_result(&test.client, "default", Some(Duration::from_secs(5)))
        .await?
        .expect("completed task should have a result");

    assert_eq!(result, Output { value: 7 });
    assert_eq!(step_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// Verifies that event waits suspend and resume after emission.
#[tokio::test]
async fn event_wait_resumes_after_emit() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let task = Task::<Input, Output>::builder("wait-event")?
        .queue("default")?
        .handler(|context, _input| async move {
            let output: Output = context.await_event("ready").await?;
            Ok(output)
        })
        .build();
    let router = Router::new().task(task.clone())?;

    let spawned = test.client.spawn(&task, Input { value: 0 }).send().await?;
    work_batch(&test.client, &router, "default").await?;
    test.client
        .emit_event("default", "ready", Output { value: 99 })
        .await?;
    work_batch(&test.client, &router, "default").await?;
    let result = spawned
        .await_result(&test.client, "default", Some(Duration::from_secs(5)))
        .await?
        .expect("completed task should have a result");

    assert_eq!(result, Output { value: 99 });
    Ok(())
}
