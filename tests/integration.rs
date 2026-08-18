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

use elephant::{
    client::Client,
    error::Error,
    schema,
    task::{Router, Task},
    types::{
        CreateQueueOptions, PgInterval, QueueDetachMode, QueuePolicyOptions, RetryStrategy,
        SpawnOptions, TaskResultState,
    },
    worker::{LeaseWatchdogOptions, WorkerOptions, work_batch},
};
use serde::{Deserialize, Serialize};
use sqlx::{Executor, PgPool};
use tokio_util::sync::CancellationToken;

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

/// Represents fallible test completion.
type TestResult = Result<(), Box<dyn StdError + Send + Sync>>;

/// Owns an ephemeral test database.
struct TestDb {
    /// Keeps the database fixture alive.
    _db: pgdb::DbInstance,
    /// Holds the Elephant client.
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

/// Panics for panic conversion tests.
///
/// # Panic
///
/// Always panics.
async fn panic_output() -> elephant::error::Result<Output> {
    panic!("intentional panic")
}

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

/// Verifies that schema inspection reports the installed fixture.
#[tokio::test]
async fn schema_version_is_reported() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;

    schema::assert_installed(test.client.pool()).await?;
    schema::assert_version(test.client.pool(), "main").await?;
    assert_eq!(
        schema::version(test.client.pool()).await?,
        Some("main".to_string())
    );
    Ok(())
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
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await?;

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
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await?;

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
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await?;

    assert_eq!(result, Output { value: 99 });
    Ok(())
}

/// Verifies that durable sleeps complete after a wakeup.
#[tokio::test]
async fn sleep_replays_after_wakeup() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let task_attempts = Arc::clone(&attempts);
    let task = Task::<Input, Output>::builder("sleepy")?
        .queue("default")?
        .handler(move |context, _input| {
            let attempts = Arc::clone(&task_attempts);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                context
                    .sleep_for_named("short-sleep", Duration::from_millis(100))
                    .await?;
                Ok(Output { value: 5 })
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;

    let spawned = test.client.spawn(&task, Input { value: 0 }).send().await?;
    work_batch(&test.client, &router, "default").await?;
    let sleeping = test
        .client
        .fetch_task_result("default", spawned.result.task_id.as_uuid())
        .await?
        .expect("sleeping task should be visible");
    assert_eq!(sleeping.state, TaskResultState::Sleeping);

    tokio::time::sleep(Duration::from_millis(150)).await;
    work_batch(&test.client, &router, "default").await?;
    let result = spawned
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await?;

    assert_eq!(result, Output { value: 5 });
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    Ok(())
}

/// Verifies that idempotency keys return the existing task.
#[tokio::test]
async fn idempotency_key_reuses_existing_task() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let task = Task::<Input, Output>::builder("idempotent")?
        .queue("default")?
        .handler(|_context, input| async move { Ok(Output { value: input.value }) })
        .build();

    let first = test
        .client
        .spawn(&task, Input { value: 1 })
        .idempotency_key("same-key")
        .send()
        .await?;
    let second = test
        .client
        .spawn(&task, Input { value: 2 })
        .idempotency_key("same-key")
        .send()
        .await?;

    assert!(first.result.created);
    assert!(!second.result.created);
    assert_eq!(first.result.task_id, second.result.task_id);
    assert_eq!(first.result.run_id, second.result.run_id);
    Ok(())
}

/// Verifies cancellation SQLSTATE mapping for active runs.
#[tokio::test]
async fn cancellation_maps_absurd_sqlstate() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let spawned = test
        .client
        .spawn_untyped("cancel-me", Input { value: 0 }, SpawnOptions::default())
        .await?;
    let mut leases = test
        .client
        .claim_task("default", &elephant::worker::ClaimOptions::default())
        .await?;
    let lease = leases.pop().expect("spawned task should be claimable");

    test.client
        .cancel_task("default", spawned.task_id.as_uuid())
        .await?;
    let error = lease
        .complete(Output { value: 0 })
        .await
        .expect_err("completion should report cancellation");

    assert!(matches!(error, Error::Cancelled));
    Ok(())
}

/// Verifies already-failed SQLSTATE mapping.
#[tokio::test]
async fn already_failed_run_maps_absurd_sqlstate() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let spawned = test
        .client
        .spawn_untyped("fail-me", Input { value: 0 }, SpawnOptions::default())
        .await?;
    let mut leases = test
        .client
        .claim_task("default", &elephant::worker::ClaimOptions::default())
        .await?;
    let lease = leases.pop().expect("spawned task should be claimable");
    lease.fail(elephant::error::FailureReason::panic()).await?;

    let error = test
        .client
        .fail_run(
            "default",
            spawned.run_id.as_uuid(),
            elephant::error::FailureReason::panic(),
        )
        .await
        .expect_err("second failure should report already-failed state");

    assert!(matches!(error, Error::RunAlreadyFailed));
    Ok(())
}

/// Verifies unknown tasks are deferred instead of failed.
#[tokio::test]
async fn unknown_task_is_deferred() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let router = Router::new().unknown_task_delay(Duration::from_millis(1));
    let spawned = test
        .client
        .spawn_untyped("future-task", Input { value: 0 }, SpawnOptions::default())
        .await?;

    work_batch(&test.client, &router, "default").await?;
    let snapshot = test
        .client
        .fetch_task_result("default", spawned.task_id.as_uuid())
        .await?
        .expect("deferred task should be visible");

    assert_eq!(snapshot.state, TaskResultState::Sleeping);
    Ok(())
}

/// Verifies panic conversion to failed task results.
#[tokio::test]
async fn panic_becomes_failed_task_result() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let task = Task::<Input, Output>::builder("panic-task")?
        .queue("default")?
        .default_max_attempts(1)
        .handler(|_context, _input| panic_output())
        .build();
    let router = Router::new().task(task.clone())?;

    let spawned = test.client.spawn(&task, Input { value: 0 }).send().await?;
    work_batch(&test.client, &router, "default").await?;
    let snapshot = test
        .client
        .await_task_result(
            "default",
            spawned.result.task_id.as_uuid(),
            Some(Duration::from_secs(5)),
        )
        .await?;

    let typed_error = spawned
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await
        .expect_err("typed await should surface failed tasks");

    assert_eq!(snapshot.state, TaskResultState::Failed);
    assert!(snapshot.failure.is_some());
    assert!(matches!(typed_error, Error::TaskFailed { .. }));
    Ok(())
}

/// Verifies panic conversion covers handler construction.
#[tokio::test]
async fn synchronous_panic_becomes_failed_task_result() -> TestResult {
    let test = setup().await?;
    let task = Task::<Input, Output>::builder("sync-panic-task")?
        .queue("default")?
        .default_max_attempts(1)
        .handler(
            |_context, _input| -> std::future::Ready<elephant::error::Result<Output>> {
                panic!("intentional synchronous panic")
            },
        )
        .build();
    let router = Router::new().task(task.clone())?;

    let spawned = test.client.spawn(&task, Input { value: 0 }).send().await?;
    work_batch(&test.client, &router, "default").await?;
    let snapshot = test
        .client
        .await_task_result(
            "default",
            spawned.result.task_id.as_uuid(),
            Some(Duration::from_secs(5)),
        )
        .await?;

    assert_eq!(snapshot.state, TaskResultState::Failed);
    assert!(snapshot.failure.is_some());
    Ok(())
}

/// Verifies retry after failure can complete on a later attempt.
#[tokio::test]
async fn retry_strategy_runs_later_attempt() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let task_attempts = Arc::clone(&attempts);
    let task = Task::<Input, Output>::builder("retry-task")?
        .queue("default")?
        .default_max_attempts(2)
        .handler(move |_context, _input| {
            let attempts = Arc::clone(&task_attempts);
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(Error::handler(Box::new(TestFailure)));
                }
                Ok(Output { value: 11 })
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;

    let spawned = test
        .client
        .spawn(&task, Input { value: 0 })
        .retry_strategy(RetryStrategy::Fixed {
            base: Duration::ZERO,
        })
        .send()
        .await?;
    work_batch(&test.client, &router, "default").await?;
    work_batch(&test.client, &router, "default").await?;
    let result = spawned
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await?;

    assert_eq!(result, Output { value: 11 });
    Ok(())
}

/// Verifies queue policy values round-trip through Absurd.
#[tokio::test]
async fn queue_policy_round_trips() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    test.client
        .set_queue_policy(
            "default",
            QueuePolicyOptions {
                cleanup_limit: Some(17),
                cleanup_ttl: Some(PgInterval::from(Duration::from_secs(60))),
                detach_mode: Some(QueueDetachMode::Empty),
                ..QueuePolicyOptions::default()
            },
        )
        .await?;
    let policy = test
        .client
        .get_queue_policy("default")
        .await?
        .expect("queue policy should exist");

    assert_eq!(policy.cleanup_limit, 17);
    assert_eq!(policy.detach_mode, QueueDetachMode::Empty);
    Ok(())
}

/// Verifies same-queue waits are rejected in task contexts.
#[tokio::test]
async fn same_queue_task_wait_is_rejected() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let child = Task::<Input, Output>::builder("child")?
        .queue("default")?
        .handler(|_context, input| async move { Ok(Output { value: input.value }) })
        .build();
    let child_for_parent = child.clone();
    let parent = Task::<Input, Output>::builder("parent")?
        .queue("default")?
        .handler(move |context, _input| {
            let child = child_for_parent.clone();
            async move {
                let spawned = context
                    .client()
                    .spawn(&child, Input { value: 0 })
                    .send()
                    .await?;
                match context
                    .await_task_result(&spawned, Some(Duration::from_millis(1)))
                    .await
                {
                    Err(Error::SameQueueWait) => Ok(Output { value: 1 }),
                    Err(error) => Err(error),
                    Ok(_) => Err(Error::handler(Box::new(TestFailure))),
                }
            }
        })
        .build();
    let router = Router::new().task(child)?.task(parent.clone())?;

    let spawned = test
        .client
        .spawn(&parent, Input { value: 0 })
        .send()
        .await?;
    work_batch(&test.client, &router, "default").await?;
    let result = spawned
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await?;

    assert_eq!(result, Output { value: 1 });
    Ok(())
}

/// Verifies explicit event checkpoint names allow repeated event waits.
#[tokio::test]
async fn explicit_event_wait_names_can_repeat_event() -> Result<(), Box<dyn StdError + Send + Sync>>
{
    let test = setup().await?;
    let task = Task::<Input, Output>::builder("repeat-event")?
        .queue("default")?
        .handler(|context, _input| async move {
            let first: Output = context.await_event_named("first-ready", "ready").await?;
            let second: Output = context.await_event_named("second-ready", "ready").await?;
            Ok(Output {
                value: first.value + second.value,
            })
        })
        .build();
    let router = Router::new().task(task.clone())?;

    let spawned = test.client.spawn(&task, Input { value: 0 }).send().await?;
    work_batch(&test.client, &router, "default").await?;
    test.client
        .emit_event("default", "ready", Output { value: 20 })
        .await?;
    work_batch(&test.client, &router, "default").await?;
    let result = spawned
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await?;

    assert_eq!(result, Output { value: 40 });
    Ok(())
}

/// Verifies worker shutdown waits for in-flight tasks.
#[tokio::test]
async fn worker_shutdown_waits_for_in_flight_task() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let task = Task::<Input, Output>::builder("worker-task")?
        .queue("default")?
        .handler(|_context, _input| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(Output { value: 33 })
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, Input { value: 0 }).send().await?;
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let client = test.client.clone();
    let worker = tokio::spawn(async move {
        elephant::worker::run_worker(
            client,
            router,
            WorkerOptions {
                queue_name: "default".to_string(),
                lease_watchdog: Some(LeaseWatchdogOptions {
                    interval: Duration::from_millis(10),
                    extend_by: Duration::from_secs(1),
                }),
                ..WorkerOptions::default()
            },
            worker_shutdown,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    shutdown.cancel();
    worker
        .await
        .expect("worker task should join without panicking")?;
    let result = spawned
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await?;

    assert_eq!(result, Output { value: 33 });
    Ok(())
}
