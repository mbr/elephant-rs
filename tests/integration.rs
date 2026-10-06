//! Integration tests against ephemeral PostgreSQL.

#[path = "integration/coverage.rs"]
mod coverage;
#[path = "integration/enum_jobs.rs"]
mod enum_jobs;
#[path = "integration/handoff.rs"]
mod handoff;
#[path = "integration/operations.rs"]
mod operations;
#[path = "integration/references.rs"]
mod references;
#[path = "integration/regressions.rs"]
mod regressions;

use std::{
    collections::BTreeMap,
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
    context::Step,
    error::Error,
    run::{ExecutionOptions, LeaseRenewal, LeaseWatchdogOptions, StallTimeout},
    schema,
    task::{Router, Task},
    types::{
        CreateQueueOptions, PgInterval, QueueDetachMode, QueueName, QueuePolicyOptions,
        RetryStrategy, SpawnOptions, TaskResultSnapshot, TaskResultState,
    },
    worker::{WorkerOptions, work_batch},
};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use sqlx::{Executor, postgres::PgPoolOptions};
use tokio::sync::Notify;
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
type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

/// Owns an ephemeral test database.
struct TestDb {
    /// Holds the Elephant client.
    client: Client,
    /// Keeps the database fixture alive until after the client is dropped.
    _db: pgdb::DbInstance,
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
    setup_with_max_connections(10).await
}

/// Creates a test database with a bounded connection pool.
async fn setup_with_max_connections(
    max_connections: u32,
) -> Result<TestDb, Box<dyn StdError + Send + Sync>> {
    let db = pgdb::db_fixture();
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(db.as_str())
        .await?;
    pool.execute(sqlx::raw_sql(include_str!("../testdata/absurd.sql")))
        .await?;
    let mut builder = Client::builder(pool);
    builder.default_queue(QueueName::from_static("default"));
    let client = builder.build();
    client
        .create_queue("default", CreateQueueOptions::default())
        .await?;
    Ok(TestDb { client, _db: db })
}

/// Verifies that schema inspection reports the installed fixture.
#[tokio::test]
async fn schema_version_is_reported() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;

    schema::assert_installed(test.client.pool()).await?;
    schema::assert_version(test.client.pool(), "0.5.0").await?;
    assert_eq!(
        schema::version(test.client.pool()).await?,
        Some("0.5.0".to_string())
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
    let mismatch = test
        .client
        .spawn(&task, Input { value: 0 })
        .queue("other")
        .expect_err("task queue overrides should not conflict");
    assert!(matches!(mismatch, Error::TaskQueueMismatch { .. }));

    let spawned = test.client.spawn(&task, Input { value: 42 }).send().await?;
    work_batch(&test.client, &router, "default").await?;
    let result = spawned
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await?;

    assert_eq!(result, Output { value: 42 });
    Ok(())
}

/// Verifies that typed handlers receive application headers.
#[tokio::test]
async fn headers_reach_typed_handlers() -> TestResult {
    let test = setup().await?;
    let wrapper_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&wrapper_calls);
    let task = Task::<Input, Output>::builder("headers")?
        .handler(|context, _| async move {
            let headers = context
                .metadata()
                .headers
                .as_ref()
                .expect("spawned headers should be retained");
            assert_eq!(headers["traceparent"], "parent-span");
            Ok(Output { value: 1 })
        })
        .build();
    let router = Router::new()
        .task(task.clone())?
        .wrap_execution(move |context, execute| {
            calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                context
                    .metadata()
                    .headers
                    .as_ref()
                    .expect("headers should reach wrapper")["traceparent"],
                "parent-span"
            );
            execute
        });
    let spawned = test
        .client
        .spawn(&task, Input { value: 0 })
        .headers(serde_json::json!({"traceparent": "parent-span"}))?
        .send()
        .await?;
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        spawned
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?,
        Output { value: 1 }
    );
    assert_eq!(wrapper_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// Verifies that business writes, task spawning, and events share transactions.
#[tokio::test]
async fn enqueue_and_emit_follow_caller_transaction() -> TestResult {
    let test = setup().await?;
    sqlx::query("CREATE TABLE business_values (value integer NOT NULL)")
        .execute(test.client.pool())
        .await?;
    let task = Task::<Input, Output>::builder("transactional")?
        .queue("default")?
        .handler(|context, _| async move { context.await_event("transaction-event").await })
        .build();
    let router = Router::new().task(task.clone())?;
    for commit in [false, true] {
        let value = if commit { 42 } else { 1 };
        let mut transaction = test.client.pool().begin().await?;
        sqlx::query("INSERT INTO business_values VALUES ($1)")
            .bind(value)
            .execute(&mut *transaction)
            .await?;
        let spawned = test
            .client
            .spawn(&task, Input { value })
            .idempotency_key("business-value")
            .send_on(&mut transaction)
            .await?;
        test.client
            .emit_event_on(
                &mut transaction,
                "default",
                "transaction-event",
                Output { value },
            )
            .await?;
        assert!(
            test.client
                .fetch_task_result("default", spawned.result.task_id.as_uuid())
                .await?
                .is_none()
        );
        if commit {
            transaction.commit().await?;
            work_batch(&test.client, &router, "default").await?;
            assert_eq!(
                spawned
                    .await_result(&test.client, Some(Duration::from_secs(1)))
                    .await?,
                Output { value: 42 }
            );
        } else {
            let raw = test
                .client
                .spawn_untyped_on(
                    &mut transaction,
                    "also-rolled-back",
                    Input { value },
                    SpawnOptions::default(),
                )
                .await?;
            transaction.rollback().await?;
            for task_id in [raw.task_id, spawned.result.task_id] {
                assert!(
                    test.client
                        .fetch_task_result("default", task_id.as_uuid())
                        .await?
                        .is_none()
                );
            }
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM business_values")
                .fetch_one(test.client.pool())
                .await?;
            assert_eq!(count, 0);
        }
    }
    let values: Vec<i32> = sqlx::query_scalar("SELECT value FROM business_values")
        .fetch_all(test.client.pool())
        .await?;
    assert_eq!(values, [42]);
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
                let repeated = context.step("once", || async { Ok(999) }).await?;
                assert_eq!(repeated, 999);
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

/// Verifies replay of Go checkpoints and matching Rust-written wire formats.
#[tokio::test]
async fn checkpoints_interoperate_with_go() -> TestResult {
    let fixture: BTreeMap<String, serde_json::Value> =
        serde_json::from_str(include_str!("../testdata/go-checkpoints/checkpoints.json"))?;
    for replay in [false, true] {
        let test = setup().await?;
        let expected = fixture.clone();
        let task = Task::<bool, ()>::builder("fixture")?
            .default_max_attempts(1)
            .handler(move |context, replay| {
                let expected = expected.clone();
                async move {
                    for i in 1..=3 {
                        let value = if i == 2 {
                            match context.clone().begin_step("charge").await? {
                                Step::Done(done) => done.into_value(),
                                Step::Pending(pending) => {
                                    assert!(!replay, "decomposed checkpoint must replay");
                                    pending.complete(i * 7).await?
                                }
                            }
                        } else {
                            context
                                .step("charge", || async {
                                    assert!(!replay, "step checkpoint must replay");
                                    Ok(i * 7)
                                })
                                .await?
                        };
                        assert_eq!(value, i * 7);
                    }
                    for year in [2000, 2001] {
                        if replay {
                            context
                                .sleep_for_named("nap", Duration::from_secs(3600))
                                .await?;
                        } else {
                            context
                                .sleep_until_named(
                                    "nap",
                                    format!("{year}-01-01T00:00:00.123456789Z").parse()?,
                                )
                                .await?;
                        }
                    }
                    for _ in 0..2 {
                        assert_eq!(context.await_event::<i32>("ready").await?, 99);
                    }
                    for name in [
                        "child-completed",
                        "child-null",
                        "child-failed",
                        "child-cancelled",
                    ] {
                        if replay {
                            let result = context
                                .await_task_result_by_id_named::<serde_json::Value>(
                                    name,
                                    "children",
                                    context.metadata().task_id.as_uuid(),
                                    Some(Duration::ZERO),
                                )
                                .await;
                            match name {
                                "child-completed" => {
                                    assert_eq!(result?, serde_json::json!({"value": 42}))
                                }
                                "child-null" => assert_eq!(result?, serde_json::Value::Null),
                                "child-failed" => {
                                    assert!(matches!(result, Err(Error::TaskFailed { .. })))
                                }
                                _ => assert!(matches!(result, Err(Error::TaskCancelled { .. }))),
                            }
                        } else {
                            let snapshot: TaskResultSnapshot =
                                serde_json::from_value(expected[name].clone())
                                    .map_err(Error::json)?;
                            context.step(name, || async { Ok(snapshot) }).await?;
                        }
                    }
                    Ok(())
                }
            })
            .build();
        let router = Router::new().task(task.clone())?;
        let spawned = test.client.spawn(&task, replay).send().await?;
        if replay {
            for (name, state) in &fixture {
                test.client
                    .set_checkpoint(
                        "default",
                        spawned.result.task_id.as_uuid(),
                        name,
                        state,
                        spawned.result.run_id.as_uuid(),
                        None,
                    )
                    .await?;
            }
        } else {
            test.client.emit_event("default", "ready", 99).await?;
        }
        work_batch(&test.client, &router, "default").await?;
        spawned
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?;
        let actual: BTreeMap<_, _> = test
            .client
            .get_checkpoints(
                "default",
                spawned.result.task_id.as_uuid(),
                spawned.result.run_id.as_uuid(),
            )
            .await?
            .into_iter()
            .collect();
        assert_eq!(actual, fixture);
    }
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
    let checkpoint = test
        .client
        .get_checkpoint(
            "default",
            spawned.result.task_id.as_uuid(),
            "$awaitEvent:ready",
            false,
        )
        .await?
        .expect("default event key should remain compatible");
    assert_eq!(checkpoint.state, serde_json::json!({"value": 99}));
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
    let checkpoint = test
        .client
        .get_checkpoint(
            "default",
            spawned.result.task_id.as_uuid(),
            "short-sleep",
            false,
        )
        .await?
        .expect("sleep should persist its wake time");
    let wake_at = checkpoint
        .state
        .as_str()
        .expect("sleep format is a timestamp string");
    wake_at.parse::<Timestamp>()?;

    tokio::time::sleep(Duration::from_millis(150)).await;
    work_batch(&test.client, &router, "default").await?;
    let result = spawned
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await?;

    assert_eq!(result, Output { value: 5 });
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    Ok(())
}

/// Verifies that sleeps use the database clock for scheduling.
#[tokio::test]
async fn sleep_schedules_relative_to_database_clock() -> TestResult {
    let test = setup_with_max_connections(1).await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let task_attempts = Arc::clone(&attempts);
    let task = Task::<Input, Output>::builder("database-clock-sleep")?
        .queue("default")?
        .handler(move |context, _input| {
            let attempts = Arc::clone(&task_attempts);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                context
                    .sleep_for_named("wait", Duration::from_secs(10))
                    .await?;
                Ok(Output { value: 1 })
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let database_now = Timestamp::now().saturating_add(Duration::from_secs(60))?;
    sqlx::query("SELECT set_config('absurd.fake_now', $1, false)")
        .bind(database_now.to_string())
        .execute(test.client.pool())
        .await?;

    let spawned = test.client.spawn(&task, Input { value: 0 }).send().await?;
    work_batch(&test.client, &router, "default").await?;
    let available_at: String =
        sqlx::query_scalar("SELECT available_at::text FROM absurd.r_default WHERE run_id = $1")
            .bind(spawned.result.run_id.as_uuid())
            .fetch_one(test.client.pool())
            .await?;
    let available_at = available_at.parse::<Timestamp>()?;
    let delay = available_at.duration_since(database_now).as_secs_f64();

    assert!((9.0..=11.0).contains(&delay));
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        test.client
            .fetch_task_result("default", spawned.result.task_id.as_uuid())
            .await?
            .expect("sleeping task")
            .state,
        TaskResultState::Sleeping
    );

    let after_wakeup = database_now.saturating_add(Duration::from_secs(11))?;
    sqlx::query("SELECT set_config('absurd.fake_now', $1, false)")
        .bind(after_wakeup.to_string())
        .execute(test.client.pool())
        .await?;
    work_batch(&test.client, &router, "default").await?;
    let result = spawned
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await?;

    assert_eq!(result, Output { value: 1 });
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
    assert_eq!(
        snapshot
            .failure
            .as_ref()
            .expect("panic should have diagnostics")["message"],
        "intentional synchronous panic"
    );
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

/// Verifies that Absurd rejects retry delays beyond its global limit.
#[tokio::test]
async fn invalid_retry_strategy_maps_absurd_sqlstate() -> TestResult {
    let test = setup().await?;
    let options = SpawnOptions {
        retry_strategy: Some(RetryStrategy::Fixed {
            base: Duration::from_secs(86_401),
        }),
        ..SpawnOptions::default()
    };

    let error = test
        .client
        .spawn_untyped("invalid-retry", Input { value: 0 }, options)
        .await
        .expect_err("retry delays beyond one day should fail");

    assert!(matches!(error, Error::InvalidRetryStrategy { .. }));
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

/// Verifies that manual dispatch renews its lease while polling a child.
#[tokio::test]
async fn child_wait_renews_manual_claim() -> TestResult {
    let test = setup().await?;
    test.client
        .create_queue("children", CreateQueueOptions::default())
        .await?;
    let child = Task::<Input, Output>::builder("child")?
        .queue("children")?
        .handler(|_, input| async move { Ok(Output { value: input.value }) })
        .build();
    let child_router = Router::new().task(child.clone())?;
    let child = test.client.spawn(&child, Input { value: 7 }).send().await?;
    let parent = Task::<Input, Output>::builder("parent")?
        .handler(move |context, _| {
            let child = child.clone();
            async move {
                context
                    .await_task_result_by_id_named::<Output>(
                        "child-result",
                        child.queue_name.as_str(),
                        child.result.task_id.as_uuid(),
                        Some(Duration::from_secs(5)),
                    )
                    .await
            }
        })
        .build();
    let router = Router::new().task(parent.clone())?;
    let parent = test
        .client
        .spawn(&parent, Input { value: 0 })
        .send()
        .await?;
    let lease = test
        .client
        .claim_task(
            "default",
            &elephant::worker::ClaimOptions {
                claim_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .await?
        .pop()
        .expect("parent should be claimable");
    let original_expiry: String =
        sqlx::query_scalar("SELECT claim_expires_at::text FROM absurd.r_default WHERE run_id = $1")
            .bind(parent.result.run_id.as_uuid())
            .fetch_one(test.client.pool())
            .await?;
    let dispatch = tokio::spawn(async move { router.dispatch(lease).await });
    let extended = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let extended: bool = sqlx::query_scalar(
                "SELECT claim_expires_at > $2::timestamptz FROM absurd.r_default WHERE run_id = $1",
            )
            .bind(parent.result.run_id.as_uuid())
            .bind(&original_expiry)
            .fetch_one(test.client.pool())
            .await?;
            if extended {
                return Ok::<_, sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    work_batch(&test.client, &child_router, "children").await?;
    dispatch.await.expect("dispatch should join")?;
    extended??;
    assert_eq!(
        parent
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?,
        Output { value: 7 }
    );
    Ok(())
}

/// Verifies that an observed child result survives cleanup and parent retry.
#[tokio::test]
async fn child_result_replays_after_cleanup() -> TestResult {
    let test = setup().await?;
    test.client
        .create_queue("children", CreateQueueOptions::default())
        .await?;
    let contract = Task::<Input, Output>::builder("child")?
        .queue("children")?
        .build();
    let child_router = Router::new()
        .task(contract.handler(|_, input| async move { Ok(Output { value: input.value }) }))?;
    let child = test
        .client
        .spawn(&contract, Input { value: 42 })
        .send()
        .await?;
    work_batch(&test.client, &child_router, "children").await?;
    let parent = Task::<Input, Output>::builder("parent")?
        .default_max_attempts(2)
        .handler(move |context, _| {
            let child = child.clone();
            async move {
                let child = context
                    .step("child-reference", || async { Ok(child) })
                    .await?;
                let result = context.await_task_result(&child, None).await?;
                if context.metadata().attempt == 1 {
                    return Err(Error::handler(Box::new(TestFailure)));
                }
                Ok(result)
            }
        })
        .build();
    let router = Router::new().task(parent.clone())?;
    let parent = test
        .client
        .spawn(&parent, Input { value: 0 })
        .send()
        .await?;
    work_batch(&test.client, &router, "default").await?;
    test.client
        .set_queue_policy(
            "children",
            QueuePolicyOptions {
                cleanup_ttl: Some(PgInterval::from(Duration::ZERO)),
                ..Default::default()
            },
        )
        .await?;
    let cleanup = test.client.cleanup_queue("children").await?;
    assert_eq!(cleanup.iter().map(|row| row.tasks_deleted).sum::<i32>(), 1);
    work_batch(&test.client, &router, "default").await?;
    let result = parent
        .await_result(&test.client, Some(Duration::from_secs(1)))
        .await?;
    assert_eq!(result, Output { value: 42 });
    Ok(())
}

/// Verifies that a cancelled child fails its parent instead of abandoning it.
#[tokio::test]
async fn cancelled_child_resolves_parent_run() -> TestResult {
    let test = setup().await?;
    test.client
        .create_queue("children", CreateQueueOptions::default())
        .await?;
    let child = Task::<Input, Output>::builder("child")?
        .queue("children")?
        .handler(|_, input| async move { Ok(Output { value: input.value }) })
        .build();
    let child = test.client.spawn(&child, Input { value: 1 }).send().await?;
    test.client
        .cancel_task("children", child.result.task_id.as_uuid())
        .await?;
    let parent = Task::<Input, Output>::builder("parent")?
        .default_max_attempts(1)
        .handler(move |context, _| {
            let child = child.clone();
            async move { context.await_task_result(&child, None).await }
        })
        .build();
    let router = Router::new().task(parent.clone())?;
    let parent = test
        .client
        .spawn(&parent, Input { value: 0 })
        .send()
        .await?;
    work_batch(&test.client, &router, "default").await?;
    let snapshot = test
        .client
        .fetch_task_result("default", parent.result.task_id.as_uuid())
        .await?
        .expect("parent should exist");
    assert_eq!(snapshot.state, TaskResultState::Failed);
    let checkpoints = test
        .client
        .get_checkpoints(
            "default",
            parent.result.task_id.as_uuid(),
            parent.result.run_id.as_uuid(),
        )
        .await?;
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].1["state"], "cancelled");
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

/// Verifies that default supervision retries a hung task and releases worker capacity.
#[tokio::test]
async fn default_stall_recovery_retries_and_releases_capacity() -> TestResult {
    let test = setup().await?;
    let started = Arc::new(Notify::new());
    let task_started = started.clone();
    let task = Task::<Input, Output>::builder("stall")?
        .default_max_attempts(2)
        .handler(move |context, input| {
            let started = task_started.clone();
            async move {
                if input.value == 0 && context.metadata().attempt == 1 {
                    started.notify_one();
                    return std::future::pending().await;
                }
                Ok(Output { value: input.value })
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let stalled = test.client.spawn(&task, Input { value: 0 }).send().await?;
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let client = test.client.clone();
    let mut worker = tokio::spawn(async move {
        client
            .worker(router)
            .claim_timeout(Duration::from_secs(1))
            .run(worker_shutdown)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), started.notified()).await?;
    let next = test.client.spawn(&task, Input { value: 9 }).send().await?;
    let next_result = next
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await;
    let retried = stalled
        .await_result(&test.client, Some(Duration::from_secs(5)))
        .await;
    shutdown.cancel();
    let joined = tokio::time::timeout(Duration::from_secs(2), &mut worker).await;
    if joined.is_err() {
        worker.abort();
        let _ = worker.await;
    }
    joined?.expect("worker should join")?;
    assert_eq!(next_result?, Output { value: 9 });
    assert_eq!(retried?, Output { value: 0 });
    let failure: serde_json::Value =
        sqlx::query_scalar("SELECT failure_reason FROM absurd.r_default WHERE run_id = $1")
            .bind(stalled.result.run_id.as_uuid())
            .fetch_one(test.client.pool())
            .await?;
    assert_eq!(
        failure["message"],
        "execution stalled without checkpoint or heartbeat progress"
    );
    Ok(())
}

/// Verifies that checkpoint writes and explicit heartbeats independently prevent stalls.
#[tokio::test]
async fn application_progress_resets_default_stall_deadline() -> TestResult {
    let test = setup().await?;
    let task = Task::<i32, ()>::builder("progress")?
        .default_max_attempts(1)
        .handler(|context, kind| async move {
            if kind == 2 {
                context.heartbeat(Duration::from_millis(1100)).await?;
                tokio::time::sleep(Duration::from_millis(1400)).await;
                return Ok(());
            }
            for i in 0..4 {
                tokio::time::sleep(Duration::from_millis(400)).await;
                let context = context.clone();
                if kind == 1 {
                    context.heartbeat(Duration::from_secs(1)).await?;
                } else {
                    context.step("progress", || async { Ok(i) }).await?;
                }
            }
            Ok(())
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let checkpointed = test.client.spawn(&task, 0).send().await?;
    let heartbeating = test.client.spawn(&task, 1).send().await?;
    let extended = test.client.spawn(&task, 2).send().await?;
    let leases = test
        .client
        .claim_task(
            "default",
            &elephant::worker::ClaimOptions {
                claim_timeout: Duration::from_secs(1),
                batch_size: 3,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(leases.len(), 3);
    futures::future::try_join_all(
        leases
            .into_iter()
            .map(|lease| router.dispatch_with(lease, ExecutionOptions::default())),
    )
    .await?;
    for spawned in [checkpointed, heartbeating, extended] {
        spawned
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?;
    }
    Ok(())
}

/// Verifies that applications can select or explicitly disable the inactivity window.
#[tokio::test]
async fn stall_policy_can_be_overridden() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("stall-policy")?
        .default_max_attempts(1)
        .handler(|_, ()| async {
            tokio::time::sleep(Duration::from_millis(1100)).await;
            Ok(())
        })
        .build();
    let router = Router::new().task(task.clone())?;
    for (stall_timeout, expected) in [
        (
            StallTimeout::After(Duration::from_millis(50)),
            TaskResultState::Failed,
        ),
        (StallTimeout::Disabled, TaskResultState::Completed),
    ] {
        let spawned = test.client.spawn(&task, ()).send().await?;
        let lease = test
            .client
            .claim_task(
                "default",
                &elephant::worker::ClaimOptions {
                    claim_timeout: Duration::from_secs(1),
                    ..Default::default()
                },
            )
            .await?
            .pop()
            .expect("task should be claimable");
        router
            .dispatch_with(
                lease,
                ExecutionOptions {
                    stall_timeout,
                    cancellation_grace: Duration::ZERO,
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            test.client
                .fetch_task_result("default", spawned.result.task_id.as_uuid())
                .await?
                .expect("task should exist")
                .state,
            expected
        );
    }
    Ok(())
}

/// Verifies that blocked terminal persistence cannot stall supervised dispatch.
#[tokio::test]
async fn run_resolution_is_bounded_when_database_rows_are_locked() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("blocked-completion")?
        .handler(|_, ()| async { Ok(()) })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, ()).send().await?;
    let lease = test
        .client
        .claim_task(
            "default",
            &elephant::worker::ClaimOptions {
                claim_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .await?
        .pop()
        .expect("task should be claimable");
    let mut lock = test.client.pool().begin().await?;
    sqlx::query("SELECT task_id FROM absurd.t_default WHERE task_id = $1 FOR UPDATE")
        .bind(spawned.result.task_id.as_uuid())
        .fetch_one(&mut *lock)
        .await?;
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        router.dispatch_with(lease, ExecutionOptions::default()),
    )
    .await;
    lock.rollback().await?;
    assert!(matches!(result?, Err(Error::RunResolutionTimeout)));
    Ok(())
}

/// Verifies that local cancellation cannot be overwritten by a late success.
#[tokio::test]
async fn local_cancellation_wins_over_handler_success() -> TestResult {
    let test = setup().await?;
    let task = Task::<Input, Output>::builder("local-cancel")?
        .default_max_attempts(1)
        .handler(|context, _| async move {
            context.cancellation_token().cancel();
            Ok(Output { value: 1 })
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, Input { value: 0 }).send().await?;
    work_batch(&test.client, &router, "default").await?;
    let snapshot = test
        .client
        .fetch_task_result("default", spawned.result.task_id.as_uuid())
        .await?
        .expect("cancelled execution should exist");
    assert_eq!(snapshot.state, TaskResultState::Failed);
    Ok(())
}

/// Verifies deadlines signal cleanup and fail even noncooperative dispatches.
#[tokio::test]
async fn execution_deadlines_resolve_manual_dispatch() -> TestResult {
    let test = setup().await?;
    let cleanups = Arc::new(AtomicUsize::new(0));
    for cooperative in [true, false] {
        let handler_cleanups = Arc::clone(&cleanups);
        let task = Task::<Input, Output>::builder("deadline")?
            .default_max_attempts(1)
            .handler(move |context, _| {
                let cleanups = Arc::clone(&handler_cleanups);
                async move {
                    if cooperative {
                        context.cancellation_token().cancelled().await;
                        cleanups.fetch_add(1, Ordering::SeqCst);
                        Ok(Output { value: 0 })
                    } else {
                        std::future::pending().await
                    }
                }
            })
            .build();
        let router = Router::new().task(task.clone())?;
        let spawned = test.client.spawn(&task, Input { value: 0 }).send().await?;
        let lease = test
            .client
            .claim_task("default", &Default::default())
            .await?
            .pop()
            .expect("deadline task should be claimable");
        tokio::time::timeout(
            Duration::from_secs(2),
            router.dispatch_with(
                lease,
                ExecutionOptions {
                    timeout: Some(Duration::from_millis(200)),
                    cancellation_grace: Duration::from_millis(10),
                    ..Default::default()
                },
            ),
        )
        .await??;
        let snapshot = test
            .client
            .fetch_task_result("default", spawned.result.task_id.as_uuid())
            .await?
            .expect("task should exist");
        assert_eq!(snapshot.state, TaskResultState::Failed);
        assert_eq!(
            snapshot.failure.expect("deadline should be recorded")["message"],
            "execution deadline expired"
        );
    }
    assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    Ok(())
}

/// Verifies that buffered claims are not given a fresh lease at dispatch time.
#[tokio::test]
async fn expired_buffered_claim_does_not_start_execution() -> TestResult {
    let test = setup().await?;
    test.client
        .spawn_untyped("expired", Input { value: 0 }, SpawnOptions::default())
        .await?;
    let lease = test
        .client
        .claim_task(
            "default",
            &elephant::worker::ClaimOptions {
                claim_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .await?
        .pop()
        .expect("task should be claimable");
    tokio::time::sleep(Duration::from_millis(1050)).await;
    let calls = AtomicUsize::new(0);
    let cancellation = CancellationToken::new();
    let result = lease
        .run_supervised(
            async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            ExecutionOptions::default(),
            cancellation.clone(),
        )
        .await;
    assert!(matches!(result, Err(Error::LeaseRenewalTimeout)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(cancellation.is_cancelled());
    Ok(())
}

/// Verifies that a stalled renewal cannot outlive the local claim deadline.
#[tokio::test]
async fn stalled_renewal_is_bounded_by_claim_deadline() -> TestResult {
    let test = setup_with_max_connections(1).await?;
    test.client
        .spawn_untyped("stalled", Input { value: 0 }, SpawnOptions::default())
        .await?;
    let lease = test
        .client
        .claim_task(
            "default",
            &elephant::worker::ClaimOptions {
                claim_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .await?
        .pop()
        .expect("task should be claimable");
    let connection = test.client.pool().acquire().await?;
    let cancellation = CancellationToken::new();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        lease.run_supervised(
            std::future::pending::<elephant::error::Result<()>>(),
            ExecutionOptions {
                cancellation_grace: Duration::ZERO,
                ..Default::default()
            },
            cancellation.clone(),
        ),
    )
    .await?;
    drop(connection);
    assert!(matches!(result, Err(Error::LeaseRenewalTimeout)));
    assert!(cancellation.is_cancelled());
    Ok(())
}

/// Verifies renewal infrastructure errors stop work and leave database recovery.
#[tokio::test]
async fn renewal_errors_stop_dispatch_and_surface_to_supervisor() -> TestResult {
    let test = setup().await?;
    let cleaned_up = Arc::new(AtomicUsize::new(0));
    let handler_cleanup = Arc::clone(&cleaned_up);
    let task = Task::<Input, Output>::builder("renewal-error")?
        .handler(move |context, _| {
            let cleaned_up = Arc::clone(&handler_cleanup);
            async move {
                context.cancellation_token().cancelled().await;
                cleaned_up.fetch_add(1, Ordering::SeqCst);
                Ok(Output { value: 1 })
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, Input { value: 0 }).send().await?;
    let lease = test
        .client
        .claim_task("default", &Default::default())
        .await?
        .pop()
        .expect("task should be claimable");
    sqlx::query("ALTER FUNCTION absurd.extend_claim(text, uuid, integer) RENAME TO unavailable_extend_claim")
        .execute(test.client.pool()).await?;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        router.dispatch_with(
            lease,
            ExecutionOptions {
                lease_renewal: LeaseRenewal::Custom(LeaseWatchdogOptions {
                    interval: Duration::from_millis(100),
                    extend_by: Duration::from_secs(1),
                }),
                ..Default::default()
            },
        ),
    )
    .await?;
    assert!(matches!(result, Err(Error::Sqlx { .. })));
    assert_eq!(cleaned_up.load(Ordering::SeqCst), 1);
    let snapshot = test
        .client
        .fetch_task_result("default", spawned.result.task_id.as_uuid())
        .await?
        .expect("lease should remain recoverable");
    assert_eq!(snapshot.state, TaskResultState::Running);
    Ok(())
}

/// Verifies that detected cancellation stops a handler that does not cooperate.
#[tokio::test]
async fn cancelled_lease_stops_handler() -> TestResult {
    let test = setup().await?;
    let started = Arc::new(Notify::new());
    let release = CancellationToken::new();
    let task_started = Arc::clone(&started);
    let task_release = release.clone();
    let task = Task::<Input, Output>::builder("cancel-active")?
        .handler(move |_, input| {
            let started = Arc::clone(&task_started);
            let release = task_release.clone();
            async move {
                started.notify_one();
                release.cancelled().await;
                Ok(Output { value: input.value })
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, Input { value: 1 }).send().await?;
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let client = test.client.clone();
    let mut worker = tokio::spawn(async move {
        client
            .worker(router)
            .lease_watchdog(LeaseWatchdogOptions {
                interval: Duration::from_millis(10),
                extend_by: Duration::from_secs(1),
            })
            .run(worker_shutdown)
            .await
    });
    let did_start = tokio::time::timeout(Duration::from_secs(2), started.notified()).await;
    test.client
        .cancel_task("default", spawned.result.task_id.as_uuid())
        .await?;
    shutdown.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(2), &mut worker).await;
    let did_stop = match stopped {
        Ok(result) => {
            result.expect("worker should join")?;
            true
        }
        Err(_) => {
            release.cancel();
            worker.await.expect("worker should join")?;
            false
        }
    };
    did_start?;
    assert!(did_stop, "worker ignored detected task cancellation");
    Ok(())
}

/// Verifies that a convenience worker inherits its client's default queue.
#[tokio::test]
async fn worker_uses_client_default_queue() -> TestResult {
    let test = setup().await?;
    let mut builder = Client::builder(test.client.pool().clone());
    builder.default_queue(QueueName::from_static("workers"));
    let client = builder.build();
    client
        .create_queue("workers", CreateQueueOptions::default())
        .await?;
    let task = Task::<Input, Output>::builder("default-queue")?
        .handler(|_, input| async move { Ok(Output { value: input.value }) })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = client.spawn(&task, Input { value: 9 }).send().await?;
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_client = client.clone();
    let worker =
        tokio::spawn(async move { worker_client.worker(router).run(worker_shutdown).await });
    let result = spawned
        .await_result(&client, Some(Duration::from_secs(1)))
        .await;
    shutdown.cancel();
    worker.await.expect("worker should join")?;
    assert_eq!(result?, Output { value: 9 });
    Ok(())
}

/// Verifies that spare capacity does not prevent execution or completion.
#[tokio::test]
async fn worker_executes_below_capacity() -> TestResult {
    let test = setup().await?;
    let task = Task::<Input, Output>::builder("single")?
        .handler(|_, input| async move { Ok(Output { value: input.value }) })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, Input { value: 1 }).send().await?;
    let shutdown = CancellationToken::new();
    let client = test.client.clone();
    let worker_shutdown = shutdown.clone();
    let worker = tokio::spawn(async move {
        client
            .worker(router)
            .concurrency(2)
            .run(worker_shutdown)
            .await
    });
    let result = spawned
        .await_result(&test.client, Some(Duration::from_secs(1)))
        .await;
    shutdown.cancel();
    worker.await.expect("worker should join")?;
    assert_eq!(result?, Output { value: 1 });
    Ok(())
}

/// Verifies that batch claims do not exceed available worker slots.
#[tokio::test]
async fn worker_limits_claims_to_capacity() -> TestResult {
    let test = setup().await?;
    let started = Arc::new(Notify::new());
    let release = CancellationToken::new();
    let task_started = Arc::clone(&started);
    let task_release = release.clone();
    let task = Task::<Input, Output>::builder("blocked")?
        .handler(move |_, input| {
            let started = Arc::clone(&task_started);
            let release = task_release.clone();
            async move {
                started.notify_one();
                release.cancelled().await;
                Ok(Output { value: input.value })
            }
        })
        .build();
    for value in 0..4 {
        test.client.spawn(&task, Input { value }).send().await?;
    }
    let router = Router::new().task(task)?;
    let shutdown = CancellationToken::new();
    let client = test.client.clone();
    let worker_shutdown = shutdown.clone();
    let worker = tokio::spawn(async move {
        elephant::worker::run_worker(
            client,
            router,
            WorkerOptions {
                concurrency: 2,
                claim: elephant::worker::ClaimOptions {
                    batch_size: 8,
                    ..Default::default()
                },
                ..Default::default()
            },
            worker_shutdown,
        )
        .await
    });
    let did_start = tokio::time::timeout(Duration::from_secs(2), started.notified()).await;
    let claimed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM absurd.r_default WHERE state = 'running'")
            .fetch_one(test.client.pool())
            .await?;
    shutdown.cancel();
    release.cancel();
    worker.await.expect("worker should join")?;
    did_start?;
    assert_eq!(claimed, 2);
    Ok(())
}

/// Verifies worker shutdown waits for in-flight tasks.
#[tokio::test]
async fn worker_shutdown_waits_for_in_flight_task() -> Result<(), Box<dyn StdError + Send + Sync>> {
    let test = setup().await?;
    let started = Arc::new(Notify::new());
    let task_started = Arc::clone(&started);
    let task = Task::<Input, Output>::builder("worker-task")?
        .queue("default")?
        .handler(move |_context, _input| {
            let started = Arc::clone(&task_started);
            async move {
                started.notify_one();
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(Output { value: 33 })
            }
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
                execution: ExecutionOptions {
                    lease_renewal: LeaseRenewal::Custom(LeaseWatchdogOptions {
                        interval: Duration::from_millis(10),
                        extend_by: Duration::from_secs(1),
                    }),
                    ..Default::default()
                },
                ..WorkerOptions::default()
            },
            worker_shutdown,
        )
        .await
    });

    started.notified().await;
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
