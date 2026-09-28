//! Behavioral coverage shared with the upstream SDK suites.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use elephant::{
    client::Client,
    error::Error,
    task::{Router, Task},
    types::{
        CancellationPolicy, CreateQueueOptions, QueueDetachMode, QueuePolicy, QueuePolicyOptions,
        QueueStorageMode, RetryStrategy, TaskId, TaskResultState,
    },
    worker::work_batch,
};
use serde_json::Value;

use super::{TestFailure, TestResult, setup};

/// Reads a task's persisted fields from the default queue.
async fn task_row(client: &Client, task_id: TaskId) -> elephant::error::Result<Value> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(t) FROM absurd.t_default t WHERE task_id = $1")
            .bind(task_id.as_uuid())
            .fetch_one(client.pool())
            .await?,
    )
}

/// Reads a run's persisted fields from the default queue.
async fn run_row(
    client: &Client,
    run_id: elephant::types::RunId,
) -> elephant::error::Result<Value> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(r) FROM absurd.r_default r WHERE run_id = $1")
            .bind(run_id.as_uuid())
            .fetch_one(client.pool())
            .await?,
    )
}

/// Verifies partitioned queue options produce the expected relation kinds.
#[tokio::test]
async fn partitioned_queue_relations() -> TestResult {
    let test = setup().await?;
    test.client
        .create_queue(
            "partitioned",
            CreateQueueOptions {
                storage_mode: QueueStorageMode::Partitioned,
                ..CreateQueueOptions::default()
            },
        )
        .await?;
    for (prefix, kind) in [
        ("t", "p"),
        ("r", "p"),
        ("c", "p"),
        ("w", "p"),
        ("e", "r"),
        ("i", "r"),
    ] {
        let actual: String =
            sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE oid = to_regclass($1)")
                .bind(format!("absurd.{prefix}_partitioned"))
                .fetch_one(test.client.pool())
                .await?;
        assert_eq!(actual, kind, "{prefix}_partitioned");
    }
    Ok(())
}

/// Verifies every policy field survives creation and partial updates.
#[tokio::test]
async fn full_queue_policy_round_trip() -> TestResult {
    let test = setup().await?;
    test.client
        .create_queue(
            "policy",
            CreateQueueOptions {
                storage_mode: QueueStorageMode::Partitioned,
                policy: QueuePolicyOptions {
                    partition_lookahead: Some("2 days".parse()?),
                    partition_lookback: Some("1 day".parse()?),
                    cleanup_ttl: Some("1 hour".parse()?),
                    cleanup_limit: Some(321),
                    detach_mode: Some(QueueDetachMode::Empty),
                    detach_min_age: Some("1 day".parse()?),
                },
            },
        )
        .await?;
    let mut expected = QueuePolicy {
        queue_name: "policy".parse()?,
        storage_mode: QueueStorageMode::Partitioned,
        partition_lookahead: "2 days".into(),
        partition_lookback: "1 day".into(),
        cleanup_ttl: "01:00:00".into(),
        cleanup_limit: 321,
        detach_mode: QueueDetachMode::Empty,
        detach_min_age: "1 day".into(),
    };
    assert_eq!(
        test.client.get_queue_policy("policy").await?,
        Some(expected.clone())
    );
    test.client
        .set_queue_policy(
            "policy",
            QueuePolicyOptions {
                cleanup_ttl: Some("2 hours".parse()?),
                cleanup_limit: Some(32),
                detach_mode: Some(QueueDetachMode::None),
                detach_min_age: Some("3 days".parse()?),
                ..QueuePolicyOptions::default()
            },
        )
        .await?;
    expected.cleanup_ttl = "02:00:00".into();
    expected.cleanup_limit = 32;
    expected.detach_mode = QueueDetachMode::None;
    expected.detach_min_age = "3 days".into();
    assert_eq!(
        test.client.get_queue_policy("policy").await?,
        Some(expected)
    );
    Ok(())
}

/// Verifies valid non-identifier queue names work through the SQL boundary.
#[tokio::test]
async fn permissive_queue_names() -> TestResult {
    let test = setup().await?;
    for name in ["Uppercase", "with spaces", "with-hyphens", "   "] {
        test.client
            .create_queue(name, CreateQueueOptions::default())
            .await?;
        test.client.emit_event(name, "event", &42).await?;
        assert!(
            test.client
                .list_queues()
                .await?
                .iter()
                .any(|queue| queue.as_str() == name)
        );
        assert_eq!(
            test.client
                .get_queue_policy(name)
                .await?
                .expect("created queue")
                .queue_name
                .as_str(),
            name
        );
        test.client.drop_queue(name).await?;
        assert!(
            !test
                .client
                .list_queues()
                .await?
                .iter()
                .any(|queue| queue.as_str() == name)
        );
    }
    Ok(())
}

/// Verifies client defaults, task defaults, and spawn overrides select retry limits.
#[tokio::test]
async fn retry_limit_precedence() -> TestResult {
    let test = setup().await?;
    let mut builder = Client::builder(test.client.pool().clone());
    builder.default_queue("default")?.default_max_attempts(2);
    let client = builder.build();
    for (index, limit) in [2, 3, 4].into_iter().enumerate() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = calls.clone();
        let mut builder = Task::<(), ()>::builder(format!("limit-{index}"))?;
        if index > 0 {
            builder = builder.default_max_attempts(3);
        }
        let task = builder.build().handler(move |_, ()| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async { Err(Error::handler(Box::new(TestFailure))) }
        });
        let router = Router::new().task(task.clone())?;
        let mut spawn = client.spawn(&task, ());
        if index == 2 {
            spawn = spawn.max_attempts(4);
        }
        let spawned = spawn.send().await?;
        assert_eq!(
            task_row(&client, spawned.result.task_id).await?["max_attempts"],
            limit
        );
        for _ in 0..limit {
            work_batch(&client, &router, "default").await?;
        }
        let snapshot = client
            .fetch_task_result("default", spawned.result.task_id.as_uuid())
            .await?
            .expect("task");
        assert_eq!(snapshot.state, TaskResultState::Failed);
        work_batch(&client, &router, "default").await?;
        assert_eq!(calls.load(Ordering::SeqCst), limit as usize);
    }
    Ok(())
}

/// Verifies duplicate idempotency keys create one task and execute one handler.
#[tokio::test]
async fn idempotent_spawns_execute_once() -> TestResult {
    let test = setup().await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let task = Task::<i32, i32>::builder("once")?
        .handler(move |_, value| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(value) }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let first = test
        .client
        .spawn(&task, 1)
        .idempotency_key("key")
        .send()
        .await?;
    assert!(first.result.created);
    assert_eq!(first.result.attempt, 1);
    assert_eq!(
        task_row(&test.client, first.result.task_id).await?["state"],
        "pending"
    );
    for value in [2, 3] {
        let duplicate = test
            .client
            .spawn(&task, value)
            .idempotency_key("key")
            .send()
            .await?;
        assert_eq!(
            duplicate.result,
            elephant::types::SpawnResult {
                created: false,
                ..first.result
            }
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM absurd.t_default")
        .fetch_one(test.client.pool())
        .await?;
    assert_eq!(count, 1);
    for _ in 0..3 {
        work_batch(&test.client, &router, "default").await?;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let snapshot = test
        .client
        .fetch_task_result("default", first.result.task_id.as_uuid())
        .await?
        .expect("task");
    assert_eq!(snapshot.state, TaskResultState::Completed);
    assert_eq!(snapshot.result, Some(serde_json::json!(1)));
    Ok(())
}

/// Verifies distinct keys and absent keys create independent tasks.
#[tokio::test]
async fn distinct_and_absent_idempotency_keys() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("independent")?.build();
    let mut ids = Vec::new();
    for key in [Some("one"), Some("two"), None, None] {
        let mut spawn = test.client.spawn(&task, ());
        if let Some(key) = key {
            spawn = spawn.idempotency_key(key);
        }
        let spawned = spawn.send().await?;
        assert!(spawned.result.created);
        assert_eq!(spawned.result.attempt, 1);
        assert!(!ids.contains(&spawned.result.task_id));
        ids.push(spawned.result.task_id);
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM absurd.t_default")
        .fetch_one(test.client.pool())
        .await?;
    assert_eq!(count, 4);
    Ok(())
}

/// Verifies completion does not release an idempotency key.
#[tokio::test]
async fn completed_task_retains_idempotency_key() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("retained")?
        .handler(|_, ()| async { Ok(()) })
        .build();
    let first = test
        .client
        .spawn(&task, ())
        .idempotency_key("key")
        .send()
        .await?;
    work_batch(&test.client, &Router::new().task(task.clone())?, "default").await?;
    assert_eq!(
        task_row(&test.client, first.result.task_id).await?["state"],
        "completed"
    );
    let duplicate = test
        .client
        .spawn(&task, ())
        .idempotency_key("key")
        .send()
        .await?;
    assert_eq!(
        duplicate.result,
        elephant::types::SpawnResult {
            created: false,
            ..first.result
        }
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM absurd.t_default")
        .fetch_one(test.client.pool())
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

/// Verifies idempotency keys do not deduplicate tasks across queues.
#[tokio::test]
async fn idempotency_keys_are_queue_scoped() -> TestResult {
    let test = setup().await?;
    test.client
        .create_queue("other", CreateQueueOptions::default())
        .await?;
    let task = Task::<(), ()>::builder("scoped")?.build();
    let first = test
        .client
        .spawn(&task, ())
        .idempotency_key("key")
        .send()
        .await?;
    let second = test
        .client
        .spawn(&task, ())
        .queue("other")?
        .idempotency_key("key")
        .send()
        .await?;
    assert!(first.result.created && second.result.created);
    assert_ne!(first.result.task_id, second.result.task_id);
    assert_ne!(first.result.run_id, second.result.run_id);
    for spawned in [&first, &second] {
        assert_eq!(
            test.client
                .fetch_task_result(
                    spawned.queue_name.as_str(),
                    spawned.result.task_id.as_uuid()
                )
                .await?
                .expect("task")
                .state,
            TaskResultState::Pending
        );
    }
    Ok(())
}

/// Verifies immediate retry state accounting and preservation of failed runs.
#[tokio::test]
async fn immediate_retry_exhaustion_preserves_runs() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("exhaust")?
        .default_max_attempts(2)
        .handler(|_, ()| async { Err(Error::handler(Box::new(TestFailure))) })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &router, "default").await?;
    let pending = task_row(&test.client, spawned.result.task_id).await?;
    assert_eq!(pending["state"], "pending");
    assert_eq!(pending["attempts"], 2);
    let first = run_row(&test.client, spawned.result.run_id).await?;
    assert_eq!(first["state"], "failed");
    assert_eq!(
        first["failure_reason"]["message"],
        "intentional test failure"
    );
    let lease = test
        .client
        .claim_task("default", &Default::default())
        .await?
        .pop()
        .expect("retry");
    assert_eq!(lease.claimed_run().attempt, 2);
    assert_ne!(lease.claimed_run().run_id, spawned.result.run_id);
    router.dispatch(lease).await?;
    let failed = task_row(&test.client, spawned.result.task_id).await?;
    assert_eq!(failed["state"], "failed");
    assert_eq!(failed["attempts"], 2);
    assert_eq!(run_row(&test.client, spawned.result.run_id).await?, first);
    assert!(
        test.client
            .claim_task("default", &Default::default())
            .await?
            .is_empty()
    );
    Ok(())
}

/// Sets deterministic database time on a single-connection test pool.
async fn clock(client: &Client, seconds: i32) -> elephant::error::Result<()> {
    sqlx::query("SELECT set_config('absurd.fake_now', (timestamptz '2025-01-01 00:00:00Z' + make_interval(secs => $1::double precision))::text, false)")
        .bind(f64::from(seconds)).execute(client.pool()).await?;
    Ok(())
}

/// Exercises scheduled retries before and at each configured backoff deadline.
async fn check_backoff(strategy: RetryStrategy, delays: &[i32]) -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let limit = i32::try_from(delays.len())? + 1;
    let task = Task::<(), i32>::builder("backoff")?
        .default_max_attempts(limit)
        .handler(move |context, ()| async move {
            if context.metadata().attempt < limit {
                Err(Error::handler(Box::new(TestFailure)))
            } else {
                Ok(context.metadata().attempt)
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test
        .client
        .spawn(&task, ())
        .retry_strategy(strategy.clone())
        .send()
        .await?;
    assert_eq!(
        task_row(&test.client, spawned.result.task_id).await?["retry_strategy"],
        strategy.to_json()
    );
    let mut now = 0;
    for (index, delay) in delays.iter().copied().enumerate() {
        work_batch(&test.client, &router, "default").await?;
        let row = task_row(&test.client, spawned.result.task_id).await?;
        assert_eq!(row["state"], "sleeping");
        assert_eq!(row["attempts"], index + 2);
        let remaining: f64 = sqlx::query_scalar("SELECT extract(epoch FROM available_at - absurd.current_time())::float8 FROM absurd.r_default WHERE task_id = $1 ORDER BY attempt DESC LIMIT 1")
            .bind(spawned.result.task_id.as_uuid()).fetch_one(test.client.pool()).await?;
        assert_eq!(remaining, f64::from(delay));
        clock(&test.client, now + delay - 1).await?;
        assert!(
            test.client
                .claim_task("default", &Default::default())
                .await?
                .is_empty()
        );
        now += delay;
        clock(&test.client, now).await?;
    }
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        spawned
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?,
        limit
    );
    Ok(())
}

/// Verifies a positive fixed delay is persisted and enforced before retry.
#[tokio::test]
async fn positive_fixed_retry_backoff() -> TestResult {
    check_backoff(
        RetryStrategy::Fixed {
            base: Duration::from_secs(10),
        },
        &[10],
    )
    .await
}

/// Verifies exponential backoff grows across successive failed attempts.
#[tokio::test]
async fn exponential_retry_backoff() -> TestResult {
    check_backoff(
        RetryStrategy::Exponential {
            base: Duration::from_secs(40),
            factor: 2.0,
            max: None,
        },
        &[40, 80],
    )
    .await
}

/// Verifies explicit retry reopens an exhausted task with another run.
#[tokio::test]
async fn manual_retry_extends_exhausted_task() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("manual-retry")?
        .default_max_attempts(1)
        .handler(|context, ()| async move {
            if context.metadata().attempt == 1 {
                Err(Error::handler(Box::new(TestFailure)))
            } else {
                Ok(())
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let original = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        task_row(&test.client, original.result.task_id).await?["state"],
        "failed"
    );
    let retried = test
        .client
        .retry_task(
            "default",
            original.result.task_id.as_uuid(),
            Default::default(),
        )
        .await?;
    assert_eq!(retried.task_id, original.result.task_id);
    assert_ne!(retried.run_id, original.result.run_id);
    assert_eq!(retried.attempt, 2);
    assert!(!retried.created);
    let pending = task_row(&test.client, retried.task_id).await?;
    assert_eq!(pending["state"], "pending");
    assert_eq!(pending["attempts"], 2);
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        task_row(&test.client, retried.task_id).await?["state"],
        "completed"
    );
    assert_eq!(
        run_row(&test.client, original.result.run_id).await?["state"],
        "failed"
    );
    Ok(())
}

/// Verifies retry-as-new leaves the original task and checkpoints behind.
#[tokio::test]
async fn retry_as_new_discards_checkpoints() -> TestResult {
    let test = setup().await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let task = Task::<(), ()>::builder("fresh-retry")?
        .default_max_attempts(1)
        .handler(move |context, ()| {
            let calls = handler_calls.clone();
            async move {
                context
                    .step("effect", || async {
                        Ok(calls.fetch_add(1, Ordering::SeqCst))
                    })
                    .await?;
                Err(Error::handler(Box::new(TestFailure)))
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let original = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &router, "default").await?;
    assert!(
        test.client
            .get_checkpoint("default", original.result.task_id.as_uuid(), "effect", true)
            .await?
            .is_some()
    );
    let retried = test
        .client
        .retry_task(
            "default",
            original.result.task_id.as_uuid(),
            elephant::types::RetryTaskOptions {
                spawn_new: true,
                ..Default::default()
            },
        )
        .await?;
    assert!(retried.created);
    assert_ne!(retried.task_id, original.result.task_id);
    assert_eq!(retried.attempt, 1);
    assert_eq!(
        task_row(&test.client, retried.task_id).await?["state"],
        "pending"
    );
    assert!(
        test.client
            .get_checkpoints(
                "default",
                retried.task_id.as_uuid(),
                retried.run_id.as_uuid()
            )
            .await?
            .is_empty()
    );
    assert_eq!(
        task_row(&test.client, original.result.task_id).await?["state"],
        "failed"
    );
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

/// Verifies explicit and task-default cancellation policies cancel durable work.
#[tokio::test]
async fn durable_cancellation_policies() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    for case in 0..3 {
        clock(&test.client, case * 100).await?;
        let delay = CancellationPolicy {
            max_delay: Some(Duration::from_secs(20)),
            ..Default::default()
        };
        let policy = if case == 2 {
            CancellationPolicy {
                max_duration: Some(Duration::from_secs(30)),
                ..Default::default()
            }
        } else {
            delay.clone()
        };
        let mut builder =
            Task::<(), ()>::builder(format!("cancel-policy-{case}"))?.default_max_attempts(2);
        if case > 0 {
            builder = builder.default_cancellation(delay);
        }
        let task = builder
            .handler(|_, ()| async { Err(Error::handler(Box::new(TestFailure))) })
            .build();
        let router = Router::new().task(task.clone())?;
        let mut spawn = test
            .client
            .spawn(&task, ())
            .retry_strategy(RetryStrategy::Fixed {
                base: Duration::from_secs(10),
            });
        if case != 1 {
            spawn = spawn.cancellation(policy.clone());
        }
        let spawned = spawn.send().await?;
        assert_eq!(
            task_row(&test.client, spawned.result.task_id).await?["cancellation"],
            policy.to_json()
        );
        if case == 2 {
            work_batch(&test.client, &router, "default").await?;
        }
        clock(&test.client, case * 100 + 40).await?;
        assert!(
            test.client
                .claim_task("default", &Default::default())
                .await?
                .is_empty()
        );
        let row = task_row(&test.client, spawned.result.task_id).await?;
        assert_eq!(row["state"], "cancelled");
        assert!(!row["cancelled_at"].is_null());
        let state: String = sqlx::query_scalar(
            "SELECT state FROM absurd.r_default WHERE task_id = $1 ORDER BY attempt DESC LIMIT 1",
        )
        .bind(spawned.result.task_id.as_uuid())
        .fetch_one(test.client.pool())
        .await?;
        assert_eq!(state, "cancelled");
    }
    Ok(())
}

/// Verifies cancellation terminates pending and running task/run pairs.
#[tokio::test]
async fn cancellation_terminal_database_state() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("cancel-state")?.build();
    for running in [false, true] {
        let spawned = test.client.spawn(&task, ()).send().await?;
        let leases = if running {
            test.client
                .claim_task("default", &Default::default())
                .await?
        } else {
            Vec::new()
        };
        assert_eq!(leases.len(), usize::from(running));
        test.client
            .cancel_task("default", spawned.result.task_id.as_uuid())
            .await?;
        let row = task_row(&test.client, spawned.result.task_id).await?;
        assert_eq!(row["state"], "cancelled");
        assert!(!row["cancelled_at"].is_null());
        assert_eq!(
            run_row(&test.client, spawned.result.run_id).await?["state"],
            "cancelled"
        );
        assert!(
            test.client
                .claim_task("default", &Default::default())
                .await?
                .is_empty()
        );
        for lease in leases {
            lease.forget();
        }
    }
    Ok(())
}

/// Verifies repeated cancellation preserves the original cancellation timestamp.
#[tokio::test]
async fn cancellation_is_idempotent() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let task = Task::<(), ()>::builder("cancel-twice")?.build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    test.client
        .cancel_task("default", spawned.result.task_id.as_uuid())
        .await?;
    let first = task_row(&test.client, spawned.result.task_id).await?;
    assert!(!first["cancelled_at"].is_null());
    clock(&test.client, 60).await?;
    test.client
        .cancel_task("default", spawned.result.task_id.as_uuid())
        .await?;
    assert_eq!(task_row(&test.client, spawned.result.task_id).await?, first);
    Ok(())
}

/// Verifies cancellation leaves successful and failed tasks unchanged.
#[tokio::test]
async fn terminal_task_cancellation_is_noop() -> TestResult {
    let test = setup().await?;
    let task = Task::<bool, ()>::builder("terminal-cancel")?
        .default_max_attempts(1)
        .handler(|_, fail| async move {
            if fail {
                Err(Error::handler(Box::new(TestFailure)))
            } else {
                Ok(())
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    for (fail, state) in [(false, "completed"), (true, "failed")] {
        let spawned = test.client.spawn(&task, fail).send().await?;
        work_batch(&test.client, &router, "default").await?;
        let before = task_row(&test.client, spawned.result.task_id).await?;
        assert_eq!(before["state"], state);
        assert!(before["cancelled_at"].is_null());
        test.client
            .cancel_task("default", spawned.result.task_id.as_uuid())
            .await?;
        assert_eq!(
            task_row(&test.client, spawned.result.task_id).await?,
            before
        );
    }
    Ok(())
}

/// Verifies cancellation terminates an event-suspended run.
#[tokio::test]
async fn cancel_event_suspended_task() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("sleeping-cancel")?
        .handler(|context, ()| async move { context.await_event("never").await })
        .build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &Router::new().task(task)?, "default").await?;
    assert_eq!(
        task_row(&test.client, spawned.result.task_id).await?["state"],
        "sleeping"
    );
    test.client
        .cancel_task("default", spawned.result.task_id.as_uuid())
        .await?;
    assert_eq!(
        task_row(&test.client, spawned.result.task_id).await?["state"],
        "cancelled"
    );
    assert_eq!(
        run_row(&test.client, spawned.result.run_id).await?["state"],
        "cancelled"
    );
    test.client.emit_event("default", "never", &()).await?;
    assert!(
        test.client
            .claim_task("default", &Default::default())
            .await?
            .is_empty()
    );
    Ok(())
}

/// Verifies cancellation of a missing task retains the database's not-found cause.
#[tokio::test]
async fn cancel_missing_task_reports_error() -> TestResult {
    let test = setup().await?;
    let id = uuid::Uuid::nil();
    let error = test
        .client
        .cancel_task("default", id)
        .await
        .expect_err("missing task");
    let Error::Sqlx { source } = error else {
        panic!("expected database error, got {error:?}")
    };
    let database = source.as_database_error().expect("database cause");
    assert!(database.message().contains("not found"));
    assert!(database.message().contains(&id.to_string()));
    Ok(())
}

/// Verifies cancelled leases cannot write checkpoints or register event waits.
#[tokio::test]
async fn cancellation_blocks_checkpoint_and_event_writes() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("cancel-writes")?.build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    let lease = test
        .client
        .claim_task("default", &Default::default())
        .await?
        .pop()
        .expect("claim");
    let task_id = spawned.result.task_id.as_uuid();
    let run_id = spawned.result.run_id.as_uuid();
    test.client.cancel_task("default", task_id).await?;
    assert!(matches!(
        test.client
            .set_checkpoint("default", task_id, "late", 1, run_id, None)
            .await,
        Err(Error::Cancelled)
    ));
    assert!(matches!(
        test.client
            .await_event_raw("default", task_id, run_id, "wait", "event", None)
            .await,
        Err(Error::Cancelled)
    ));
    assert!(
        test.client
            .get_checkpoint("default", task_id, "late", true)
            .await?
            .is_none()
    );
    let waits: i64 = sqlx::query_scalar("SELECT count(*) FROM absurd.w_default WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(test.client.pool())
        .await?;
    assert_eq!(waits, 0);
    lease.forget();
    Ok(())
}

/// Verifies listing and dropping queues also removes their physical tables.
#[tokio::test]
async fn queue_lifecycle() -> TestResult {
    let test = setup().await?;
    test.client
        .create_queue("other", CreateQueueOptions::default())
        .await?;
    let mut names = test.client.list_queues().await?;
    names.sort();
    assert_eq!(
        names.iter().map(|name| name.as_str()).collect::<Vec<_>>(),
        ["default", "other"]
    );
    for queue in ["default", "other"] {
        for prefix in ["t", "r", "c", "w", "e"] {
            let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                .bind(format!("absurd.{prefix}_{queue}"))
                .fetch_one(test.client.pool())
                .await?;
            assert!(exists, "missing {prefix}_{queue}");
        }
    }
    test.client.drop_queue("other").await?;
    assert_eq!(test.client.list_queues().await?, vec!["default".parse()?]);
    for prefix in ["t", "r", "c", "w", "e"] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("absurd.{prefix}_other"))
            .fetch_one(test.client.pool())
            .await?;
        assert!(!exists);
    }
    test.client.emit_event("default", "survivor", &true).await?;
    Ok(())
}
