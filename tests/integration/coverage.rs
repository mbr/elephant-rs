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

/// Verifies terminal races preserve database state without failing dispatch.
#[tokio::test]
async fn dispatch_preserves_external_terminal_states() -> TestResult {
    let test = setup().await?;
    let observed = Arc::new(AtomicUsize::new(0));
    let handler_observed = observed.clone();
    let task = Task::<u8, ()>::builder("terminal-races")?
        .default_max_attempts(1)
        .handler(move |context, case| {
            let observed = handler_observed.clone();
            async move {
                let metadata = context.metadata();
                if case < 3 {
                    context
                        .client()
                        .fail_run(
                            "default",
                            metadata.run_id.as_uuid(),
                            serde_json::json!({"message": "external failure"}),
                        )
                        .await?;
                } else {
                    context
                        .client()
                        .cancel_task("default", metadata.task_id.as_uuid())
                        .await?;
                }
                let result = match case {
                    0 => context.heartbeat(Duration::from_secs(60)).await,
                    1 => context.step("late", || async { Ok(1) }).await.map(|_| ()),
                    4 => Err(Error::handler(Box::new(TestFailure))),
                    _ => Ok(()),
                };
                let expected = if case < 2 {
                    matches!(result, Err(Error::RunAlreadyFailed))
                } else if case == 4 {
                    matches!(result, Err(Error::Handler { .. }))
                } else {
                    result.is_ok()
                };
                if expected {
                    observed.fetch_or(1 << case, Ordering::SeqCst);
                }
                result
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    for case in 0..5 {
        let spawned = test.client.spawn(&task, case).send().await?;
        let lease = test
            .client
            .claim_task("default", &Default::default())
            .await?
            .pop()
            .expect("claim");
        router
            .dispatch_with(
                lease,
                elephant::run::ExecutionOptions {
                    lease_renewal: elephant::run::LeaseRenewal::Disabled,
                    ..Default::default()
                },
            )
            .await?;
        let expected = if case < 3 { "failed" } else { "cancelled" };
        assert_eq!(
            task_row(&test.client, spawned.result.task_id).await?["state"],
            expected
        );
        let run = run_row(&test.client, spawned.result.run_id).await?;
        assert_eq!(run["state"], expected);
        if case < 3 {
            assert_eq!(
                run["failure_reason"],
                serde_json::json!({"message": "external failure"})
            );
        }
        assert!(
            test.client
                .get_checkpoint("default", spawned.result.task_id.as_uuid(), "late", true)
                .await?
                .is_none()
        );
    }
    assert_eq!(observed.load(Ordering::SeqCst), 31);
    Ok(())
}

/// Verifies explicit context heartbeats forward their full lease extension.
#[tokio::test]
async fn heartbeat_uses_requested_database_extension() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let task = Task::<(), ()>::builder("heartbeat-extension")?.build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    let lease = test
        .client
        .claim_task(
            "default",
            &elephant::worker::ClaimOptions {
                claim_timeout: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .await?
        .pop()
        .expect("claim");
    let original = run_row(&test.client, spawned.result.run_id).await?;
    clock(&test.client, 1).await?;
    let context = elephant::context::TaskContext::new(
        test.client.clone(),
        lease.claimed_run(),
        Default::default(),
    );
    context.heartbeat(Duration::from_secs(120)).await?;
    let updated = run_row(&test.client, spawned.result.run_id).await?;
    let before = original["claim_expires_at"]
        .as_str()
        .expect("expiry")
        .parse::<jiff::Timestamp>()?;
    let after = updated["claim_expires_at"]
        .as_str()
        .expect("expiry")
        .parse::<jiff::Timestamp>()?;
    assert_eq!(after.duration_since(before).as_secs(), 61);
    lease.complete(()).await?;
    Ok(())
}

/// Verifies zero heartbeat extensions fail the task without modifying expiry.
#[tokio::test]
async fn zero_heartbeat_preserves_expiry_and_fails() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let task = Task::<(), ()>::builder("zero-heartbeat")?
        .default_max_attempts(1)
        .handler(|context, ()| async move { context.heartbeat(Duration::ZERO).await })
        .build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    let lease = test
        .client
        .claim_task("default", &Default::default())
        .await?
        .pop()
        .expect("claim");
    let before = run_row(&test.client, spawned.result.run_id).await?;
    Router::new().task(task)?.dispatch(lease).await?;
    let after = run_row(&test.client, spawned.result.run_id).await?;
    assert_eq!(after["claim_expires_at"], before["claim_expires_at"]);
    assert_eq!(after["state"], "failed");
    assert_eq!(
        task_row(&test.client, spawned.result.task_id).await?["state"],
        "failed"
    );
    assert!(
        after["failure_reason"]["message"]
            .as_str()
            .expect("failure message")
            .contains("extend_by must be > 0")
    );
    Ok(())
}

/// Verifies batch claim identities, ownership, expiry, and exclusion of competitors.
#[tokio::test]
async fn live_claims_exclude_other_workers() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let task = Task::<(), ()>::builder("exclusive")?.build();
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(test.client.spawn(&task, ()).send().await?.result.task_id);
    }
    let leases = test
        .client
        .claim_task(
            "default",
            &elephant::worker::ClaimOptions {
                worker_id: "owner".into(),
                claim_timeout: Duration::from_secs(60),
                batch_size: 3,
                ..Default::default()
            },
        )
        .await?;
    let mut claimed = leases
        .iter()
        .map(|lease| lease.claimed_run().task_id)
        .collect::<Vec<_>>();
    ids.sort();
    claimed.sort();
    assert_eq!(claimed, ids);
    for lease in &leases {
        let row = run_row(&test.client, lease.claimed_run().run_id).await?;
        assert_eq!(row["claimed_by"], "owner");
        assert_eq!(
            row["claim_expires_at"]
                .as_str()
                .expect("expiry")
                .parse::<jiff::Timestamp>()?,
            "2025-01-01T00:01:00Z".parse::<jiff::Timestamp>()?
        );
    }
    clock(&test.client, 59).await?;
    assert!(
        test.client
            .claim_task(
                "default",
                &elephant::worker::ClaimOptions {
                    worker_id: "competitor".into(),
                    batch_size: 3,
                    ..Default::default()
                }
            )
            .await?
            .is_empty()
    );
    for lease in leases {
        lease.complete(()).await?;
    }
    Ok(())
}

/// Verifies database recovery fences an abandoned run and transfers ownership.
#[tokio::test]
async fn expired_claim_is_retried_by_another_worker() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let task = Task::<(), ()>::builder("abandoned")?
        .default_max_attempts(2)
        .build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    let old = test
        .client
        .claim_task(
            "default",
            &elephant::worker::ClaimOptions {
                worker_id: "worker-a".into(),
                ..Default::default()
            },
        )
        .await?
        .pop()
        .expect("first claim");
    old.forget();
    clock(&test.client, 300).await?;
    let new = test
        .client
        .claim_task(
            "default",
            &elephant::worker::ClaimOptions {
                worker_id: "worker-b".into(),
                claim_timeout: Duration::from_secs(45),
                ..Default::default()
            },
        )
        .await?
        .pop()
        .expect("reclaimed run");
    assert_eq!(new.claimed_run().task_id, spawned.result.task_id);
    assert_ne!(new.claimed_run().run_id, spawned.result.run_id);
    assert_eq!(new.claimed_run().attempt, 2);
    let expired = run_row(&test.client, spawned.result.run_id).await?;
    assert_eq!(expired["state"], "failed");
    assert_eq!(expired["failure_reason"]["name"], "$ClaimTimeout");
    assert_eq!(expired["failure_reason"]["workerId"], "worker-a");
    assert_eq!(expired["failure_reason"]["attempt"], 1);
    let current = run_row(&test.client, new.claimed_run().run_id).await?;
    assert_eq!(current["state"], "running");
    assert_eq!(current["claimed_by"], "worker-b");
    let row = task_row(&test.client, spawned.result.task_id).await?;
    assert_eq!(row["state"], "running");
    assert_eq!(row["attempts"], 2);
    assert!(matches!(
        test.client
            .complete_run("default", spawned.result.run_id.as_uuid(), ())
            .await,
        Err(Error::RunAlreadyFailed)
    ));
    new.complete(()).await?;
    Ok(())
}

/// Verifies task/event cleanup honors retention and completed runs retain ownership history.
#[tokio::test]
async fn cleanup_respects_task_and_event_ttl() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    test.client
        .set_queue_policy(
            "default",
            QueuePolicyOptions {
                cleanup_ttl: Some("1 hour".parse()?),
                cleanup_limit: Some(10),
                ..Default::default()
            },
        )
        .await?;
    let task = Task::<(), ()>::builder("cleanup")?.build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    let lease = test
        .client
        .claim_task(
            "default",
            &elephant::worker::ClaimOptions {
                worker_id: "cleaner".into(),
                claim_timeout: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .await?
        .pop()
        .expect("claim");
    let before = run_row(&test.client, spawned.result.run_id).await?;
    clock(&test.client, 600).await?;
    lease.complete(()).await?;
    test.client
        .emit_event("default", "cleanup-event", &true)
        .await?;
    let completed = run_row(&test.client, spawned.result.run_id).await?;
    assert_eq!(completed["claimed_by"], "cleaner");
    assert_eq!(completed["claim_expires_at"], before["claim_expires_at"]);
    for (time, deleted, remaining) in [(2400, 0, 1_i64), (4201, 1, 0)] {
        clock(&test.client, time).await?;
        let result = test.client.cleanup_queue("default").await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].queue_name.as_str(), "default");
        assert_eq!(result[0].tasks_deleted, deleted);
        assert_eq!(result[0].events_deleted, deleted);
        let counts: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM absurd.t_default), (SELECT count(*) FROM absurd.e_default)").fetch_one(test.client.pool()).await?;
        assert_eq!(counts, (remaining, remaining));
    }
    Ok(())
}

/// Verifies ordinary handler diagnostics survive run and task persistence.
#[tokio::test]
async fn handler_failure_retains_category_and_message() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("diagnostic")?
        .default_max_attempts(1)
        .handler(|_, ()| async { Err(Error::handler(Box::new(TestFailure))) })
        .build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &Router::new().task(task)?, "default").await?;
    let snapshot = test
        .client
        .fetch_task_result("default", spawned.result.task_id.as_uuid())
        .await?
        .expect("task");
    assert_eq!(snapshot.state, TaskResultState::Failed);
    let failure = snapshot.failure.expect("failure");
    assert_eq!(failure["name"], "handler_error");
    assert_eq!(failure["message"], "intentional test failure");
    assert_eq!(
        run_row(&test.client, spawned.result.run_id).await?["failure_reason"],
        failure
    );
    Ok(())
}

/// Verifies unrelated SQLSTATE errors preserve their structured database source.
#[tokio::test]
async fn database_error_preserves_sqlstate_and_source() -> TestResult {
    let test = setup().await?;
    let error = test
        .client
        .set_queue_policy(
            "default",
            QueuePolicyOptions {
                cleanup_ttl: Some("not an interval".parse()?),
                ..Default::default()
            },
        )
        .await
        .expect_err("invalid interval");
    assert!(
        std::error::Error::source(&error)
            .expect("source")
            .to_string()
            .contains("not an interval")
    );
    let Error::Sqlx { source } = error else {
        panic!("unexpected error: {error:?}")
    };
    let database = source.as_database_error().expect("database source");
    assert_eq!(database.code().as_deref(), Some("22007"));
    assert!(database.message().contains("not an interval"));
    Ok(())
}

/// Verifies unknown-task deferral schedules positive delays without spending attempts.
#[tokio::test]
async fn unknown_deferral_preserves_attempt_and_failure_state() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let task = Task::<(), ()>::builder("unknown")?.build();
    for delay in [Duration::from_micros(500), Duration::from_secs(5)] {
        let spawned = test.client.spawn(&task, ()).send().await?;
        work_batch(
            &test.client,
            &Router::new().unknown_task_delay(delay),
            "default",
        )
        .await?;
        let task = task_row(&test.client, spawned.result.task_id).await?;
        let run = run_row(&test.client, spawned.result.run_id).await?;
        assert_eq!(task["state"], "sleeping");
        assert_eq!(task["attempts"], 1);
        assert_eq!(run["state"], "sleeping");
        assert_eq!(run["attempt"], 1);
        assert!(run["failure_reason"].is_null());
        let remaining: f64 = sqlx::query_scalar("SELECT extract(epoch FROM available_at - absurd.current_time())::float8 FROM absurd.r_default WHERE run_id = $1")
            .bind(spawned.result.run_id.as_uuid()).fetch_one(test.client.pool()).await?;
        assert!(
            remaining > 0.0 && remaining <= delay.as_secs_f64(),
            "deferral: {remaining}"
        );
        assert!(
            test.client
                .claim_task("default", &Default::default())
                .await?
                .is_empty()
        );
    }
    Ok(())
}

/// Verifies unknown-task scheduling failures retain their cause and recoverable lease.
#[tokio::test]
async fn unknown_deferral_preserves_database_error() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("unregistered")?.build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    sqlx::query("CREATE OR REPLACE FUNCTION absurd.schedule_run(p_queue_name text, p_run_id uuid, p_wake_at timestamptz) RETURNS void LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE = 'XX000', MESSAGE = 'deferral unavailable'; END $$")
        .execute(test.client.pool()).await?;
    let error = work_batch(&test.client, &Router::new(), "default")
        .await
        .expect_err("scheduling failure");
    let Error::Sqlx { source } = error else {
        panic!("unexpected error: {error:?}")
    };
    let database = source.as_database_error().expect("database source");
    assert_eq!(database.code().as_deref(), Some("XX000"));
    assert_eq!(database.message(), "deferral unavailable");
    let task = task_row(&test.client, spawned.result.task_id).await?;
    let run = run_row(&test.client, spawned.result.run_id).await?;
    assert_eq!(task["state"], "running");
    assert_eq!(task["attempts"], 1);
    assert_eq!(run["state"], "running");
    assert!(run["failure_reason"].is_null());
    assert!(!run["claim_expires_at"].is_null());
    Ok(())
}

/// Verifies external producers cannot execute a queue-bound handler on another queue.
#[tokio::test]
async fn router_rejects_mismatched_claim_queue() -> TestResult {
    let test = setup().await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let task = Task::<(), ()>::builder("queue-bound")?
        .queue("other")?
        .handler(move |_, ()| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let producer = Task::<(), ()>::builder("queue-bound")?.build();
    let wrong = test
        .client
        .spawn(&producer, ())
        .max_attempts(1)
        .send()
        .await?;
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let snapshot = test
        .client
        .fetch_task_result("default", wrong.result.task_id.as_uuid())
        .await?
        .expect("task");
    assert_eq!(snapshot.state, TaskResultState::Failed);
    let failure = snapshot.failure.expect("queue mismatch");
    assert!(
        failure["message"]
            .as_str()
            .expect("message")
            .contains("uses queue \"other\", not \"default\"")
    );
    test.client
        .create_queue("other", CreateQueueOptions::default())
        .await?;
    let right = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &router, "other").await?;
    assert_eq!(
        test.client
            .fetch_task_result("other", right.result.task_id.as_uuid())
            .await?
            .expect("task")
            .state,
        TaskResultState::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// Verifies failed step bodies leave no checkpoint and execute again on retry.
#[tokio::test]
async fn failed_step_is_reexecuted() -> TestResult {
    let test = setup().await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let task = Task::<(), usize>::builder("failed-step")?
        .default_max_attempts(2)
        .handler(move |context, ()| {
            let calls = handler_calls.clone();
            async move {
                context
                    .step("fallible", || async {
                        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            Err(Error::handler(Box::new(TestFailure)))
                        } else {
                            Ok(42)
                        }
                    })
                    .await
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        test.client
            .get_checkpoint(
                "default",
                spawned.result.task_id.as_uuid(),
                "fallible",
                true
            )
            .await?
            .is_none()
    );
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        spawned
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?,
        42
    );
    assert_eq!(
        test.client
            .get_checkpoint(
                "default",
                spawned.result.task_id.as_uuid(),
                "fallible",
                true
            )
            .await?
            .expect("successful checkpoint")
            .state,
        42
    );
    Ok(())
}

/// Verifies retry replays a completed prefix and executes only the unfinished suffix.
#[tokio::test]
async fn partial_workflow_executes_unfinished_suffix() -> TestResult {
    let test = setup().await?;
    let calls = Arc::new([
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    ]);
    let handler_calls = calls.clone();
    let task = Task::<(), Vec<usize>>::builder("partial")?
        .default_max_attempts(2)
        .handler(move |context, ()| {
            let calls = handler_calls.clone();
            async move {
                let mut values = Vec::new();
                for index in 0..3 {
                    if index == 2 && context.metadata().attempt == 1 {
                        return Err(Error::handler(Box::new(TestFailure)));
                    }
                    values.push(
                        context
                            .step(format!("step-{index}"), || async {
                                calls[index].fetch_add(1, Ordering::SeqCst);
                                Ok(index + 1)
                            })
                            .await?,
                    );
                }
                Ok(values)
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        calls.each_ref().map(|count| count.load(Ordering::SeqCst)),
        [1, 1, 0]
    );
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        calls.each_ref().map(|count| count.load(Ordering::SeqCst)),
        [1, 1, 1]
    );
    assert_eq!(
        spawned
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?,
        vec![1, 2, 3]
    );
    assert_eq!(
        task_row(&test.client, spawned.result.task_id).await?["attempts"],
        2
    );
    Ok(())
}

/// Verifies absolute and relative sleeps preserve checkpoint ownership and run identity.
#[tokio::test]
async fn sleep_preserves_deadline_and_run_identity() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    let wake = "2025-01-01T00:00:10Z".parse::<jiff::Timestamp>()?;
    for absolute in [false, true] {
        clock(&test.client, 0).await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = calls.clone();
        let task = Task::<(), ()>::builder("sleep-identity")?
            .handler(move |context, ()| {
                handler_calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if absolute {
                        context.sleep_until_named("sleep", wake).await
                    } else {
                        context
                            .sleep_for_named("sleep", Duration::from_secs(10))
                            .await
                    }
                }
            })
            .build();
        let router = Router::new().task(task.clone())?;
        let spawned = test.client.spawn(&task, ()).send().await?;
        work_batch(&test.client, &router, "default").await?;
        let checkpoint = test
            .client
            .get_checkpoint("default", spawned.result.task_id.as_uuid(), "sleep", true)
            .await?
            .expect("sleep checkpoint");
        assert_eq!(checkpoint.owner_run_id, Some(spawned.result.run_id));
        assert_eq!(
            checkpoint
                .state
                .as_str()
                .expect("timestamp")
                .parse::<jiff::Timestamp>()?,
            wake
        );
        let scheduled = run_row(&test.client, spawned.result.run_id).await?;
        assert_eq!(scheduled["state"], "sleeping");
        assert_eq!(
            scheduled["available_at"]
                .as_str()
                .expect("availability")
                .parse::<jiff::Timestamp>()?,
            wake
        );
        assert_eq!(
            task_row(&test.client, spawned.result.task_id).await?["state"],
            "sleeping"
        );
        clock(&test.client, 10).await?;
        let lease = test
            .client
            .claim_task("default", &Default::default())
            .await?
            .pop()
            .expect("woken run");
        assert_eq!(lease.claimed_run().run_id, spawned.result.run_id);
        assert_eq!(lease.claimed_run().attempt, 1);
        let resumed = run_row(&test.client, spawned.result.run_id).await?;
        assert_eq!(
            resumed["started_at"]
                .as_str()
                .expect("start")
                .parse::<jiff::Timestamp>()?,
            wake
        );
        router.dispatch(lease).await?;
        assert_eq!(
            task_row(&test.client, spawned.result.task_id).await?["state"],
            "completed"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
    Ok(())
}

/// Verifies event registration suspends indefinitely and emission makes it pending.
#[tokio::test]
async fn event_registration_state_transitions() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), i32>::builder("event-state")?
        .handler(|context, ()| async move { context.await_event("event").await })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        task_row(&test.client, spawned.result.task_id).await?["state"],
        "sleeping"
    );
    let sleeping = run_row(&test.client, spawned.result.run_id).await?;
    assert_eq!(sleeping["state"], "sleeping");
    assert_eq!(sleeping["wake_event"], "event");
    assert_eq!(sleeping["available_at"], "infinity");
    let unlimited: bool =
        sqlx::query_scalar("SELECT timeout_at IS NULL FROM absurd.w_default WHERE task_id = $1")
            .bind(spawned.result.task_id.as_uuid())
            .fetch_one(test.client.pool())
            .await?;
    assert!(unlimited);
    test.client.emit_event("default", "event", &42).await?;
    assert_eq!(
        task_row(&test.client, spawned.result.task_id).await?["state"],
        "pending"
    );
    assert_eq!(
        run_row(&test.client, spawned.result.run_id).await?["state"],
        "pending"
    );
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        spawned
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?,
        42
    );
    Ok(())
}

/// Verifies duplicate event emission preserves the first payload and timestamp.
#[tokio::test]
async fn event_emission_is_first_write_wins() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    test.client.emit_event("default", "immutable", &1).await?;
    let original: Value = sqlx::query_scalar(
        "SELECT to_jsonb(e) FROM absurd.e_default e WHERE event_name = 'immutable'",
    )
    .fetch_one(test.client.pool())
    .await?;
    clock(&test.client, 10).await?;
    test.client.emit_event("default", "immutable", &2).await?;
    let repeated: Value = sqlx::query_scalar(
        "SELECT to_jsonb(e) FROM absurd.e_default e WHERE event_name = 'immutable'",
    )
    .fetch_one(test.client.pool())
    .await?;
    assert_eq!(original, repeated);
    assert_eq!(repeated["payload"], 1);
    assert_eq!(
        repeated["emitted_at"]
            .as_str()
            .expect("emission timestamp")
            .parse::<jiff::Timestamp>()?,
        "2025-01-01T00:00:00Z".parse::<jiff::Timestamp>()?
    );
    let task = Task::<(), i32>::builder("late-event")?
        .handler(|context, ()| async move { context.await_event("immutable").await })
        .build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &Router::new().task(task)?, "default").await?;
    assert_eq!(
        spawned
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?,
        1
    );
    Ok(())
}

/// Verifies an event timeout removes its registration and can be handled normally.
#[tokio::test]
async fn event_timeout_resumes_and_cleans_wait() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let task = Task::<(), bool>::builder("event-timeout")?
        .handler(|context, ()| async move {
            match context
                .await_event_with_timeout::<Value>("missing", Some(Duration::from_secs(10)))
                .await
            {
                Err(Error::EventTimeout) => Ok(true),
                Err(error) => Err(error),
                Ok(_) => Ok(false),
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &router, "default").await?;
    let registered: (i64, bool) = sqlx::query_as("SELECT count(*), bool_and(timeout_at = timestamptz '2025-01-01 00:00:10Z') FROM absurd.w_default WHERE task_id = $1")
        .bind(spawned.result.task_id.as_uuid()).fetch_one(test.client.pool()).await?;
    assert_eq!(registered, (1, true));
    let sleeping = run_row(&test.client, spawned.result.run_id).await?;
    assert_eq!(
        sleeping["available_at"]
            .as_str()
            .expect("deadline")
            .parse::<jiff::Timestamp>()?,
        "2025-01-01T00:00:10Z".parse::<jiff::Timestamp>()?
    );
    clock(&test.client, 10).await?;
    work_batch(&test.client, &router, "default").await?;
    assert!(
        spawned
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?
    );
    let waits: i64 = sqlx::query_scalar("SELECT count(*) FROM absurd.w_default")
        .fetch_one(test.client.pool())
        .await?;
    assert_eq!(waits, 0);
    Ok(())
}

/// Verifies a single event wakes every registered task independently.
#[tokio::test]
async fn event_broadcast_wakes_all_waiters() -> TestResult {
    let test = setup().await?;
    let task = Task::<i32, (i32, String)>::builder("broadcast")?
        .handler(
            |context, value| async move { Ok((value, context.await_event("broadcast").await?)) },
        )
        .build();
    let router = Router::new().task(task.clone())?;
    let mut spawned = Vec::new();
    for value in 0..3 {
        spawned.push(test.client.spawn(&task, value).send().await?);
        work_batch(&test.client, &router, "default").await?;
    }
    let waits: i64 = sqlx::query_scalar("SELECT count(*) FROM absurd.w_default")
        .fetch_one(test.client.pool())
        .await?;
    assert_eq!(waits, 3);
    test.client
        .emit_event("default", "broadcast", &"payload")
        .await?;
    for _ in 0..3 {
        work_batch(&test.client, &router, "default").await?;
    }
    for (index, spawned) in spawned.iter().enumerate() {
        assert_eq!(
            spawned
                .await_result(&test.client, Some(Duration::from_secs(1)))
                .await?,
            (i32::try_from(index)?, "payload".into())
        );
    }
    Ok(())
}

/// Verifies repeated waits acknowledge a resumed timeout without recreating the wait.
#[tokio::test]
async fn repeated_wait_after_timeout_does_not_resuspend() -> TestResult {
    let test = super::setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let timeouts = Arc::new(AtomicUsize::new(0));
    let handler_timeouts = timeouts.clone();
    let task = Task::<(), Value>::builder("repeat-timeout")?
        .handler(move |context, ()| {
            let timeouts = handler_timeouts.clone();
            async move {
                let second = context.clone();
                match context
                    .await_event_named_with_timeout::<Value>(
                        "wait",
                        "missing",
                        Some(Duration::from_secs(10)),
                    )
                    .await
                {
                    Err(Error::EventTimeout) => {
                        timeouts.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(error) => return Err(error),
                    Ok(value) => return Ok(value),
                }
                second.await_event_named("wait", "missing").await
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let spawned = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &router, "default").await?;
    clock(&test.client, 10).await?;
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(timeouts.load(Ordering::SeqCst), 1);
    assert_eq!(
        task_row(&test.client, spawned.result.task_id).await?["state"],
        "completed"
    );
    assert_eq!(
        spawned
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?,
        Value::Null
    );
    let waits: i64 = sqlx::query_scalar("SELECT count(*) FROM absurd.w_default")
        .fetch_one(test.client.pool())
        .await?;
    assert_eq!(waits, 0);
    Ok(())
}

/// Verifies full task result snapshots through enqueue, claim, and completion.
#[tokio::test]
async fn task_result_snapshot_lifecycle() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), Value>::builder("snapshot")?.build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    let mut expected = elephant::types::TaskResultSnapshot {
        state: TaskResultState::Pending,
        result: None,
        failure: None,
    };
    assert_eq!(
        test.client
            .fetch_task_result("default", spawned.result.task_id.as_uuid())
            .await?,
        Some(expected.clone())
    );
    let lease = test
        .client
        .claim_task("default", &Default::default())
        .await?
        .pop()
        .expect("claim");
    expected.state = TaskResultState::Running;
    assert_eq!(
        test.client
            .fetch_task_result("default", spawned.result.task_id.as_uuid())
            .await?,
        Some(expected.clone())
    );
    let value = serde_json::json!({"answer": 42});
    lease.complete(&value).await?;
    expected.state = TaskResultState::Completed;
    expected.result = Some(value);
    assert_eq!(
        test.client
            .fetch_task_result("default", spawned.result.task_id.as_uuid())
            .await?,
        Some(expected)
    );
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
