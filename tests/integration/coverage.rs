//! Behavioral coverage shared with the upstream SDK suites.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use elephant::{
    client::Client,
    error::Error,
    task::{Router, Task},
    types::{
        CreateQueueOptions, QueueDetachMode, QueuePolicy, QueuePolicyOptions, QueueStorageMode,
        TaskId, TaskResultState,
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
