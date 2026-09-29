//! Regressions discovered while integrating a separate workflow application.

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
    types::TaskResultState,
    worker::work_batch,
};
use serde_json::Value;

use super::{TestFailure, TestResult, setup_with_max_connections};

/// Advances the clock on a fixture restricted to one database connection.
async fn clock(client: &Client, seconds: i32) -> elephant::error::Result<()> {
    sqlx::query("SELECT set_config('absurd.fake_now', (TIMESTAMPTZ '2025-01-01' + make_interval(secs => $1))::text, false)")
        .bind(seconds).execute(client.pool()).await?;
    Ok(())
}

/// Preserves an observed timeout across a later sleep, late event, and task retry.
#[tokio::test]
async fn event_timeout_survives_sleep_and_retry() -> TestResult {
    let test = setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let task = Task::<(), bool>::builder("deadline")?
        .default_max_attempts(2)
        .handler(|context, ()| async move {
            let approved = match context
                .await_event_named_with_timeout::<bool>(
                    "approval",
                    "approval",
                    Some(Duration::from_secs(1)),
                )
                .await
            {
                Ok(approved) => approved,
                Err(Error::EventTimeout) => false,
                Err(error) => return Err(error),
            };
            context
                .sleep_for_named("cooldown", Duration::from_secs(1))
                .await?;
            if context.metadata().attempt == 1 {
                return Err(Error::handler(Box::new(TestFailure)));
            }
            Ok(approved)
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let handle = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &router, "default").await?;
    clock(&test.client, 1).await?;
    work_batch(&test.client, &router, "default").await?;
    assert!(
        test.client
            .get_checkpoint(
                "default",
                handle.result.task_id.as_uuid(),
                "cooldown",
                false
            )
            .await?
            .is_some()
    );
    test.client.emit_event("default", "approval", true).await?;
    clock(&test.client, 2).await?;
    work_batch(&test.client, &router, "default").await?;
    work_batch(&test.client, &router, "default").await?;
    assert!(
        !handle
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?
    );
    Ok(())
}

/// Keeps distinct timed-out occurrences and null acknowledgements stable on replay.
#[tokio::test]
async fn repeated_event_timeouts_keep_their_own_wake_identity() -> TestResult {
    let test = setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let timeouts = Arc::new(AtomicUsize::new(0));
    let observed = timeouts.clone();
    let task = Task::<(), ()>::builder("repeated-deadline")?
        .default_max_attempts(1)
        .handler(move |context, ()| {
            let observed = observed.clone();
            async move {
                for _ in 0..2 {
                    match context
                        .await_event_named_with_timeout::<Value>(
                            "wait",
                            "missing",
                            Some(Duration::from_secs(1)),
                        )
                        .await
                    {
                        Err(Error::EventTimeout) => {
                            observed.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(error) => return Err(error),
                        Ok(value) => panic!("expected a timeout, got {value}"),
                    }
                    assert_eq!(
                        context
                            .clone()
                            .await_event_named::<Value>("wait", "missing")
                            .await?,
                        Value::Null
                    );
                }
                context
                    .sleep_for_named("cooldown", Duration::from_secs(1))
                    .await
            }
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let handle = test.client.spawn(&task, ()).send().await?;
    for second in 0..=2 {
        clock(&test.client, second).await?;
        work_batch(&test.client, &router, "default").await?;
    }
    assert!(
        test.client
            .get_checkpoint(
                "default",
                handle.result.task_id.as_uuid(),
                "cooldown",
                false
            )
            .await?
            .is_some()
    );
    test.client
        .emit_event("default", "missing", serde_json::json!({"timed_out": true}))
        .await?;
    clock(&test.client, 3).await?;
    work_batch(&test.client, &router, "default").await?;
    handle
        .await_result(&test.client, Some(Duration::from_secs(1)))
        .await?;
    assert_eq!(timeouts.load(Ordering::SeqCst), 5);
    Ok(())
}

/// Rolls back event suspension when its durable wake identity cannot be stored.
#[tokio::test]
async fn event_wait_and_wake_identity_commit_together() -> TestResult {
    let test = setup_with_max_connections(1).await?;
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION public.reject_wait_metadata() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF NEW.checkpoint_name = '$elephant:awaitEventPending' THEN
                RAISE EXCEPTION 'cannot persist wake identity';
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER reject_wait_metadata BEFORE INSERT ON absurd.c_default
            FOR EACH ROW EXECUTE FUNCTION public.reject_wait_metadata();
    "#,
    )
    .execute(test.client.pool())
    .await?;
    let task = Task::<(), ()>::builder("atomic-wait")?
        .default_max_attempts(1)
        .handler(|context, ()| async move {
            assert!(matches!(
                context
                    .step("$elephant:reserved", || async { Ok(()) })
                    .await,
                Err(Error::InvalidName { .. })
            ));
            context.await_event("missing").await
        })
        .build();
    let handle = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &Router::new().task(task)?, "default").await?;
    let snapshot = test
        .client
        .fetch_task_result("default", handle.result.task_id.as_uuid())
        .await?
        .expect("task");
    assert_eq!(snapshot.state, TaskResultState::Failed);
    assert!(
        snapshot.failure.expect("failure")["message"]
            .as_str()
            .expect("message")
            .contains("cannot persist wake identity")
    );
    let waits: i64 = sqlx::query_scalar("SELECT count(*) FROM absurd.w_default")
        .fetch_one(test.client.pool())
        .await?;
    assert_eq!(waits, 0);
    assert!(
        test.client
            .get_checkpoints(
                "default",
                handle.result.task_id.as_uuid(),
                handle.result.run_id.as_uuid()
            )
            .await?
            .is_empty()
    );
    Ok(())
}

/// Bounds result polling across both pool acquisition and blocked database reads.
#[tokio::test]
async fn result_deadline_covers_pool_and_database_waits() -> TestResult {
    for exhaust_pool in [true, false] {
        let test = setup_with_max_connections(2).await?;
        let task = Task::<(), ()>::builder("waiting")?.build();
        let spawned = test.client.spawn(&task, ()).send().await?;
        let mut blocker = test.client.pool().begin().await?;
        let held = if exhaust_pool {
            Some(test.client.pool().acquire().await?)
        } else {
            sqlx::query("LOCK TABLE absurd.t_default IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *blocker)
                .await?;
            None
        };
        for budget in [Duration::from_millis(30), Duration::ZERO] {
            let result = tokio::time::timeout(
                Duration::from_millis(500),
                spawned.await_result(&test.client, Some(budget)),
            )
            .await?;
            assert!(
                matches!(result, Err(Error::TaskResultTimeout { task_id }) if task_id == spawned.result.task_id.as_uuid())
            );
        }
        drop(held);
        blocker.rollback().await?;
        let snapshot = test
            .client
            .fetch_task_result("default", spawned.result.task_id.as_uuid())
            .await?
            .expect("task exists");
        assert_eq!(snapshot.state, TaskResultState::Pending);
        let lease = test
            .client
            .claim_task("default", &Default::default())
            .await?
            .pop()
            .expect("claim");
        lease.complete(&()).await?;
        spawned
            .await_result(&test.client, Some(Duration::from_secs(1)))
            .await?;
    }
    Ok(())
}
