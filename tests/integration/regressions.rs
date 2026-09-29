//! Regressions discovered while integrating a separate workflow application.

use std::time::Duration;

use elephant::{
    client::Client,
    error::Error,
    task::{Router, Task},
    types::TaskResultState,
    worker::work_batch,
};
use serde_json::Value;

use super::{TestFailure, TestResult, setup_with_max_connections};

/// Carries an SDK error through an application's own error type.
#[derive(Debug, thiserror::Error)]
#[error("application helper failed")]
struct ApplicationError {
    /// Preserves the SDK error as the underlying cause.
    #[source]
    source: Error,
}

/// Adds a domain-error boundary without discarding the source chain.
fn application_error(source: Error) -> Error {
    Error::handler(Box::new(ApplicationError { source }))
}

/// Preserves suspension through both a domain helper and an execution wrapper.
#[tokio::test]
async fn wrapped_suspension_preserves_workflow_control_flow() -> TestResult {
    let test = setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let task = Task::<(), ()>::builder("wrapped-sleep")?
        .default_max_attempts(1)
        .handler(|context, ()| async move {
            assert_eq!(context.metadata().attempt, 1);
            context
                .sleep_for(Duration::from_secs(1))
                .await
                .map_err(application_error)
        })
        .build();
    let router = Router::new()
        .task(task.clone())?
        .wrap_execution(|_, execute| async { execute.await.map_err(application_error) });
    let handle = test.client.spawn(&task, ()).send().await?;
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        test.client
            .fetch_task_result("default", handle.result.task_id.as_uuid())
            .await?
            .expect("task")
            .state,
        TaskResultState::Sleeping
    );
    clock(&test.client, 1).await?;
    work_batch(&test.client, &router, "default").await?;
    handle
        .await_result(&test.client, Some(Duration::from_secs(1)))
        .await?;
    Ok(())
}

/// Keeps wrapped child cancellation and event timeout as ordinary parent failures.
#[tokio::test]
async fn wrapped_application_failures_are_not_control_signals() -> TestResult {
    let test = setup_with_max_connections(1).await?;
    for event_timeout in [false, true] {
        let task = Task::<(), ()>::builder("wrapped-failure")?
            .default_max_attempts(1)
            .handler(move |context, ()| async move {
                let error = if event_timeout {
                    Error::EventTimeout
                } else {
                    Error::TaskCancelled {
                        task_id: context.metadata().task_id,
                    }
                };
                Err(application_error(error))
            })
            .build();
        let router = Router::new()
            .task(task.clone())?
            .wrap_execution(|_, execute| async { execute.await.map_err(application_error) });
        let handle = test.client.spawn(&task, ()).send().await?;
        work_batch(&test.client, &router, "default").await?;
        let snapshot = test
            .client
            .fetch_task_result("default", handle.result.task_id.as_uuid())
            .await?
            .expect("task");
        assert_eq!(snapshot.state, TaskResultState::Failed);
        let failure = snapshot.failure.expect("failure");
        assert_eq!(failure["name"], "handler_error");
        assert!(
            failure["message"]
                .as_str()
                .expect("message")
                .contains(if event_timeout {
                    "event wait timed out"
                } else {
                    "was cancelled"
                })
        );
    }
    Ok(())
}

/// Advances the clock on a fixture restricted to one database connection.
async fn clock(client: &Client, seconds: i32) -> elephant::error::Result<()> {
    sqlx::query("SELECT set_config('absurd.fake_now', (TIMESTAMPTZ '2025-01-01' + make_interval(secs => $1))::text, false)")
        .bind(seconds).execute(client.pool()).await?;
    Ok(())
}

/// Reproduces the missing durable timeout outcome in the shared Absurd protocol.
#[tokio::test]
#[ignore = "upstream event timeout outcome is not checkpointed; run explicitly to reproduce"]
async fn event_timeout_survives_sleep_and_retry() -> TestResult {
    assert!(!approval_after_timeout(false).await?);
    Ok(())
}

/// Preserves the timeout fallback by checkpointing the complete business decision.
#[tokio::test]
async fn checkpointed_event_decision_survives_sleep_and_retry() -> TestResult {
    assert!(!approval_after_timeout(true).await?);
    Ok(())
}

/// Replays a timeout decision after a sleep, a late event, and a failed attempt.
async fn approval_after_timeout(checkpoint_decision: bool) -> TestResult<bool> {
    let test = setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let task = Task::<(), bool>::builder("deadline")?
        .default_max_attempts(2)
        .handler(move |context, ()| async move {
            let decide = || async {
                match context
                    .await_event_named_with_timeout::<bool>(
                        "approval",
                        "approval",
                        Some(Duration::from_secs(1)),
                    )
                    .await
                {
                    Ok(approved) => Ok(approved),
                    Err(Error::EventTimeout) => Ok(false),
                    Err(error) => Err(error),
                }
            };
            let approved = if checkpoint_decision {
                context.step("approval-decision", decide).await?
            } else {
                decide().await?
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
    assert_eq!(
        test.client
            .get_checkpoint(
                "default",
                handle.result.task_id.as_uuid(),
                "approval-decision",
                false
            )
            .await?
            .map(|checkpoint| checkpoint.state),
        checkpoint_decision.then_some(Value::Bool(false))
    );
    assert!(
        test.client
            .get_checkpoint(
                "default",
                handle.result.task_id.as_uuid(),
                "approval",
                false
            )
            .await?
            .is_none()
    );
    test.client.emit_event("default", "approval", true).await?;
    clock(&test.client, 2).await?;
    work_batch(&test.client, &router, "default").await?;
    work_batch(&test.client, &router, "default").await?;
    Ok(handle
        .await_result(&test.client, Some(Duration::from_secs(1)))
        .await?)
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
