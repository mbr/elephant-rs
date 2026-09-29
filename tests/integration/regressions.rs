//! Regressions discovered while integrating a separate workflow application.

use std::time::Duration;

use elephant::{error::Error, task::Task, types::TaskResultState};

use super::{TestResult, setup_with_max_connections};

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
