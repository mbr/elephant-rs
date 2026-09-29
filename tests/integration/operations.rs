//! Operational limits and worker identity at real PostgreSQL boundaries.

use std::time::Duration;

use elephant::{
    client::Client,
    error::Error,
    task::{Router, Task},
    types::TaskResultState,
    worker::work_batch,
};
use sqlx::postgres::PgPoolOptions;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{TestResult, setup};

/// Waits for the worker's actual server-side lock wait before requesting drain.
async fn wait_for_blocked_claim(client: &Client, application: &str) -> TestResult {
    timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND application_name = $1 AND wait_event_type = 'Lock')",
            )
            .bind(application)
            .fetch_one(client.pool())
            .await?;
            if blocked {
                return Ok::<(), sqlx::Error>(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    }).await??;
    Ok(())
}

/// Retains an issued claim on shutdown, or surfaces a server-confirmed rollback.
#[tokio::test]
async fn blocked_claim_shutdown_and_statement_timeout() -> TestResult {
    for server_timeout in [false, true] {
        let test = setup().await?;
        let identity = format!("operations-{}", Uuid::now_v7());
        let options = test
            .client
            .pool()
            .connect_options()
            .as_ref()
            .clone()
            .application_name(&identity)
            .options([(
                "statement_timeout",
                if server_timeout { "1000" } else { "0" },
            )]);
        let pool = PgPoolOptions::new()
            .max_connections(3)
            .acquire_timeout(Duration::from_secs(2))
            .connect_with(options)
            .await?;
        let client = Client::builder(pool).build();
        let task = Task::<(), i32>::builder("blocked-claim")?
            .default_max_attempts(1)
            .handler(|_, ()| async { Ok(42) })
            .build();
        let handle = test.client.spawn(&task, ()).send().await?;
        let router = Router::new().task(task)?;
        let mut lock = test.client.pool().begin().await?;
        sqlx::query("LOCK TABLE absurd.r_default IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await?;
        let shutdown = CancellationToken::new();
        let worker = client
            .worker(router.clone())
            .worker_id(&identity)
            .claim_timeout(Duration::from_secs(30))
            .run(shutdown.clone());
        tokio::pin!(worker);
        tokio::select! {
            result = &mut worker => panic!("worker exited before its query blocked: {result:?}"),
            result = wait_for_blocked_claim(&test.client, &identity) => result?,
        }
        shutdown.cancel();
        if server_timeout {
            let error = timeout(Duration::from_secs(5), &mut worker)
                .await?
                .expect_err("statement timeout must reach the supervisor");
            let Error::Sqlx { source } = error else {
                panic!("unexpected worker failure: {error:?}");
            };
            assert_eq!(
                source
                    .as_database_error()
                    .and_then(|error| error.code())
                    .as_deref(),
                Some("57014")
            );
            lock.rollback().await?;
            let snapshot = test
                .client
                .fetch_task_result("default", handle.result.task_id.as_uuid())
                .await?
                .expect("unclaimed task survives server rollback");
            assert_eq!(snapshot.state, TaskResultState::Pending);
            work_batch(&test.client, &router, "default").await?;
        } else {
            assert!(
                timeout(Duration::from_millis(150), &mut worker)
                    .await
                    .is_err(),
                "cancellation must not discard the pending claim"
            );
            lock.rollback().await?;
            timeout(Duration::from_secs(5), &mut worker).await??;
            let owner: String =
                sqlx::query_scalar("SELECT claimed_by FROM absurd.r_default WHERE run_id = $1")
                    .bind(handle.result.run_id.as_uuid())
                    .fetch_one(test.client.pool())
                    .await?;
            assert_eq!(owner, identity);
        }
        assert_eq!(
            handle
                .await_result(&test.client, Some(Duration::from_secs(2)))
                .await?,
            42
        );
        let attempts: i32 =
            sqlx::query_scalar("SELECT attempts FROM absurd.t_default WHERE task_id = $1")
                .bind(handle.result.task_id.as_uuid())
                .fetch_one(test.client.pool())
                .await?;
        assert_eq!(
            attempts, 1,
            "server rollback or graceful drain must not abandon a run"
        );
        client.pool().close().await;
    }
    Ok(())
}
