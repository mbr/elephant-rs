//! Checks the example's business guarantees and drain behavior against PostgreSQL.

use std::{error::Error as StdError, time::Duration};

use elephant::{client::Client, error::Error as WorkflowError, schema, types::TaskResultState};
use sqlx::Executor;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{DatabaseUrl, Error, QUEUE, connect, initialize, run_worker, store, submit};

/// Carries test failures without discarding their sources.
type TestResult = Result<(), Box<dyn StdError + Send + Sync>>;

/// Keeps private database state alive through every application operation.
struct Fixture {
    /// Owns the configured application client.
    client: Client,
    /// Owns the database after all client handles have been dropped.
    _database: pgdb::DbInstance,
}

/// Creates isolated application state without consulting external credentials.
async fn setup() -> Result<Fixture, Box<dyn StdError + Send + Sync>> {
    let database = pgdb::db_fixture();
    let client = connect(
        &database.as_str().parse::<DatabaseUrl>()?,
        "operations-test",
    )
    .await?;
    client
        .pool()
        .execute(sqlx::raw_sql(include_str!("../../testdata/absurd.sql")))
        .await?;
    schema::assert_version(client.pool(), "0.5.0").await?;
    initialize(&client).await?;
    Ok(Fixture {
        client,
        _database: database,
    })
}

/// Exercises progress beyond the initial lease and replay without a step checkpoint.
#[tokio::test]
async fn idempotent_effects_survive_reexecution_and_graceful_drain() -> TestResult {
    let fixture = setup().await?;
    let client = &fixture.client;
    let entry_id = Uuid::now_v7();
    let handle = submit(client, entry_id, 42).await?;
    let duplicate = submit(client, entry_id, 42).await?;
    assert!(!duplicate.result.created);
    assert_eq!(duplicate.result.task_id, handle.result.task_id);
    assert!(matches!(
        submit(client, entry_id, 99).await,
        Err(Error::ConflictingAmount { .. })
    ));

    let mut transaction = client.pool().begin().await?;
    let request = store::load_request(&mut transaction, entry_id).await?;
    let posting = store::post(&mut transaction, &request).await?;
    transaction.commit().await?;
    assert_eq!(posting.amount, 42);
    assert!(
        client
            .get_checkpoint(QUEUE, handle.result.task_id.as_uuid(), "post-v1", false)
            .await?
            .is_none()
    );

    let shutdown = CancellationToken::new();
    let worker = run_worker(client, "drain-test", shutdown.clone());
    tokio::pin!(worker);
    let claimed = async {
        loop {
            if client
                .fetch_task_result(QUEUE, handle.result.task_id.as_uuid())
                .await?
                .is_some_and(|snapshot| snapshot.state == TaskResultState::Running)
            {
                return Ok::<(), WorkflowError>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::select! {
        result = &mut worker => panic!("worker exited before claim: {result:?}"),
        result = timeout(Duration::from_secs(5), claimed) => result??,
    }
    shutdown.cancel();
    timeout(Duration::from_secs(20), &mut worker).await??;
    let result = handle
        .await_result(client, Some(Duration::from_secs(2)))
        .await?;
    assert_eq!(result.entry_id, entry_id);
    assert_eq!(result.amount, 42);
    let request = store::load_request(&mut *client.pool().acquire().await?, entry_id).await?;
    assert_eq!(request.amount, 42);
    let posting = store::load_posting(&mut *client.pool().acquire().await?, entry_id)
        .await?
        .expect("one durable effect");
    assert_eq!(posting.amount, 42);
    let attempts: i32 =
        sqlx::query_scalar("SELECT attempts FROM absurd.t_operations WHERE task_id = $1")
            .bind(handle.result.task_id.as_uuid())
            .fetch_one(client.pool())
            .await?;
    assert_eq!(
        attempts, 1,
        "progress protects work longer than its initial claim"
    );
    let owner: String =
        sqlx::query_scalar("SELECT claimed_by FROM absurd.r_operations WHERE run_id = $1")
            .bind(handle.result.run_id.as_uuid())
            .fetch_one(client.pool())
            .await?;
    assert_eq!(owner, "drain-test");
    client.pool().close().await;
    Ok(())
}

/// Rolls back enqueue when saving the business request fails in the same transaction.
#[tokio::test]
async fn failed_submission_does_not_leave_an_orphan_task() -> TestResult {
    let fixture = setup().await?;
    let client = &fixture.client;
    sqlx::query("ALTER TABLE operations.requests ADD CONSTRAINT reject_input CHECK (amount <> 99)")
        .execute(client.pool())
        .await?;
    let entry_id = Uuid::now_v7();
    let error = submit(client, entry_id, 99)
        .await
        .expect_err("the business input constraint must reject this request");
    let Error::Database(sqlx::Error::Database(database_error)) = error else {
        panic!("expected a database constraint failure, got {error:?}");
    };
    assert_eq!(database_error.code().as_deref(), Some("23514"));
    assert_eq!(database_error.constraint(), Some("reject_input"));
    let tasks: i64 = sqlx::query_scalar("SELECT count(*) FROM absurd.t_operations")
        .fetch_one(client.pool())
        .await?;
    assert_eq!(tasks, 0);
    let handle = submit(client, entry_id, 42).await?;
    assert!(
        handle.result.created,
        "rollback also removed the idempotency record"
    );
    client.pool().close().await;
    Ok(())
}
