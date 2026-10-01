//! Transactional successor handoffs and externally settled worker leases.

use std::time::Duration;

use elephant::{
    client::Client,
    error::Error,
    run::RunLease,
    task::{Router, Task},
    types::{RunId, Spawned},
    worker::ClaimOptions,
};
use serde_json::Value;
use sqlx::{Connection, PgConnection};
use tokio::time::{sleep, timeout};

use super::{TestResult, setup};

/// Claims the only ready task without automatically resolving its lease.
///
/// # Panic
///
/// Panics if the fixture has anything other than one ready task.
async fn claim(client: &Client) -> TestResult<RunLease> {
    let mut leases = client
        .claim_task("default", &ClaimOptions::default())
        .await?;
    assert_eq!(leases.len(), 1);
    Ok(leases.pop().expect("one ready task"))
}

/// Stages a successor and predecessor completion in the caller's transaction.
async fn handoff(
    client: &Client,
    connection: &mut PgConnection,
    predecessor: RunId,
    task: &Task<(), ()>,
) -> elephant::error::Result<Spawned<()>> {
    let next = client.spawn(task, ()).send_on(connection).await?;
    sqlx::query("SELECT absurd.complete_run('default', $1, 'null'::jsonb)")
        .bind(predecessor.as_uuid())
        .execute(connection)
        .await?;
    Ok(next)
}

/// Counts committed tasks and completions without observing uncommitted successors.
async fn counts(client: &Client) -> TestResult<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE state = 'completed') FROM absurd.t_default",
    )
    .fetch_one(client.pool())
    .await?)
}

/// Registers an event wait and optional deadline in the caller's transaction.
async fn wait_on(
    connection: &mut PgConnection,
    lease: &RunLease,
    event: &str,
    seconds: Option<i32>,
) -> TestResult<(bool, Option<Value>)> {
    let run = lease.claimed_run();
    Ok(sqlx::query_as(
        "SELECT should_suspend, payload FROM absurd.await_event('default', $1, $2, $3, $3, $4)",
    )
    .bind(run.task_id.as_uuid())
    .bind(run.run_id.as_uuid())
    .bind(event)
    .bind(seconds)
    .fetch_one(connection)
    .await?)
}

/// Waits for a confirmed database lock conflict rather than relying on timing.
async fn blocked(client: &Client, pid: i32) -> TestResult {
    timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT cardinality(pg_blocking_pids($1)) > 0")
                .bind(pid)
                .fetch_one(client.pool())
                .await?;
            if waiting {
                return Ok::<(), sqlx::Error>(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    Ok(())
}

/// Preserves a single successor across connection loss before and after commit.
#[tokio::test]
async fn transactional_handoff_survives_connection_loss() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("successor")?.build();
    test.client.spawn(&task, ()).send().await?;
    let lease = claim(&test.client).await?;
    let predecessor = lease.claimed_run().run_id;

    for commit in [false, true] {
        let mut connection =
            PgConnection::connect_with(test.client.pool().connect_options().as_ref()).await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut connection)
            .await?;
        let mut tx = connection.begin().await?;
        let successor = handoff(&test.client, &mut tx, predecessor, &task).await?;
        assert_eq!(counts(&test.client).await?, (1, 0));
        assert!(
            test.client
                .fetch_task_result("default", successor.result.task_id.as_uuid())
                .await?
                .is_none()
        );
        if commit {
            assert!(
                sqlx::raw_sql("COMMIT; SELECT pg_terminate_backend(pg_backend_pid());")
                    .execute(&mut *tx)
                    .await
                    .is_err()
            );
            drop(tx);
            assert_eq!(counts(&test.client).await?, (2, 1));
        } else {
            let killed: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1, 5000)")
                .bind(pid)
                .fetch_one(test.client.pool())
                .await?;
            assert!(killed);
            assert!(tx.commit().await.is_err());
            assert_eq!(counts(&test.client).await?, (1, 0));
        }
    }

    lease.forget();
    let next = claim(&test.client).await?;
    assert_ne!(next.claimed_run().run_id, predecessor);
    next.complete(()).await?;
    assert_eq!(counts(&test.client).await?, (2, 2));
    Ok(())
}

/// Rolls back duplicate and cancelled handoffs, including already-staged successors.
#[tokio::test]
async fn transactional_handoff_fences_competing_settlement() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("successor")?.build();
    test.client.spawn(&task, ()).send().await?;
    let lease = claim(&test.client).await?;
    let predecessor = lease.claimed_run().run_id;
    let mut winner = test.client.pool().begin().await?;
    handoff(&test.client, &mut winner, predecessor, &task).await?;
    let mut loser = test.client.pool().begin().await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *loser)
        .await?;
    let compete = async {
        let result = handoff(&test.client, &mut loser, predecessor, &task).await;
        loser.rollback().await?;
        TestResult::<_>::Ok(result)
    };
    let release = async {
        blocked(&test.client, pid).await?;
        winner.commit().await?;
        TestResult::Ok(())
    };
    let (result, ()) = tokio::try_join!(compete, release)?;
    assert!(matches!(result, Err(Error::Sqlx { .. })));
    lease.forget();
    assert_eq!(counts(&test.client).await?, (2, 1));

    let next = claim(&test.client).await?;
    test.client
        .cancel_task("default", next.claimed_run().task_id.as_uuid())
        .await?;
    let mut tx = test.client.pool().begin().await?;
    assert!(matches!(
        handoff(&test.client, &mut tx, next.claimed_run().run_id, &task).await,
        Err(Error::Cancelled)
    ));
    tx.rollback().await?;
    next.forget();
    assert_eq!(counts(&test.client).await?, (2, 1));
    Ok(())
}

/// Commits event registration before releasing the worker without losing racing emits.
#[tokio::test]
async fn transactional_wait_handles_rollback_and_both_event_orderings() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("waiter")?.build();
    test.client.spawn(&task, ()).send().await?;
    let lease = claim(&test.client).await?;
    let run_id = lease.claimed_run().run_id;

    let mut tx = test.client.pool().begin().await?;
    assert!(wait_on(&mut tx, &lease, "before", None).await?.0);
    tx.rollback().await?;
    let waits: i64 = sqlx::query_scalar("SELECT count(*) FROM absurd.w_default")
        .fetch_one(test.client.pool())
        .await?;
    assert_eq!(waits, 0);
    test.client.emit_event("default", "before", &true).await?;
    let mut tx = test.client.pool().begin().await?;
    assert_eq!(
        wait_on(&mut tx, &lease, "before", None).await?,
        (false, Some(Value::Bool(true)))
    );
    tx.commit().await?;

    let mut sleeper = test.client.pool().begin().await?;
    assert!(wait_on(&mut sleeper, &lease, "after", None).await?.0);
    let mut emitter = test.client.pool().begin().await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *emitter)
        .await?;
    let emit = async {
        test.client
            .emit_event_on(&mut emitter, "default", "after", true)
            .await?;
        emitter.commit().await?;
        TestResult::Ok(())
    };
    let release = async {
        blocked(&test.client, pid).await?;
        sleeper.commit().await?;
        TestResult::Ok(())
    };
    tokio::try_join!(emit, release)?;
    lease.forget();
    let awakened = claim(&test.client).await?;
    assert_eq!(awakened.claimed_run().run_id, run_id);
    assert_eq!(
        awakened.claimed_run().event_payload,
        Some(Value::Bool(true))
    );
    let mut tx = test.client.pool().begin().await?;
    assert!(wait_on(&mut tx, &awakened, "timer", Some(0)).await?.0);
    assert!(
        test.client
            .claim_task("default", &ClaimOptions::default())
            .await?
            .is_empty()
    );
    tx.commit().await?;
    awakened.forget();
    let timed = claim(&test.client).await?;
    assert_eq!(timed.claimed_run().run_id, run_id);
    assert_eq!(timed.claimed_run().wake_event.as_deref(), Some("timer"));
    assert_eq!(timed.claimed_run().event_payload, None);
    timed.complete(()).await?;
    Ok(())
}

/// Records the router's lack of an externally-completed handler outcome.
#[tokio::test]
async fn externally_completed_handler_is_completed_again_by_router() -> TestResult {
    let test = setup().await?;
    let task = Task::<(), ()>::builder("external-completion")?
        .handler(|context, ()| async move {
            let mut tx = context.client().pool().begin().await?;
            sqlx::query("SELECT absurd.complete_run('default', $1, 'null'::jsonb)")
                .bind(context.metadata().run_id.as_uuid())
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        })
        .build();
    let spawned = test.client.spawn(&task, ()).send().await?;
    let router = Router::new().task(task)?;
    let error = router
        .dispatch(claim(&test.client).await?)
        .await
        .expect_err("ordinary handler success attempts a second completion");
    let Error::Sqlx { source } = error else {
        panic!("unexpected settlement error: {error:?}");
    };
    assert_eq!(
        source
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("P0001")
    );
    spawned
        .await_result(&test.client, Some(Duration::from_secs(1)))
        .await?;
    assert_eq!(counts(&test.client).await?, (1, 1));
    Ok(())
}
