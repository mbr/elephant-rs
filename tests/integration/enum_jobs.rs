//! Contracts, wire compatibility, and supervised execution of enum jobs.

use std::time::Duration;

use elephant::{
    client::Client,
    context::TaskContext,
    error::Error,
    task::{AbsurdJob, Router},
    types::{CancellationPolicy, CreateQueueOptions, RetryStrategy, TaskId},
};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::{TestResult, setup};

/// Covers the supported variant shapes with one shared result contract.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "task", content = "params")]
enum Job {
    /// Generates a report for a customer and reporting period.
    #[serde(rename = "generate-report-v1", alias = "generate-report")]
    Generate {
        /// Identifies the customer.
        customer_id: u64,
        /// Selects the reporting period.
        year: u16,
    },
    /// Exercises a variant with no parameters.
    Ping,
    /// Exercises a scalar parameter payload.
    Count(u64),
    /// Exercises a tuple parameter payload.
    Pair(u64, bool),
    /// Exercises an inner discriminator sharing the envelope field name.
    Nested {
        /// Selects behavior within an already selected job.
        task: Mode,
    },
}

impl AbsurdJob for Job {
    /// Returns a user-facing message without requiring `String`.
    type Output = Box<str>;
}

/// Distinguishes nested enum errors from unsupported outer job names.
#[derive(Debug, Deserialize, Serialize)]
enum Mode {
    /// Selects the supported nested behavior.
    Normal,
}

/// Produces a deterministic message from each variant's decoded fields.
async fn handle_job(_context: TaskContext, job: Job) -> elephant::error::Result<Box<str>> {
    Ok(match job {
        Job::Generate { customer_id, year } => format!("report:{customer_id}:{year}").into(),
        Job::Ping => "pong".into(),
        Job::Count(count) => count.to_string().into(),
        Job::Pair(count, enabled) => format!("{count}:{enabled}").into(),
        Job::Nested { task: Mode::Normal } => "nested".into(),
    })
}

/// Reads the persisted invocation independently of SDK decoding.
async fn task_row(client: &Client, id: TaskId) -> TestResult<Value> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(t) FROM absurd.t_default t WHERE task_id = $1")
            .bind(id.as_uuid())
            .fetch_one(client.pool())
            .await?,
    )
}

/// Supplies arbitrary serialized envelopes to the producer boundary.
#[derive(Deserialize, Serialize)]
#[serde(transparent)]
struct WireJob(Value);

impl AbsurdJob for WireJob {
    /// Discards results for producer validation probes.
    type Output = ();
}

/// Fails serialization before an invocation reaches PostgreSQL.
#[derive(Deserialize)]
struct EncodingFailure;

impl Serialize for EncodingFailure {
    /// Reports an application encoding failure.
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("job encoding failed"))
    }
}

impl AbsurdJob for EncodingFailure {
    /// Discards results for the invalid invocation.
    type Output = ();
}

/// Rejects malformed serialization before acquiring a database connection.
#[tokio::test]
async fn enum_envelopes_are_validated_before_database_access() -> TestResult {
    let pool = PgPoolOptions::new().connect_lazy("postgresql://localhost/unused")?;
    pool.close().await;
    let client = Client::builder(pool).default_queue("default")?.build();
    for value in [
        Value::Null,
        json!([]),
        json!("Unit"),
        json!({"params": 42}),
        json!({"task": 42}),
        json!({"task": "known", "content": 42}),
        json!({"task": "known", "params": 42, "extra": true}),
    ] {
        let error = client
            .spawn_job(WireJob(value))
            .send()
            .await
            .expect_err("invalid envelope");
        assert!(
            matches!(error, Error::InvalidJobEnvelope { .. }),
            "{error:?}"
        );
    }
    for name in ["", "   "] {
        let error = client
            .spawn_job(WireJob(json!({"task": name})))
            .send()
            .await
            .expect_err("invalid task name");
        assert!(matches!(error, Error::InvalidName { kind: "task", .. }));
    }
    let error = client
        .spawn_job(EncodingFailure)
        .send()
        .await
        .expect_err("encoding failure");
    let Error::Json { source } = error else {
        panic!("unexpected error: {error:?}")
    };
    assert_eq!(source.to_string(), "job encoding failed");
    for value in [
        json!({"task": "Unit"}),
        json!({"task": "Unit", "params": null}),
    ] {
        let error = client
            .spawn_job(WireJob(value))
            .send()
            .await
            .expect_err("closed pool");
        assert!(matches!(
            error,
            Error::Sqlx {
                source: sqlx::Error::PoolClosed
            }
        ));
    }
    Ok(())
}

/// Preserves variant payloads and shared output types across real dispatch.
#[tokio::test]
async fn enum_variants_preserve_wire_shapes_and_typed_results() -> TestResult {
    let test = setup().await?;
    let mut expected = Vec::new();
    for (job, name, params, message) in [
        (
            Job::Generate {
                customer_id: 123,
                year: 2026,
            },
            "generate-report-v1",
            json!({"customer_id": 123, "year": 2026}),
            "report:123:2026",
        ),
        (Job::Ping, "Ping", Value::Null, "pong"),
        (
            Job::Count(9007199254740993),
            "Count",
            json!(9007199254740993_u64),
            "9007199254740993",
        ),
        (Job::Pair(42, false), "Pair", json!([42, false]), "42:false"),
        (
            Job::Nested { task: Mode::Normal },
            "Nested",
            json!({"task": "Normal"}),
            "nested",
        ),
    ] {
        let handle = test.client.spawn_job(job).send().await?;
        let row = task_row(&test.client, handle.result.task_id).await?;
        assert_eq!(row["task_name"], name);
        assert_eq!(row["params"], params);
        expected.push((handle, message));
    }
    let shutdown = CancellationToken::new();
    let worker = test
        .client
        .worker(Router::from_job_handler(handle_job))
        .concurrency(3)
        .run(shutdown.clone());
    let observe = async {
        let result: TestResult = async {
            for (handle, expected) in expected {
                let message: Box<str> = handle
                    .await_result(&test.client, Some(Duration::from_secs(5)))
                    .await?;
                assert_eq!(&*message, expected);
                let row = task_row(&test.client, handle.result.task_id).await?;
                assert_eq!(row["completed_payload"], expected);
                assert_eq!(row["attempts"], 1);
            }
            Ok(())
        }
        .await;
        shutdown.cancel();
        result
    };
    timeout(Duration::from_secs(8), async {
        let (worker, observed) = tokio::join!(worker, observe);
        worker?;
        observed
    })
    .await??;
    Ok(())
}

/// Retains queue selection and every existing spawn option.
#[tokio::test]
async fn enum_jobs_preserve_spawn_options_and_queue_selection() -> TestResult {
    let test = setup().await?;
    let client = Client::builder(test.client.pool().clone())
        .default_queue("default")?
        .default_max_attempts(3)
        .build();
    let default = client.spawn_job(Job::Ping).send().await?;
    assert_eq!(default.queue_name.as_str(), "default");
    assert_eq!(
        task_row(&client, default.result.task_id).await?["max_attempts"],
        3
    );
    let configured = client
        .spawn_job(Job::Ping)
        .max_attempts(7)
        .retry_strategy(RetryStrategy::Fixed {
            base: Duration::from_secs(2),
        })
        .cancellation(CancellationPolicy {
            max_duration: Some(Duration::from_secs(120)),
            max_delay: Some(Duration::from_secs(60)),
        })
        .headers(json!({"traceparent": "enum-trace"}))?
        .idempotency_key("one-ping")
        .send()
        .await?;
    let row = task_row(&client, configured.result.task_id).await?;
    assert_eq!(row["max_attempts"], 7);
    assert_eq!(
        row["retry_strategy"],
        json!({"kind": "fixed", "base_seconds": 2.0})
    );
    assert_eq!(
        row["cancellation"],
        json!({"max_duration": 120, "max_delay": 60})
    );
    assert_eq!(row["headers"], json!({"traceparent": "enum-trace"}));
    let duplicate = client
        .spawn_job(Job::Count(99))
        .idempotency_key("one-ping")
        .send()
        .await?;
    assert_eq!(duplicate.result.task_id, configured.result.task_id);
    assert!(!duplicate.result.created);
    assert_eq!(
        task_row(&client, duplicate.result.task_id).await?["task_name"],
        "Ping"
    );

    client
        .create_queue("other", CreateQueueOptions::default())
        .await?;
    let overridden = client.spawn_job(Job::Ping).queue("other")?.send().await?;
    assert_eq!(overridden.queue_name.as_str(), "other");
    let stored: String =
        sqlx::query_scalar("SELECT task_name FROM absurd.t_other WHERE task_id = $1")
            .bind(overridden.result.task_id.as_uuid())
            .fetch_one(client.pool())
            .await?;
    assert_eq!(stored, "Ping");
    let unconfigured = Client::builder(client.pool().clone()).build();
    assert!(matches!(
        unconfigured.spawn_job(Job::Ping).send().await,
        Err(Error::InvalidName { kind: "queue", .. })
    ));
    assert!(matches!(
        client.spawn_job(Job::Ping).queue("missing")?.send().await,
        Err(Error::Sqlx { .. })
    ));
    assert!(
        !client
            .list_queues()
            .await?
            .iter()
            .any(|queue| queue.as_str() == "missing")
    );
    Ok(())
}

/// Keeps business mutations and enum enqueueing in the caller's transaction.
#[tokio::test]
async fn enum_jobs_enqueue_atomically_in_caller_transactions() -> TestResult {
    let test = setup().await?;
    sqlx::query("CREATE TABLE public.requests (task_id uuid PRIMARY KEY)")
        .execute(test.client.pool())
        .await?;
    for commit in [false, true] {
        let mut transaction = test.client.pool().begin().await?;
        let invalid = test
            .client
            .spawn_job(WireJob(json!({"wrong": true})))
            .send_on(&mut transaction)
            .await;
        assert!(matches!(invalid, Err(Error::InvalidJobEnvelope { .. })));
        let handle = test
            .client
            .spawn_job(Job::Ping)
            .idempotency_key("transactional-ping")
            .send_on(&mut transaction)
            .await?;
        assert!(handle.result.created);
        sqlx::query("INSERT INTO public.requests (task_id) VALUES ($1)")
            .bind(handle.result.task_id.as_uuid())
            .execute(&mut *transaction)
            .await?;
        assert!(
            test.client
                .fetch_task_result("default", handle.result.task_id.as_uuid())
                .await?
                .is_none()
        );
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM public.requests")
            .fetch_one(test.client.pool())
            .await?;
        assert_eq!(visible, 0);
        if commit {
            transaction.commit().await?;
            let duplicate = test
                .client
                .spawn_job(Job::Ping)
                .idempotency_key("transactional-ping")
                .send()
                .await?;
            assert_eq!(duplicate.result.task_id, handle.result.task_id);
            assert!(!duplicate.result.created);
        } else {
            transaction.rollback().await?;
            assert!(
                test.client
                    .fetch_task_result("default", handle.result.task_id.as_uuid())
                    .await?
                    .is_none()
            );
        }
        let counts: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM public.requests), (SELECT count(*) FROM absurd.t_default)")
            .fetch_one(test.client.pool()).await?;
        let expected = i64::from(commit);
        assert_eq!(counts, (expected, expected));
    }
    Ok(())
}

/// Exchanges invocations with independently registered named handlers.
#[tokio::test]
async fn enum_and_named_handlers_share_the_wire_protocol() -> TestResult {
    todo!(
        "Dispatch enum-produced jobs with named handlers and named-produced jobs with enum handlers, including aliases and null results"
    )
}

/// Rejects mixed modes while retaining shared handlers and wrappers on cloning.
#[tokio::test]
async fn enum_router_rejects_named_registrations_and_clones_handlers() -> TestResult {
    todo!(
        "Reject adding named registrations to enum routers; execute through clones and verify shared handler and wrapper behavior"
    )
}

/// Defers unsupported tags for a capable worker without spending retry attempts.
#[tokio::test]
async fn unknown_enum_tags_defer_without_consuming_attempts() -> TestResult {
    todo!(
        "Run an older worker against a newer tag, observe sleeping state and unchanged attempt, then complete the same run with a capable worker"
    )
}

/// Fails known malformed jobs without confusing nested enum errors with tags.
#[tokio::test]
async fn malformed_enum_params_fail_without_deferral() -> TestResult {
    todo!(
        "Persist failures for wrong types, missing fields, and unknown nested enums including a nested task field, while valid jobs still execute"
    )
}

/// Replays saved results across both failed attempts and durable suspension.
#[tokio::test]
async fn enum_jobs_replay_checkpoints_across_retries_and_suspension() -> TestResult {
    todo!(
        "Count expensive work once despite a retry and sleep; inspect checkpoints and verify final typed output and attempt identity"
    )
}

/// Preserves wrapped owning-run control flow and application metadata.
#[tokio::test]
async fn enum_handlers_preserve_wrappers_and_owning_control_signals() -> TestResult {
    todo!(
        "Wrap a suspended enum handler with source-preserving errors; resume successfully and verify headers, identity, and wrapper invocations"
    )
}

/// Contains application panics and output encoding failures within the run.
#[tokio::test]
async fn enum_dispatch_converts_panics_and_serialization_errors() -> TestResult {
    todo!(
        "Catch decoder, handler construction, handler polling, and output serialization panics; persist serializer errors without completing or deferring the job"
    )
}

/// Renews active enum jobs and drains them after worker shutdown is requested.
#[tokio::test]
async fn enum_workers_renew_claims_and_drain_on_shutdown() -> TestResult {
    todo!(
        "Observe lease renewal beyond the initial claim, verify no competing claim, request shutdown while executing, and drain to one completed attempt"
    )
}

/// Applies deadlines and local cancellation before accepting successful output.
#[tokio::test]
async fn enum_supervision_bounds_deadlines_and_local_cancellation() -> TestResult {
    todo!(
        "Fail a hanging enum handler on its deadline and reject late success after local cancellation"
    )
}

/// Surfaces deferral persistence errors instead of converting them into retries.
#[tokio::test]
async fn enum_deferral_errors_reach_worker_supervisor() -> TestResult {
    todo!(
        "Inject a PostgreSQL scheduling failure for an unsupported tag, verify the worker returns that SQL error, and retain the unresolved run for lease recovery"
    )
}

/// Replays a typed enum child result after the child task has been cleaned up.
#[tokio::test]
async fn enum_child_waits_replay_typed_outputs() -> TestResult {
    todo!(
        "Await an enum child's output through a typed handle from another queue, checkpoint it, remove the child, and replay the parent's saved observation"
    )
}
