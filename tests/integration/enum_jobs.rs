//! Contracts, wire compatibility, and supervised execution of enum jobs.

use elephant::{client::Client, error::Error, task::AbsurdJob};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;

use super::TestResult;

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
    todo!(
        "Store only variant contents for struct, unit, newtype, and tuple jobs; recover typed boxed-string results through the real worker"
    )
}

/// Retains queue selection and every existing spawn option.
#[tokio::test]
async fn enum_jobs_preserve_spawn_options_and_queue_selection() -> TestResult {
    todo!(
        "Verify client defaults, explicit queues, missing queues, headers, attempts, retries, cancellation, and idempotent submission"
    )
}

/// Keeps business mutations and enum enqueueing in the caller's transaction.
#[tokio::test]
async fn enum_jobs_enqueue_atomically_in_caller_transactions() -> TestResult {
    todo!(
        "Observe uncommitted isolation, roll back business rows and jobs together, then commit and verify deduplication remains usable"
    )
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
