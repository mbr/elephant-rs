//! Contracts, wire compatibility, and supervised execution of enum jobs.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use elephant::{
    client::Client,
    context::TaskContext,
    error::Error,
    run::{ExecutionOptions, StallTimeout},
    task::{AbsurdJob, Router, Task, TaskExecution},
    types::{
        CancellationPolicy, CreateQueueOptions, PgInterval, QueueName, QueuePolicyOptions,
        RetryStrategy, RunId, SpawnOptions, Spawned, TaskId, TaskResultState,
    },
    worker::{ClaimOptions, work_batch},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tokio::{
    sync::Notify,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use super::{TestFailure, TestResult, setup, setup_with_max_connections};

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

/// Provides an explicit unit-output contract independent of boxed results.
#[derive(Deserialize, Serialize)]
#[serde(tag = "task", content = "params")]
enum UnitJob {
    /// Runs an operation whose completion has no additional data.
    Cleanup,
}

impl AbsurdJob for UnitJob {
    /// Persists successful completion as JSON null.
    type Output = ();
}

/// Carries a typed child reference in another enum job's input.
#[derive(Deserialize, Serialize)]
#[serde(tag = "task", content = "params")]
enum ParentJob {
    /// Observes the child before continuing the workflow.
    Observe {
        /// Retains the queue, task identity, and expected result type.
        child: Spawned<Box<str>>,
    },
}

impl AbsurdJob for ParentJob {
    /// Returns the child's user-facing message.
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

/// Reads a run's state independently of its task snapshot.
async fn run_row(client: &Client, id: RunId) -> TestResult<Value> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(r) FROM absurd.r_default r WHERE run_id = $1")
            .bind(id.as_uuid())
            .fetch_one(client.pool())
            .await?,
    )
}

/// Advances database time on fixtures restricted to one connection.
async fn clock(client: &Client, seconds: i32) -> TestResult {
    sqlx::query("SELECT set_config('absurd.fake_now', (TIMESTAMPTZ '2025-01-01' + make_interval(secs => $1))::text, false)")
        .bind(seconds).execute(client.pool()).await?;
    Ok(())
}

/// Observes an intermediate persisted state while a real worker is polled.
async fn wait_for_state(client: &Client, id: TaskId, state: &str) -> TestResult<Value> {
    timeout(Duration::from_secs(5), async {
        loop {
            let row = task_row(client, id).await?;
            if row["state"] == state {
                return Ok(row);
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await?
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

/// Exercises user-defined decoding panics inside dispatch.
#[derive(Serialize)]
struct DecodePanic;

impl<'de> Deserialize<'de> for DecodePanic {
    /// Panics during job reconstruction.
    ///
    /// # Panic
    ///
    /// Always panics to exercise the dispatch boundary.
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        panic!("decoder panic")
    }
}

impl AbsurdJob for DecodePanic {
    /// Discards results from an invocation that cannot decode.
    type Output = ();
}

/// Selects an output encoding failure after successful dispatch.
#[derive(Deserialize, Serialize)]
#[serde(tag = "task", content = "params")]
enum EncodingJob {
    /// Selects how the returned value fails to serialize.
    Encode {
        /// Chooses unwinding instead of a serialization error.
        panic: bool,
    },
}

impl AbsurdJob for EncodingJob {
    /// Exercises serialization of a typed handler result.
    type Output = BrokenOutput;
}

/// Fails when the runtime serializes a successful handler return value.
#[derive(Deserialize)]
struct BrokenOutput {
    /// Chooses an unwind instead of an ordinary error.
    panic: bool,
}

impl Serialize for BrokenOutput {
    /// Produces the requested output encoding failure.
    ///
    /// # Panic
    ///
    /// Panics when the test requests unwinding during serialization.
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        assert!(!self.panic, "output serialization panic");
        Err(serde::ser::Error::custom("output encoding failed"))
    }
}

/// Panics before returning a handler future.
///
/// # Panic
///
/// Always panics to exercise handler construction.
fn panic_on_construction(
    _context: TaskContext,
    _job: Job,
) -> std::future::Ready<elephant::error::Result<Box<str>>> {
    panic!("handler construction panic")
}

/// Panics while polling the handler future.
///
/// # Panic
///
/// Always panics to exercise async execution.
async fn panic_during_poll(_context: TaskContext, _job: Job) -> elephant::error::Result<Box<str>> {
    panic!("handler polling panic")
}

/// Panics before returning an execution-wrapper future.
///
/// # Panic
///
/// Always panics to exercise wrapper construction.
fn panic_in_wrapper(_context: TaskContext, _execute: TaskExecution) -> TaskExecution {
    panic!("wrapper construction panic")
}

/// Rejects malformed serialization before acquiring a database connection.
#[tokio::test]
async fn enum_envelopes_are_validated_before_database_access() -> TestResult {
    let pool = PgPoolOptions::new().connect_lazy("postgresql://localhost/unused")?;
    pool.close().await;
    let client = Client::builder(pool)
        .default_queue(QueueName::from_static("default"))
        .build();
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
        .default_queue(QueueName::from_static("default"))
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
    let test = setup().await?;
    let named = Task::<Value, Box<str>>::builder("generate-report-v1")?
        .handler(|_, params| async move {
            assert_eq!(params, json!({"customer_id": 123, "year": 2026}));
            Ok("report:123:2026".into())
        })
        .build();
    let named_router = Router::new().task(named.clone())?;
    let from_enum = test
        .client
        .spawn_job(Job::Generate {
            customer_id: 123,
            year: 2026,
        })
        .send()
        .await?;
    work_batch(&test.client, &named_router, "default").await?;
    assert_eq!(
        &*from_enum
            .await_result(&test.client, Some(Duration::from_secs(2)))
            .await?,
        "report:123:2026"
    );
    let enum_router = Router::from_job_handler(handle_job);
    for name in ["generate-report-v1", "generate-report"] {
        let contract = Task::<Value, Box<str>>::builder(name)?.build();
        let from_named = test
            .client
            .spawn(&contract, json!({"customer_id": 123, "year": 2026}))
            .send()
            .await?;
        work_batch(&test.client, &enum_router, "default").await?;
        assert_eq!(
            &*from_named
                .await_result(&test.client, Some(Duration::from_secs(2)))
                .await?,
            "report:123:2026"
        );
    }
    let unit = test.client.spawn_job(UnitJob::Cleanup).send().await?;
    let cleanup = Task::<(), ()>::builder("Cleanup")?
        .handler(|_, ()| async { Ok(()) })
        .build();
    work_batch(
        &test.client,
        &Router::new().task(cleanup.clone())?,
        "default",
    )
    .await?;
    unit.await_result(&test.client, Some(Duration::from_secs(2)))
        .await?;
    assert_eq!(
        task_row(&test.client, unit.result.task_id).await?["completed_payload"],
        Value::Null
    );
    let unit = test.client.spawn(&cleanup, ()).send().await?;
    work_batch(
        &test.client,
        &Router::from_job_handler(|_, _: UnitJob| async { Ok(()) }),
        "default",
    )
    .await?;
    unit.await_result(&test.client, Some(Duration::from_secs(2)))
        .await?;
    assert_eq!(
        task_row(&test.client, unit.result.task_id).await?["state"],
        "completed"
    );
    Ok(())
}

/// Rejects mixed modes while retaining shared handlers and wrappers on cloning.
#[tokio::test]
async fn enum_router_rejects_named_registrations_and_clones_handlers() -> TestResult {
    let test = setup().await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let wrappers = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::clone(&calls);
    let wrapper_calls = Arc::clone(&wrappers);
    let router = Router::from_job_handler(move |context, job: Job| {
        handler_calls.fetch_add(1, Ordering::SeqCst);
        handle_job(context, job)
    })
    .wrap_execution(move |_, execute| {
        wrapper_calls.fetch_add(1, Ordering::SeqCst);
        execute
    });
    let named = Task::<(), ()>::builder("named")?
        .handler(|_, ()| async { Ok(()) })
        .build();
    assert!(matches!(
        router.clone().task(named),
        Err(Error::MixedRouterModes)
    ));
    for router in [router.clone(), router] {
        let job = test.client.spawn_job(Job::Ping).send().await?;
        work_batch(&test.client, &router, "default").await?;
        assert_eq!(
            &*job
                .await_result(&test.client, Some(Duration::from_secs(2)))
                .await?,
            "pong"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(wrappers.load(Ordering::SeqCst), 2);
    Ok(())
}

/// Defers unsupported tags for a capable worker without spending retry attempts.
#[tokio::test]
async fn unknown_enum_tags_defer_without_consuming_attempts() -> TestResult {
    let test = setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let handle = test
        .client
        .spawn_job(UnitJob::Cleanup)
        .max_attempts(1)
        .send()
        .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let wrapper_calls = Arc::clone(&calls);
    let old = Router::from_job_handler(handle_job)
        .unknown_task_delay(Duration::from_secs(1))
        .wrap_execution(move |_, execute| {
            wrapper_calls.fetch_add(1, Ordering::SeqCst);
            execute
        });
    let shutdown = CancellationToken::new();
    let worker = test.client.worker(old.clone()).run(shutdown.clone());
    let observe = async {
        let result = wait_for_state(&test.client, handle.result.task_id, "sleeping").await;
        shutdown.cancel();
        result
    };
    let row = timeout(Duration::from_secs(8), async {
        let (worker, row) = tokio::join!(worker, observe);
        worker?;
        row
    })
    .await??;
    assert_eq!(row["attempts"], 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let run = run_row(&test.client, handle.result.run_id).await?;
    assert_eq!(run["state"], "sleeping");
    assert!(run["failure_reason"].is_null());
    let delay: f64 = sqlx::query_scalar("SELECT extract(epoch FROM available_at - absurd.current_time())::float8 FROM absurd.r_default WHERE run_id = $1")
        .bind(handle.result.run_id.as_uuid()).fetch_one(test.client.pool()).await?;
    assert!(
        delay > 0.0 && delay <= 1.0,
        "unexpected deferral delay: {delay}"
    );
    assert!(
        test.client
            .claim_task("default", &ClaimOptions::default())
            .await?
            .is_empty()
    );
    clock(&test.client, 2).await?;
    let expected_run = handle.result.run_id;
    let new = Router::from_job_handler(move |context: TaskContext, _: UnitJob| async move {
        assert_eq!(context.metadata().run_id, expected_run);
        assert_eq!(context.metadata().attempt, 1);
        Ok(())
    });
    work_batch(&test.client, &new, "default").await?;
    handle
        .await_result(&test.client, Some(Duration::from_secs(2)))
        .await?;
    assert_eq!(
        task_row(&test.client, handle.result.task_id).await?["attempts"],
        1
    );
    Ok(())
}

/// Fails known malformed jobs without confusing nested enum errors with tags.
#[tokio::test]
async fn malformed_enum_params_fail_without_deferral() -> TestResult {
    let test = setup().await?;
    let mut invalid = Vec::new();
    for (name, params, diagnostic) in [
        (
            "generate-report-v1",
            json!({"customer_id": "wrong", "year": 2026}),
            "invalid type",
        ),
        (
            "generate-report-v1",
            json!({"customer_id": 123}),
            "missing field",
        ),
        ("generate-report", Value::Null, "invalid type"),
        ("Nested", json!({"task": "Unknown"}), "unknown variant"),
        ("Count", json!("wrong"), "invalid type"),
        ("Pair", json!([1]), "invalid length"),
        ("Ping", json!({"unexpected": true}), "invalid type"),
    ] {
        let contract = Task::<Value, Box<str>>::builder(name)?
            .default_max_attempts(1)
            .build();
        invalid.push((
            test.client.spawn(&contract, params).send().await?,
            diagnostic,
        ));
    }
    let valid = test
        .client
        .spawn_job(Job::Ping)
        .max_attempts(1)
        .send()
        .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::clone(&calls);
    let router = Router::from_job_handler(move |context, job: Job| {
        handler_calls.fetch_add(1, Ordering::SeqCst);
        handle_job(context, job)
    });
    let shutdown = CancellationToken::new();
    let worker = test
        .client
        .worker(router)
        .concurrency(3)
        .run(shutdown.clone());
    let observe = async {
        let result: TestResult = async {
            for (handle, diagnostic) in invalid {
                let error = handle
                    .await_result(&test.client, Some(Duration::from_secs(3)))
                    .await
                    .expect_err("malformed parameters must fail");
                let Error::TaskFailed { task_id, failure } = error else {
                    panic!("unexpected error: {error:?}")
                };
                assert_eq!(task_id, handle.result.task_id.as_uuid());
                let failure = failure.expect("persisted failure");
                assert_eq!(failure["name"], "elephant_error");
                let message = failure["message"].as_str().expect("failure message");
                assert!(
                    message.contains("job decoding failed") && message.contains(diagnostic),
                    "{message}"
                );
                assert_eq!(
                    task_row(&test.client, handle.result.task_id).await?["attempts"],
                    1
                );
            }
            assert_eq!(
                &*valid
                    .await_result(&test.client, Some(Duration::from_secs(3)))
                    .await?,
                "pong"
            );
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
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// Replays saved results across both failed attempts and durable suspension.
#[tokio::test]
async fn enum_jobs_replay_checkpoints_across_retries_and_suspension() -> TestResult {
    let test = setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let bodies = Arc::clone(&calls);
    let router = Router::from_job_handler(move |context: TaskContext, _: Job| {
        let bodies = Arc::clone(&bodies);
        async move {
            let message: Box<str> = context
                .step("expensive", || async move {
                    bodies.fetch_add(1, Ordering::SeqCst);
                    Ok("saved message".into())
                })
                .await?;
            if context.metadata().attempt == 1 {
                return Err(Error::handler(Box::new(TestFailure)));
            }
            context.sleep_for(Duration::from_secs(1)).await?;
            Ok(message)
        }
    });
    let handle = test
        .client
        .spawn_job(Job::Ping)
        .max_attempts(2)
        .retry_strategy(RetryStrategy::Fixed {
            base: Duration::from_secs(1),
        })
        .send()
        .await?;
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        run_row(&test.client, handle.result.run_id).await?["state"],
        "failed"
    );
    let checkpoint: Value = sqlx::query_scalar(
        "SELECT state FROM absurd.c_default WHERE task_id = $1 AND checkpoint_name = 'expensive'",
    )
    .bind(handle.result.task_id.as_uuid())
    .fetch_one(test.client.pool())
    .await?;
    assert_eq!(checkpoint, json!("saved message"));
    clock(&test.client, 2).await?;
    work_batch(&test.client, &router, "default").await?;
    let sleeping = task_row(&test.client, handle.result.task_id).await?;
    assert_eq!(sleeping["state"], "sleeping");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    clock(&test.client, 4).await?;
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        &*handle
            .await_result(&test.client, Some(Duration::from_secs(2)))
            .await?,
        "saved message"
    );
    let completed = task_row(&test.client, handle.result.task_id).await?;
    assert_eq!(completed["last_attempt_run"], sleeping["last_attempt_run"]);
    assert_eq!(completed["attempts"], 2);
    assert_eq!(completed["task_name"], "Ping");
    assert_eq!(completed["params"], Value::Null);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// Preserves wrapped owning-run control flow and application metadata.
#[tokio::test]
async fn enum_handlers_preserve_wrappers_and_owning_control_signals() -> TestResult {
    let test = setup_with_max_connections(1).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    for mode in 0..3_u64 {
        clock(&test.client, mode as i32 * 10).await?;
        let handle = test
            .client
            .spawn_job(Job::Count(mode))
            .max_attempts(1)
            .headers(json!({"mode": mode}))?
            .send()
            .await?;
        let task_id = handle.result.task_id;
        let run_id = handle.result.run_id;
        let wrapper_calls = Arc::clone(&calls);
        let router = Router::from_job_handler(move |context: TaskContext, job: Job| async move {
            assert!(matches!(job, Job::Count(value) if value == mode));
            match mode {
                0 => context.sleep_for(Duration::from_secs(1)).await?,
                1 => {
                    context
                        .client()
                        .cancel_task("default", task_id.as_uuid())
                        .await?;
                    context.heartbeat(Duration::from_secs(30)).await?;
                }
                _ => {
                    context
                        .client()
                        .fail_run(
                            "default",
                            run_id.as_uuid(),
                            json!({"name": "original", "message": "original failure"}),
                        )
                        .await?;
                    context.heartbeat(Duration::from_secs(30)).await?;
                }
            }
            Ok("resumed".into())
        })
        .wrap_execution(move |context, execute| {
            wrapper_calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(context.metadata().task_id, task_id);
            assert_eq!(context.metadata().run_id, run_id);
            assert_eq!(context.metadata().task_name.as_str(), "Count");
            assert_eq!(context.metadata().queue_name.as_str(), "default");
            assert_eq!(context.metadata().attempt, 1);
            assert_eq!(
                context.metadata().headers.as_ref(),
                Some(&json!({"mode": mode}))
            );
            async move {
                execute
                    .await
                    .map_err(|error| Error::handler(Box::new(error)))
            }
        });
        work_batch(&test.client, &router, "default").await?;
        match mode {
            0 => {
                assert_eq!(task_row(&test.client, task_id).await?["state"], "sleeping");
                clock(&test.client, 2).await?;
                work_batch(&test.client, &router, "default").await?;
                assert_eq!(
                    &*handle
                        .await_result(&test.client, Some(Duration::from_secs(2)))
                        .await?,
                    "resumed"
                );
            }
            1 => assert_eq!(task_row(&test.client, task_id).await?["state"], "cancelled"),
            _ => {
                let run = run_row(&test.client, run_id).await?;
                assert_eq!(run["state"], "failed");
                assert_eq!(run["failure_reason"]["message"], "original failure");
            }
        }
        assert_eq!(task_row(&test.client, task_id).await?["attempts"], 1);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    Ok(())
}

/// Contains application panics and output encoding failures within the run.
#[tokio::test]
async fn enum_dispatch_converts_panics_and_serialization_errors() -> TestResult {
    let test = setup().await?;
    let encoding = Router::from_job_handler(|_, job: EncodingJob| async move {
        let EncodingJob::Encode { panic } = job;
        Ok(BrokenOutput { panic })
    });
    for (router, name, params, category, message) in [
        (
            Router::from_job_handler(|_, _: DecodePanic| async { Ok(()) }),
            "DecodePanic",
            Value::Null,
            "panic",
            "decoder panic",
        ),
        (
            Router::from_job_handler(panic_on_construction),
            "Ping",
            Value::Null,
            "panic",
            "handler construction panic",
        ),
        (
            Router::from_job_handler(panic_during_poll),
            "Ping",
            Value::Null,
            "panic",
            "handler polling panic",
        ),
        (
            encoding.clone(),
            "Encode",
            json!({"panic": true}),
            "panic",
            "output serialization panic",
        ),
        (
            encoding,
            "Encode",
            json!({"panic": false}),
            "elephant_error",
            "output encoding failed",
        ),
        (
            Router::from_job_handler(handle_job).wrap_execution(panic_in_wrapper),
            "Ping",
            Value::Null,
            "panic",
            "wrapper construction panic",
        ),
        (
            Router::from_job_handler(handle_job)
                .wrap_execution(|_, _| async { panic!("wrapper polling panic") }),
            "Ping",
            Value::Null,
            "panic",
            "wrapper polling panic",
        ),
    ] {
        let handle = test
            .client
            .spawn_untyped(
                name,
                params,
                SpawnOptions {
                    max_attempts: Some(1),
                    ..SpawnOptions::default()
                },
            )
            .await?;
        work_batch(&test.client, &router, "default").await?;
        let snapshot = test
            .client
            .await_task_result(
                "default",
                handle.task_id.as_uuid(),
                Some(Duration::from_secs(2)),
            )
            .await?;
        assert_eq!(snapshot.state, TaskResultState::Failed);
        assert!(snapshot.result.is_none());
        let failure = snapshot.failure.expect("persisted failure");
        assert_eq!(failure["name"], category);
        assert!(
            failure["message"]
                .as_str()
                .expect("failure message")
                .contains(message),
            "{failure}"
        );
        assert_eq!(task_row(&test.client, handle.task_id).await?["attempts"], 1);
    }
    let valid = test.client.spawn_job(Job::Ping).send().await?;
    work_batch(
        &test.client,
        &Router::from_job_handler(handle_job),
        "default",
    )
    .await?;
    assert_eq!(
        &*valid
            .await_result(&test.client, Some(Duration::from_secs(2)))
            .await?,
        "pong"
    );
    Ok(())
}

/// Renews active enum jobs and drains them after worker shutdown is requested.
#[tokio::test]
async fn enum_workers_renew_claims_and_drain_on_shutdown() -> TestResult {
    let test = setup().await?;
    let started = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let handler_started = Arc::clone(&started);
    let handler_finish = Arc::clone(&finish);
    let router = Router::from_job_handler(move |_, _: Job| {
        let started = Arc::clone(&handler_started);
        let finish = Arc::clone(&handler_finish);
        async move {
            started.notify_one();
            finish.notified().await;
            Ok("drained".into())
        }
    });
    let handle = test
        .client
        .spawn_job(Job::Ping)
        .max_attempts(1)
        .send()
        .await?;
    let shutdown = CancellationToken::new();
    let worker = test
        .client
        .worker(router)
        .worker_id("enum-drain")
        .claim_timeout(Duration::from_secs(1))
        .execution(ExecutionOptions {
            stall_timeout: StallTimeout::Disabled,
            ..ExecutionOptions::default()
        })
        .run(shutdown.clone());
    tokio::pin!(worker);
    let observe = async {
        started.notified().await;
        let initial = run_row(&test.client, handle.result.run_id).await?;
        let initial_expiry: jiff::Timestamp = initial["claim_expires_at"]
            .as_str()
            .expect("initial lease")
            .parse()?;
        sleep(Duration::from_millis(1200)).await;
        let renewed = run_row(&test.client, handle.result.run_id).await?;
        let renewed_expiry: jiff::Timestamp = renewed["claim_expires_at"]
            .as_str()
            .expect("renewed lease")
            .parse()?;
        assert!(renewed_expiry > initial_expiry);
        assert_eq!(renewed["claimed_by"], "enum-drain");
        assert!(
            test.client
                .claim_task("default", &ClaimOptions::default())
                .await?
                .is_empty()
        );
        assert_eq!(
            task_row(&test.client, handle.result.task_id).await?["state"],
            "running"
        );
        shutdown.cancel();
        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            task_row(&test.client, handle.result.task_id).await?["state"],
            "running"
        );
        finish.notify_one();
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };
    tokio::select! {
        result = &mut worker => panic!("worker stopped before drain was released: {result:?}"),
        result = timeout(Duration::from_secs(6), observe) => result??,
    }
    timeout(Duration::from_secs(3), &mut worker).await??;
    assert_eq!(
        &*handle
            .await_result(&test.client, Some(Duration::from_secs(2)))
            .await?,
        "drained"
    );
    assert_eq!(
        task_row(&test.client, handle.result.task_id).await?["attempts"],
        1
    );
    Ok(())
}

/// Applies deadlines and local cancellation before accepting successful output.
#[tokio::test]
async fn enum_supervision_bounds_deadlines_and_local_cancellation() -> TestResult {
    let test = setup().await?;
    for (mode, message) in [
        ("deadline", "execution deadline expired"),
        ("cancel", "execution was cancelled locally"),
        (
            "stall",
            "execution stalled without checkpoint or heartbeat progress",
        ),
    ] {
        let router = Router::from_job_handler(move |context: TaskContext, _: Job| async move {
            if mode == "cancel" {
                context.cancellation_token().cancel();
                Ok("late success".into())
            } else {
                std::future::pending::<elephant::error::Result<Box<str>>>().await
            }
        });
        let handle = test
            .client
            .spawn_job(Job::Ping)
            .max_attempts(1)
            .send()
            .await?;
        let lease = test
            .client
            .claim_task("default", &ClaimOptions::default())
            .await?
            .pop()
            .expect("claimed enum job");
        let options = ExecutionOptions {
            timeout: (mode == "deadline").then_some(Duration::from_millis(30)),
            stall_timeout: if mode == "stall" {
                StallTimeout::After(Duration::from_millis(30))
            } else {
                StallTimeout::Disabled
            },
            cancellation_grace: Duration::ZERO,
            ..ExecutionOptions::default()
        };
        timeout(Duration::from_secs(3), router.dispatch_with(lease, options)).await??;
        let snapshot = test
            .client
            .fetch_task_result("default", handle.result.task_id.as_uuid())
            .await?
            .expect("failed task");
        assert_eq!(snapshot.state, TaskResultState::Failed);
        assert!(snapshot.result.is_none());
        assert_eq!(
            snapshot.failure.expect("supervision failure")["message"],
            message
        );
        assert_eq!(
            task_row(&test.client, handle.result.task_id).await?["attempts"],
            1
        );
    }
    Ok(())
}

/// Surfaces deferral persistence errors instead of converting them into retries.
#[tokio::test]
async fn enum_deferral_errors_reach_worker_supervisor() -> TestResult {
    let test = setup().await?;
    let handle = test
        .client
        .spawn_job(UnitJob::Cleanup)
        .max_attempts(2)
        .send()
        .await?;
    sqlx::query("CREATE OR REPLACE FUNCTION absurd.schedule_run(p_queue_name text, p_run_id uuid, p_wake_at timestamptz) RETURNS void LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE = 'XX000', MESSAGE = 'enum deferral unavailable'; END $$")
        .execute(test.client.pool()).await?;
    let error = timeout(
        Duration::from_secs(3),
        test.client
            .worker(Router::from_job_handler(handle_job))
            .run(CancellationToken::new()),
    )
    .await?
    .expect_err("deferral database failure must stop claiming");
    let Error::Sqlx { source } = error else {
        panic!("unexpected worker error: {error:?}")
    };
    assert_eq!(
        source
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("XX000")
    );
    assert_eq!(
        source.as_database_error().expect("server error").message(),
        "enum deferral unavailable"
    );
    let task = task_row(&test.client, handle.result.task_id).await?;
    assert_eq!(task["state"], "running");
    assert_eq!(task["attempts"], 1);
    let run = run_row(&test.client, handle.result.run_id).await?;
    assert_eq!(run["state"], "running");
    assert!(run["failure_reason"].is_null());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM absurd.r_default WHERE task_id = $1")
        .bind(handle.result.task_id.as_uuid())
        .fetch_one(test.client.pool())
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

/// Replays a typed enum child result after the child task has been cleaned up.
#[tokio::test]
async fn enum_child_waits_replay_typed_outputs() -> TestResult {
    let test = setup_with_max_connections(1).await?;
    clock(&test.client, 0).await?;
    test.client
        .create_queue("children", CreateQueueOptions::default())
        .await?;
    let child = test
        .client
        .spawn_job(Job::Ping)
        .queue("children")?
        .send()
        .await?;
    work_batch(
        &test.client,
        &Router::from_job_handler(handle_job),
        "children",
    )
    .await?;
    let parent = test
        .client
        .spawn_job(ParentJob::Observe {
            child: child.clone(),
        })
        .max_attempts(2)
        .retry_strategy(RetryStrategy::Fixed {
            base: Duration::from_secs(1),
        })
        .send()
        .await?;
    let router = Router::from_job_handler(|context: TaskContext, job: ParentJob| async move {
        let ParentJob::Observe { child } = job;
        let budget = if context.metadata().attempt == 1 {
            Duration::from_secs(1)
        } else {
            Duration::ZERO
        };
        let result = context.await_task_result(&child, Some(budget)).await?;
        if context.metadata().attempt == 1 {
            return Err(Error::handler(Box::new(TestFailure)));
        }
        Ok(result)
    });
    work_batch(&test.client, &router, "default").await?;
    let checkpoint: Value = sqlx::query_scalar(
        "SELECT state FROM absurd.c_default WHERE task_id = $1 AND checkpoint_name = $2",
    )
    .bind(parent.result.task_id.as_uuid())
    .bind(format!("$awaitTaskResult:{}", child.result.task_id))
    .fetch_one(test.client.pool())
    .await?;
    assert_eq!(checkpoint["state"], "completed");
    assert_eq!(checkpoint["result"], "pong");
    clock(&test.client, 2).await?;
    test.client
        .set_queue_policy(
            "children",
            QueuePolicyOptions {
                cleanup_ttl: Some(PgInterval::from(Duration::ZERO)),
                ..QueuePolicyOptions::default()
            },
        )
        .await?;
    let cleaned = test.client.cleanup_queue("children").await?;
    assert_eq!(cleaned.iter().map(|row| row.tasks_deleted).sum::<i32>(), 1);
    assert!(
        test.client
            .fetch_task_result("children", child.result.task_id.as_uuid())
            .await?
            .is_none()
    );
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        &*parent
            .await_result(&test.client, Some(Duration::from_secs(2)))
            .await?,
        "pong"
    );
    assert_eq!(
        task_row(&test.client, parent.result.task_id).await?["attempts"],
        2
    );
    Ok(())
}
