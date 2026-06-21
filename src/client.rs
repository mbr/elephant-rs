//! Client APIs over Absurd's stored procedures.

use std::{marker::PhantomData, str::FromStr, time::Duration};

use jiff::Timestamp;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::{PgPool, Row, types::Json};
use tokio::time::sleep;
use uuid::Uuid;

use crate::{
    error::{Error, Result},
    run::{ClaimedRun, RunLease},
    task::Task,
    types::{
        CancellationPolicy, CleanupResult, CreateQueueOptions, QueueDetachMode, QueueName,
        QueuePolicy, QueuePolicyOptions, QueueStorageMode, RetryStrategy, RetryTaskOptions, RunId,
        SpawnOptions, SpawnResult, Spawned, TaskId, TaskName, TaskResultSnapshot, TaskResultState,
    },
    worker::{ClaimOptions, ClaimStream, WorkerBuilder},
};

/// Provides typed access to Absurd stored procedures.
#[derive(Clone, Debug)]
pub struct Client {
    /// Holds the PostgreSQL pool.
    pool: PgPool,
    /// Carries the default queue.
    default_queue: Option<QueueName>,
    /// Carries the default maximum attempts.
    default_max_attempts: Option<i32>,
}

impl Client {
    /// Creates a client builder.
    pub fn builder(pool: PgPool) -> ClientBuilder {
        ClientBuilder {
            pool,
            default_queue: None,
            default_max_attempts: None,
        }
    }

    /// Returns the underlying PostgreSQL pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns the configured default queue.
    pub fn default_queue(&self) -> Option<&QueueName> {
        self.default_queue.as_ref()
    }

    /// Creates an Absurd queue.
    pub async fn create_queue(
        &self,
        queue_name: impl AsRef<str>,
        options: CreateQueueOptions,
    ) -> Result<()> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        sqlx::query("SELECT absurd.create_queue($1, $2)")
            .bind(queue_name.as_str())
            .bind(options.storage_mode.as_str())
            .execute(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        self.set_queue_policy(queue_name.as_str(), options.policy)
            .await
    }

    /// Drops an Absurd queue.
    pub async fn drop_queue(&self, queue_name: impl AsRef<str>) -> Result<()> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        sqlx::query("SELECT absurd.drop_queue($1)")
            .bind(queue_name.as_str())
            .execute(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        Ok(())
    }

    /// Lists Absurd queues.
    pub async fn list_queues(&self) -> Result<Vec<QueueName>> {
        let rows = sqlx::query("SELECT queue_name FROM absurd.list_queues()")
            .fetch_all(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        rows.into_iter()
            .map(|row| {
                row.try_get::<String, _>("queue_name")
                    .map_err(Error::from_sqlx)
            })
            .map(|value| value.and_then(|name| QueueName::from_str(&name)))
            .collect()
    }

    /// Sets queue policy fields.
    pub async fn set_queue_policy(
        &self,
        queue_name: impl AsRef<str>,
        options: QueuePolicyOptions,
    ) -> Result<()> {
        if options.is_empty() {
            return Ok(());
        }
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let payload = serde_json::to_value(options).map_err(Error::json)?;
        sqlx::query("SELECT absurd.set_queue_policy($1, $2::jsonb)")
            .bind(queue_name.as_str())
            .bind(Json(payload))
            .execute(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        Ok(())
    }

    /// Fetches a queue policy snapshot.
    pub async fn get_queue_policy(
        &self,
        queue_name: impl AsRef<str>,
    ) -> Result<Option<QueuePolicy>> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let row = sqlx::query(
            "SELECT queue_name, storage_mode, partition_lookahead::text, \
             partition_lookback::text, cleanup_ttl::text, cleanup_limit, \
             detach_mode, detach_min_age::text FROM absurd.get_queue_policy($1)",
        )
        .bind(queue_name.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(Error::from_sqlx)?;

        row.map(|row| {
            let queue_name = row
                .try_get::<String, _>("queue_name")
                .map_err(Error::from_sqlx)?;
            let storage_mode = row
                .try_get::<String, _>("storage_mode")
                .map_err(Error::from_sqlx)?;
            let detach_mode = row
                .try_get::<String, _>("detach_mode")
                .map_err(Error::from_sqlx)?;
            Ok(QueuePolicy {
                queue_name: QueueName::from_str(&queue_name)?,
                storage_mode: storage_mode
                    .parse()
                    .unwrap_or(QueueStorageMode::Unpartitioned),
                partition_lookahead: row
                    .try_get("partition_lookahead")
                    .map_err(Error::from_sqlx)?,
                partition_lookback: row
                    .try_get("partition_lookback")
                    .map_err(Error::from_sqlx)?,
                cleanup_ttl: row.try_get("cleanup_ttl").map_err(Error::from_sqlx)?,
                cleanup_limit: row.try_get("cleanup_limit").map_err(Error::from_sqlx)?,
                detach_mode: detach_mode.parse().unwrap_or(QueueDetachMode::None),
                detach_min_age: row.try_get("detach_min_age").map_err(Error::from_sqlx)?,
            })
        })
        .transpose()
    }

    /// Starts building a typed spawn call.
    pub fn spawn<P, R>(&self, task: &Task<P, R>, params: P) -> SpawnBuilder<P, R>
    where
        P: Serialize,
    {
        SpawnBuilder {
            client: self.clone(),
            task_name: task.name().clone(),
            queue_name: task.queue_name().cloned(),
            default_max_attempts: task.default_max_attempts().or(self.default_max_attempts),
            default_cancellation: task.default_cancellation().cloned(),
            params,
            options: SpawnOptions::default(),
            marker: PhantomData,
        }
    }

    /// Spawns an untyped task.
    pub async fn spawn_untyped<P: Serialize>(
        &self,
        task_name: impl AsRef<str>,
        params: P,
        options: SpawnOptions,
    ) -> Result<SpawnResult> {
        let task_name = TaskName::from_str(task_name.as_ref())?;
        let queue_name = match options
            .queue_name
            .clone()
            .or_else(|| self.default_queue.clone())
        {
            Some(queue_name) => queue_name,
            None => {
                return Err(Error::InvalidName {
                    kind: "queue",
                    value: String::new(),
                    reason: "must be provided",
                });
            }
        };
        self.spawn_raw(
            queue_name,
            task_name,
            params,
            options,
            self.default_max_attempts,
        )
        .await
    }

    /// Claims a batch of runs from a queue.
    pub async fn claim_task(
        &self,
        queue_name: impl AsRef<str>,
        options: &ClaimOptions,
    ) -> Result<Vec<RunLease>> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let claim_timeout = seconds_i32(options.claim_timeout)?;
        let rows = sqlx::query(
            "SELECT run_id, task_id, attempt, task_name, params, headers, wake_event, event_payload \
             FROM absurd.claim_task($1, $2, $3, $4)",
        )
        .bind(queue_name.as_str())
        .bind(&options.worker_id)
        .bind(claim_timeout)
        .bind(options.batch_size)
        .fetch_all(&self.pool)
        .await
        .map_err(Error::from_sqlx)?;

        rows.into_iter()
            .map(|row| {
                let run = ClaimedRun {
                    queue_name: queue_name.clone(),
                    run_id: RunId::from(
                        row.try_get::<Uuid, _>("run_id").map_err(Error::from_sqlx)?,
                    ),
                    task_id: TaskId::from(
                        row.try_get::<Uuid, _>("task_id")
                            .map_err(Error::from_sqlx)?,
                    ),
                    attempt: row.try_get("attempt").map_err(Error::from_sqlx)?,
                    task_name: TaskName::from_str(
                        &row.try_get::<String, _>("task_name")
                            .map_err(Error::from_sqlx)?,
                    )?,
                    params: row
                        .try_get::<Json<Value>, _>("params")
                        .map_err(Error::from_sqlx)?
                        .0,
                    headers: row
                        .try_get::<Option<Json<Value>>, _>("headers")
                        .map_err(Error::from_sqlx)?
                        .map(|value| value.0),
                    wake_event: row.try_get("wake_event").map_err(Error::from_sqlx)?,
                    event_payload: row
                        .try_get::<Option<Json<Value>>, _>("event_payload")
                        .map_err(Error::from_sqlx)?
                        .map(|value| value.0),
                };
                Ok(RunLease::new(self.clone(), run))
            })
            .collect()
    }

    /// Creates a stream of claimed runs.
    pub fn claims(&self, queue_name: impl AsRef<str>, options: ClaimOptions) -> ClaimStream {
        ClaimStream::new(self.clone(), queue_name.as_ref().to_string(), options)
    }

    /// Creates a convenience worker builder.
    pub fn worker(&self, router: crate::task::Router) -> WorkerBuilder {
        WorkerBuilder::new(self.clone(), router)
    }

    /// Completes a running run.
    pub async fn complete_run<T: Serialize>(
        &self,
        queue_name: impl AsRef<str>,
        run_id: Uuid,
        result: T,
    ) -> Result<()> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let payload = serde_json::to_value(result).map_err(Error::json)?;
        sqlx::query("SELECT absurd.complete_run($1, $2, $3::jsonb)")
            .bind(queue_name.as_str())
            .bind(run_id)
            .bind(Json(payload))
            .execute(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        Ok(())
    }

    /// Fails a running or sleeping run.
    pub async fn fail_run<T: Serialize>(
        &self,
        queue_name: impl AsRef<str>,
        run_id: Uuid,
        reason: T,
    ) -> Result<()> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let payload = serde_json::to_value(reason).map_err(Error::json)?;
        sqlx::query("SELECT absurd.fail_run($1, $2, $3::jsonb)")
            .bind(queue_name.as_str())
            .bind(run_id)
            .bind(Json(payload))
            .execute(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        Ok(())
    }

    /// Schedules a running run for future availability.
    pub async fn schedule_run(
        &self,
        queue_name: impl AsRef<str>,
        run_id: Uuid,
        wake_at: Timestamp,
    ) -> Result<()> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        sqlx::query("SELECT absurd.schedule_run($1, $2, $3::timestamptz)")
            .bind(queue_name.as_str())
            .bind(run_id)
            .bind(wake_at.to_string())
            .execute(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        Ok(())
    }

    /// Writes a task checkpoint.
    pub async fn set_checkpoint<T: Serialize>(
        &self,
        queue_name: impl AsRef<str>,
        task_id: Uuid,
        step_name: impl AsRef<str>,
        state: T,
        owner_run: Uuid,
        extend_claim_by: Option<Duration>,
    ) -> Result<()> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let payload = serde_json::to_value(state).map_err(Error::json)?;
        sqlx::query("SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4::jsonb, $5, $6)")
            .bind(queue_name.as_str())
            .bind(task_id)
            .bind(step_name.as_ref())
            .bind(Json(payload))
            .bind(owner_run)
            .bind(extend_claim_by.map(seconds_i32).transpose()?)
            .execute(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        Ok(())
    }

    /// Fetches all visible checkpoints for a run.
    pub async fn get_checkpoints(
        &self,
        queue_name: impl AsRef<str>,
        task_id: Uuid,
        run_id: Uuid,
    ) -> Result<Vec<(String, Value)>> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let rows = sqlx::query(
            "SELECT checkpoint_name, state FROM absurd.get_task_checkpoint_states($1, $2, $3)",
        )
        .bind(queue_name.as_str())
        .bind(task_id)
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Error::from_sqlx)?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get("checkpoint_name").map_err(Error::from_sqlx)?,
                    row.try_get::<Json<Value>, _>("state")
                        .map_err(Error::from_sqlx)?
                        .0,
                ))
            })
            .collect()
    }

    /// Awaits or suspends on an event.
    pub async fn await_event_raw(
        &self,
        queue_name: impl AsRef<str>,
        task_id: Uuid,
        run_id: Uuid,
        step_name: impl AsRef<str>,
        event_name: impl AsRef<str>,
        timeout: Option<Duration>,
    ) -> Result<AwaitEventRaw> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let timeout = timeout.map(seconds_i32).transpose()?;
        let row = sqlx::query(
            "SELECT should_suspend, payload FROM absurd.await_event($1, $2, $3, $4, $5, $6)",
        )
        .bind(queue_name.as_str())
        .bind(task_id)
        .bind(run_id)
        .bind(step_name.as_ref())
        .bind(event_name.as_ref())
        .bind(timeout)
        .fetch_one(&self.pool)
        .await
        .map_err(Error::from_sqlx)?;
        Ok(AwaitEventRaw {
            should_suspend: row.try_get("should_suspend").map_err(Error::from_sqlx)?,
            payload: row
                .try_get::<Option<Json<Value>>, _>("payload")
                .map_err(Error::from_sqlx)?
                .map(|payload| payload.0),
        })
    }

    /// Emits an immutable queue event.
    pub async fn emit_event<T: Serialize>(
        &self,
        queue_name: impl AsRef<str>,
        event_name: impl AsRef<str>,
        payload: T,
    ) -> Result<()> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let payload = serde_json::to_value(payload).map_err(Error::json)?;
        sqlx::query("SELECT absurd.emit_event($1, $2, $3::jsonb)")
            .bind(queue_name.as_str())
            .bind(event_name.as_ref())
            .bind(Json(payload))
            .execute(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        Ok(())
    }

    /// Extends an active claim.
    pub async fn extend_claim(
        &self,
        queue_name: impl AsRef<str>,
        run_id: Uuid,
        extend_by: Duration,
    ) -> Result<()> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        sqlx::query("SELECT absurd.extend_claim($1, $2, $3)")
            .bind(queue_name.as_str())
            .bind(run_id)
            .bind(seconds_i32(extend_by)?)
            .execute(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        Ok(())
    }

    /// Fetches a task result snapshot.
    pub async fn fetch_task_result(
        &self,
        queue_name: impl AsRef<str>,
        task_id: Uuid,
    ) -> Result<Option<TaskResultSnapshot>> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let row =
            sqlx::query("SELECT state, result, failure_reason FROM absurd.get_task_result($1, $2)")
                .bind(queue_name.as_str())
                .bind(task_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(Error::from_sqlx)?;
        row.map(|row| {
            Ok(TaskResultSnapshot {
                state: TaskResultState::from(
                    row.try_get::<String, _>("state")
                        .map_err(Error::from_sqlx)?,
                ),
                result: row
                    .try_get::<Option<Json<Value>>, _>("result")
                    .map_err(Error::from_sqlx)?
                    .map(|value| value.0),
                failure: row
                    .try_get::<Option<Json<Value>>, _>("failure_reason")
                    .map_err(Error::from_sqlx)?
                    .map(|value| value.0),
            })
        })
        .transpose()
    }

    /// Waits for a terminal task result.
    pub async fn await_task_result(
        &self,
        queue_name: impl AsRef<str>,
        task_id: Uuid,
        timeout: Option<Duration>,
    ) -> Result<TaskResultSnapshot> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let started = std::time::Instant::now();
        let mut delay = Duration::from_millis(50);
        loop {
            if let Some(snapshot) = self.fetch_task_result(queue_name.as_str(), task_id).await?
                && snapshot.is_terminal()
            {
                return Ok(snapshot);
            }
            if timeout.is_some_and(|timeout| started.elapsed() >= timeout) {
                return Err(Error::TaskResultTimeout { task_id });
            }
            sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(1));
        }
    }

    /// Awaits and decodes a typed task result.
    pub async fn await_typed_task_result<R: DeserializeOwned>(
        &self,
        queue_name: impl AsRef<str>,
        task_id: Uuid,
        timeout: Option<Duration>,
    ) -> Result<Option<R>> {
        self.await_task_result(queue_name, task_id, timeout)
            .await?
            .decode()
    }

    /// Retries a task.
    pub async fn retry_task(
        &self,
        queue_name: impl AsRef<str>,
        task_id: Uuid,
        options: RetryTaskOptions,
    ) -> Result<SpawnResult> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        let row = sqlx::query(
            "SELECT task_id, run_id, attempt, created FROM absurd.retry_task($1, $2, $3::jsonb)",
        )
        .bind(queue_name.as_str())
        .bind(task_id)
        .bind(Json(options.to_json()))
        .fetch_one(&self.pool)
        .await
        .map_err(Error::from_sqlx)?;
        row_to_spawn_result(row)
    }

    /// Cancels a task.
    pub async fn cancel_task(&self, queue_name: impl AsRef<str>, task_id: Uuid) -> Result<()> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        sqlx::query("SELECT absurd.cancel_task($1, $2)")
            .bind(queue_name.as_str())
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        Ok(())
    }

    /// Runs Absurd cleanup across queues.
    pub async fn cleanup_all_queues(&self) -> Result<Vec<CleanupResult>> {
        self.cleanup_queues(None).await
    }

    /// Runs Absurd cleanup for one queue.
    pub async fn cleanup_queue(&self, queue_name: impl AsRef<str>) -> Result<Vec<CleanupResult>> {
        let queue_name = QueueName::from_str(queue_name.as_ref())?;
        self.cleanup_queues(Some(queue_name)).await
    }

    /// Runs Absurd cleanup with an optional queue filter.
    async fn cleanup_queues(&self, queue_name: Option<QueueName>) -> Result<Vec<CleanupResult>> {
        let rows = sqlx::query(
            "SELECT queue_name, tasks_deleted, events_deleted FROM absurd.cleanup_all_queues($1)",
        )
        .bind(queue_name.as_ref().map(QueueName::as_str))
        .fetch_all(&self.pool)
        .await
        .map_err(Error::from_sqlx)?;
        rows.into_iter()
            .map(|row| {
                Ok(CleanupResult {
                    queue_name: QueueName::from_str(
                        &row.try_get::<String, _>("queue_name")
                            .map_err(Error::from_sqlx)?,
                    )?,
                    tasks_deleted: row.try_get("tasks_deleted").map_err(Error::from_sqlx)?,
                    events_deleted: row.try_get("events_deleted").map_err(Error::from_sqlx)?,
                })
            })
            .collect()
    }

    /// Spawns a task with normalized raw data.
    async fn spawn_raw<P: Serialize>(
        &self,
        queue_name: QueueName,
        task_name: TaskName,
        params: P,
        options: SpawnOptions,
        default_max_attempts: Option<i32>,
    ) -> Result<SpawnResult> {
        let params = serde_json::to_value(params).map_err(Error::json)?;
        let options = options.to_json(default_max_attempts);
        let row = sqlx::query("SELECT task_id, run_id, attempt, created FROM absurd.spawn_task($1, $2, $3::jsonb, $4::jsonb)")
            .bind(queue_name.as_str())
            .bind(task_name.as_str())
            .bind(Json(params))
            .bind(Json(options))
            .fetch_one(&self.pool)
            .await
            .map_err(Error::from_sqlx)?;
        row_to_spawn_result(row)
    }
}

/// Builds a [`Client`].
#[derive(Debug)]
pub struct ClientBuilder {
    /// Holds the PostgreSQL pool.
    pool: PgPool,
    /// Carries the default queue.
    default_queue: Option<QueueName>,
    /// Carries the default maximum attempts.
    default_max_attempts: Option<i32>,
}

impl ClientBuilder {
    /// Sets the default queue.
    pub fn default_queue(&mut self, queue_name: impl AsRef<str>) -> Result<&mut Self> {
        self.default_queue = Some(QueueName::from_str(queue_name.as_ref())?);
        Ok(self)
    }

    /// Sets default maximum attempts.
    pub fn default_max_attempts(&mut self, max_attempts: i32) -> &mut Self {
        self.default_max_attempts = Some(max_attempts);
        self
    }

    /// Builds the client.
    pub fn build(&self) -> Client {
        Client {
            pool: self.pool.clone(),
            default_queue: self.default_queue.clone(),
            default_max_attempts: self.default_max_attempts,
        }
    }
}

/// Builds a typed task spawn call.
#[derive(Debug)]
pub struct SpawnBuilder<P, R> {
    /// Holds the client.
    client: Client,
    /// Names the task.
    task_name: TaskName,
    /// Carries the target queue.
    queue_name: Option<QueueName>,
    /// Carries the default maximum attempts.
    default_max_attempts: Option<i32>,
    /// Carries the default cancellation policy.
    default_cancellation: Option<CancellationPolicy>,
    /// Carries task parameters.
    params: P,
    /// Carries spawn options.
    options: SpawnOptions,
    /// Carries the result marker.
    marker: PhantomData<R>,
}

impl<P, R> SpawnBuilder<P, R>
where
    P: Serialize,
{
    /// Sets the target queue.
    pub fn queue(mut self, queue_name: impl AsRef<str>) -> Result<Self> {
        self.queue_name = Some(QueueName::from_str(queue_name.as_ref())?);
        Ok(self)
    }

    /// Sets maximum attempts.
    pub fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.options.max_attempts = Some(max_attempts);
        self
    }

    /// Sets retry behavior.
    pub fn retry_strategy(mut self, retry_strategy: RetryStrategy) -> Self {
        self.options.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets application headers.
    pub fn headers<T: Serialize>(mut self, headers: T) -> Result<Self> {
        self.options.headers = Some(serde_json::to_value(headers).map_err(Error::json)?);
        Ok(self)
    }

    /// Sets cancellation behavior.
    pub fn cancellation(mut self, cancellation: CancellationPolicy) -> Self {
        self.options.cancellation = Some(cancellation);
        self
    }

    /// Sets an idempotency key.
    pub fn idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.options.idempotency_key = Some(idempotency_key.into());
        self
    }

    /// Sends the spawn request.
    pub async fn send(mut self) -> Result<Spawned<R>> {
        let queue_name = match self
            .queue_name
            .take()
            .or_else(|| self.client.default_queue.clone())
        {
            Some(queue_name) => queue_name,
            None => {
                return Err(Error::InvalidName {
                    kind: "queue",
                    value: String::new(),
                    reason: "must be provided",
                });
            }
        };
        self.options.queue_name = Some(queue_name.clone());
        if self.options.cancellation.is_none() {
            self.options.cancellation = self.default_cancellation.take();
        }
        let result = self
            .client
            .spawn_raw(
                queue_name,
                self.task_name,
                self.params,
                self.options,
                self.default_max_attempts,
            )
            .await?;
        Ok(Spawned::new(result))
    }
}

impl<R> Spawned<R>
where
    R: DeserializeOwned,
{
    /// Awaits and decodes this spawned task's result.
    pub async fn await_result(
        &self,
        client: &Client,
        queue_name: impl AsRef<str>,
        timeout: Option<Duration>,
    ) -> Result<Option<R>> {
        client
            .await_typed_task_result(queue_name, self.result.task_id.as_uuid(), timeout)
            .await
    }
}

/// Describes a raw event wait result.
#[derive(Clone, Debug, PartialEq)]
pub struct AwaitEventRaw {
    /// Indicates whether the run has been suspended.
    pub should_suspend: bool,
    /// Carries the event payload when available.
    pub payload: Option<Value>,
}

/// Converts a row into a spawn result.
fn row_to_spawn_result(row: sqlx::postgres::PgRow) -> Result<SpawnResult> {
    Ok(SpawnResult {
        task_id: TaskId::from(
            row.try_get::<Uuid, _>("task_id")
                .map_err(Error::from_sqlx)?,
        ),
        run_id: RunId::from(row.try_get::<Uuid, _>("run_id").map_err(Error::from_sqlx)?),
        attempt: row.try_get("attempt").map_err(Error::from_sqlx)?,
        created: row.try_get("created").map_err(Error::from_sqlx)?,
    })
}

/// Converts a duration to Absurd seconds.
fn seconds_i32(duration: Duration) -> Result<i32> {
    i32::try_from(duration.as_secs()).map_err(Error::duration_out_of_range)
}
