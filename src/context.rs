//! Durable task context APIs.

use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use jiff::Timestamp;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::time::{Instant, interval_at};
use tokio_util::sync::CancellationToken;

use crate::{
    client::Client,
    error::{Error, Result},
    run::{ClaimedRun, ExecutionActivity},
    types::{EventName, QueueName, RunId, Spawned, StepName, TaskId, TaskName, TaskResultSnapshot},
};

/// Carries metadata for an active task run.
#[derive(Clone, Debug)]
pub struct TaskMetadata {
    /// Identifies the queue.
    pub queue_name: QueueName,
    /// Identifies the task.
    pub task_id: TaskId,
    /// Identifies the run.
    pub run_id: RunId,
    /// Carries the attempt number.
    pub attempt: i32,
    /// Names the task.
    pub task_name: TaskName,
    /// Carries application headers without interpreting or logging their content.
    pub headers: Option<Value>,
}

/// Provides durable capabilities to a task handler.
#[derive(Clone, Debug)]
pub struct TaskContext {
    /// Holds the client.
    client: Client,
    /// Carries task metadata.
    metadata: Arc<TaskMetadata>,
    /// Carries the original lease duration for durable polling waits.
    claim_timeout: Duration,
    /// Signals that supervised execution is stopping.
    cancellation: CancellationToken,
    /// Reports successful application progress to the dispatch supervisor.
    activity: Option<ExecutionActivity>,
    /// Caches visible checkpoint payloads.
    checkpoints: Arc<Mutex<HashMap<String, Value>>>,
    /// Counts each base name's occurrences within this execution.
    checkpoint_counts: Arc<Mutex<HashMap<String, usize>>>,
    /// Carries an event timeout to deliver once across context clones.
    pending_event_timeout: Arc<Mutex<Option<String>>>,
    /// Carries the claim extension used by checkpoint writes.
    checkpoint_extend_by: Option<Duration>,
}

impl TaskContext {
    /// Creates a context from a claimed run and checkpoint payloads.
    pub fn new(client: Client, run: &ClaimedRun, checkpoints: HashMap<String, Value>) -> Self {
        Self {
            client,
            metadata: Arc::new(TaskMetadata {
                queue_name: run.queue_name.clone(),
                task_id: run.task_id,
                run_id: run.run_id,
                attempt: run.attempt,
                task_name: run.task_name.clone(),
                headers: run.headers.clone(),
            }),
            claim_timeout: run.claim_timeout,
            cancellation: CancellationToken::new(),
            activity: None,
            checkpoints: Arc::new(Mutex::new(checkpoints)),
            checkpoint_counts: Arc::default(),
            pending_event_timeout: Arc::new(Mutex::new(
                run.wake_event
                    .clone()
                    .filter(|_| run.event_payload.is_none()),
            )),
            checkpoint_extend_by: Some(run.claim_timeout),
        }
    }

    /// Shares cancellation with the execution supervisor.
    pub(crate) fn with_supervision(
        mut self,
        cancellation: CancellationToken,
        activity: ExecutionActivity,
    ) -> Self {
        self.cancellation = cancellation;
        self.activity = Some(activity);
        self
    }

    /// Returns the signal used for cooperative execution cancellation.
    ///
    /// Cancelling this token requests local run failure, not database-wide task
    /// cancellation. Worker shutdown alone does not cancel it.
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Returns task metadata.
    pub fn metadata(&self) -> &TaskMetadata {
        &self.metadata
    }

    /// Returns the client associated with the active run.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Sets the claim extension used when writing checkpoints.
    pub fn with_checkpoint_extend_by(mut self, extend_by: Duration) -> Self {
        self.checkpoint_extend_by = Some(extend_by);
        self
    }

    /// Runs a durable step or returns its cached result.
    pub async fn step<T, Fut, Fun>(&self, step_name: impl AsRef<str>, run: Fun) -> Result<T>
    where
        T: DeserializeOwned + Serialize,
        Fut: Future<Output = Result<T>>,
        Fun: FnOnce() -> Fut,
    {
        match self.begin_step(step_name).await? {
            Step::Done(done) => Ok(done.into_value()),
            Step::Pending(pending) => pending.complete(run().await?).await,
        }
    }

    /// Begins a decomposed durable step.
    pub async fn begin_step<T>(&self, step_name: impl AsRef<str>) -> Result<Step<T>>
    where
        T: DeserializeOwned + Serialize,
    {
        let step_name = self.next_checkpoint_name(step_name.as_ref())?;
        if let Some(value) = self.checkpoint_value(step_name.as_str()) {
            let value = serde_json::from_value(value).map_err(Error::json)?;
            Ok(Step::Done(DoneStep {
                value,
                marker: PhantomData,
            }))
        } else {
            Ok(Step::Pending(PendingStep {
                context: self.clone(),
                step_name,
                marker: PhantomData,
            }))
        }
    }

    /// Schedules the active run to resume at an absolute time.
    pub async fn sleep_until(&self, wake_at: Timestamp) -> Result<()> {
        self.sleep_until_named("sleep", wake_at).await
    }

    /// Schedules the active run to resume at an absolute time under a name.
    pub async fn sleep_until_named(
        &self,
        step_name: impl AsRef<str>,
        wake_at: Timestamp,
    ) -> Result<()> {
        let step_name = self.next_checkpoint_name(step_name.as_ref())?;
        let wake_at = self.sleep_checkpoint(step_name.as_str(), wake_at).await?;
        self.suspend_until(wake_at).await
    }

    /// Schedules the active run to resume after a duration.
    pub async fn sleep_for(&self, duration: Duration) -> Result<()> {
        self.sleep_for_named("sleep", duration).await
    }

    /// Schedules the active run to resume after a duration under a name.
    pub async fn sleep_for_named(
        &self,
        step_name: impl AsRef<str>,
        duration: Duration,
    ) -> Result<()> {
        let step_name = self.next_checkpoint_name(step_name.as_ref())?;
        let wake_at = match self.checkpoint_value(step_name.as_str()) {
            Some(value) => serde_json::from_value(value).map_err(Error::json)?,
            None => {
                let wake_at = self.client.current_time().await?.saturating_add(duration)?;
                self.persist_sleep_checkpoint(step_name.as_str(), wake_at)
                    .await?;
                wake_at
            }
        };
        self.suspend_until(wake_at).await
    }

    /// Awaits an event or suspends the active run.
    pub async fn await_event<T>(&self, event_name: impl AsRef<str>) -> Result<T>
    where
        T: DeserializeOwned,
    {
        self.await_event_with_timeout(event_name, None).await
    }

    /// Awaits an event with an optional timeout.
    pub async fn await_event_with_timeout<T>(
        &self,
        event_name: impl AsRef<str>,
        timeout: Option<Duration>,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let event_name = event_name.as_ref().parse::<EventName>()?;
        let step_name = format!("$awaitEvent:{event_name}");
        self.await_event_named_with_timeout(step_name, event_name.as_str(), timeout)
            .await
    }

    /// Awaits an event under an explicit checkpoint name.
    pub async fn await_event_named<T>(
        &self,
        step_name: impl AsRef<str>,
        event_name: impl AsRef<str>,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        self.await_event_named_with_timeout(step_name, event_name, None)
            .await
    }

    /// Awaits an event under an explicit checkpoint name and timeout.
    pub async fn await_event_named_with_timeout<T>(
        &self,
        step_name: impl AsRef<str>,
        event_name: impl AsRef<str>,
        timeout: Option<Duration>,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let event_name = event_name.as_ref().parse::<EventName>()?;
        let step_name = self.next_checkpoint_name(step_name.as_ref())?;
        if let Some(payload) = self.checkpoint_value(step_name.as_str()) {
            return serde_json::from_value(payload).map_err(Error::json);
        }
        {
            let mut pending_timeout = self
                .pending_event_timeout
                .lock()
                .expect("event timeout lock poisoned");
            if pending_timeout.as_deref() == Some(event_name.as_str()) {
                pending_timeout.take();
                return Err(Error::EventTimeout);
            }
        }
        let raw = self
            .client
            .await_event_raw(
                self.metadata.queue_name.as_str(),
                self.metadata.task_id.as_uuid(),
                self.metadata.run_id.as_uuid(),
                step_name.as_str(),
                event_name.as_str(),
                timeout,
            )
            .await?;
        if raw.should_suspend {
            return Err(Error::Suspended);
        }
        let persisted = raw.payload.is_some();
        let payload = raw.payload.unwrap_or(Value::Null);
        self.insert_checkpoint(step_name.as_str(), payload.clone());
        if persisted {
            self.record_progress(None);
        }
        serde_json::from_value(payload).map_err(Error::json)
    }

    /// Emits an event in the active run's queue.
    pub async fn emit_event<T: Serialize>(
        &self,
        event_name: impl AsRef<str>,
        payload: T,
    ) -> Result<()> {
        self.client
            .emit_event(self.metadata.queue_name.as_str(), event_name, payload)
            .await
    }

    /// Extends the active claim.
    pub async fn heartbeat(&self, extend_by: Duration) -> Result<()> {
        self.client
            .extend_claim(
                self.metadata.queue_name.as_str(),
                self.metadata.run_id.as_uuid(),
                extend_by,
            )
            .await?;
        self.record_progress(Some(extend_by));
        Ok(())
    }

    /// Durably observes a spawned task result from a different queue.
    ///
    /// The terminal snapshot is checkpointed before decoding, including failed
    /// and cancelled results. Polling occupies a worker slot and renews the
    /// current claim; it does not suspend the run in PostgreSQL.
    pub async fn await_task_result<R>(
        &self,
        spawned: &Spawned<R>,
        timeout: Option<Duration>,
    ) -> Result<R>
    where
        R: DeserializeOwned,
    {
        self.await_task_result_by_id(
            spawned.queue_name.as_str(),
            spawned.result.task_id.as_uuid(),
            timeout,
        )
        .await
    }

    /// Durably observes a child result under an explicit checkpoint name.
    pub async fn await_task_result_named<R>(
        &self,
        step_name: impl AsRef<str>,
        spawned: &Spawned<R>,
        timeout: Option<Duration>,
    ) -> Result<R>
    where
        R: DeserializeOwned,
    {
        self.await_task_result_by_id_named(
            step_name,
            spawned.queue_name.as_str(),
            spawned.result.task_id.as_uuid(),
            timeout,
        )
        .await
    }

    /// Durably observes another queue's task using only its queue and task ID.
    ///
    /// Uses the same checkpoint name and terminal snapshot as
    /// [`Self::await_task_result`], without requiring spawn or run metadata.
    /// Polling occupies a worker slot and reports progress through heartbeats.
    /// Same-queue waits are rejected, including when replaying a cached result.
    pub async fn await_task_result_by_id<R>(
        &self,
        queue_name: impl AsRef<str>,
        task_id: uuid::Uuid,
        timeout: Option<Duration>,
    ) -> Result<R>
    where
        R: DeserializeOwned,
    {
        self.await_task_result_by_id_named(
            format!("$awaitTaskResult:{task_id}"),
            queue_name,
            task_id,
            timeout,
        )
        .await
    }

    /// Durably observes a task by identity under an explicit checkpoint name.
    ///
    /// Shares naming and snapshot semantics with [`Self::await_task_result_named`].
    /// The queue is validated and same-queue waits are rejected before replay.
    /// A terminal snapshot is checkpointed before decoding, including failures,
    /// cancellation, and JSON null; cached results survive child cleanup.
    pub async fn await_task_result_by_id_named<R>(
        &self,
        step_name: impl AsRef<str>,
        queue_name: impl AsRef<str>,
        task_id: uuid::Uuid,
        timeout: Option<Duration>,
    ) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let queue_name = queue_name.as_ref().parse::<QueueName>()?;
        if queue_name == self.metadata.queue_name {
            return Err(Error::SameQueueWait);
        }
        let snapshot: TaskResultSnapshot = self
            .step(step_name, || async {
                let wait = self
                    .client
                    .await_task_result(queue_name.as_str(), task_id, timeout);
                tokio::pin!(wait);
                let interval = (self.claim_timeout / 3).max(Duration::from_millis(1));
                let mut heartbeat = interval_at(Instant::now() + interval, interval);
                loop {
                    tokio::select! {
                        result = &mut wait => return result,
                        _ = heartbeat.tick() => self.heartbeat(self.claim_timeout).await?,
                    }
                }
            })
            .await?;
        snapshot.decode_completed(task_id.into())
    }

    /// Creates or reads a durable sleep checkpoint.
    async fn sleep_checkpoint(&self, step_name: &str, wake_at: Timestamp) -> Result<Timestamp> {
        if let Some(value) = self.checkpoint_value(step_name) {
            return serde_json::from_value(value).map_err(Error::json);
        }
        self.persist_sleep_checkpoint(step_name, wake_at).await?;
        Ok(wake_at)
    }

    /// Persists a durable sleep checkpoint.
    async fn persist_sleep_checkpoint(&self, step_name: &str, wake_at: Timestamp) -> Result<()> {
        let payload = serde_json::to_value(wake_at).map_err(Error::json)?;
        self.persist_checkpoint(step_name, payload).await
    }

    /// Persists and caches a checkpoint before reporting application progress.
    async fn persist_checkpoint(&self, step_name: &str, payload: Value) -> Result<()> {
        self.client
            .set_checkpoint(
                self.metadata.queue_name.as_str(),
                self.metadata.task_id.as_uuid(),
                step_name,
                &payload,
                self.metadata.run_id.as_uuid(),
                self.checkpoint_extend_by,
            )
            .await?;
        self.insert_checkpoint(step_name, payload);
        self.record_progress(self.checkpoint_extend_by);
        Ok(())
    }

    /// Suspends the active run until a database timestamp.
    async fn suspend_until(&self, wake_at: Timestamp) -> Result<()> {
        if self.client.current_time().await? >= wake_at {
            return Ok(());
        }
        self.client
            .schedule_run(
                self.metadata.queue_name.as_str(),
                self.metadata.run_id.as_uuid(),
                wake_at,
            )
            .await?;
        Err(Error::Suspended)
    }

    /// Allocates the next occurrence of a base checkpoint name.
    fn next_checkpoint_name(&self, name: &str) -> Result<StepName> {
        let name = name.parse::<StepName>()?;
        let mut counts = self
            .checkpoint_counts
            .lock()
            .expect("checkpoint counter lock poisoned");
        let count = counts.entry(name.to_string()).or_default();
        *count += 1;
        if *count == 1 {
            Ok(name)
        } else {
            format!("{name}#{count}").parse()
        }
    }

    /// Fetches a cached checkpoint payload.
    fn checkpoint_value(&self, step_name: &str) -> Option<Value> {
        let guard = self
            .checkpoints
            .lock()
            .expect("checkpoint cache lock poisoned");
        guard.get(step_name).cloned()
    }

    /// Inserts a cached checkpoint payload.
    fn insert_checkpoint(&self, step_name: &str, value: Value) {
        let mut guard = self
            .checkpoints
            .lock()
            .expect("checkpoint cache lock poisoned");
        guard.insert(step_name.to_string(), value);
    }

    /// Notifies supervision after successful writes or explicit heartbeats.
    fn record_progress(&self, extend_by: Option<Duration>) {
        if let Some(activity) = &self.activity {
            activity.record(extend_by);
        }
    }
}

/// Marks a pending step.
#[derive(Debug)]
pub struct Pending;

/// Marks a completed step.
#[derive(Debug)]
pub struct Done;

/// Represents a durable step state.
#[derive(Debug)]
pub enum Step<T> {
    /// Carries a completed step.
    Done(DoneStep<T>),
    /// Carries a pending step.
    Pending(PendingStep<T>),
}

/// Represents a completed durable step.
#[derive(Debug)]
pub struct DoneStep<T> {
    /// Carries the cached value.
    value: T,
    /// Carries the typestate marker.
    marker: PhantomData<Done>,
}

impl<T> DoneStep<T> {
    /// Consumes the step into its cached value.
    pub fn into_value(self) -> T {
        self.value
    }
}

/// Represents a pending durable step.
#[derive(Debug)]
pub struct PendingStep<T> {
    /// Holds the task context.
    context: TaskContext,
    /// Names the checkpoint.
    step_name: StepName,
    /// Carries the value marker.
    marker: PhantomData<T>,
}

impl<T> PendingStep<T>
where
    T: Serialize,
{
    /// Completes a pending step with a value.
    pub async fn complete(self, value: T) -> Result<T> {
        let payload = serde_json::to_value(&value).map_err(Error::json)?;
        self.context
            .persist_checkpoint(self.step_name.as_str(), payload)
            .await?;
        Ok(value)
    }
}
