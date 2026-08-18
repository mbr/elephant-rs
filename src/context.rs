//! Durable task context APIs.

use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use jiff::Timestamp;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    client::Client,
    error::{Error, Result},
    run::ClaimedRun,
    types::{EventName, QueueName, RunId, Spawned, StepName, TaskId, TaskName},
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
}

/// Provides durable capabilities to a task handler.
#[derive(Clone, Debug)]
pub struct TaskContext {
    /// Holds the client.
    client: Client,
    /// Carries task metadata.
    metadata: TaskMetadata,
    /// Caches visible checkpoint payloads.
    checkpoints: Arc<Mutex<HashMap<String, Value>>>,
    /// Carries the claim extension used by checkpoint writes.
    checkpoint_extend_by: Option<Duration>,
}

impl TaskContext {
    /// Creates a context from a claimed run and checkpoint payloads.
    pub fn new(client: Client, run: &ClaimedRun, checkpoints: HashMap<String, Value>) -> Self {
        Self {
            client,
            metadata: TaskMetadata {
                queue_name: run.queue_name.clone(),
                task_id: run.task_id,
                run_id: run.run_id,
                attempt: run.attempt,
                task_name: run.task_name.clone(),
            },
            checkpoints: Arc::new(Mutex::new(checkpoints)),
            checkpoint_extend_by: None,
        }
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
        let step_name = step_name.as_ref().parse::<StepName>()?;
        if let Some(value) = self.checkpoint_value(step_name.as_str()) {
            return serde_json::from_value(value).map_err(Error::json);
        }
        let value = run().await?;
        let payload = serde_json::to_value(&value).map_err(Error::json)?;
        self.client
            .set_checkpoint(
                self.metadata.queue_name.as_str(),
                self.metadata.task_id.as_uuid(),
                step_name.as_str(),
                &payload,
                self.metadata.run_id.as_uuid(),
                self.checkpoint_extend_by,
            )
            .await?;
        self.insert_checkpoint(step_name.as_str(), payload);
        Ok(value)
    }

    /// Begins a decomposed durable step.
    pub async fn begin_step<T>(&self, step_name: impl AsRef<str>) -> Result<Step<T>>
    where
        T: DeserializeOwned + Serialize,
    {
        let step_name = step_name.as_ref().parse::<StepName>()?;
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
        let step_name = step_name.as_ref().parse::<StepName>()?;
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
        let step_name = step_name.as_ref().parse::<StepName>()?;
        let wake_at = match self.checkpoint_value(step_name.as_str()) {
            Some(value) => {
                let checkpoint: SleepCheckpoint =
                    serde_json::from_value(value).map_err(Error::json)?;
                checkpoint.wake_at
            }
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
        let step_name = format!("event:{event_name}");
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
        let step_name = step_name.as_ref().parse::<StepName>()?;
        let event_name = event_name.as_ref().parse::<EventName>()?;
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
        let payload = match raw.payload {
            Some(payload) => payload,
            None => return Err(Error::EventTimeout),
        };
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
            .await
    }

    /// Awaits a spawned task result from a different queue.
    pub async fn await_task_result<R>(
        &self,
        spawned: &Spawned<R>,
        timeout: Option<Duration>,
    ) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let queue_name = &spawned.queue_name;
        if queue_name == &self.metadata.queue_name {
            return Err(Error::SameQueueWait);
        }
        self.client
            .await_typed_task_result(
                queue_name.as_str(),
                spawned.result.task_id.as_uuid(),
                timeout,
            )
            .await
    }

    /// Creates or reads a durable sleep checkpoint.
    async fn sleep_checkpoint(&self, step_name: &str, wake_at: Timestamp) -> Result<Timestamp> {
        if let Some(value) = self.checkpoint_value(step_name) {
            let checkpoint: SleepCheckpoint = serde_json::from_value(value).map_err(Error::json)?;
            return Ok(checkpoint.wake_at);
        }
        self.persist_sleep_checkpoint(step_name, wake_at).await?;
        Ok(wake_at)
    }

    /// Persists a durable sleep checkpoint.
    async fn persist_sleep_checkpoint(&self, step_name: &str, wake_at: Timestamp) -> Result<()> {
        let checkpoint = SleepCheckpoint { wake_at };
        let payload = serde_json::to_value(checkpoint).map_err(Error::json)?;
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
}

/// Describes a durable sleep checkpoint.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct SleepCheckpoint {
    /// Carries the time when the sleep should be complete.
    wake_at: Timestamp,
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
            .client
            .set_checkpoint(
                self.context.metadata.queue_name.as_str(),
                self.context.metadata.task_id.as_uuid(),
                self.step_name.as_str(),
                &payload,
                self.context.metadata.run_id.as_uuid(),
                self.context.checkpoint_extend_by,
            )
            .await?;
        self.context
            .insert_checkpoint(self.step_name.as_str(), payload);
        Ok(value)
    }
}
