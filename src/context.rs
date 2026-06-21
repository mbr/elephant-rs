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

use crate::{
    client::Client,
    error::{Error, Result},
    run::ClaimedRun,
    types::{QueueName, RunId, StepName, TaskId, TaskName},
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
        if let Some(value) = self.checkpoint_value(step_name.as_str())? {
            return serde_json::from_value(value).map_err(Error::json);
        }
        let value = run().await?;
        self.client
            .set_checkpoint(
                self.metadata.queue_name.as_str(),
                self.metadata.task_id.as_uuid(),
                step_name.as_str(),
                &value,
                self.metadata.run_id.as_uuid(),
                self.checkpoint_extend_by,
            )
            .await?;
        self.insert_checkpoint(
            step_name.as_str(),
            serde_json::to_value(&value).map_err(Error::json)?,
        )?;
        Ok(value)
    }

    /// Begins a decomposed durable step.
    pub async fn begin_step<T>(&self, step_name: impl AsRef<str>) -> Result<Step<T, Pending>>
    where
        T: DeserializeOwned + Serialize,
    {
        let step_name = step_name.as_ref().parse::<StepName>()?;
        if let Some(value) = self.checkpoint_value(step_name.as_str())? {
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
        self.client
            .schedule_run(
                self.metadata.queue_name.as_str(),
                self.metadata.run_id.as_uuid(),
                wake_at,
            )
            .await?;
        Err(Error::Suspended)
    }

    /// Schedules the active run to resume after a duration.
    pub async fn sleep_for(&self, duration: Duration) -> Result<()> {
        let wake_at = Timestamp::now().saturating_add(duration)?;
        self.sleep_until(wake_at).await
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
        let event_name = event_name.as_ref();
        let step_name = format!("event:{event_name}");
        let raw = self
            .client
            .await_event_raw(
                self.metadata.queue_name.as_str(),
                self.metadata.task_id.as_uuid(),
                self.metadata.run_id.as_uuid(),
                &step_name,
                event_name,
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

    /// Fetches a cached checkpoint payload.
    fn checkpoint_value(&self, step_name: &str) -> Result<Option<Value>> {
        let guard = self
            .checkpoints
            .lock()
            .expect("checkpoint cache lock poisoned");
        Ok(guard.get(step_name).cloned())
    }

    /// Inserts a cached checkpoint payload.
    fn insert_checkpoint(&self, step_name: &str, value: Value) -> Result<()> {
        let mut guard = self
            .checkpoints
            .lock()
            .expect("checkpoint cache lock poisoned");
        guard.insert(step_name.to_string(), value);
        Ok(())
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
pub enum Step<T, S> {
    /// Carries a completed step.
    Done(DoneStep<T>),
    /// Carries a pending step.
    Pending(PendingStep<T>),
    /// Carries the typestate marker.
    Marker(PhantomData<S>),
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
        self.context
            .client
            .set_checkpoint(
                self.context.metadata.queue_name.as_str(),
                self.context.metadata.task_id.as_uuid(),
                self.step_name.as_str(),
                &value,
                self.context.metadata.run_id.as_uuid(),
                self.context.checkpoint_extend_by,
            )
            .await?;
        self.context.insert_checkpoint(
            self.step_name.as_str(),
            serde_json::to_value(&value).map_err(Error::json)?,
        )?;
        Ok(value)
    }
}
