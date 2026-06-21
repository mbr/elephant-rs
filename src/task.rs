//! Typed task definitions and routing.

use std::{
    collections::HashMap,
    error,
    future::Future,
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::Duration,
};

use futures::{FutureExt, future::BoxFuture};
use rand::Rng;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    context::TaskContext,
    error::{Error, FailureReason, Result},
    run::RunLease,
    types::{CancellationPolicy, QueueName, TaskName},
};

/// Represents a typed task definition.
pub struct Task<P, R> {
    /// Holds the erased task implementation.
    erased: Arc<ErasedTask>,
    /// Carries the parameter marker.
    params: PhantomData<P>,
    /// Carries the result marker.
    result: PhantomData<R>,
}

impl<P, R> Clone for Task<P, R> {
    /// Clones a task definition.
    fn clone(&self) -> Self {
        Self {
            erased: Arc::clone(&self.erased),
            params: PhantomData,
            result: PhantomData,
        }
    }
}

impl<P, R> Task<P, R> {
    /// Creates a task builder.
    pub fn builder(name: impl AsRef<str>) -> Result<TaskBuilder<P, R, NoHandler>> {
        Ok(TaskBuilder {
            name: name.as_ref().parse()?,
            queue_name: None,
            default_max_attempts: None,
            default_cancellation: None,
            handler: NoHandler,
            params: PhantomData,
            result: PhantomData,
        })
    }

    /// Returns the task name.
    pub fn name(&self) -> &TaskName {
        &self.erased.name
    }

    /// Returns the task queue override.
    pub fn queue_name(&self) -> Option<&QueueName> {
        self.erased.queue_name.as_ref()
    }

    /// Returns the default maximum attempts.
    pub fn default_max_attempts(&self) -> Option<i32> {
        self.erased.default_max_attempts
    }

    /// Returns the default cancellation policy.
    pub fn default_cancellation(&self) -> Option<&CancellationPolicy> {
        self.erased.default_cancellation.as_ref()
    }
}

/// Builds a typed task definition.
pub struct TaskBuilder<P, R, H> {
    /// Names the task.
    name: TaskName,
    /// Carries the task queue override.
    queue_name: Option<QueueName>,
    /// Carries default maximum attempts.
    default_max_attempts: Option<i32>,
    /// Carries default cancellation behavior.
    default_cancellation: Option<CancellationPolicy>,
    /// Holds the task handler.
    handler: H,
    /// Carries the parameter marker.
    params: PhantomData<P>,
    /// Carries the result marker.
    result: PhantomData<R>,
}

impl<P, R, H> TaskBuilder<P, R, H> {
    /// Sets the task queue override.
    pub fn queue(mut self, queue_name: impl AsRef<str>) -> Result<Self> {
        self.queue_name = Some(queue_name.as_ref().parse()?);
        Ok(self)
    }

    /// Sets default maximum attempts.
    pub fn default_max_attempts(mut self, max_attempts: i32) -> Self {
        self.default_max_attempts = Some(max_attempts);
        self
    }

    /// Sets default cancellation behavior.
    pub fn default_cancellation(mut self, cancellation: CancellationPolicy) -> Self {
        self.default_cancellation = Some(cancellation);
        self
    }
}

impl<P, R> TaskBuilder<P, R, NoHandler> {
    /// Sets the typed task handler.
    pub fn handler<F, Fut>(self, handler: F) -> TaskBuilder<P, R, F>
    where
        F: Fn(TaskContext, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        TaskBuilder {
            name: self.name,
            queue_name: self.queue_name,
            default_max_attempts: self.default_max_attempts,
            default_cancellation: self.default_cancellation,
            handler,
            params: PhantomData,
            result: PhantomData,
        }
    }
}

impl<P, R, F, Fut> TaskBuilder<P, R, F>
where
    P: DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + Sync + 'static,
    F: Fn(TaskContext, P) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
{
    /// Builds the task definition.
    pub fn build(self) -> Task<P, R> {
        let handler =
            move |context: TaskContext, params: Value| -> BoxFuture<'static, Result<Value>> {
                let params = match serde_json::from_value(params).map_err(Error::json) {
                    Ok(params) => params,
                    Err(error) => return Box::pin(async move { Err(error) }),
                };
                let future = catch_unwind(AssertUnwindSafe(|| (self.handler)(context, params)));
                Box::pin(async move {
                    let Ok(future) = future else {
                        return Err(Error::HandlerPanicked);
                    };
                    let result = AssertUnwindSafe(future).catch_unwind().await;
                    match result {
                        Ok(value) => serde_json::to_value(value?).map_err(Error::json),
                        Err(_) => Err(Error::HandlerPanicked),
                    }
                })
            };
        Task {
            erased: Arc::new(ErasedTask {
                name: self.name,
                queue_name: self.queue_name,
                default_max_attempts: self.default_max_attempts,
                default_cancellation: self.default_cancellation,
                handler: Arc::new(handler),
            }),
            params: PhantomData,
            result: PhantomData,
        }
    }
}

/// Marks a builder without a handler.
#[derive(Clone, Copy, Debug)]
pub struct NoHandler;

/// Routes claimed runs to registered tasks.
#[derive(Clone, Default)]
pub struct Router {
    /// Holds tasks by name.
    tasks: HashMap<TaskName, Arc<ErasedTask>>,
    /// Carries the unknown-task deferral range.
    unknown_task_delay: Duration,
}

impl Router {
    /// Creates an empty router.
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            unknown_task_delay: Duration::from_secs(5),
        }
    }

    /// Registers a typed task.
    pub fn task<P, R>(mut self, task: Task<P, R>) -> Result<Self> {
        if self.tasks.contains_key(task.name()) {
            return Err(Error::InvalidName {
                kind: "task",
                value: task.name().to_string(),
                reason: "is already registered",
            });
        }
        self.tasks
            .insert(task.name().clone(), Arc::clone(&task.erased));
        Ok(self)
    }

    /// Sets the unknown-task deferral duration.
    pub fn unknown_task_delay(mut self, delay: Duration) -> Self {
        self.unknown_task_delay = delay;
        self
    }

    /// Dispatches a claimed run.
    pub async fn dispatch(&self, lease: RunLease) -> Result<()> {
        let client = lease.client().clone();
        let run = lease.claimed_run().clone();
        let Some(task) = self.tasks.get(&run.task_name).cloned() else {
            return self.defer_unknown(lease).await;
        };
        let checkpoints = client
            .get_checkpoints(
                run.queue_name.as_str(),
                run.task_id.as_uuid(),
                run.run_id.as_uuid(),
            )
            .await?
            .into_iter()
            .collect();
        let context = TaskContext::new(client, &run, checkpoints);
        match task.handle(context, run.params).await {
            Ok(result) => complete_lease(lease, result).await,
            Err(Error::Suspended | Error::Cancelled | Error::RunAlreadyFailed) => {
                lease.forget();
                Ok(())
            }
            Err(error) => {
                let reason = failure_reason(&error);
                fail_lease(lease, reason).await
            }
        }
    }

    /// Defers a run with an unknown task name.
    async fn defer_unknown(&self, lease: RunLease) -> Result<()> {
        let jitter = jitter_duration(self.unknown_task_delay);
        let wake_at = jiff::Timestamp::now().saturating_add(jitter)?;
        match lease.sleep_until(wake_at).await {
            Ok(()) | Err(Error::Cancelled | Error::RunAlreadyFailed) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// Stores a type-erased task handler.
pub struct ErasedTask {
    /// Names the task.
    name: TaskName,
    /// Carries the task queue override.
    queue_name: Option<QueueName>,
    /// Carries default maximum attempts.
    default_max_attempts: Option<i32>,
    /// Carries default cancellation behavior.
    default_cancellation: Option<CancellationPolicy>,
    /// Holds the erased handler.
    handler: Arc<dyn Fn(TaskContext, Value) -> BoxFuture<'static, Result<Value>> + Send + Sync>,
}

impl ErasedTask {
    /// Invokes the erased task handler.
    fn handle(&self, context: TaskContext, params: Value) -> BoxFuture<'static, Result<Value>> {
        (self.handler)(context, params)
    }
}

/// Completes a lease while treating terminal races as control flow.
async fn complete_lease(lease: RunLease, result: Value) -> Result<()> {
    match lease.complete(result).await {
        Ok(()) | Err(Error::Cancelled | Error::RunAlreadyFailed) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Fails a lease while treating terminal races as control flow.
async fn fail_lease(lease: RunLease, reason: FailureReason) -> Result<()> {
    match lease.fail(reason).await {
        Ok(()) | Err(Error::Cancelled | Error::RunAlreadyFailed) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Returns a jitter duration up to the provided maximum.
fn jitter_duration(maximum: Duration) -> Duration {
    let millis = maximum.as_millis();
    if millis == 0 {
        return Duration::ZERO;
    }
    let upper = u64::try_from(millis).unwrap_or(u64::MAX);
    Duration::from_millis(rand::rng().random_range(0..=upper))
}

/// Converts a handler error into an Absurd failure payload.
fn failure_reason(error: &Error) -> FailureReason {
    match error {
        Error::Handler { source } => {
            FailureReason::from_error_named("handler_error", source.as_ref())
        }
        Error::HandlerPanicked => FailureReason::panic(),
        _ => FailureReason::from_error_named(
            "room_error",
            error as &(dyn error::Error + Send + Sync),
        ),
    }
}
