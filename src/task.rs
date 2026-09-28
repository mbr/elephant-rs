//! Typed task contracts, handler registration, and routing.

use std::{
    collections::HashMap, future::Future, marker::PhantomData, ops::Deref, panic::AssertUnwindSafe,
    sync::Arc, time::Duration,
};

use futures::{FutureExt, future::BoxFuture};
use rand::Rng;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::{
    context::TaskContext,
    error::{Error, Result},
    run::{ExecutionOptions, LeaseRenewal, RunLease},
    types::{CancellationPolicy, QueueName, TaskName},
};

/// Describes a typed task independently of its executable implementation.
pub struct Task<P, R> {
    /// Holds the shared contract.
    definition: Arc<TaskDefinition>,
    /// Identifies the wire parameter and result types without owning them.
    marker: PhantomData<fn(P) -> R>,
}

impl<P, R> Clone for Task<P, R> {
    /// Clones the task contract without cloning parameters or results.
    fn clone(&self) -> Self {
        Self {
            definition: Arc::clone(&self.definition),
            marker: PhantomData,
        }
    }
}

impl<P, R> Task<P, R> {
    /// Creates a task contract builder.
    pub fn builder(name: impl AsRef<str>) -> Result<TaskBuilder<P, R, NoHandler>> {
        Ok(TaskBuilder {
            definition: TaskDefinition {
                name: name.as_ref().parse()?,
                queue_name: None,
                default_max_attempts: None,
                default_cancellation: None,
            },
            handler: NoHandler,
            marker: PhantomData,
        })
    }

    /// Returns the task name.
    pub fn name(&self) -> &TaskName {
        &self.definition.name
    }

    /// Returns the task queue override.
    pub fn queue_name(&self) -> Option<&QueueName> {
        self.definition.queue_name.as_ref()
    }

    /// Returns the default maximum attempts.
    pub fn default_max_attempts(&self) -> Option<i32> {
        self.definition.default_max_attempts
    }

    /// Returns the default cancellation policy.
    pub fn default_cancellation(&self) -> Option<&CancellationPolicy> {
        self.definition.default_cancellation.as_ref()
    }
}

impl<P, R> Task<P, R>
where
    P: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
{
    /// Binds an executable handler without changing the shared task contract.
    pub fn handler<F, Fut>(&self, handler: F) -> TaskRegistration<P, R>
    where
        F: Fn(TaskContext, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let erased = move |context: TaskContext, params: Value| -> TaskExecution {
            let handler = Arc::clone(&handler);
            async move {
                let params = serde_json::from_value(params).map_err(Error::json)?;
                let value = handler(context, params).await?;
                serde_json::to_value(value).map_err(Error::json)
            }
            .boxed()
        };
        TaskRegistration {
            task: self.clone(),
            erased: Arc::new(ErasedTask {
                handler: Arc::new(erased),
            }),
        }
    }
}

/// Carries routing and spawning defaults shared by producers and workers.
struct TaskDefinition {
    /// Names the task.
    name: TaskName,
    /// Carries the queue override.
    queue_name: Option<QueueName>,
    /// Carries the retry limit.
    default_max_attempts: Option<i32>,
    /// Carries cancellation defaults.
    default_cancellation: Option<CancellationPolicy>,
}

/// Builds a typed contract, optionally with a local handler.
pub struct TaskBuilder<P, R, H> {
    /// Carries the shared contract.
    definition: TaskDefinition,
    /// Carries the optional executable handler.
    handler: H,
    /// Identifies the wire types.
    marker: PhantomData<fn(P) -> R>,
}

impl<P, R, H> TaskBuilder<P, R, H> {
    /// Sets the task queue override.
    pub fn queue(mut self, queue_name: impl AsRef<str>) -> Result<Self> {
        self.definition.queue_name = Some(queue_name.as_ref().parse()?);
        Ok(self)
    }

    /// Sets default maximum attempts.
    pub fn default_max_attempts(mut self, max_attempts: i32) -> Self {
        self.definition.default_max_attempts = Some(max_attempts);
        self
    }

    /// Sets default cancellation behavior.
    pub fn default_cancellation(mut self, cancellation: CancellationPolicy) -> Self {
        self.definition.default_cancellation = Some(cancellation);
        self
    }
}

impl<P, R> TaskBuilder<P, R, NoHandler> {
    /// Builds a producer-ready contract without any handler dependency.
    pub fn build(self) -> Task<P, R> {
        Task {
            definition: Arc::new(self.definition),
            marker: PhantomData,
        }
    }

    /// Adds a handler while building a contract and registration together.
    pub fn handler<F, Fut>(self, handler: F) -> TaskBuilder<P, R, F>
    where
        F: Fn(TaskContext, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        TaskBuilder {
            definition: self.definition,
            handler,
            marker: PhantomData,
        }
    }
}

impl<P, R, F, Fut> TaskBuilder<P, R, F>
where
    P: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    F: Fn(TaskContext, P) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
{
    /// Builds a local registration together with its task contract.
    pub fn build(self) -> TaskRegistration<P, R> {
        Task {
            definition: Arc::new(self.definition),
            marker: PhantomData,
        }
        .handler(self.handler)
    }
}

/// Marks a contract builder without a handler.
#[derive(Clone, Copy, Debug)]
pub struct NoHandler;

/// Couples a shared task contract to one local executable implementation.
pub struct TaskRegistration<P, R> {
    /// Holds the producer-facing contract.
    task: Task<P, R>,
    /// Holds the erased handler.
    erased: Arc<ErasedTask>,
}

impl<P, R> Clone for TaskRegistration<P, R> {
    /// Clones the registration without cloning parameters or results.
    fn clone(&self) -> Self {
        Self {
            task: self.task.clone(),
            erased: Arc::clone(&self.erased),
        }
    }
}

impl<P, R> Deref for TaskRegistration<P, R> {
    /// Exposes the registration's shared task contract for typed spawning.
    type Target = Task<P, R>;

    fn deref(&self) -> &Self::Target {
        &self.task
    }
}

/// Represents one erased handler invocation, including its control-flow errors.
pub type TaskExecution = BoxFuture<'static, Result<Value>>;

/// Wraps an invocation with application-level execution context.
type ExecutionWrapper = dyn Fn(TaskContext, TaskExecution) -> TaskExecution + Send + Sync;

/// Routes claimed runs to registered handlers.
#[derive(Clone)]
pub struct Router {
    /// Shares the immutable dispatch table between active runs.
    tasks: Arc<HashMap<TaskName, Arc<ErasedTask>>>,
    /// Installs application context around all registered handlers.
    execution_wrapper: Option<Arc<ExecutionWrapper>>,
    /// Carries the unknown-task deferral range.
    unknown_task_delay: Duration,
}

impl Default for Router {
    /// Creates an empty router with unknown-task deferral enabled.
    fn default() -> Self {
        Self {
            tasks: Arc::new(HashMap::new()),
            execution_wrapper: None,
            unknown_task_delay: Duration::from_secs(5),
        }
    }
}

impl Router {
    /// Creates an empty router.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a local handler for a typed contract.
    pub fn task<P, R>(mut self, task: TaskRegistration<P, R>) -> Result<Self> {
        if self.tasks.contains_key(task.name()) {
            return Err(Error::InvalidName {
                kind: "task",
                value: task.name().to_string(),
                reason: "is already registered",
            });
        }
        Arc::make_mut(&mut self.tasks).insert(task.name().clone(), task.erased);
        Ok(self)
    }

    /// Installs a wrapper around every handler, including convenience workers.
    ///
    /// Use metadata headers to establish application tracing or request context.
    /// The wrapper must preserve execution errors, including suspension and
    /// owning-run cancellation. Calling this again replaces the previous wrapper.
    pub fn wrap_execution<F, Fut>(mut self, wrapper: F) -> Self
    where
        F: Fn(TaskContext, TaskExecution) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.execution_wrapper = Some(Arc::new(move |context, execute| {
            wrapper(context, execute).boxed()
        }));
        self
    }

    /// Sets the unknown-task deferral duration.
    pub fn unknown_task_delay(mut self, delay: Duration) -> Self {
        self.unknown_task_delay = delay;
        self
    }

    /// Dispatches a claimed run without background renewal or a deadline.
    pub async fn dispatch(&self, lease: RunLease) -> Result<()> {
        self.dispatch_with(
            lease,
            ExecutionOptions {
                lease_renewal: LeaseRenewal::Disabled,
                ..ExecutionOptions::default()
            },
        )
        .await
    }

    /// Dispatches with the same supervision primitives used by workers.
    #[tracing::instrument(
        level = "error", skip_all,
        fields(queue = %lease.claimed_run().queue_name,
            task_id = %lease.claimed_run().task_id,
            run_id = %lease.claimed_run().run_id,
            task_name = %lease.claimed_run().task_name,
            attempt = lease.claimed_run().attempt)
    )]
    pub async fn dispatch_with(&self, lease: RunLease, options: ExecutionOptions) -> Result<()> {
        options.validate(lease.claimed_run().claim_timeout)?;
        let client = lease.client().clone();
        let run = lease.claimed_run().clone();
        let Some(task) = self.tasks.get(&run.task_name).cloned() else {
            return self.defer_unknown(lease).await;
        };
        let cancellation = CancellationToken::new();
        let context_cancellation = cancellation.clone();
        let activity = lease.activity();
        let wrapper = self.execution_wrapper.clone();
        let execute = async move {
            let checkpoints = client
                .get_checkpoints(
                    run.queue_name.as_str(),
                    run.task_id.as_uuid(),
                    run.run_id.as_uuid(),
                )
                .await?
                .into_iter()
                .collect();
            let context = TaskContext::new(client, &run, checkpoints)
                .with_supervision(context_cancellation, activity);
            let wrapped_context = context.clone();
            let execute = async move { (task.handler)(context, run.params).await }.boxed();
            match wrapper {
                Some(wrapper) => wrapper(wrapped_context, execute).await,
                None => execute.await,
            }
        };
        lease
            .run_supervised(
                async move {
                    AssertUnwindSafe(execute)
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|payload| Err(Error::handler_panicked(payload)))
                },
                options,
                cancellation,
            )
            .await
    }

    /// Defers a run with an unknown task name.
    async fn defer_unknown(&self, lease: RunLease) -> Result<()> {
        let jitter = jitter_duration(self.unknown_task_delay);
        match lease.sleep_for(jitter).await {
            Ok(()) | Err(Error::Cancelled | Error::RunAlreadyFailed) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// Stores a type-erased executable handler.
struct ErasedTask {
    /// Invokes the typed handler through JSON boundaries.
    handler: Arc<dyn Fn(TaskContext, Value) -> BoxFuture<'static, Result<Value>> + Send + Sync>,
}

/// Returns positive jitter at database precision unless deferral is disabled.
fn jitter_duration(maximum: Duration) -> Duration {
    if maximum.is_zero() {
        return Duration::ZERO;
    }
    let upper = u64::try_from(maximum.as_micros())
        .unwrap_or(u64::MAX)
        .max(1);
    Duration::from_micros(rand::rng().random_range(1..=upper))
}
