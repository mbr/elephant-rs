//! Claimed run primitives.

use std::{
    future::{Future, pending},
    time::Duration,
};

use jiff::Timestamp;
use serde::Serialize;
use serde_json::Value;
use tokio::time::{Instant, sleep, sleep_until, timeout, timeout_at};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    client::Client,
    error::{Error, FailureReason, Result},
    types::{QueueName, RunId, TaskId, TaskName},
};

/// Defines the default claim duration for worker and lease configuration.
pub(crate) const DEFAULT_CLAIM_TIMEOUT: Duration = Duration::from_secs(30);

/// Configures background claim renewal.
#[derive(Clone, Debug)]
pub struct LeaseWatchdogOptions {
    /// Configures the delay between successful renewals.
    pub interval: Duration,
    /// Configures the lease duration requested by each renewal.
    pub extend_by: Duration,
}

impl LeaseWatchdogOptions {
    /// Derives a renewal schedule from a claim duration.
    pub fn for_claim_timeout(claim_timeout: Duration) -> Self {
        Self {
            interval: (claim_timeout / 3).max(Duration::from_millis(1)),
            extend_by: claim_timeout,
        }
    }
}

impl Default for LeaseWatchdogOptions {
    /// Uses the default worker claim duration.
    fn default() -> Self {
        Self::for_claim_timeout(DEFAULT_CLAIM_TIMEOUT)
    }
}

/// Selects how an execution keeps its database claim alive.
#[derive(Clone, Debug, Default)]
pub enum LeaseRenewal {
    /// Leaves renewal to explicit heartbeats and checkpoint writes.
    Disabled,
    /// Derives renewal timing from the actual claimed run.
    #[default]
    Automatic,
    /// Uses an application-selected renewal schedule.
    Custom(LeaseWatchdogOptions),
}

/// Configures per-run supervision independently of worker shutdown.
#[derive(Clone, Debug)]
pub struct ExecutionOptions {
    /// Configures background lease maintenance.
    pub lease_renewal: LeaseRenewal,
    /// Bounds one dispatch, including checkpoint loading and wrappers.
    pub timeout: Option<Duration>,
    /// Allows cooperative cleanup before dropping an interrupted future.
    pub cancellation_grace: Duration,
}

impl Default for ExecutionOptions {
    /// Enables automatic renewal without imposing an execution deadline.
    fn default() -> Self {
        Self {
            lease_renewal: LeaseRenewal::Automatic,
            timeout: None,
            cancellation_grace: Duration::from_secs(1),
        }
    }
}

impl ExecutionOptions {
    /// Rejects renewal settings that cannot maintain a lease.
    pub(crate) fn validate(&self, claim_timeout: Duration) -> Result<()> {
        if claim_timeout.is_zero() {
            return Err(Error::InvalidExecutionOptions {
                reason: "claim duration must be positive",
            });
        }
        if let LeaseRenewal::Custom(options) = &self.lease_renewal
            && (options.interval.is_zero()
                || options.interval >= claim_timeout
                || options.interval >= options.extend_by)
        {
            return Err(Error::InvalidExecutionOptions {
                reason: "renewal interval must be positive and shorter than both lease durations",
            });
        }
        Ok(())
    }
}

/// Represents a task run claimed from Absurd.
#[derive(Clone, Debug)]
pub struct ClaimedRun {
    /// Identifies the queue containing the run.
    pub queue_name: QueueName,
    /// Identifies the run.
    pub run_id: RunId,
    /// Identifies the logical task.
    pub task_id: TaskId,
    /// Carries the attempt number.
    pub attempt: i32,
    /// Carries the effective lease duration requested when claiming this run.
    pub claim_timeout: Duration,
    /// Records the local request start as a conservative claim-age reference.
    pub claim_started_at: Instant,
    /// Names the task handler.
    pub task_name: TaskName,
    /// Carries the raw parameter payload.
    pub params: Value,
    /// Carries the raw header payload.
    pub headers: Option<Value>,
    /// Carries the event that woke the run.
    pub wake_event: Option<String>,
    /// Carries the event payload that woke the run.
    pub event_payload: Option<Value>,
}

/// Represents an active claimed run lease.
#[derive(Debug)]
#[must_use = "a run lease should be resolved, scheduled, or explicitly forgotten"]
pub struct RunLease {
    /// Holds the database client.
    client: Client,
    /// Holds the claimed run.
    run: Option<ClaimedRun>,
}

impl RunLease {
    /// Creates a run lease from a claimed run.
    pub fn new(client: Client, run: ClaimedRun) -> Self {
        Self {
            client,
            run: Some(run),
        }
    }

    /// Returns the client that owns the lease.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Returns the claimed run metadata.
    pub fn claimed_run(&self) -> &ClaimedRun {
        self.run.as_ref().expect("run lease must contain a run")
    }

    /// Consumes the lease and returns the claimed run.
    pub fn into_run(mut self) -> ClaimedRun {
        self.take_run()
    }

    /// Completes the run with a serialized result.
    pub async fn complete<T: Serialize>(mut self, result: T) -> Result<()> {
        let queue_name = self.claimed_run().queue_name.clone();
        let run_id = self.claimed_run().run_id;
        let outcome = self
            .client
            .complete_run(queue_name.as_str(), run_id.as_uuid(), result)
            .await;
        if is_resolved_outcome(&outcome) {
            self.run.take();
        }
        outcome
    }

    /// Fails the run with a serialized failure reason.
    pub async fn fail(mut self, reason: FailureReason) -> Result<()> {
        let queue_name = self.claimed_run().queue_name.clone();
        let run_id = self.claimed_run().run_id;
        let outcome = self
            .client
            .fail_run(queue_name.as_str(), run_id.as_uuid(), reason)
            .await;
        if is_resolved_outcome(&outcome) {
            self.run.take();
        }
        outcome
    }

    /// Runs work and resolves the lease from its outcome.
    pub async fn run<T, Fut>(self, future: Fut) -> Result<()>
    where
        T: Serialize,
        Fut: Future<Output = Result<T>>,
    {
        let outcome = match future.await {
            Ok(result) => self.complete(result).await,
            Err(Error::Suspended | Error::Cancelled | Error::RunAlreadyFailed) => {
                self.forget();
                return Ok(());
            }
            Err(error) => {
                let reason = failure_reason(&error);
                self.fail(reason).await
            }
        };
        ignore_terminal_error(outcome)
    }

    /// Supervises work with renewal, a deadline, and cooperative cancellation.
    ///
    /// Cancellation signals cleanup through the supplied token, then drops the
    /// future after the grace period. Local deadlines/cancellation fail the run.
    /// Renewal errors abandon ownership and return infrastructure errors; known
    /// database terminal states are successful local resolution. Blocking code
    /// and side effects already sent externally cannot be interrupted reliably.
    pub async fn run_supervised<T, Fut>(
        self,
        future: Fut,
        options: ExecutionOptions,
        cancellation: CancellationToken,
    ) -> Result<()>
    where
        T: Serialize,
        Fut: Future<Output = Result<T>>,
    {
        options.validate(self.claimed_run().claim_timeout)?;
        let client = self.client.clone();
        let run = self.claimed_run().clone();
        if !matches!(&options.lease_renewal, LeaseRenewal::Disabled)
            && Instant::now() >= run.claim_started_at + run.claim_timeout
        {
            cancellation.cancel();
            self.forget();
            return Err(Error::LeaseRenewalTimeout);
        }
        let mut future = Box::pin(future);
        let stop = tokio::select! {
            result = &mut future => {
                drop(future);
                let result = if cancellation.is_cancelled() {
                    Err(Error::ExecutionCancelled)
                } else {
                    result
                };
                return self.run(async { result }).await;
            }
            _ = cancellation.cancelled() => ExecutionStop::Cancelled,
            _ = async {
                match options.timeout {
                    Some(duration) => sleep(duration).await,
                    None => pending().await,
                }
            } => ExecutionStop::TimedOut,
            error = watch_lease(client, run, &options.lease_renewal) => ExecutionStop::Lease(error),
        };
        cancellation.cancel();
        let _ = timeout(options.cancellation_grace, &mut future).await;
        drop(future);
        match stop {
            ExecutionStop::Lease(error) => {
                self.forget();
                ignore_terminal_error(Err(error))
            }
            ExecutionStop::TimedOut => {
                self.run(async { Err::<T, _>(Error::ExecutionTimedOut) })
                    .await
            }
            ExecutionStop::Cancelled => {
                self.run(async { Err::<T, _>(Error::ExecutionCancelled) })
                    .await
            }
        }
    }

    /// Schedules the run to wake at an absolute timestamp.
    pub async fn sleep_until(mut self, wake_at: Timestamp) -> Result<()> {
        let queue_name = self.claimed_run().queue_name.clone();
        let run_id = self.claimed_run().run_id;
        let outcome = self
            .client
            .schedule_run(queue_name.as_str(), run_id.as_uuid(), wake_at)
            .await;
        if is_resolved_outcome(&outcome) {
            self.run.take();
        }
        outcome
    }

    /// Schedules the run to wake after a database-relative duration.
    pub async fn sleep_for(mut self, duration: Duration) -> Result<()> {
        let queue_name = self.claimed_run().queue_name.clone();
        let run_id = self.claimed_run().run_id;
        let outcome = self
            .client
            .schedule_run_after(queue_name.as_str(), run_id.as_uuid(), duration)
            .await;
        if is_resolved_outcome(&outcome) {
            self.run.take();
        }
        outcome
    }

    /// Explicitly abandons the lease.
    pub fn forget(mut self) {
        let _ = self.run.take();
    }

    /// Removes the run from the lease.
    fn take_run(&mut self) -> ClaimedRun {
        self.run.take().expect("run lease must contain a run")
    }
}

/// Identifies why execution was interrupted without conflating task results.
enum ExecutionStop {
    /// Carries a renewal failure or database terminal-state notification.
    Lease(Error),
    /// Indicates that the local dispatch deadline expired.
    TimedOut,
    /// Indicates that the application requested local cancellation.
    Cancelled,
}

/// Renews claims until renewal fails or the monitor future is dropped.
async fn watch_lease(client: Client, run: ClaimedRun, renewal: &LeaseRenewal) -> Error {
    let options = match renewal {
        LeaseRenewal::Disabled => return pending().await,
        LeaseRenewal::Automatic => LeaseWatchdogOptions::for_claim_timeout(run.claim_timeout),
        LeaseRenewal::Custom(options) => options.clone(),
    };
    let mut deadline = run.claim_started_at + run.claim_timeout;
    let mut next_renewal = run.claim_started_at + options.interval;
    loop {
        let renewal = async {
            sleep_until(next_renewal).await;
            let requested_at = Instant::now();
            client
                .extend_claim(
                    run.queue_name.as_str(),
                    run.run_id.as_uuid(),
                    options.extend_by,
                )
                .await?;
            Ok(requested_at + options.extend_by)
        };
        match timeout_at(deadline, renewal).await {
            Ok(Ok(renewed_deadline)) => {
                deadline = renewed_deadline;
                next_renewal = Instant::now() + options.interval;
            }
            Ok(Err(error)) => return error,
            Err(_) => return Error::LeaseRenewalTimeout,
        }
    }
}

/// Returns whether an operation resolved the local lease obligation.
fn is_resolved_outcome(outcome: &Result<()>) -> bool {
    matches!(
        outcome,
        Ok(()) | Err(Error::Cancelled | Error::RunAlreadyFailed)
    )
}

/// Treats distributed terminal-state races as successful local resolution.
fn ignore_terminal_error(outcome: Result<()>) -> Result<()> {
    match outcome {
        Ok(()) | Err(Error::Cancelled | Error::RunAlreadyFailed) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Converts an Elephant error into a stable failure reason.
fn failure_reason(error: &Error) -> FailureReason {
    match error {
        Error::Handler { source } => {
            FailureReason::from_error_named("handler_error", source.as_ref())
        }
        Error::HandlerPanicked { message } => FailureReason::from_parts("panic", message),
        _ => FailureReason::from_error_named("elephant_error", error),
    }
}

impl Drop for RunLease {
    /// Reports unresolved leases without performing async cleanup.
    fn drop(&mut self) {
        if let Some(run) = &self.run {
            warn!(
                queue = %run.queue_name,
                task_id = %run.task_id,
                run_id = %run.run_id,
                task_name = %run.task_name,
                "run lease dropped without resolution"
            );
        }
    }
}
