//! Claimed run primitives.

use std::{future::Future, time::Duration};

use jiff::Timestamp;
use serde::Serialize;
use serde_json::Value;
use tracing::warn;

use crate::{
    client::Client,
    error::{Error, FailureReason, Result},
    types::{QueueName, RunId, TaskId, TaskName},
};

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
        Error::HandlerPanicked => FailureReason::panic(),
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
