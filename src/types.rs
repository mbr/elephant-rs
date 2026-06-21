//! Shared types for queues, tasks, policies, and results.

use std::{fmt, marker::PhantomData, str::FromStr, time::Duration};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{Error, Result};

/// Defines Absurd's queue-name byte limit.
pub const MAX_QUEUE_NAME_BYTES: usize = 50;

/// Represents a validated queue name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct QueueName(String);

impl QueueName {
    /// Returns the queue name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the queue name into its string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl FromStr for QueueName {
    /// Parses and validates a queue name.
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        validate_named("queue", value, Some(MAX_QUEUE_NAME_BYTES)).map(Self)
    }
}

impl fmt::Display for QueueName {
    /// Formats the queue name.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for QueueName {
    /// Returns the queue name as a string slice.
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Represents a validated task name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskName(String);

impl TaskName {
    /// Returns the task name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the task name into its string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl FromStr for TaskName {
    /// Parses and validates a task name.
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        validate_named("task", value, None).map(Self)
    }
}

impl fmt::Display for TaskName {
    /// Formats the task name.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for TaskName {
    /// Returns the task name as a string slice.
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Represents a validated step name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StepName(String);

impl StepName {
    /// Returns the step name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for StepName {
    /// Parses and validates a step name.
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        validate_named("step", value, None).map(Self)
    }
}

impl fmt::Display for StepName {
    /// Formats the step name.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for StepName {
    /// Returns the step name as a string slice.
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Represents a validated event name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventName(String);

impl EventName {
    /// Returns the event name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for EventName {
    /// Parses and validates an event name.
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        validate_named("event", value, None).map(Self)
    }
}

impl fmt::Display for EventName {
    /// Formats the event name.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for EventName {
    /// Returns the event name as a string slice.
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Represents a task identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskId(Uuid);

impl TaskId {
    /// Returns the underlying UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl From<Uuid> for TaskId {
    /// Wraps a task UUID.
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl From<TaskId> for Uuid {
    /// Unwraps a task UUID.
    fn from(value: TaskId) -> Self {
        value.0
    }
}

impl FromStr for TaskId {
    /// Parses a task identifier.
    type Err = uuid::Error;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

impl fmt::Display for TaskId {
    /// Formats the task identifier.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Represents a run identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RunId(Uuid);

impl RunId {
    /// Returns the underlying UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl From<Uuid> for RunId {
    /// Wraps a run UUID.
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl From<RunId> for Uuid {
    /// Unwraps a run UUID.
    fn from(value: RunId) -> Self {
        value.0
    }
}

impl FromStr for RunId {
    /// Parses a run identifier.
    type Err = uuid::Error;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

impl fmt::Display for RunId {
    /// Formats the run identifier.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Describes Absurd queue storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueStorageMode {
    /// Stores all queue rows in unpartitioned tables.
    Unpartitioned,
    /// Stores queue rows in partitioned tables.
    Partitioned,
}

impl QueueStorageMode {
    /// Returns the value expected by Absurd.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unpartitioned => "unpartitioned",
            Self::Partitioned => "partitioned",
        }
    }
}

impl Default for QueueStorageMode {
    /// Returns the default queue storage mode.
    fn default() -> Self {
        Self::Unpartitioned
    }
}

impl FromStr for QueueStorageMode {
    /// Parses a queue storage mode.
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "unpartitioned" => Ok(Self::Unpartitioned),
            "partitioned" => Ok(Self::Partitioned),
            _ => Err(()),
        }
    }
}

impl fmt::Display for QueueStorageMode {
    /// Formats the queue storage mode.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Describes detached partition handling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueDetachMode {
    /// Leaves detached partitions alone.
    Keep,
    /// Drops detached partitions during cleanup.
    Drop,
}

impl QueueDetachMode {
    /// Returns the value expected by Absurd.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Keep => "keep",
            Self::Drop => "drop",
        }
    }
}

impl FromStr for QueueDetachMode {
    /// Parses a detach mode.
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "keep" => Ok(Self::Keep),
            "drop" => Ok(Self::Drop),
            _ => Err(()),
        }
    }
}

impl fmt::Display for QueueDetachMode {
    /// Formats the detach mode.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Configures queue creation.
#[derive(Clone, Debug)]
pub struct CreateQueueOptions {
    /// Selects the queue storage mode.
    pub storage_mode: QueueStorageMode,
    /// Carries queue policy updates applied after creation.
    pub policy: QueuePolicyOptions,
}

impl Default for CreateQueueOptions {
    /// Creates default queue creation options.
    fn default() -> Self {
        Self {
            storage_mode: QueueStorageMode::Unpartitioned,
            policy: QueuePolicyOptions::default(),
        }
    }
}

/// Configures queue policy updates.
#[derive(Clone, Debug, Default, Serialize)]
pub struct QueuePolicyOptions {
    /// Configures partition lookahead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_lookahead: Option<String>,
    /// Configures partition lookback.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_lookback: Option<String>,
    /// Configures cleanup time-to-live.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_ttl: Option<String>,
    /// Configures cleanup batch size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_limit: Option<i32>,
    /// Configures detached partition handling.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detach_mode: Option<String>,
    /// Configures detached partition minimum age.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detach_min_age: Option<String>,
}

impl QueuePolicyOptions {
    /// Returns whether no policy fields are set.
    pub fn is_empty(&self) -> bool {
        self.partition_lookahead.is_none()
            && self.partition_lookback.is_none()
            && self.cleanup_ttl.is_none()
            && self.cleanup_limit.is_none()
            && self.detach_mode.is_none()
            && self.detach_min_age.is_none()
    }
}

/// Describes a queue policy snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuePolicy {
    /// Names the queue.
    pub queue_name: QueueName,
    /// Describes the storage mode.
    pub storage_mode: QueueStorageMode,
    /// Carries partition lookahead as an interval string.
    pub partition_lookahead: String,
    /// Carries partition lookback as an interval string.
    pub partition_lookback: String,
    /// Carries cleanup time-to-live as an interval string.
    pub cleanup_ttl: String,
    /// Carries cleanup batch size.
    pub cleanup_limit: i32,
    /// Describes detached partition handling.
    pub detach_mode: QueueDetachMode,
    /// Carries detached partition minimum age as an interval string.
    pub detach_min_age: String,
}

/// Configures retry behavior for a task.
#[derive(Clone, Debug, PartialEq)]
pub enum RetryStrategy {
    /// Disables automatic retry.
    None,
    /// Retries with a fixed delay.
    Fixed {
        /// Carries the retry delay.
        base: Duration,
    },
    /// Retries with exponential backoff.
    Exponential {
        /// Carries the first retry delay.
        base: Duration,
        /// Carries the multiplier applied after each attempt.
        factor: f64,
        /// Carries the optional maximum delay.
        max: Option<Duration>,
    },
}

impl RetryStrategy {
    /// Converts the retry strategy to Absurd JSON.
    pub fn to_json(&self) -> Value {
        match self {
            Self::None => serde_json::json!({ "kind": "none" }),
            Self::Fixed { base } => serde_json::json!({
                "kind": "fixed",
                "base_seconds": base.as_secs_f64(),
            }),
            Self::Exponential { base, factor, max } => {
                let mut value = serde_json::json!({
                    "kind": "exponential",
                    "base_seconds": base.as_secs_f64(),
                    "factor": *factor,
                });
                if let Some(max) = max {
                    value["max_seconds"] = serde_json::json!(max.as_secs_f64());
                }
                value
            }
        }
    }
}

/// Configures cancellation behavior for a task.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CancellationPolicy {
    /// Limits total time after first start.
    pub max_duration: Option<Duration>,
    /// Limits queue delay before first start.
    pub max_delay: Option<Duration>,
}

impl CancellationPolicy {
    /// Converts the cancellation policy to Absurd JSON.
    pub fn to_json(&self) -> Value {
        let mut value = serde_json::Map::new();
        if let Some(max_duration) = self.max_duration {
            value.insert(
                "max_duration".to_string(),
                serde_json::json!(max_duration.as_secs()),
            );
        }
        if let Some(max_delay) = self.max_delay {
            value.insert(
                "max_delay".to_string(),
                serde_json::json!(max_delay.as_secs()),
            );
        }
        Value::Object(value)
    }
}

/// Configures untyped task spawning.
#[derive(Clone, Debug, Default)]
pub struct SpawnOptions {
    /// Overrides the target queue.
    pub queue_name: Option<QueueName>,
    /// Overrides maximum attempts.
    pub max_attempts: Option<i32>,
    /// Overrides retry behavior.
    pub retry_strategy: Option<RetryStrategy>,
    /// Carries application headers.
    pub headers: Option<Value>,
    /// Overrides cancellation behavior.
    pub cancellation: Option<CancellationPolicy>,
    /// Carries an idempotency key.
    pub idempotency_key: Option<String>,
}

impl SpawnOptions {
    /// Converts spawn options to Absurd JSON.
    pub fn to_json(&self, default_max_attempts: Option<i32>) -> Value {
        let mut value = serde_json::Map::new();
        if let Some(max_attempts) = self.max_attempts.or(default_max_attempts) {
            value.insert("max_attempts".to_string(), serde_json::json!(max_attempts));
        }
        if let Some(retry_strategy) = &self.retry_strategy {
            value.insert("retry_strategy".to_string(), retry_strategy.to_json());
        }
        if let Some(headers) = &self.headers {
            value.insert("headers".to_string(), headers.clone());
        }
        if let Some(cancellation) = &self.cancellation {
            value.insert("cancellation".to_string(), cancellation.to_json());
        }
        if let Some(idempotency_key) = &self.idempotency_key {
            value.insert(
                "idempotency_key".to_string(),
                Value::String(idempotency_key.clone()),
            );
        }
        Value::Object(value)
    }
}

/// Describes the result of spawning a task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpawnResult {
    /// Identifies the task.
    pub task_id: TaskId,
    /// Identifies the initial or existing run.
    pub run_id: RunId,
    /// Carries the run attempt.
    pub attempt: i32,
    /// Reports whether a new task was created.
    pub created: bool,
}

/// Represents a typed spawned task handle.
#[derive(Clone, Debug)]
pub struct Spawned<R> {
    /// Carries the untyped spawn result.
    pub result: SpawnResult,
    /// Carries the result type marker.
    pub marker: PhantomData<R>,
}

impl<R> Spawned<R> {
    /// Creates a typed spawned task handle.
    pub fn new(result: SpawnResult) -> Self {
        Self {
            result,
            marker: PhantomData,
        }
    }
}

/// Describes a task result state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskResultState {
    /// Indicates a pending task.
    Pending,
    /// Indicates a running task.
    Running,
    /// Indicates a sleeping task.
    Sleeping,
    /// Indicates a completed task.
    Completed,
    /// Indicates a failed task.
    Failed,
    /// Indicates a cancelled task.
    Cancelled,
    /// Indicates an unrecognized state.
    Other(String),
}

impl TaskResultState {
    /// Returns whether the state is terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl From<String> for TaskResultState {
    /// Converts Absurd's state string to a task result state.
    fn from(value: String) -> Self {
        match value.as_str() {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "sleeping" => Self::Sleeping,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => Self::Other(value),
        }
    }
}

/// Describes a task result snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskResultSnapshot {
    /// Carries the task state.
    pub state: TaskResultState,
    /// Carries the successful result payload.
    pub result: Option<Value>,
    /// Carries the failure payload.
    pub failure: Option<Value>,
}

impl TaskResultSnapshot {
    /// Returns whether the task has a terminal result.
    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Decodes a completed task result.
    pub fn decode<R: DeserializeOwned>(&self) -> Result<Option<R>> {
        match &self.result {
            Some(value) => serde_json::from_value(value.clone())
                .map(Some)
                .map_err(Error::json),
            None => Ok(None),
        }
    }
}

/// Describes retry operation options.
#[derive(Clone, Debug, Default)]
pub struct RetryTaskOptions {
    /// Overrides maximum attempts for retry.
    pub max_attempts: Option<i32>,
    /// Requests creation of a new task.
    pub spawn_new: bool,
}

impl RetryTaskOptions {
    /// Converts retry options to Absurd JSON.
    pub fn to_json(&self) -> Value {
        let mut value = serde_json::Map::new();
        if let Some(max_attempts) = self.max_attempts {
            value.insert("max_attempts".to_string(), serde_json::json!(max_attempts));
        }
        if self.spawn_new {
            value.insert("spawn_new".to_string(), serde_json::json!(true));
        }
        Value::Object(value)
    }
}

/// Validates a named string.
fn validate_named(kind: &'static str, value: &str, max_bytes: Option<usize>) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidName {
            kind,
            value: value.to_string(),
            reason: "must not be empty",
        });
    }
    if let Some(max_bytes) = max_bytes
        && trimmed.len() > max_bytes
    {
        return Err(Error::InvalidName {
            kind,
            value: value.to_string(),
            reason: "is too long",
        });
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::types::{QueueName, RetryStrategy, TaskName};

    #[test]
    fn names_validate_basic_constraints() {
        assert!("default".parse::<QueueName>().is_ok());
        assert!("".parse::<QueueName>().is_err());
        assert!("task".parse::<TaskName>().is_ok());
    }

    #[test]
    fn retry_strategy_serializes_to_absurd_shape() {
        let retry = RetryStrategy::Exponential {
            base: Duration::from_secs(2),
            factor: 3.0,
            max: Some(Duration::from_secs(60)),
        };

        assert_eq!(retry.to_json()["kind"], "exponential");
        assert_eq!(retry.to_json()["max_seconds"], 60.0);
    }
}
