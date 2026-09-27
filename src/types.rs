//! Shared types for queues, tasks, policies, and results.

use std::{fmt, marker::PhantomData, str::FromStr, time::Duration};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{Error, Result};

/// Defines Absurd's queue-name byte limit.
pub const MAX_QUEUE_NAME_BYTES: usize = 57;

/// Represents a validated queue name.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String")]
pub struct QueueName(String);

impl TryFrom<String> for QueueName {
    /// Validates a deserialized queue name.
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        value.parse()
    }
}

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
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String")]
pub struct TaskName(String);

impl TryFrom<String> for TaskName {
    /// Validates a deserialized task name.
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        value.parse()
    }
}

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
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String")]
pub struct StepName(String);

impl TryFrom<String> for StepName {
    /// Validates a deserialized checkpoint name.
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        value.parse()
    }
}

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
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String")]
pub struct EventName(String);

impl TryFrom<String> for EventName {
    /// Validates a deserialized event name.
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        value.parse()
    }
}

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
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
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
    /// Disables automatic partition detaching.
    None,
    /// Detaches empty partitions during cleanup.
    Empty,
}

impl QueueDetachMode {
    /// Returns the value expected by Absurd.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Empty => "empty",
        }
    }
}

impl FromStr for QueueDetachMode {
    /// Parses a detach mode.
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "none" => Ok(Self::None),
            "empty" => Ok(Self::Empty),
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

impl Serialize for QueueDetachMode {
    /// Serializes the detach mode for Absurd.
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

/// Represents a PostgreSQL interval expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgInterval(String);

impl PgInterval {
    /// Returns the interval as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<Duration> for PgInterval {
    /// Creates an interval from a duration.
    fn from(value: Duration) -> Self {
        Self(format!("{} seconds", value.as_secs_f64()))
    }
}

impl FromStr for PgInterval {
    /// Parses an interval expression.
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(Error::InvalidName {
                kind: "interval",
                value: value.to_string(),
                reason: "must not be empty",
            });
        }
        Ok(Self(trimmed.to_string()))
    }
}

impl fmt::Display for PgInterval {
    /// Formats the interval expression.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for PgInterval {
    /// Serializes the interval expression for PostgreSQL.
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
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
    pub partition_lookahead: Option<PgInterval>,
    /// Configures partition lookback.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_lookback: Option<PgInterval>,
    /// Configures cleanup time-to-live.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_ttl: Option<PgInterval>,
    /// Configures cleanup batch size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_limit: Option<i32>,
    /// Configures detached partition handling.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detach_mode: Option<QueueDetachMode>,
    /// Configures detached partition minimum age.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detach_min_age: Option<PgInterval>,
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
                serde_json::json!(whole_seconds_ceil(max_duration)),
            );
        }
        if let Some(max_delay) = self.max_delay {
            value.insert(
                "max_delay".to_string(),
                serde_json::json!(whole_seconds_ceil(max_delay)),
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
        let mut max_attempts = self.max_attempts.or(default_max_attempts);
        if matches!(self.retry_strategy, Some(RetryStrategy::None)) {
            max_attempts = Some(1);
        }
        if let Some(max_attempts) = max_attempts {
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
#[derive(Debug, Deserialize, Serialize)]
#[serde(bound = "")]
pub struct Spawned<R> {
    /// Names the queue containing the task.
    pub queue_name: QueueName,
    /// Carries the untyped spawn result.
    pub result: SpawnResult,
    /// Carries the result type marker.
    #[serde(skip)]
    marker: PhantomData<fn() -> R>,
}

impl<R> Clone for Spawned<R> {
    /// Clones the reference without requiring the task result to be cloneable.
    fn clone(&self) -> Self {
        Self::new(self.queue_name.clone(), self.result)
    }
}

impl<R> Spawned<R> {
    /// Creates a typed spawned task handle.
    pub fn new(queue_name: QueueName, result: SpawnResult) -> Self {
        Self {
            queue_name,
            result,
            marker: PhantomData,
        }
    }
}

/// Describes a task result state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TaskResultSnapshot {
    /// Carries the task state.
    pub state: TaskResultState,
    /// Carries the successful result payload, preserving JSON null as a value.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_payload"
    )]
    pub result: Option<Value>,
    /// Carries the failure payload.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_payload"
    )]
    pub failure: Option<Value>,
}

impl TaskResultSnapshot {
    /// Returns whether the task has a terminal result.
    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Decodes a task result without interpreting task state.
    pub fn decode<R: DeserializeOwned>(&self) -> Result<Option<R>> {
        match &self.result {
            Some(value) => serde_json::from_value(value.clone())
                .map(Some)
                .map_err(Error::json),
            None => Ok(None),
        }
    }

    /// Decodes a successful terminal task result.
    pub fn decode_completed<R: DeserializeOwned>(&self, task_id: TaskId) -> Result<R> {
        match self.state {
            TaskResultState::Completed => match &self.result {
                Some(value) => serde_json::from_value(value.clone()).map_err(Error::json),
                None => Err(Error::TaskResultMissing {
                    task_id: task_id.as_uuid(),
                }),
            },
            TaskResultState::Cancelled => Err(Error::TaskCancelled { task_id }),
            TaskResultState::Failed => Err(Error::TaskFailed {
                task_id: task_id.as_uuid(),
                failure: self.failure.clone(),
            }),
            _ => Err(Error::TaskResultTimeout {
                task_id: task_id.as_uuid(),
            }),
        }
    }
}

/// Describes a checkpoint state row.
#[derive(Clone, Debug, PartialEq)]
pub struct CheckpointSnapshot {
    /// Names the checkpoint.
    pub checkpoint_name: StepName,
    /// Carries the checkpoint payload.
    pub state: Value,
    /// Carries the storage status.
    pub status: String,
    /// Identifies the run that wrote the row.
    pub owner_run_id: Option<RunId>,
    /// Carries the update timestamp as reported by PostgreSQL.
    pub updated_at: String,
}

/// Describes a cleanup result row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupResult {
    /// Names the cleaned queue.
    pub queue_name: QueueName,
    /// Counts deleted tasks.
    pub tasks_deleted: i32,
    /// Counts deleted events.
    pub events_deleted: i32,
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

/// Distinguishes a present JSON null payload from an absent snapshot field.
fn deserialize_payload<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

/// Converts a duration to whole seconds without truncating nonzero fractions.
fn whole_seconds_ceil(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() > 0))
}

/// Validates a named string.
fn validate_named(kind: &'static str, value: &str, max_bytes: Option<usize>) -> Result<String> {
    if value.trim().is_empty() {
        return Err(Error::InvalidName {
            kind,
            value: value.to_string(),
            reason: "must not be empty",
        });
    }
    if let Some(max_bytes) = max_bytes
        && value.len() > max_bytes
    {
        return Err(Error::InvalidName {
            kind,
            value: value.to_string(),
            reason: "is too long",
        });
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::types::{
        CancellationPolicy, MAX_QUEUE_NAME_BYTES, PgInterval, QueueName, RetryStrategy, RunId,
        SpawnOptions, SpawnResult, Spawned, TaskId, TaskName, TaskResultSnapshot, TaskResultState,
    };

    /// Round-trips typed references without serializing their result type.
    #[test]
    fn handles_serialize_and_validate_names() {
        /// Represents a result without serialization or clone implementations.
        struct ResultType;

        let handle = Spawned::<ResultType>::new(
            "default".parse().expect("valid queue"),
            SpawnResult {
                task_id: TaskId::from(uuid::Uuid::nil()),
                run_id: RunId::from(uuid::Uuid::nil()),
                attempt: 1,
                created: true,
            },
        );
        let mut encoded = serde_json::to_value(handle.clone()).expect("handle should serialize");
        let decoded: Spawned<ResultType> =
            serde_json::from_value(encoded.clone()).expect("handle should decode");
        assert_eq!(decoded.result, handle.result);
        assert_eq!(decoded.queue_name, handle.queue_name);
        assert!(encoded.get("marker").is_none());
        encoded["queue_name"] = serde_json::json!("");
        assert!(serde_json::from_value::<Spawned<ResultType>>(encoded).is_err());
    }

    /// Preserves unit results distinctly from missing results in checkpoints.
    #[test]
    fn snapshots_preserve_json_null() {
        for result in [None, Some(serde_json::Value::Null)] {
            let snapshot = TaskResultSnapshot {
                state: TaskResultState::Completed,
                result,
                failure: None,
            };
            let encoded = serde_json::to_value(&snapshot).expect("snapshot should serialize");
            let decoded: TaskResultSnapshot =
                serde_json::from_value(encoded).expect("snapshot should decode");
            assert_eq!(decoded, snapshot);
        }
    }

    #[test]
    fn names_validate_basic_constraints() {
        assert!("default".parse::<QueueName>().is_ok());
        assert!(
            "a".repeat(MAX_QUEUE_NAME_BYTES)
                .parse::<QueueName>()
                .is_ok()
        );
        assert!(
            "a".repeat(MAX_QUEUE_NAME_BYTES + 1)
                .parse::<QueueName>()
                .is_err()
        );
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
        let options = SpawnOptions {
            retry_strategy: Some(RetryStrategy::None),
            ..SpawnOptions::default()
        };

        assert_eq!(retry.to_json()["kind"], "exponential");
        assert_eq!(retry.to_json()["max_seconds"], 60.0);
        assert_eq!(options.to_json(Some(5))["max_attempts"], 1);

        let cancellation = CancellationPolicy {
            max_duration: Some(Duration::from_millis(1)),
            max_delay: None,
        };
        assert_eq!(cancellation.to_json()["max_duration"], 1);
        assert_eq!(
            PgInterval::from(Duration::from_millis(500)).as_str(),
            "0.5 seconds"
        );
    }
}
