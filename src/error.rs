//! Error types for Elephant.

use std::{error, fmt, num::TryFromIntError};

use jiff::Error as JiffError;
use serde_json::Error as JsonError;
use sqlx::Error as SqlxError;
use thiserror::Error;

/// Represents the crate-wide result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Represents errors returned by Elephant operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Indicates that an operation's owning task was cancelled by Absurd.
    ///
    /// Dispatch treats this as control flow for the active run, not as an
    /// ordinary handler failure. Task result inspection uses [`Self::TaskCancelled`].
    #[error("task was cancelled")]
    Cancelled,
    /// Indicates that a duration cannot be represented for Absurd.
    #[error("duration cannot be represented as seconds")]
    DurationOutOfRange {
        /// Carries the conversion failure.
        #[source]
        source: TryFromIntError,
    },
    /// Indicates that an event wait reached its timeout.
    #[error("event wait timed out")]
    EventTimeout,
    /// Indicates that a task handler returned an error.
    #[error("handler failed: {source}")]
    Handler {
        /// Carries the task handler error.
        #[source]
        source: Box<dyn error::Error + Send + Sync>,
    },
    /// Indicates that a task handler panicked.
    #[error("handler panicked")]
    HandlerPanicked,
    /// Indicates that a retry strategy is invalid.
    #[error("invalid retry strategy")]
    InvalidRetryStrategy {
        /// Carries the PostgreSQL failure.
        #[source]
        source: SqlxError,
    },
    /// Indicates that a configured name is invalid.
    #[error("invalid {kind} name {value:?}: {reason}")]
    InvalidName {
        /// Identifies the name category.
        kind: &'static str,
        /// Carries the invalid value.
        value: String,
        /// Explains the validation failure.
        reason: &'static str,
    },
    /// Indicates that worker configuration cannot make progress.
    #[error("invalid worker options: {reason}")]
    InvalidWorkerOptions {
        /// Explains the invalid configuration.
        reason: &'static str,
    },
    /// Indicates that time arithmetic failed.
    #[error("time error")]
    Jiff {
        /// Carries the time failure.
        #[source]
        source: JiffError,
    },
    /// Indicates that JSON encoding or decoding failed.
    #[error("json error")]
    Json {
        /// Carries the JSON failure.
        #[source]
        source: JsonError,
    },
    /// Indicates that a claimed run was already failed.
    #[error("run has already failed")]
    RunAlreadyFailed,
    /// Indicates that the operation cannot be performed in this context.
    #[error("same-queue task waits are rejected")]
    SameQueueWait,
    /// Indicates that Absurd is not installed in the database.
    #[error("absurd schema is not installed")]
    SchemaNotInstalled,
    /// Indicates that the installed Absurd schema is not compatible.
    #[error("absurd schema version {actual:?} does not match expected version {expected:?}")]
    SchemaVersionMismatch {
        /// Carries the version required by the caller.
        expected: String,
        /// Carries the version reported by Absurd.
        actual: Option<String>,
    },
    /// Indicates that PostgreSQL returned an error.
    #[error("postgres error")]
    Sqlx {
        /// Carries the database failure.
        #[source]
        source: SqlxError,
    },
    /// Indicates that a task run suspended itself.
    #[error("task run suspended")]
    Suspended,
    /// Indicates that an inspected task finished by cancellation.
    #[error("task {task_id} was cancelled")]
    TaskCancelled {
        /// Identifies the cancelled task, which need not be the active task.
        task_id: crate::types::TaskId,
    },
    /// Indicates that a task does not exist in the selected queue.
    #[error("task {task_id} was not found")]
    TaskNotFound {
        /// Identifies the missing task.
        task_id: uuid::Uuid,
    },
    /// Indicates that a task finished without a decodable result.
    #[error("task {task_id} completed without a result")]
    TaskResultMissing {
        /// Identifies the task whose result was awaited.
        task_id: uuid::Uuid,
    },
    /// Indicates that a task completed with a failed state.
    #[error("task {task_id} failed")]
    TaskFailed {
        /// Identifies the task whose result was awaited.
        task_id: uuid::Uuid,
        /// Carries the recorded failure payload.
        failure: Option<serde_json::Value>,
    },
    /// Indicates that a task result wait reached its timeout.
    #[error("timed out waiting for task {task_id}")]
    TaskResultTimeout {
        /// Identifies the task whose result was awaited.
        task_id: uuid::Uuid,
    },
    /// Indicates that a spawn queue conflicts with its task definition.
    #[error("task {task_name:?} uses queue {expected:?}, not {actual:?}")]
    TaskQueueMismatch {
        /// Names the task being spawned.
        task_name: String,
        /// Names the queue configured on the task.
        expected: String,
        /// Names the conflicting requested queue.
        actual: String,
    },
    /// Indicates that a task is unknown to the local router.
    #[error("unknown task {task_name}")]
    UnknownTask {
        /// Identifies the unregistered task name.
        task_name: String,
    },
}

impl Error {
    /// Creates an error for a task handler failure.
    pub fn handler(source: Box<dyn error::Error + Send + Sync>) -> Self {
        Self::Handler { source }
    }

    /// Maps database-specific Absurd states to typed errors.
    pub fn from_sqlx(source: SqlxError) -> Self {
        let code = match &source {
            SqlxError::Database(database) => database.code().map(|code| code.into_owned()),
            _ => None,
        };
        match code.as_deref() {
            Some("AB001") => Self::Cancelled,
            Some("AB002") => Self::RunAlreadyFailed,
            Some("AB003") => Self::InvalidRetryStrategy { source },
            _ => Self::Sqlx { source },
        }
    }

    /// Creates a duration conversion error.
    pub fn duration_out_of_range(source: TryFromIntError) -> Self {
        Self::DurationOutOfRange { source }
    }

    /// Creates a time conversion error.
    pub fn jiff(source: JiffError) -> Self {
        Self::Jiff { source }
    }

    /// Creates a JSON conversion error.
    pub fn json(source: JsonError) -> Self {
        Self::Json { source }
    }
}

impl From<SqlxError> for Error {
    /// Converts SQLx errors into Elephant errors.
    fn from(source: SqlxError) -> Self {
        Self::from_sqlx(source)
    }
}

impl From<JsonError> for Error {
    /// Converts JSON errors into Elephant errors.
    fn from(source: JsonError) -> Self {
        Self::json(source)
    }
}

impl From<TryFromIntError> for Error {
    /// Converts integer conversion errors into Elephant errors.
    fn from(source: TryFromIntError) -> Self {
        Self::duration_out_of_range(source)
    }
}

impl From<JiffError> for Error {
    /// Converts time errors into Elephant errors.
    fn from(source: JiffError) -> Self {
        Self::jiff(source)
    }
}

/// Describes a serializable task failure.
#[derive(Clone, Debug, serde::Serialize)]
pub struct FailureReason {
    /// Names the failure category.
    pub name: String,
    /// Carries the human-readable message.
    pub message: String,
    /// Carries optional diagnostic text.
    pub traceback: Option<String>,
}

impl FailureReason {
    /// Creates a failure reason from an error display value.
    pub fn from_error(error: &(dyn error::Error + Send + Sync)) -> Self {
        Self::from_error_named("error", error)
    }

    /// Creates a failure reason with a stable category name.
    pub fn from_error_named(
        name: impl Into<String>,
        error: &(dyn error::Error + Send + Sync),
    ) -> Self {
        Self {
            name: name.into(),
            message: format_error(error),
            traceback: None,
        }
    }

    /// Creates a failure reason from parts.
    pub fn from_parts(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            message: message.into(),
            traceback: None,
        }
    }

    /// Creates a failure reason from panic diagnostics.
    pub fn panic() -> Self {
        Self {
            name: "panic".to_string(),
            message: "task handler panicked".to_string(),
            traceback: None,
        }
    }
}

/// Formats a dynamic error chain.
fn format_error(error: &(dyn error::Error + Send + Sync)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(next) = source {
        message.push_str(": ");
        message.push_str(&next.to_string());
        source = next.source();
    }
    message
}

impl fmt::Display for FailureReason {
    /// Formats the failure message.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.name, self.message)
    }
}
