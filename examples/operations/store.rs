//! Checked SQL for immutable requests and idempotent local effects.

use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::{Error, Result};

/// Records the immutable business input and its latest workflow identity.
#[derive(Debug, Serialize)]
pub struct Request {
    /// Identifies the business operation across retries and resubmission.
    pub entry_id: Uuid,
    /// Carries an integer amount without floating-point rounding.
    pub amount: i64,
    /// Locates the associated task independently of its attempts.
    pub task_id: Uuid,
}

/// Describes the unique effect recorded by a successful posting.
#[derive(Debug, Deserialize, Serialize)]
pub struct Posting {
    /// Identifies the business operation, not a particular execution attempt.
    pub entry_id: Uuid,
    /// Carries the committed amount.
    pub amount: i64,
}

/// Saves a request in the caller's enqueue transaction, rejecting changed input.
pub async fn request(
    connection: &mut PgConnection,
    entry_id: Uuid,
    amount: i64,
    task_id: Uuid,
) -> Result<Request> {
    sqlx::query_as!(
        Request,
        "INSERT INTO operations.requests (entry_id, amount, task_id) VALUES ($1, $2, $3)
         ON CONFLICT (entry_id) DO UPDATE SET task_id = EXCLUDED.task_id
         WHERE operations.requests.amount = EXCLUDED.amount
         RETURNING entry_id, amount, task_id",
        entry_id,
        amount,
        task_id
    )
    .fetch_optional(connection)
    .await?
    .ok_or(Error::ConflictingAmount { entry_id })
}

/// Loads the authoritative input instead of trusting a repeated task's payload.
pub async fn load_request(connection: &mut PgConnection, entry_id: Uuid) -> Result<Request> {
    Ok(sqlx::query_as!(
        Request,
        "SELECT entry_id, amount, task_id FROM operations.requests WHERE entry_id = $1",
        entry_id
    )
    .fetch_one(connection)
    .await?)
}

/// Records an effect once and verifies a repeated request has the same meaning.
pub async fn post(connection: &mut PgConnection, request: &Request) -> Result<Posting> {
    sqlx::query!(
        "INSERT INTO operations.postings (entry_id, amount) VALUES ($1, $2) ON CONFLICT (entry_id) DO NOTHING",
        request.entry_id, request.amount)
        .execute(&mut *connection).await?;
    let posting =
        load_posting(connection, request.entry_id)
            .await?
            .ok_or(Error::MissingPosting {
                entry_id: request.entry_id,
            })?;
    if posting.amount != request.amount {
        return Err(Error::ConflictingAmount {
            entry_id: request.entry_id,
        });
    }
    Ok(posting)
}

/// Reads an effect independently of its workflow's terminal state.
pub async fn load_posting(
    connection: &mut PgConnection,
    entry_id: Uuid,
) -> Result<Option<Posting>> {
    Ok(sqlx::query_as!(
        Posting,
        "SELECT entry_id, amount FROM operations.postings WHERE entry_id = $1",
        entry_id
    )
    .fetch_optional(connection)
    .await?)
}
