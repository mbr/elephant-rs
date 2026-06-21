//! Schema inspection helpers.

use sqlx::{PgPool, Row};

use crate::error::{Error, Result};

/// Checks whether the Absurd schema appears to be installed.
pub async fn is_installed(pool: &PgPool) -> Result<bool> {
    let row = sqlx::query(
        "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'absurd') AS installed",
    )
    .fetch_one(pool)
    .await
    .map_err(Error::from_sqlx)?;
    row.try_get("installed").map_err(Error::from_sqlx)
}

/// Returns an error if Absurd is not installed.
pub async fn assert_installed(pool: &PgPool) -> Result<()> {
    if is_installed(pool).await? {
        Ok(())
    } else {
        Err(Error::SchemaNotInstalled)
    }
}
