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

/// Returns the installed Absurd schema version when it is available.
pub async fn version(pool: &PgPool) -> Result<Option<String>> {
    let row = sqlx::query(
        "SELECT EXISTS ( \
         SELECT 1 FROM pg_proc p \
         JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE n.nspname = 'absurd' \
         AND p.proname = 'get_schema_version' \
         AND pg_get_function_identity_arguments(p.oid) = '' \
         ) AS installed",
    )
    .fetch_one(pool)
    .await
    .map_err(Error::from_sqlx)?;
    let installed: bool = row.try_get("installed").map_err(Error::from_sqlx)?;
    if !installed {
        return Ok(None);
    }

    let row = sqlx::query("SELECT absurd.get_schema_version() AS version")
        .fetch_one(pool)
        .await
        .map_err(Error::from_sqlx)?;
    row.try_get("version").map_err(Error::from_sqlx)
}

/// Returns an error if Absurd is not installed.
pub async fn assert_installed(pool: &PgPool) -> Result<()> {
    if is_installed(pool).await? {
        Ok(())
    } else {
        Err(Error::SchemaNotInstalled)
    }
}

/// Returns an error if Absurd does not report the expected schema version.
pub async fn assert_version(pool: &PgPool, expected: impl Into<String>) -> Result<()> {
    assert_installed(pool).await?;
    let expected = expected.into();
    let actual = version(pool).await?;
    if actual.as_deref() == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(Error::SchemaVersionMismatch { expected, actual })
    }
}
