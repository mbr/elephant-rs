//! Behavioral coverage shared with the upstream SDK suites.

use elephant::types::{CreateQueueOptions, QueueStorageMode};

use super::{TestResult, setup};

/// Verifies partitioned queue options produce the expected relation kinds.
#[tokio::test]
async fn partitioned_queue_relations() -> TestResult {
    let test = setup().await?;
    test.client
        .create_queue(
            "partitioned",
            CreateQueueOptions {
                storage_mode: QueueStorageMode::Partitioned,
                ..CreateQueueOptions::default()
            },
        )
        .await?;
    for (prefix, kind) in [
        ("t", "p"),
        ("r", "p"),
        ("c", "p"),
        ("w", "p"),
        ("e", "r"),
        ("i", "r"),
    ] {
        let actual: String =
            sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE oid = to_regclass($1)")
                .bind(format!("absurd.{prefix}_partitioned"))
                .fetch_one(test.client.pool())
                .await?;
        assert_eq!(actual, kind, "{prefix}_partitioned");
    }
    Ok(())
}

/// Verifies listing and dropping queues also removes their physical tables.
#[tokio::test]
async fn queue_lifecycle() -> TestResult {
    let test = setup().await?;
    test.client
        .create_queue("other", CreateQueueOptions::default())
        .await?;
    let mut names = test.client.list_queues().await?;
    names.sort();
    assert_eq!(
        names.iter().map(|name| name.as_str()).collect::<Vec<_>>(),
        ["default", "other"]
    );
    for queue in ["default", "other"] {
        for prefix in ["t", "r", "c", "w", "e"] {
            let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                .bind(format!("absurd.{prefix}_{queue}"))
                .fetch_one(test.client.pool())
                .await?;
            assert!(exists, "missing {prefix}_{queue}");
        }
    }
    test.client.drop_queue("other").await?;
    assert_eq!(test.client.list_queues().await?, vec!["default".parse()?]);
    for prefix in ["t", "r", "c", "w", "e"] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("absurd.{prefix}_other"))
            .fetch_one(test.client.pool())
            .await?;
        assert!(!exists);
    }
    test.client.emit_event("default", "survivor", &true).await?;
    Ok(())
}
