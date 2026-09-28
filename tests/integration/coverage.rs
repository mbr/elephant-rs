//! Behavioral coverage shared with the upstream SDK suites.

use elephant::types::{
    CreateQueueOptions, QueueDetachMode, QueuePolicy, QueuePolicyOptions, QueueStorageMode,
};

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

/// Verifies every policy field survives creation and partial updates.
#[tokio::test]
async fn full_queue_policy_round_trip() -> TestResult {
    let test = setup().await?;
    test.client
        .create_queue(
            "policy",
            CreateQueueOptions {
                storage_mode: QueueStorageMode::Partitioned,
                policy: QueuePolicyOptions {
                    partition_lookahead: Some("2 days".parse()?),
                    partition_lookback: Some("1 day".parse()?),
                    cleanup_ttl: Some("1 hour".parse()?),
                    cleanup_limit: Some(321),
                    detach_mode: Some(QueueDetachMode::Empty),
                    detach_min_age: Some("1 day".parse()?),
                },
            },
        )
        .await?;
    let mut expected = QueuePolicy {
        queue_name: "policy".parse()?,
        storage_mode: QueueStorageMode::Partitioned,
        partition_lookahead: "2 days".into(),
        partition_lookback: "1 day".into(),
        cleanup_ttl: "01:00:00".into(),
        cleanup_limit: 321,
        detach_mode: QueueDetachMode::Empty,
        detach_min_age: "1 day".into(),
    };
    assert_eq!(
        test.client.get_queue_policy("policy").await?,
        Some(expected.clone())
    );
    test.client
        .set_queue_policy(
            "policy",
            QueuePolicyOptions {
                cleanup_ttl: Some("2 hours".parse()?),
                cleanup_limit: Some(32),
                detach_mode: Some(QueueDetachMode::None),
                detach_min_age: Some("3 days".parse()?),
                ..QueuePolicyOptions::default()
            },
        )
        .await?;
    expected.cleanup_ttl = "02:00:00".into();
    expected.cleanup_limit = 32;
    expected.detach_mode = QueueDetachMode::None;
    expected.detach_min_age = "3 days".into();
    assert_eq!(
        test.client.get_queue_policy("policy").await?,
        Some(expected)
    );
    Ok(())
}

/// Verifies valid non-identifier queue names work through the SQL boundary.
#[tokio::test]
async fn permissive_queue_names() -> TestResult {
    let test = setup().await?;
    for name in ["Uppercase", "with spaces", "with-hyphens", "   "] {
        test.client
            .create_queue(name, CreateQueueOptions::default())
            .await?;
        test.client.emit_event(name, "event", &42).await?;
        assert!(
            test.client
                .list_queues()
                .await?
                .iter()
                .any(|queue| queue.as_str() == name)
        );
        assert_eq!(
            test.client
                .get_queue_policy(name)
                .await?
                .expect("created queue")
                .queue_name
                .as_str(),
            name
        );
        test.client.drop_queue(name).await?;
        assert!(
            !test
                .client
                .list_queues()
                .await?
                .iter()
                .any(|queue| queue.as_str() == name)
        );
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
