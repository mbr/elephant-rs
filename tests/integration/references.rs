//! Identity-only child waits and compatibility with handle-based checkpoints.

use std::{collections::BTreeMap, time::Duration};

use elephant::{
    error::Error,
    task::{Router, Task},
    types::{CreateQueueOptions, PgInterval, QueuePolicyOptions},
    worker::work_batch,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{TestFailure, TestResult, setup};

/// Defines an application wire contract with no spawn or run metadata.
#[derive(Debug, Deserialize, Serialize)]
struct Reference {
    /// Names the child queue.
    queue_name: String,
    /// Identifies the durable child task.
    task_id: Uuid,
}

/// Shares numbered and named snapshots in both API directions after child cleanup.
#[tokio::test]
async fn identity_and_handle_waits_replay_each_others_snapshots() -> TestResult {
    for identity_first in [false, true] {
        let test = setup().await?;
        test.client
            .create_queue("children", CreateQueueOptions::default())
            .await?;
        let child_task = Task::<usize, Value>::builder("child")?
            .queue("children")?
            .default_max_attempts(1)
            .handler(|_, kind| async move {
                match kind {
                    0 => Ok(json!({"amount": 42})),
                    1 => Ok(Value::Null),
                    _ => Err(Error::handler(Box::new(TestFailure))),
                }
            })
            .build();
        let child_router = Router::new().task(child_task.clone())?;
        let mut children = Vec::new();
        for kind in 0..4 {
            children.push(test.client.spawn(&child_task, kind).send().await?);
        }
        test.client
            .cancel_task("children", children[3].result.task_id.as_uuid())
            .await?;
        for _ in 0..3 {
            work_batch(&test.client, &child_router, "children").await?;
        }
        let mut snapshots = Vec::new();
        for child in &children {
            let snapshot = test
                .client
                .fetch_task_result("children", child.result.task_id.as_uuid())
                .await?
                .expect("terminal child");
            assert!(snapshot.is_terminal());
            snapshots.push(serde_json::to_value(snapshot)?);
        }
        let references: Vec<_> = children
            .iter()
            .map(|child| Reference {
                queue_name: "children".to_owned(),
                task_id: child.result.task_id.as_uuid(),
            })
            .collect();
        let handles = children.clone();
        let parent_task = Task::<Vec<Reference>, ()>::builder("parent")?
            .default_max_attempts(2)
            .handler(move |context, references| {
                let handles = handles.clone();
                async move {
                    let first = (context.metadata().attempt == 1) == identity_first;
                    let budget = Some(if context.metadata().attempt == 1 {
                        Duration::from_secs(2)
                    } else {
                        Duration::ZERO
                    });
                    for (kind, (reference, child)) in
                        references.into_iter().zip(handles).enumerate()
                    {
                        for call in 0..3 {
                            let context = context.clone();
                            let by_identity = if call == 1 { !first } else { first };
                            let result = match (by_identity, call == 2) {
                                (true, false) => {
                                    context
                                        .await_task_result_by_id::<Value>(
                                            &reference.queue_name,
                                            reference.task_id,
                                            budget,
                                        )
                                        .await
                                }
                                (true, true) => {
                                    context
                                        .await_task_result_by_id_named::<Value>(
                                            "observed",
                                            &reference.queue_name,
                                            reference.task_id,
                                            budget,
                                        )
                                        .await
                                }
                                (false, false) => context.await_task_result(&child, budget).await,
                                (false, true) => {
                                    context
                                        .await_task_result_named("observed", &child, budget)
                                        .await
                                }
                            };
                            match (kind, result) {
                                (0, Ok(value)) => assert_eq!(value, json!({"amount": 42})),
                                (1, Ok(value)) => assert_eq!(value, Value::Null),
                                (2, Err(Error::TaskFailed { task_id, failure })) => {
                                    assert_eq!(task_id, reference.task_id);
                                    assert_eq!(
                                        failure.expect("failure reason")["name"],
                                        "handler_error"
                                    );
                                }
                                (3, Err(Error::TaskCancelled { task_id })) => {
                                    assert_eq!(task_id.as_uuid(), reference.task_id)
                                }
                                (_, result) => panic!("unexpected child observation: {result:?}"),
                            }
                        }
                    }
                    if context.metadata().attempt == 1 {
                        return Err(Error::handler(Box::new(TestFailure)));
                    }
                    Ok(())
                }
            })
            .build();
        let router = Router::new().task(parent_task.clone())?;
        let parent = test.client.spawn(&parent_task, references).send().await?;
        work_batch(&test.client, &router, "default").await?;
        let checkpoints: BTreeMap<_, _> = test
            .client
            .get_checkpoints(
                "default",
                parent.result.task_id.as_uuid(),
                parent.result.run_id.as_uuid(),
            )
            .await?
            .into_iter()
            .collect();
        assert_eq!(checkpoints.len(), 12);
        for (index, child) in children.iter().enumerate() {
            let base = format!("$awaitTaskResult:{}", child.result.task_id);
            let named = if index == 0 {
                "observed".to_owned()
            } else {
                format!("observed#{}", index + 1)
            };
            for name in [base.clone(), format!("{base}#2"), named] {
                assert_eq!(checkpoints[&name], snapshots[index]);
            }
        }
        test.client
            .set_queue_policy(
                "children",
                QueuePolicyOptions {
                    cleanup_ttl: Some(PgInterval::from(Duration::ZERO)),
                    ..Default::default()
                },
            )
            .await?;
        let cleanup = test.client.cleanup_queue("children").await?;
        assert_eq!(cleanup.iter().map(|row| row.tasks_deleted).sum::<i32>(), 4);
        for child in children {
            assert!(
                test.client
                    .fetch_task_result("children", child.result.task_id.as_uuid())
                    .await?
                    .is_none()
            );
        }
        work_batch(&test.client, &router, "default").await?;
        parent
            .await_result(&test.client, Some(Duration::from_secs(2)))
            .await?;
    }
    Ok(())
}

/// Validates identity before replay without consuming checkpoint occurrences.
#[tokio::test]
async fn identity_waits_validate_queue_before_cached_result() -> TestResult {
    let test = setup().await?;
    let task = Task::<Uuid, Value>::builder("identity-validation")?
        .default_max_attempts(1)
        .handler(|context, child_id| async move {
            for queue in ["", "default"] {
                for named in [false, true] {
                    let result = if named {
                        context
                            .await_task_result_by_id_named::<Value>(
                                "cached",
                                queue,
                                child_id,
                                Some(Duration::ZERO),
                            )
                            .await
                    } else {
                        context
                            .await_task_result_by_id::<Value>(queue, child_id, Some(Duration::ZERO))
                            .await
                    };
                    if queue.is_empty() {
                        assert!(matches!(
                            result,
                            Err(Error::InvalidName { kind: "queue", .. })
                        ));
                    } else {
                        assert!(matches!(result, Err(Error::SameQueueWait)));
                    }
                }
            }
            let value: Value = context
                .await_task_result_by_id("removed_children", child_id, Some(Duration::ZERO))
                .await?;
            assert_eq!(value, Value::Null);
            context
                .await_task_result_by_id_named(
                    "cached",
                    "removed_children",
                    child_id,
                    Some(Duration::ZERO),
                )
                .await
        })
        .build();
    let router = Router::new().task(task.clone())?;
    let child_id = Uuid::now_v7();
    let parent = test.client.spawn(&task, child_id).send().await?;
    for name in ["cached".to_owned(), format!("$awaitTaskResult:{child_id}")] {
        test.client
            .set_checkpoint(
                "default",
                parent.result.task_id.as_uuid(),
                name,
                json!({"state": "completed", "result": null}),
                parent.result.run_id.as_uuid(),
                None,
            )
            .await?;
    }
    work_batch(&test.client, &router, "default").await?;
    assert_eq!(
        parent
            .await_result(&test.client, Some(Duration::from_secs(2)))
            .await?,
        Value::Null
    );
    Ok(())
}
