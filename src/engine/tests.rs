use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use super::*;
use crate::StepError;
use crate::context::{STEPS, SuspendPoint};
use crate::error::{StateError, SubscribeError};
use crate::metadata::{self, MetadataStatus, WorkflowMetadata};

use redb::ReadableDatabase as _;

use super::retention::cleanup_expired;

fn test_engine() -> Engine {
    Engine::builder().in_memory().build()
}

#[tokio::test]
async fn simple_workflow_completes() {
    async fn add(ctx: Context) -> Result<(), EngineError> {
        let a: i32 = ctx.step("a").run(async || Ok(1)).await?;
        let b: i32 = ctx.step("b").run(async || Ok(2)).await?;
        assert_eq!(a + b, 3);
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("add", add);
    engine.start().await.unwrap();

    let state = engine.invoke("add").await.unwrap().wait().await;
    assert_eq!(state, WorkflowState::Completed(None));
}

#[tokio::test]
async fn memoisation_on_resume() {
    let counter = Arc::new(AtomicU32::new(0));
    let attempts = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("memo", move |ctx: Context| {
        let c = Arc::clone(&c);
        let a = Arc::clone(&a);
        async move {
            let c2 = Arc::clone(&c);
            let _: String = ctx
                .step("s1")
                .run(async move || {
                    c2.fetch_add(1, Ordering::Relaxed);
                    Ok("hello".to_string())
                })
                .await?;
            let _: String = ctx
                .step("s2")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    if a.fetch_add(1, Ordering::Relaxed) == 0 {
                        return Err(StepError::retryable("transient"));
                    }
                    Ok("world".to_string())
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    // First invoke — s1 succeeds, s2 fails.
    let inv = engine.invoke("memo").await.unwrap();
    let instance_id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(state, WorkflowState::Failed(_)));
    assert_eq!(counter.load(Ordering::Relaxed), 2);

    // Resume — s1 is memoised, s2 retries and succeeds.
    counter.store(0, Ordering::Relaxed);
    let state = engine
        .resume("memo", &instance_id)
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(state, WorkflowState::Completed(None));
    assert_eq!(counter.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn step_error_produces_failed_state() {
    async fn failing(ctx: Context) -> Result<(), EngineError> {
        let _: String = ctx
            .step("fail")
            .run(async || Err(StepError::permanent("boom")))
            .await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("fail", failing);
    engine.start().await.unwrap();

    let state = engine.invoke("fail").await.unwrap().wait().await;
    assert!(matches!(state, WorkflowState::Failed(_)));
}

#[tokio::test]
async fn different_instances_have_separate_caches() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let _: i32 = ctx
                .step("x")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(1)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    engine.invoke("wf").await.unwrap().wait().await;
    engine.invoke("wf").await.unwrap().wait().await;

    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn invoke_before_start_fails() {
    async fn noop(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("noop", noop);

    let result = engine.invoke("noop").await;
    assert!(matches!(result, Err(EngineError::NotStarted)));
}

#[tokio::test]
async fn invoke_unknown_workflow_fails() {
    let mut engine = test_engine();
    engine.start().await.unwrap();

    let result = engine.invoke("nonexistent").await;
    assert!(matches!(result, Err(EngineError::WorkflowNotFound(_))));
}

#[tokio::test]
async fn wait_all_waits_for_completion() {
    let counter = Arc::new(AtomicU32::new(0));

    let c = Arc::clone(&counter);
    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let _: i32 = ctx
                .step("x")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(1)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    drop(engine.invoke("wf").await.unwrap());
    drop(engine.invoke("wf").await.unwrap());
    drop(engine.invoke("wf").await.unwrap());

    engine.wait_all().await;
    assert_eq!(counter.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn invoke_after_wait_all_fails() {
    async fn noop(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("noop", noop);
    engine.start().await.unwrap();

    engine.wait_all().await;

    let result = engine.invoke("noop").await;
    assert!(matches!(result, Err(EngineError::NotStarted)));
}

#[tokio::test]
async fn wait_all_with_no_active_tasks() {
    let mut engine = test_engine();
    engine.start().await.unwrap();

    engine.wait_all().await;
}

#[tokio::test]
async fn status_persisted_and_restored_on_resume() {
    let step_counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&step_counter);
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("status-wf", move |ctx: Context| {
        let c = Arc::clone(&c);
        let a = Arc::clone(&a);
        async move {
            ctx.set_status("step-one");
            let c2 = Arc::clone(&c);
            let _: String = ctx
                .step("s1")
                .run(async move || {
                    c2.fetch_add(1, Ordering::Relaxed);
                    Ok("one".to_string())
                })
                .await?;

            ctx.set_status("step-two");
            let _: String = ctx
                .step("s2")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    if a.fetch_add(1, Ordering::Relaxed) == 0 {
                        return Err(StepError::retryable("transient"));
                    }
                    Ok("two".to_string())
                })
                .await?;

            Ok(())
        }
    });
    engine.start().await.unwrap();

    // First run — s1 succeeds, s2 fails.
    let inv = engine.invoke("status-wf").await.unwrap();
    let instance_id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(state, WorkflowState::Failed(_)));
    assert_eq!(step_counter.load(Ordering::Relaxed), 2);

    // Resume — s1 is memoised, s2 retries and succeeds.
    step_counter.store(0, Ordering::Relaxed);
    let state = engine
        .resume("status-wf", &instance_id)
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(state, WorkflowState::Completed(Some("step-two".into())));
    // Only s2 re-executed; s1 was served from cache (with its persisted status).
    assert_eq!(step_counter.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn subscribers_not_notified_during_replay() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("silent-replay", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            ctx.set_status("phase-1");
            let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;

            ctx.set_status("phase-2");
            let _: i32 = ctx
                .step("s2")
                .run(async move || {
                    if a.fetch_add(1, Ordering::Relaxed) == 0 {
                        return Err(StepError::retryable("fail first time"));
                    }
                    Ok(2)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    // First invoke — s1 ok, s2 fails.
    let inv = engine.invoke("silent-replay").await.unwrap();
    let instance_id = inv.instance_id().to_string();
    inv.wait().await;

    // Resume and watch for notifications.
    let mut inv = engine.resume("silent-replay", &instance_id).await.unwrap();
    let rx = inv.status();

    // Mark the current value as seen so changed() only fires on new updates.
    rx.borrow_and_update();

    let mut notifications = Vec::new();
    loop {
        if rx.changed().await.is_err() {
            break;
        }
        let state = rx.borrow_and_update().clone();
        let terminal = state.is_terminal();
        notifications.push(state);
        if terminal {
            break;
        }
    }

    // Replayed statuses ("phase-1", "phase-2") should NOT appear as
    // notifications. We expect only the live "phase-2" set_status (after
    // replay ends at the s2 cache miss) and the terminal Completed.
    assert!(
        !notifications
            .iter()
            .any(|s| s == &WorkflowState::InProgress("phase-1".into())),
        "replayed status 'phase-1' should not have notified subscribers, got: {notifications:?}"
    );
}

#[tokio::test]
async fn metadata_completed_on_success() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
    assert_eq!(*meta.status(), MetadataStatus::Completed(None));
    assert!(meta.completed_at().is_some());
}

#[tokio::test]
async fn metadata_failed_on_error() {
    async fn failing(ctx: Context) -> Result<(), EngineError> {
        let _: String = ctx
            .step("fail")
            .run(async || Err(StepError::permanent("boom")))
            .await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("fail", failing);
    engine.start().await.unwrap();

    let inv = engine.invoke("fail").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let meta = engine.get_metadata("fail", &id).unwrap().unwrap();
    assert!(matches!(meta.status(), MetadataStatus::Failed(msg) if msg.contains("boom")),);
    assert!(meta.completed_at().is_some());
}

#[tokio::test]
async fn metadata_updated_after_resume() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("retry-wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    if a.fetch_add(1, Ordering::Relaxed) == 0 {
                        return Err(StepError::retryable("transient"));
                    }
                    Ok(1)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke("retry-wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let meta = engine.get_metadata("retry-wf", &id).unwrap().unwrap();
    assert!(matches!(meta.status(), MetadataStatus::Failed(_)));

    engine.resume("retry-wf", &id).await.unwrap().wait().await;

    let meta = engine.get_metadata("retry-wf", &id).unwrap().unwrap();
    assert_eq!(*meta.status(), MetadataStatus::Completed(None));
}

#[tokio::test]
async fn list_instances_returns_all() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    engine.invoke("wf").await.unwrap().wait().await;
    engine.invoke("wf").await.unwrap().wait().await;
    engine.invoke("wf").await.unwrap().wait().await;

    let instances = engine.list_instances("wf").unwrap();
    assert_eq!(instances.len(), 3);
}

#[tokio::test]
async fn list_instances_filters_by_workflow_name() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("alpha", wf);
    engine.register("beta", wf);
    engine.start().await.unwrap();

    engine.invoke("alpha").await.unwrap().wait().await;
    engine.invoke("alpha").await.unwrap().wait().await;
    engine.invoke("beta").await.unwrap().wait().await;

    let alpha_instances = engine.list_instances("alpha").unwrap();
    assert_eq!(alpha_instances.len(), 2);

    let beta_instances = engine.list_instances("beta").unwrap();
    assert_eq!(beta_instances.len(), 1);

    let none_instances = engine.list_instances("nonexistent").unwrap();
    assert!(none_instances.is_empty());
}

#[tokio::test]
async fn get_metadata_returns_none_for_unknown() {
    let mut engine = test_engine();
    engine.start().await.unwrap();

    let meta = engine.get_metadata("wf", "no-such-id").unwrap();
    assert!(meta.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_consistent_after_wait_multi_thread() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
    assert_eq!(*meta.status(), MetadataStatus::Completed(None));
}

#[tokio::test]
async fn metadata_correct_after_wait_all() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let id1 = engine.invoke("wf").await.unwrap().instance_id().to_string();
    let id2 = engine.invoke("wf").await.unwrap().instance_id().to_string();

    engine.wait_all().await;

    let m1 = engine.get_metadata("wf", &id1).unwrap().unwrap();
    let m2 = engine.get_metadata("wf", &id2).unwrap().unwrap();
    assert_eq!(*m1.status(), MetadataStatus::Completed(None));
    assert_eq!(*m2.status(), MetadataStatus::Completed(None));
}

#[tokio::test]
async fn list_instances_on_fresh_engine() {
    let engine = test_engine();
    let instances = engine.list_instances("anything").unwrap();
    assert!(instances.is_empty());
}

#[tokio::test]
async fn suspend_then_signal_completes() {
    const WAIT: SuspendPoint<String> = SuspendPoint::new("wait:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        let payload: String = ctx.suspend(&WAIT).await?;
        assert_eq!(payload, "hello");
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert_eq!(
        state,
        WorkflowState::Suspended {
            key: "wait:v1".into(),
            status: "wait:v1".into()
        }
    );

    let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
    assert!(
        matches!(meta.status(), MetadataStatus::Suspended { status, .. } if status == "wait:v1"),
    );

    let state = engine
        .signal("wf", &id, &WAIT, "hello".to_string())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(state, WorkflowState::Completed(None));

    let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
    assert_eq!(*meta.status(), MetadataStatus::Completed(None));
}

#[tokio::test]
async fn suspend_with_custom_status() {
    const APPROVAL: SuspendPoint<bool> = SuspendPoint::new("approval:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: bool = ctx
            .suspend(&APPROVAL)
            .status("Waiting for manager approval")
            .await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert_eq!(
        state,
        WorkflowState::Suspended {
            key: "approval:v1".into(),
            status: "Waiting for manager approval".into()
        }
    );

    let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
    assert!(matches!(
        meta.status(),
        MetadataStatus::Suspended { status, .. } if status == "Waiting for manager approval"
    ));

    let state = engine
        .signal("wf", &id, &APPROVAL, true)
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(state, WorkflowState::Completed(None));
}

#[tokio::test]
async fn memoised_steps_preserved_across_suspend() {
    const GATE: SuspendPoint<String> = SuspendPoint::new("gate:v1");

    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let c2 = Arc::clone(&c);
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    c2.fetch_add(1, Ordering::Relaxed);
                    Ok(42)
                })
                .await?;
            let _: String = ctx.suspend(&GATE).await?;
            let _: i32 = ctx
                .step("s2")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(99)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    counter.store(0, Ordering::Relaxed);
    let state = engine
        .signal("wf", &id, &GATE, "go".to_string())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(state, WorkflowState::Completed(None));
    // s1 memoised (0 executions), s2 runs (1 execution)
    assert_eq!(counter.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn resume_without_signal_stays_suspended() {
    const WAIT: SuspendPoint<String> = SuspendPoint::new("wait:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: String = ctx.suspend(&WAIT).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert_eq!(
        state,
        WorkflowState::Suspended {
            key: "wait:v1".into(),
            status: "wait:v1".into()
        }
    );

    // Resume without signal — should still be suspended.
    let state = engine.resume("wf", &id).await.unwrap().wait().await;
    assert_eq!(
        state,
        WorkflowState::Suspended {
            key: "wait:v1".into(),
            status: "wait:v1".into()
        }
    );
}

#[tokio::test]
async fn step_rejects_suspended_entry() {
    const ACTION: SuspendPoint<String> = SuspendPoint::new("action:v1");

    let use_step = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let use_step2 = Arc::clone(&use_step);

    let wf = move |ctx: Context| {
        let use_step = Arc::clone(&use_step2);
        async move {
            if use_step.load(Ordering::Acquire) {
                let _: String = ctx.step("action:v1").run(async || Ok("x".into())).await?;
            } else {
                let _: String = ctx.suspend(&ACTION).await?;
            }
            Ok(())
        }
    };

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    // V1: workflow suspends at "action:v1".
    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert_eq!(
        state,
        WorkflowState::Suspended {
            key: "action:v1".into(),
            status: "action:v1".into()
        }
    );

    // V2: same key is now a regular step.
    use_step.store(true, Ordering::Release);
    let state = engine.resume("wf", &id).await.unwrap().wait().await;
    assert!(
        matches!(state, WorkflowState::Failed(ref msg) if msg.contains("suspended entry")),
        "expected SuspendedStepConflict, got: {state:?}"
    );
}

#[tokio::test]
async fn multiple_suspend_points() {
    const FIRST: SuspendPoint<i32> = SuspendPoint::new("first:v1");
    const SECOND: SuspendPoint<i32> = SuspendPoint::new("second:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let a: i32 = ctx.suspend(&FIRST).await?;
        let b: i32 = ctx.suspend(&SECOND).await?;
        assert_eq!(a + b, 3);
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert_eq!(
        state,
        WorkflowState::Suspended {
            key: "first:v1".into(),
            status: "first:v1".into()
        }
    );

    // Signal first suspend point.
    let state = engine
        .signal("wf", &id, &FIRST, 1i32)
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(
        state,
        WorkflowState::Suspended {
            key: "second:v1".into(),
            status: "second:v1".into()
        }
    );

    // Signal second suspend point.
    let state = engine
        .signal("wf", &id, &SECOND, 2i32)
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(state, WorkflowState::Completed(None));
}

#[tokio::test]
async fn suspend_after_failed_step_on_resume() {
    const GATE: SuspendPoint<String> = SuspendPoint::new("gate:v1");

    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    if a.fetch_add(1, Ordering::Relaxed) == 0 {
                        return Err(StepError::retryable("transient"));
                    }
                    Ok(1)
                })
                .await?;
            let _: String = ctx.suspend(&GATE).await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    // First invoke — s1 fails.
    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(state, WorkflowState::Failed(_)));

    // Resume — s1 succeeds, hits suspend.
    let state = engine.resume("wf", &id).await.unwrap().wait().await;
    assert_eq!(
        state,
        WorkflowState::Suspended {
            key: "gate:v1".into(),
            status: "gate:v1".into()
        }
    );

    // Signal — completes.
    let state = engine
        .signal("wf", &id, &GATE, "done".to_string())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(state, WorkflowState::Completed(None));
}

#[tokio::test]
async fn step_without_status_stores_none() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register("no-status", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            // No set_status call — status should be None in the record.
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(42)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke("no-status").await.unwrap();
    let instance_id = inv.instance_id().to_string();
    inv.wait().await;

    // Resume — s1 is memoised, execution counter stays at 1.
    counter.store(0, Ordering::Relaxed);
    let state = engine
        .resume("no-status", &instance_id)
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(state, WorkflowState::Completed(None));
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_fires_and_completes_workflow() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register("timer-wf", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let c2 = Arc::clone(&c);
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    c2.fetch_add(1, Ordering::Relaxed);
                    Ok(1)
                })
                .await?;
            ctx.timer("wait:v1", Duration::ZERO)?;
            let _: i32 = ctx
                .step("s2")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(2)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke("timer-wf").await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(state, WorkflowState::Suspended { .. }));
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // Wait for the poller to fire the timer.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let meta = engine.get_metadata("timer-wf", &id).unwrap().unwrap();
        if meta.status().is_terminal() {
            assert_eq!(*meta.status(), MetadataStatus::Completed(None));
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timer did not fire within 5 seconds"
        );
    }
    // s1 ran on first invoke, s2 ran after timer — s1 memoised on resume.
    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_memoised_on_resume() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register("timer-memo", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(1)
                })
                .await?;
            ctx.timer("delay:v1", Duration::ZERO)?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke("timer-memo").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // Wait for timer to fire.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let meta = engine.get_metadata("timer-memo", &id).unwrap().unwrap();
        if meta.status().is_terminal() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "timer did not fire");
    }

    // Resume — both s1 and timer are memoised.
    counter.store(0, Ordering::Relaxed);
    let state = engine.resume("timer-memo", &id).await.unwrap().wait().await;
    assert_eq!(state, WorkflowState::Completed(None));
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_skipped_when_workflow_not_suspended() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("timer-fail", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    if a.fetch_add(1, Ordering::Relaxed) == 0 {
                        return Err(StepError::retryable("transient"));
                    }
                    Ok(1)
                })
                .await?;
            ctx.timer("delay:v1", Duration::ZERO)?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    // First invoke — s1 fails before reaching timer.
    let inv = engine.invoke("timer-fail").await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(state, WorkflowState::Failed(_)));

    // Resume — s1 succeeds, hits timer.
    let state = engine.resume("timer-fail", &id).await.unwrap().wait().await;
    assert!(matches!(state, WorkflowState::Suspended { .. }));

    // Wait for timer to fire and complete.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let meta = engine.get_metadata("timer-fail", &id).unwrap().unwrap();
        if meta.status().is_terminal() {
            assert_eq!(*meta.status(), MetadataStatus::Completed(None));
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "timer did not fire");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_with_steps_before_and_after() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register("multi-timer", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let c2 = Arc::clone(&c);
            let _: i32 = ctx
                .step("before")
                .run(async move || {
                    c2.fetch_add(1, Ordering::Relaxed);
                    Ok(1)
                })
                .await?;
            ctx.timer("t1:v1", Duration::ZERO)?;
            let c3 = Arc::clone(&c);
            let _: i32 = ctx
                .step("between")
                .run(async move || {
                    c3.fetch_add(1, Ordering::Relaxed);
                    Ok(2)
                })
                .await?;
            ctx.timer("t2:v1", Duration::ZERO)?;
            let _: i32 = ctx
                .step("after")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(3)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke("multi-timer").await.unwrap();
    let id = inv.instance_id().to_string();
    // First timer suspends.
    inv.wait().await;
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // Wait for first timer and then second timer to fire.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let meta = engine.get_metadata("multi-timer", &id).unwrap().unwrap();
        if meta.status().is_terminal() {
            assert_eq!(*meta.status(), MetadataStatus::Completed(None));
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timers did not complete"
        );
    }
    assert_eq!(counter.load(Ordering::Relaxed), 3);
}

#[tokio::test(start_paused = true)]
async fn step_with_timeout_completes() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let v: i32 = ctx
            .step("s1")
            .timeout(Duration::from_secs(5))
            .run(async || Ok(42))
            .await?;
        assert_eq!(v, 42);
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    assert_eq!(state, WorkflowState::Completed(None));
}

#[tokio::test(start_paused = true)]
async fn step_timeout_exceeds_deadline() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx
            .step("slow")
            .timeout(Duration::from_millis(100))
            .run(async || {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(1)
            })
            .await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    assert!(matches!(state, WorkflowState::Failed(msg) if msg.contains("timed out")));
}

#[tokio::test(start_paused = true)]
async fn timed_out_step_not_persisted() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("flaky")
                .timeout(Duration::from_millis(100))
                .run(async move || {
                    let n = a.fetch_add(1, Ordering::Relaxed);
                    if n == 0 {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                    Ok(42)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(state, WorkflowState::Failed(_)));
    assert_eq!(attempts.load(Ordering::Relaxed), 1);

    // Resume — step re-executes (not cached) and succeeds this time.
    let state = engine.resume("wf", &id).await.unwrap().wait().await;
    assert_eq!(state, WorkflowState::Completed(None));
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn timeout_skipped_on_cache_hit() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let _: i32 = ctx
                .step("s1")
                .timeout(Duration::from_nanos(1))
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(1)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // Resume with impossibly short timeout — cache hit returns instantly.
    counter.store(0, Ordering::Relaxed);
    let state = engine.resume("wf", &id).await.unwrap().wait().await;
    assert_eq!(state, WorkflowState::Completed(None));
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn timeout_with_borrowing_closure() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let local_data = String::from("borrowed");
        let local_data_clone = local_data.clone();
        let v: String = ctx
            .step("borrow")
            .timeout(Duration::from_secs(5))
            .run(async move || Ok(local_data_clone.clone()))
            .await?;
        assert_eq!(v, "borrowed");
        assert_eq!(local_data, "borrowed");
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    assert_eq!(state, WorkflowState::Completed(None));
}

// --- Key validation (S1) tests ---

#[test]
#[should_panic(expected = "workflow name must not contain '/'")]
fn register_rejects_slash_in_name() {
    let mut engine = test_engine();
    engine.register("bad/name", |_ctx: Context| async { Ok(()) });
}

#[tokio::test]
async fn invoke_rejects_slash_in_name() {
    async fn wf(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let Err(err) = engine.invoke("bad/name").await else {
        panic!("expected InvalidKey error");
    };
    assert!(matches!(
        err,
        EngineError::InvalidKey {
            label: "workflow_name",
            ..
        }
    ));
}

#[tokio::test]
async fn resume_rejects_slash_in_workflow_name() {
    async fn wf(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let Err(err) = engine.resume("bad/name", "id-1").await else {
        panic!("expected InvalidKey error");
    };
    assert!(matches!(
        err,
        EngineError::InvalidKey {
            label: "workflow_name",
            ..
        }
    ));
}

#[tokio::test]
async fn resume_rejects_slash_in_instance_id() {
    async fn wf(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let Err(err) = engine.resume("wf", "bad/id").await else {
        panic!("expected InvalidKey error");
    };
    assert!(matches!(
        err,
        EngineError::InvalidKey {
            label: "instance_id",
            ..
        }
    ));
}

#[tokio::test]
async fn signal_rejects_slash_in_name_or_id() {
    const WAIT: SuspendPoint<bool> = SuspendPoint::new("wait:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: bool = ctx.suspend(&WAIT).await?;
        Ok(())
    }
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let Err(err) = engine.signal("bad/name", &id, &WAIT, true).await else {
        panic!("expected InvalidKey error");
    };
    assert!(matches!(
        err,
        EngineError::InvalidKey {
            label: "workflow_name",
            ..
        }
    ));

    let Err(err) = engine.signal("wf", "bad/id", &WAIT, true).await else {
        panic!("expected InvalidKey error");
    };
    assert!(matches!(
        err,
        EngineError::InvalidKey {
            label: "instance_id",
            ..
        }
    ));
}

#[test]
#[should_panic(expected = "step key must not contain '/'")]
fn step_rejects_slash_in_key() {
    use std::sync::atomic::AtomicU64;

    use redb::Database;
    use redb::backends::InMemoryBackend;

    let db = Arc::new(
        Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap(),
    );
    let (tx, _rx) = watch::channel(WorkflowState::Started);
    let ctx = Context::new(
        "wf".into(),
        "id".into(),
        db,
        tx,
        Arc::new(AtomicU64::new(0)),
        None,
    );
    let _ = ctx.step("bad/key");
}

#[test]
#[should_panic(expected = "suspend point key must not contain '/'")]
fn suspend_rejects_slash_in_key() {
    let _: SuspendPoint<bool> = SuspendPoint::new("bad/key");
}

#[tokio::test]
async fn timer_rejects_slash_in_key() {
    let mut engine = test_engine();
    engine.register("wf", |ctx: Context| async move {
        ctx.timer("bad/key", Duration::from_secs(1))?;
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    assert!(matches!(state, WorkflowState::Failed(_)));
}

#[test]
#[should_panic(expected = "step keys starting with '_' are reserved")]
fn step_rejects_reserved_prefix() {
    use std::sync::atomic::AtomicU64;

    use redb::Database;
    use redb::backends::InMemoryBackend;

    let db = Arc::new(
        Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap(),
    );
    let (tx, _rx) = watch::channel(WorkflowState::Started);
    let ctx = Context::new(
        "wf".into(),
        "id".into(),
        db,
        tx,
        Arc::new(AtomicU64::new(0)),
        None,
    );
    let _ = ctx.step("_reserved");
}

#[test]
#[should_panic(expected = "suspend point key must not start with '_'")]
fn suspend_rejects_reserved_prefix() {
    let _: SuspendPoint<bool> = SuspendPoint::new("_reserved");
}

#[tokio::test]
async fn timer_rejects_reserved_prefix() {
    let mut engine = test_engine();
    engine.register("wf", |ctx: Context| async move {
        ctx.timer("_reserved", Duration::from_secs(1))?;
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    assert!(matches!(state, WorkflowState::Failed(_)));
}

#[tokio::test]
async fn signal_rejects_when_step_does_not_exist() {
    const WAIT: SuspendPoint<bool> = SuspendPoint::new("wait:v1");
    const WRONG: SuspendPoint<bool> = SuspendPoint::new("wrong-key:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: bool = ctx.suspend(&WAIT).await?;
        Ok(())
    }
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let Err(err) = engine.signal("wf", &id, &WRONG, true).await else {
        panic!("expected SignalRejected error");
    };
    assert!(
        matches!(err, EngineError::SignalRejected { ref key, .. } if key == "wrong-key:v1"),
        "expected SignalRejected, got {err:?}"
    );
}

#[tokio::test]
async fn signal_rejects_already_completed_step() {
    const GATE: SuspendPoint<bool> = SuspendPoint::new("gate:v1");
    const GATE2: SuspendPoint<bool> = SuspendPoint::new("gate2:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: bool = ctx.suspend(&GATE).await?;
        let _: bool = ctx.suspend(&GATE2).await?;
        Ok(())
    }
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // First signal succeeds — step is Suspended.
    let inv = engine.signal("wf", &id, &GATE, true).await.unwrap();
    inv.wait().await;

    // Second signal to same step fails — it was already claimed.
    let Err(err) = engine.signal("wf", &id, &GATE, true).await else {
        panic!("expected SignalSuperseded error");
    };
    assert!(
        matches!(err, EngineError::SignalSuperseded { ref key } if key == "gate:v1"),
        "expected SignalSuperseded for completed step, got {err:?}"
    );
}

#[tokio::test]
async fn signal_rejects_pre_completing_future_step() {
    const FIRST: SuspendPoint<bool> = SuspendPoint::new("first:v1");
    const SECOND: SuspendPoint<String> = SuspendPoint::new("second:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: bool = ctx.suspend(&FIRST).await?;
        let _: String = ctx.suspend(&SECOND).await?;
        Ok(())
    }
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Attempt to signal a step the workflow hasn't reached yet.
    let Err(err) = engine
        .signal("wf", &id, &SECOND, "sneaky".to_string())
        .await
    else {
        panic!("expected SignalRejected error");
    };
    assert!(
        matches!(err, EngineError::SignalRejected { ref key, .. } if key == "second:v1"),
        "expected SignalRejected for future step, got {err:?}"
    );
}

#[tokio::test]
async fn signal_type_mismatch_returns_error() {
    const GATE_I32: SuspendPoint<i32> = SuspendPoint::new("gate:v1");
    const GATE_STRING: SuspendPoint<String> = SuspendPoint::new("gate:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.suspend(&GATE_I32).await?;
        Ok(())
    }
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Signal with String instead of the expected i32 (simulates cross-version mismatch).
    let inv = engine
        .signal("wf", &id, &GATE_STRING, "wrong type".to_string())
        .await
        .unwrap();
    let state = inv.wait().await;
    assert!(
        matches!(state, WorkflowState::Failed(ref msg) if msg.contains("type mismatch")),
        "expected TypeMismatch failure, got {state:?}"
    );
}

#[tokio::test]
async fn signal_type_mismatch_caught_for_binary_compatible_types() {
    const GATE_I32: SuspendPoint<i32> = SuspendPoint::new("gate:v1");
    const GATE_U32: SuspendPoint<u32> = SuspendPoint::new("gate:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.suspend(&GATE_I32).await?;
        Ok(())
    }
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Signal with u32 instead of i32 — same binary layout, different type name.
    let inv = engine.signal("wf", &id, &GATE_U32, 42_u32).await.unwrap();
    let state = inv.wait().await;
    assert!(
        matches!(state, WorkflowState::Failed(ref msg) if msg.contains("type mismatch")),
        "expected TypeMismatch failure for u32 vs i32, got {state:?}"
    );
}

#[tokio::test]
async fn serialize_deserialize_step_round_trip() {
    use crate::context::{StepData, deserialize_step, serialize_step};

    // Completed round-trip.
    let data: StepData<String> = StepData::Completed {
        result: "hello".to_string(),
        status: Some("done".to_string()),
    };
    let bytes = serialize_step(&data, "test-key").unwrap();
    let recovered: StepData<String> = deserialize_step(&bytes, "test-key").unwrap();
    match recovered {
        StepData::Completed { result, status } => {
            assert_eq!(result, "hello");
            assert_eq!(status.as_deref(), Some("done"));
        }
        StepData::Suspended | StepData::Failed { .. } => panic!("expected Completed"),
    }

    // Suspended round-trip.
    let data = StepData::<u64>::Suspended;
    let bytes = serialize_step(&data, "test-key").unwrap();
    let recovered: StepData<u64> = deserialize_step(&bytes, "test-key").unwrap();
    assert!(matches!(recovered, StepData::Suspended));
}

#[tokio::test]
async fn type_mismatch_error_contains_type_names() {
    use crate::context::{StepData, deserialize_step, serialize_step};

    let data: StepData<String> = StepData::Completed {
        result: "hello".to_string(),
        status: None,
    };
    let bytes = serialize_step(&data, "k").unwrap();

    let err = deserialize_step::<i32>(&bytes, "k").unwrap_err();
    match err {
        EngineError::TypeMismatch {
            key,
            expected,
            found,
        } => {
            assert_eq!(key, "k");
            assert!(
                expected.contains("i32"),
                "expected contains i32, got {expected}"
            );
            assert!(
                found.contains("String"),
                "found contains String, got {found}"
            );
        }
        other => panic!("expected TypeMismatch, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn double_signal_same_step_second_superseded() {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    const GATE: SuspendPoint<bool> = SuspendPoint::new("gate:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: bool = ctx.suspend(&GATE).await?;
        COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    COUNTER.store(0, Ordering::Relaxed);
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let (r1, r2) = tokio::join!(
        engine.signal("wf", &id, &GATE, true),
        engine.signal("wf", &id, &GATE, true),
    );

    let mut successes = 0u32;
    let mut superseded = 0u32;
    for r in [r1, r2] {
        match r {
            Ok(inv) => {
                inv.wait().await;
                successes += 1;
            }
            Err(EngineError::SignalSuperseded { .. }) => superseded += 1,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    assert_eq!(successes, 1, "exactly one signal should succeed");
    assert_eq!(superseded, 1, "exactly one signal should be superseded");

    engine.wait_all().await;
    assert_eq!(
        COUNTER.load(Ordering::Relaxed),
        1,
        "workflow should run exactly once after signal"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_after_signal_already_claimed() {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    const WAIT: SuspendPoint<()> = SuspendPoint::new("wait:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        ctx.timer("wait:v1", Duration::from_secs(60))?;
        COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    COUNTER.store(0, Ordering::Relaxed);
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Manually signal the timer step before the timer fires.
    let inv = engine.signal("wf", &id, &WAIT, ()).await.unwrap();
    inv.wait().await;

    engine.wait_all().await;
    assert_eq!(
        COUNTER.load(Ordering::Relaxed),
        1,
        "workflow should run exactly once"
    );

    let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
    assert_eq!(*meta.status(), MetadataStatus::Completed(None));
}

#[tokio::test(flavor = "multi_thread")]
async fn signal_timer_tracked_by_wait_all() {
    static COUNTER: AtomicU32 = AtomicU32::new(0);

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        ctx.timer("tick:v1", Duration::from_secs(0))?;
        COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    COUNTER.store(0, Ordering::Relaxed);
    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let _id = inv.instance_id().to_string();
    inv.wait().await;

    // Let the timer fire and resume. wait_all should wait for the
    // timer-resumed workflow (C1 fix verification).
    tokio::time::sleep(Duration::from_secs(2)).await;
    engine.wait_all().await;

    assert_eq!(
        COUNTER.load(Ordering::Relaxed),
        1,
        "timer-resumed workflow tracked by wait_all"
    );
}

// ── Retry tests ──────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn retryable_error_retries_then_exhausts() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .retry(crate::RetryPolicy::fixed(2, Duration::from_millis(10)))
                .run(async move || {
                    a.fetch_add(1, Ordering::Relaxed);
                    Err(StepError::retryable("boom"))
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    assert!(matches!(state, WorkflowState::Failed(msg) if msg.contains("3 attempts")));
    assert_eq!(attempts.load(Ordering::Relaxed), 3);
}

#[tokio::test(start_paused = true)]
async fn permanent_error_skips_retry() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .retry(crate::RetryPolicy::fixed(3, Duration::from_millis(10)))
                .run(async move || {
                    a.fetch_add(1, Ordering::Relaxed);
                    Err(StepError::permanent("fatal"))
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    assert!(matches!(state, WorkflowState::Failed(msg) if msg.contains("fatal")));
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn step_succeeds_on_second_attempt() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let v: i32 = ctx
                .step("s1")
                .retry(crate::RetryPolicy::fixed(3, Duration::from_millis(10)))
                .run(async move || {
                    if a.fetch_add(1, Ordering::Relaxed) == 0 {
                        return Err(StepError::retryable("transient"));
                    }
                    Ok(42)
                })
                .await?;
            assert_eq!(v, 42);
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    assert_eq!(state, WorkflowState::Completed(None));
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn exponential_backoff_delays() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .retry(crate::RetryPolicy::exponential(3, Duration::from_secs(1)))
                .run(async move || {
                    a.fetch_add(1, Ordering::Relaxed);
                    Err(StepError::retryable("fail"))
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let start = tokio::time::Instant::now();
    engine.invoke("wf").await.unwrap().wait().await;
    let elapsed = start.elapsed();

    assert_eq!(attempts.load(Ordering::Relaxed), 4);
    // 1s + 2s + 4s = 7s total backoff
    assert!(elapsed >= Duration::from_secs(7));
}

#[tokio::test(start_paused = true)]
async fn engine_default_retry_applies() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = Engine::builder()
        .in_memory()
        .default_retry(crate::RetryPolicy::fixed(2, Duration::from_millis(10)))
        .build();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    a.fetch_add(1, Ordering::Relaxed);
                    Err(StepError::retryable("boom"))
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    engine.invoke("wf").await.unwrap().wait().await;
    assert_eq!(attempts.load(Ordering::Relaxed), 3);
}

#[tokio::test(start_paused = true)]
async fn per_step_retry_overrides_default() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = Engine::builder()
        .in_memory()
        .default_retry(crate::RetryPolicy::fixed(5, Duration::from_millis(10)))
        .build();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .retry(crate::RetryPolicy::fixed(1, Duration::from_millis(10)))
                .run(async move || {
                    a.fetch_add(1, Ordering::Relaxed);
                    Err(StepError::retryable("boom"))
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    engine.invoke("wf").await.unwrap().wait().await;
    // Per-step policy: 1 retry = 2 total attempts (not 6 from the default).
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn no_retry_overrides_default() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = Engine::builder()
        .in_memory()
        .default_retry(crate::RetryPolicy::fixed(3, Duration::from_millis(10)))
        .build();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .no_retry()
                .run(async move || {
                    a.fetch_add(1, Ordering::Relaxed);
                    Err(StepError::retryable("boom"))
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    engine.invoke("wf").await.unwrap().wait().await;
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn dead_letter_persisted_and_resume_re_executes() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .retry(crate::RetryPolicy::fixed(1, Duration::from_millis(10)))
                .run(async move || {
                    let n = a.fetch_add(1, Ordering::Relaxed);
                    if n < 4 {
                        return Err(StepError::retryable("not yet"));
                    }
                    Ok(100)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    // First invoke: 2 attempts (1 + 1 retry), both fail → RetriesExhausted.
    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(state, WorkflowState::Failed(_)));
    assert_eq!(attempts.load(Ordering::Relaxed), 2);

    // Resume: fresh retry budget, 2 more attempts, both fail again.
    let state = engine.resume("wf", &id).await.unwrap().wait().await;
    assert!(matches!(state, WorkflowState::Failed(_)));
    assert_eq!(attempts.load(Ordering::Relaxed), 4);

    // Resume again: first attempt succeeds (n=4 >= 4).
    let state = engine.resume("wf", &id).await.unwrap().wait().await;
    assert_eq!(state, WorkflowState::Completed(None));
}

#[tokio::test(start_paused = true)]
async fn timeout_applies_per_attempt() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .timeout(Duration::from_millis(50))
                .retry(crate::RetryPolicy::fixed(2, Duration::from_millis(10)))
                .run(async move || {
                    let n = a.fetch_add(1, Ordering::Relaxed);
                    if n == 0 {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                    Ok(1)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    // First attempt times out (StepTimeout propagates immediately,
    // bypassing retry). Timeout is not retried.
    assert!(matches!(state, WorkflowState::Failed(msg) if msg.contains("timed out")));
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn memoised_step_skips_retry() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = Engine::builder()
        .in_memory()
        .default_retry(crate::RetryPolicy::fixed(3, Duration::from_millis(1)))
        .build();
    engine.register("wf", move |ctx: Context| {
        let a = Arc::clone(&a);
        async move {
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    a.fetch_add(1, Ordering::Relaxed);
                    Ok(1)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Resume — s1 is memoised, closure should NOT run again.
    engine.resume("wf", &id).await.unwrap().wait().await;
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn retryable_without_policy_behaves_like_step_failed() {
    let mut engine = test_engine();
    engine.register("wf", |ctx: Context| async move {
        let _: i32 = ctx
            .step("s1")
            .run(async || Err(StepError::retryable("boom")))
            .await?;
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    // No retry policy → StepFailed (not RetriesExhausted).
    assert!(
        matches!(state, WorkflowState::Failed(msg) if msg.contains("boom") && !msg.contains("attempts"))
    );
}

// ── input payload tests ──────────────────────────────────────

#[tokio::test]
async fn invoke_with_input_delivers_payload() {
    let mut engine = test_engine();
    engine.register("wf", |ctx: Context| async move {
        let val: i32 = ctx.input::<i32>()?.unwrap();
        assert_eq!(val, 42);
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine
        .invoke("wf")
        .input(42_i32)
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(state, WorkflowState::Completed(None));
}

#[tokio::test]
async fn invoke_without_input_backward_compatible() {
    let mut engine = test_engine();
    engine.register("wf", |ctx: Context| async move {
        let _: String = ctx
            .step("s:v1")
            .run(async || Ok("hello".to_string()))
            .await?;
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    assert_eq!(state, WorkflowState::Completed(None));
}

#[tokio::test]
async fn input_returns_none_when_not_provided() {
    let mut engine = test_engine();
    engine.register("wf", |ctx: Context| async move {
        let val = ctx.input::<String>()?;
        assert!(val.is_none());
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine.invoke("wf").await.unwrap().wait().await;
    assert_eq!(state, WorkflowState::Completed(None));
}

#[tokio::test]
async fn input_type_mismatch() {
    let mut engine = test_engine();
    engine.register("wf", |ctx: Context| async move {
        let _val = ctx.input::<i32>()?;
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine
        .invoke("wf")
        .input("not an i32".to_string())
        .await
        .unwrap()
        .wait()
        .await;
    assert!(matches!(state, WorkflowState::Failed(msg) if msg.contains("type mismatch")));
}

#[tokio::test]
async fn input_preserved_across_resume() {
    let call_count = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&call_count);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let name: String = ctx.input::<String>()?.unwrap();
            let attempt = c.fetch_add(1, Ordering::Relaxed);
            if attempt == 0 {
                return Err(EngineError::step_failed(
                    "fail:v1",
                    "deliberate failure",
                    false,
                ));
            }
            let _: String = ctx
                .step("greet:v1")
                .run(async move || Ok(format!("Hello, {name}!")))
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine
        .invoke("wf")
        .input("Alice".to_string())
        .await
        .unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(state, WorkflowState::Failed(_)));

    let state = engine.resume("wf", &id).await.unwrap().wait().await;
    assert_eq!(state, WorkflowState::Completed(None));
    assert_eq!(call_count.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn input_coexists_with_suspend_and_signal() {
    const APPROVE: SuspendPoint<bool> = SuspendPoint::new("approve:v1");

    let mut engine = test_engine();
    engine.register("wf", |ctx: Context| async move {
        let prefix: String = ctx.input::<String>()?.unwrap();
        let approval: bool = ctx.suspend(&APPROVE).await?;
        assert!(approval);
        let _: String = ctx
            .step("format:v1")
            .run(async move || Ok(format!("{prefix}: approved")))
            .await?;
        Ok(())
    });
    engine.start().await.unwrap();

    let inv = engine
        .invoke("wf")
        .input("request-1".to_string())
        .await
        .unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(state, WorkflowState::Suspended { .. }));

    let state = engine
        .signal("wf", &id, &APPROVE, true)
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(state, WorkflowState::Completed(None));
}

#[tokio::test]
async fn subscribe_live_workflow_yields_states() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        ctx.set_status("step one");
        let _: String = ctx.step("a:v1").run(async || Ok("done".into())).await?;
        ctx.set_status("step two");
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();

    let mut stream = engine.subscribe("wf", &id).unwrap();
    let first = stream.next().await.unwrap();
    assert_eq!(first, WorkflowState::Started);

    inv.wait().await;

    let mut states = vec![];
    while let Some(s) = stream.next().await {
        states.push(s);
    }
    assert!(states.contains(&WorkflowState::Completed(Some("step two".into()))));
}

#[tokio::test]
async fn subscribe_after_completion_returns_snapshot() {
    async fn wf(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let mut stream = engine.subscribe("wf", &id).unwrap();
    assert_eq!(stream.next().await, Some(WorkflowState::Completed(None)));
    assert_eq!(stream.next().await, None);
}

#[tokio::test]
async fn subscribe_unknown_instance_returns_not_found() {
    let mut engine = test_engine();
    engine.register("wf", |_ctx: Context| async { Ok(()) });
    engine.start().await.unwrap();

    let err = engine.subscribe("wf", "nonexistent").unwrap_err();
    assert!(matches!(err, SubscribeError::NotFound { .. }));
}

#[tokio::test]
async fn subscribe_stale_running_returns_error() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    {
        let db = redb::Database::create(&path).unwrap();
        metadata::write_metadata(
            &db,
            "wf",
            "crashed",
            &WorkflowMetadata::new(MetadataStatus::Running),
        )
        .unwrap();
    }

    let mut engine = Engine::builder()
        .open(&path)
        .unwrap()
        .resume_on_start(false)
        .build();
    engine.register("wf", |_ctx: Context| async { Ok(()) });
    engine.start().await.unwrap();

    let err = engine.subscribe("wf", "crashed").unwrap_err();
    assert!(matches!(err, SubscribeError::StaleRunning { .. }));
}

#[tokio::test]
async fn subscribe_survives_suspend_and_signal() {
    const GATE: SuspendPoint<bool> = SuspendPoint::new("gate:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        ctx.set_status("working");
        let _: bool = ctx.suspend(&GATE).await?;
        ctx.set_status("resumed");
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();

    let mut stream = engine.subscribe("wf", &id).unwrap();

    // Consume states through suspension
    inv.wait().await;
    let mut saw_suspended = false;
    while let Some(s) = stream.next().await {
        if matches!(s, WorkflowState::Suspended { .. }) {
            saw_suspended = true;
            break;
        }
    }
    assert!(saw_suspended);

    // Signal — subscriber should see resumed execution on same stream
    engine
        .signal("wf", &id, &GATE, true)
        .await
        .unwrap()
        .wait()
        .await;

    let mut saw_completed = false;
    while let Some(s) = stream.next().await {
        if matches!(s, WorkflowState::Completed(_)) {
            saw_completed = true;
            break;
        }
    }
    assert!(saw_completed);
}

#[tokio::test]
async fn subscribe_multiple_concurrent_subscribers() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        ctx.set_status("hello");
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();

    let mut s1 = engine.subscribe("wf", &id).unwrap();
    let mut s2 = engine.subscribe("wf", &id).unwrap();

    inv.wait().await;

    // Both subscribers receive updates
    let mut s1_states = vec![];
    while let Some(s) = s1.next().await {
        s1_states.push(s);
    }
    let mut s2_states = vec![];
    while let Some(s) = s2.next().await {
        s2_states.push(s);
    }

    assert!(s1_states.contains(&WorkflowState::Completed(Some("hello".into()))));
    assert!(s2_states.contains(&WorkflowState::Completed(Some("hello".into()))));
}

#[tokio::test]
async fn subscribe_suspended_returns_live_stream() {
    const GATE: SuspendPoint<bool> = SuspendPoint::new("gate:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: bool = ctx.suspend(&GATE).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Subscribing to a suspended workflow returns a live stream
    // (sender stays in map). First item is the current Suspended state.
    let mut stream = engine.subscribe("wf", &id).unwrap();
    let state = stream.next().await.unwrap();
    assert!(matches!(state, WorkflowState::Suspended { .. }));
}

#[tokio::test]
async fn subscribe_fallback_completed_metadata() {
    async fn wf(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // After completion the sender is removed from the map.
    // subscribe() falls through to redb metadata and returns a snapshot.
    tokio::task::yield_now().await;
    let mut stream = engine.subscribe("wf", &id).unwrap();
    assert_eq!(stream.next().await, Some(WorkflowState::Completed(None)));
    assert_eq!(stream.next().await, None);
}

#[tokio::test(start_paused = true)]
async fn auto_resume_running_instances_on_start() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Seed the DB with a Running instance (simulates a crash mid-flight).
    {
        let db = redb::Database::create(&path).unwrap();
        metadata::write_metadata(
            &db,
            "wf",
            "crashed-instance",
            &WorkflowMetadata::new(MetadataStatus::Running),
        )
        .unwrap();
    }

    // New engine on the same DB — auto-resume should pick it up.
    let exec_count = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&exec_count);
    let mut engine = Engine::builder().open(&path).unwrap().build();
    engine.register("wf", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(42)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(exec_count.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn resume_on_start_false_skips_recovery() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Write a Running instance directly.
    {
        let db = redb::Database::create(&path).unwrap();
        metadata::write_metadata(
            &db,
            "wf",
            "orphan",
            &WorkflowMetadata::new(MetadataStatus::Running),
        )
        .unwrap();
    }

    let exec_count = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&exec_count);
    let mut engine = Engine::builder()
        .open(&path)
        .unwrap()
        .resume_on_start(false)
        .build();
    engine.register("wf", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(1)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(exec_count.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn auto_resume_skips_suspended_instances() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Write a Suspended instance — should NOT be auto-resumed.
    {
        let db = redb::Database::create(&path).unwrap();
        metadata::write_metadata(
            &db,
            "wf",
            "waiting",
            &WorkflowMetadata::new(MetadataStatus::Suspended {
                key: "approval:v1".to_string(),
                status: "awaiting approval".to_string(),
            }),
        )
        .unwrap();
    }

    let exec_count = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&exec_count);
    let mut engine = Engine::builder().open(&path).unwrap().build();
    engine.register("wf", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let _: i32 = ctx
                .step("s1")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(1)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(exec_count.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn state_returns_completed_for_finished_workflow() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("a").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let state = engine.state("wf", &id).unwrap();
    assert_eq!(state, WorkflowState::Completed(None));
}

#[tokio::test]
async fn state_returns_suspended_for_waiting_workflow() {
    const GATE: SuspendPoint<bool> = SuspendPoint::new("gate:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: bool = ctx.suspend(&GATE).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let state = engine.state("wf", &id).unwrap();
    assert!(
        matches!(state, WorkflowState::Suspended { ref key, .. } if key == "gate:v1"),
        "expected Suspended, got {state:?}"
    );
}

#[tokio::test]
async fn state_returns_failed_for_failed_workflow() {
    async fn wf(_ctx: Context) -> Result<(), EngineError> {
        Err(EngineError::step_failed("boom", "kaboom", false))
    }

    let mut engine = test_engine();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let state = engine.state("wf", &id).unwrap();
    assert!(
        matches!(state, WorkflowState::Failed(_)),
        "expected Failed, got {state:?}"
    );
}

#[tokio::test]
async fn state_returns_not_found_for_missing_instance() {
    let mut engine = test_engine();
    engine.register("wf", |_ctx: Context| async { Ok(()) });
    engine.start().await.unwrap();

    let err = engine.state("wf", "no-such-id").unwrap_err();
    assert!(
        matches!(err, StateError::NotFound { .. }),
        "expected NotFound, got {err:?}"
    );
}

#[tokio::test]
async fn state_returns_started_for_stale_running() {
    let mut engine = test_engine();
    engine.register("wf", |_ctx: Context| async { Ok(()) });
    engine.start().await.unwrap();

    metadata::write_metadata(
        &engine.db,
        "wf",
        "stale-1",
        &WorkflowMetadata::new(MetadataStatus::Running),
    )
    .unwrap();

    let state = engine.state("wf", "stale-1").unwrap();
    assert_eq!(state, WorkflowState::Started);
}

#[tokio::test]
async fn state_reads_live_sender_while_running() {
    use tokio::sync::Barrier;
    let barrier = Arc::new(Barrier::new(2));

    let b = Arc::clone(&barrier);
    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let b = Arc::clone(&b);
        async move {
            ctx.set_status("working");
            b.wait().await;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let state = engine.state("wf", &id).unwrap();
    assert_eq!(state, WorkflowState::InProgress("working".into()));

    barrier.wait().await;
    inv.wait().await;
}

// ── Graceful shutdown ───────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn stop_waits_for_running_workflows() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register("wf", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let _: i32 = ctx
                .step("x")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(1)
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let _ = engine.invoke("wf").await.unwrap();
    let _ = engine.invoke("wf").await.unwrap();

    engine.stop().await;

    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn stop_aborts_after_timeout() {
    let mut engine = Engine::builder()
        .in_memory()
        .shutdown_timeout(Duration::from_millis(100))
        .build();

    engine.register("stuck", |_ctx: Context| async move {
        tokio::sync::Notify::new().notified().await;
        Ok(())
    });
    engine.start().await.unwrap();

    let inv = engine.invoke("stuck").await.unwrap();
    let instance_id = inv.instance_id().to_string();

    engine.stop().await;

    let meta = engine
        .get_metadata("stuck", &instance_id)
        .unwrap()
        .expect("instance exists");
    assert_eq!(*meta.status(), MetadataStatus::Running);
}

#[tokio::test(start_paused = true)]
async fn invoke_after_stop_fails() {
    async fn noop(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("noop", noop);
    engine.start().await.unwrap();

    engine.stop().await;

    let result = engine.invoke("noop").await;
    assert!(matches!(result, Err(EngineError::NotStarted)));
}

#[tokio::test(start_paused = true)]
async fn stop_with_no_running_workflows() {
    let mut engine = test_engine();
    engine.start().await.unwrap();

    engine.stop().await;

    assert!(
        !engine.running.load(Ordering::Acquire),
        "engine should be stopped"
    );
}

#[tokio::test(start_paused = true)]
async fn stop_preserves_completed_metadata() {
    async fn fast(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("x").run(async || Ok(42)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register("fast", fast);
    engine.start().await.unwrap();

    let inv = engine.invoke("fast").await.unwrap();
    let instance_id = inv.instance_id().to_string();

    engine.stop().await;

    let meta = engine
        .get_metadata("fast", &instance_id)
        .unwrap()
        .expect("instance exists");
    assert!(matches!(meta.status(), MetadataStatus::Completed(_)));
}

#[tokio::test(start_paused = true)]
async fn shutdown_timeout_builder_config() {
    let engine = Engine::builder()
        .in_memory()
        .shutdown_timeout(Duration::from_secs(42))
        .build();

    assert_eq!(engine.shutdown_timeout, Duration::from_secs(42));
}

#[tokio::test(start_paused = true)]
async fn stop_mixed_fast_and_stuck_workflows() {
    let completed = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&completed);

    let mut engine = Engine::builder()
        .in_memory()
        .shutdown_timeout(Duration::from_millis(200))
        .build();

    engine.register("fast", move |ctx: Context| {
        let c = Arc::clone(&c);
        async move {
            let _: i32 = ctx
                .step("x")
                .run(async move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(1)
                })
                .await?;
            Ok(())
        }
    });
    engine.register("stuck", |_ctx: Context| async move {
        tokio::sync::Notify::new().notified().await;
        Ok(())
    });
    engine.start().await.unwrap();

    let fast_inv = engine.invoke("fast").await.unwrap();
    let fast_id = fast_inv.instance_id().to_string();

    let stuck_inv = engine.invoke("stuck").await.unwrap();
    let stuck_id = stuck_inv.instance_id().to_string();

    engine.stop().await;

    assert_eq!(completed.load(Ordering::Relaxed), 1);

    let fast_meta = engine
        .get_metadata("fast", &fast_id)
        .unwrap()
        .expect("instance exists");
    assert!(matches!(fast_meta.status(), MetadataStatus::Completed(_)));

    let stuck_meta = engine
        .get_metadata("stuck", &stuck_id)
        .unwrap()
        .expect("instance exists");
    assert_eq!(*stuck_meta.status(), MetadataStatus::Running);
}

// --- Retention / cleanup tests ---

#[tokio::test]
async fn retention_cleans_expired_completed_workflows() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(42)).await?;
        Ok(())
    }

    let mut engine = Engine::builder()
        .retention(Duration::from_secs(1))
        .in_memory()
        .build();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Workflow exists before cleanup.
    assert!(engine.get_metadata("wf", &id).unwrap().is_some());

    // Verify steps exist.
    let read_txn = engine.db.begin_read().unwrap();
    let steps_table = read_txn.open_table(STEPS).unwrap();
    let step_key = format!("wf/{id}/s1");
    assert!(steps_table.get(step_key.as_str()).unwrap().is_some());
    drop(steps_table);
    drop(read_txn);

    // Sleep past retention.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Run cleanup directly.
    let cleaned = cleanup_expired(&engine.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 1);

    // Metadata gone.
    assert!(engine.get_metadata("wf", &id).unwrap().is_none());

    // Steps gone.
    let read_txn = engine.db.begin_read().unwrap();
    let steps_table = read_txn.open_table(STEPS).unwrap();
    assert!(steps_table.get(step_key.as_str()).unwrap().is_none());
}

#[tokio::test]
async fn retention_cleans_expired_failed_workflows() {
    async fn failing(_ctx: Context) -> Result<(), EngineError> {
        Err(EngineError::step_failed("s1", "boom", false))
    }

    let mut engine = Engine::builder()
        .retention(Duration::from_secs(1))
        .in_memory()
        .build();
    engine.register("wf", failing);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let cleaned = cleanup_expired(&engine.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 1);
    assert!(engine.get_metadata("wf", &id).unwrap().is_none());
}

#[tokio::test]
async fn retention_does_not_touch_running_workflows() {
    async fn slow(_ctx: Context) -> Result<(), EngineError> {
        tokio::sync::Notify::new().notified().await;
        Ok(())
    }

    let mut engine = Engine::builder()
        .retention(Duration::from_secs(1))
        .in_memory()
        .build();
    engine.register("wf", slow);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();

    // Give the task time to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Cleanup should find nothing to clean.
    let cleaned = cleanup_expired(&engine.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 0);
    assert!(engine.get_metadata("wf", &id).unwrap().is_some());

    drop(inv);
    engine.stop().await;
}

#[tokio::test]
async fn retention_does_not_touch_suspended_workflows() {
    const GATE: SuspendPoint<bool> = SuspendPoint::new("gate:v1");

    async fn suspending(ctx: Context) -> Result<(), EngineError> {
        let _: bool = ctx.suspend(&GATE).await?;
        Ok(())
    }

    let mut engine = Engine::builder()
        .retention(Duration::from_secs(1))
        .in_memory()
        .build();
    engine.register("wf", suspending);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let cleaned = cleanup_expired(&engine.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 0);
    assert!(engine.get_metadata("wf", &id).unwrap().is_some());
}

#[tokio::test]
async fn retention_per_workflow_override() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = Engine::builder()
        .retention(Duration::from_secs(10))
        .in_memory()
        .build();
    engine.register("long-lived", wf);
    engine
        .register("short-lived", wf)
        .retention(Duration::from_secs(1));
    engine.start().await.unwrap();

    let long_inv = engine.invoke("long-lived").await.unwrap();
    let long_id = long_inv.instance_id().to_string();
    long_inv.wait().await;

    let short_inv = engine.invoke("short-lived").await.unwrap();
    let short_id = short_inv.instance_id().to_string();
    short_inv.wait().await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut overrides = HashMap::new();
    overrides.insert("short-lived".to_string(), Duration::from_secs(1));

    let cleaned = cleanup_expired(&engine.db, Duration::from_secs(10), &overrides).unwrap();
    assert_eq!(cleaned, 1);

    // short-lived is gone, long-lived remains.
    assert!(
        engine
            .get_metadata("short-lived", &short_id)
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .get_metadata("long-lived", &long_id)
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn retention_no_op_when_nothing_expired() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = Engine::builder()
        .retention(Duration::from_secs(100))
        .in_memory()
        .build();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let cleaned = cleanup_expired(&engine.db, Duration::from_secs(100), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 0);
    assert!(engine.get_metadata("wf", &id).unwrap().is_some());
}

#[tokio::test]
async fn retention_cleans_multiple_instances() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = Engine::builder()
        .retention(Duration::from_secs(1))
        .in_memory()
        .build();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    let inv1 = engine.invoke("wf").await.unwrap();
    inv1.wait().await;
    let inv2 = engine.invoke("wf").await.unwrap();
    inv2.wait().await;
    let inv3 = engine.invoke("wf").await.unwrap();
    inv3.wait().await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let cleaned = cleanup_expired(&engine.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 3);

    let instances = engine.list_instances("wf").unwrap();
    assert!(instances.is_empty());
}

#[tokio::test]
async fn retention_concurrent_invoke_and_cleanup() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = Engine::builder()
        .retention(Duration::from_secs(1))
        .in_memory()
        .build();
    engine.register("wf", wf);
    engine.start().await.unwrap();

    // Create and complete an instance.
    let old_inv = engine.invoke("wf").await.unwrap();
    let old_id = old_inv.instance_id().to_string();
    old_inv.wait().await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Invoke a new instance concurrently with cleanup.
    let new_inv = engine.invoke("wf").await.unwrap();
    let new_id = new_inv.instance_id().to_string();

    let cleaned = cleanup_expired(&engine.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 1);

    new_inv.wait().await;

    // Old is gone, new survived.
    assert!(engine.get_metadata("wf", &old_id).unwrap().is_none());
    assert!(engine.get_metadata("wf", &new_id).unwrap().is_some());
}

#[tokio::test]
async fn retention_cleanup_removes_all_step_entries() {
    async fn multi_step(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("a").run(async || Ok(1)).await?;
        let _: i32 = ctx.step("b").run(async || Ok(2)).await?;
        let _: i32 = ctx.step("c").run(async || Ok(3)).await?;
        Ok(())
    }

    let mut engine = Engine::builder()
        .retention(Duration::from_secs(1))
        .in_memory()
        .build();
    engine.register("wf", multi_step);
    engine.start().await.unwrap();

    let inv = engine.invoke("wf").await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Verify all 3 steps exist.
    let read_txn = engine.db.begin_read().unwrap();
    let steps_table = read_txn.open_table(STEPS).unwrap();
    let prefix = format!("wf/{id}/");
    let end = format!("wf/{id}0");
    let count = steps_table
        .range(prefix.as_str()..end.as_str())
        .unwrap()
        .count();
    assert_eq!(count, 3);
    drop(steps_table);
    drop(read_txn);

    tokio::time::sleep(Duration::from_secs(2)).await;
    cleanup_expired(&engine.db, Duration::from_secs(1), &HashMap::new()).unwrap();

    // All steps gone.
    let read_txn = engine.db.begin_read().unwrap();
    let steps_table = read_txn.open_table(STEPS).unwrap();
    let count = steps_table
        .range(prefix.as_str()..end.as_str())
        .unwrap()
        .count();
    assert_eq!(count, 0);
}
