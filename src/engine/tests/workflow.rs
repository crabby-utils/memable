use super::*;
#[tokio::test]
async fn simple_workflow_completes() {
    const ADD: WorkflowDef = WorkflowDef::new("add");

    async fn add(ctx: Context) -> Result<(), EngineError> {
        let a: i32 = ctx.step("a").run(async || Ok(1)).await?;
        let b: i32 = ctx.step("b").run(async || Ok(2)).await?;
        assert_eq!(a + b, 3);
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&ADD, add);
    engine.start().await.unwrap();

    let state = engine.invoke(&ADD).await.unwrap().wait().await;
    let _ = state.unwrap_completed();
}

#[tokio::test]
async fn memoisation_on_resume() {
    const MEMO: WorkflowDef = WorkflowDef::new("memo");

    let counter = Arc::new(AtomicU32::new(0));
    let attempts = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&MEMO, move |ctx: Context| {
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
    let inv = engine.invoke(&MEMO).await.unwrap();
    let instance_id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(state.is_failed());
    assert_eq!(counter.load(Ordering::Relaxed), 2);

    // Resume — s1 is memoised, s2 retries and succeeds.
    counter.store(0, Ordering::Relaxed);
    let state = engine
        .resume(&MEMO, &instance_id)
        .await
        .unwrap()
        .wait()
        .await;
    let _ = state.unwrap_completed();
    assert_eq!(counter.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn step_error_produces_failed_state() {
    const FAIL: WorkflowDef = WorkflowDef::new("fail");

    async fn failing(ctx: Context) -> Result<(), EngineError> {
        let _: String = ctx
            .step("fail")
            .run(async || Err(StepError::permanent("boom")))
            .await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&FAIL, failing);
    engine.start().await.unwrap();

    let state = engine.invoke(&FAIL).await.unwrap().wait().await;
    assert!(state.is_failed());
}

#[tokio::test]
async fn different_instances_have_separate_caches() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
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

    engine.invoke(&WF).await.unwrap().wait().await;
    engine.invoke(&WF).await.unwrap().wait().await;

    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn invoke_before_start_fails() {
    const NOOP: WorkflowDef = WorkflowDef::new("noop");

    async fn noop(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&NOOP, noop);

    let result = engine.invoke(&NOOP).await;
    assert!(matches!(result, Err(EngineError::NotStarted)));
}

#[tokio::test]
async fn invoke_unknown_workflow_fails() {
    const NONEXISTENT: WorkflowDef = WorkflowDef::new("nonexistent");

    let mut engine = test_engine();
    engine.start().await.unwrap();

    let result = engine.invoke(&NONEXISTENT).await;
    assert!(matches!(result, Err(EngineError::WorkflowNotFound(_))));
}

#[tokio::test]
async fn wait_all_waits_for_completion() {
    let counter = Arc::new(AtomicU32::new(0));

    let c = Arc::clone(&counter);
    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
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

    drop(engine.invoke(&WF).await.unwrap());
    drop(engine.invoke(&WF).await.unwrap());
    drop(engine.invoke(&WF).await.unwrap());

    engine.wait_all().await;
    assert_eq!(counter.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn invoke_after_wait_all_fails() {
    const NOOP: WorkflowDef = WorkflowDef::new("noop");

    async fn noop(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&NOOP, noop);
    engine.start().await.unwrap();

    engine.wait_all().await;

    let result = engine.invoke(&NOOP).await;
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
    const STATUS_WF: WorkflowDef = WorkflowDef::new("status-wf");

    let step_counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&step_counter);
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&STATUS_WF, move |ctx: Context| {
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
    let inv = engine.invoke(&STATUS_WF).await.unwrap();
    let instance_id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(state.is_failed());
    assert_eq!(step_counter.load(Ordering::Relaxed), 2);

    // Resume — s1 is memoised, s2 retries and succeeds.
    step_counter.store(0, Ordering::Relaxed);
    let state = engine
        .resume(&STATUS_WF, &instance_id)
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(state.unwrap_completed().status(), Some("step-two"));
    // Only s2 re-executed; s1 was served from cache (with its persisted status).
    assert_eq!(step_counter.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn subscribers_not_notified_during_replay() {
    const SILENT_REPLAY: WorkflowDef = WorkflowDef::new("silent-replay");

    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&SILENT_REPLAY, move |ctx: Context| {
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
    let inv = engine.invoke(&SILENT_REPLAY).await.unwrap();
    let instance_id = inv.instance_id().to_string();
    inv.wait().await;

    // Resume and watch for notifications.
    let mut inv = engine.resume(&SILENT_REPLAY, &instance_id).await.unwrap();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let meta = engine.get_metadata(&WF, &id).unwrap().unwrap();
    assert_eq!(*meta.status(), MetadataStatus::Completed(None));
    assert!(meta.completed_at().is_some());
}

#[tokio::test]
async fn metadata_failed_on_error() {
    const FAIL: WorkflowDef = WorkflowDef::new("fail");

    async fn failing(ctx: Context) -> Result<(), EngineError> {
        let _: String = ctx
            .step("fail")
            .run(async || Err(StepError::permanent("boom")))
            .await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&FAIL, failing);
    engine.start().await.unwrap();

    let inv = engine.invoke(&FAIL).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let meta = engine.get_metadata(&FAIL, &id).unwrap().unwrap();
    assert!(matches!(meta.status(), MetadataStatus::Failed(msg) if msg.contains("boom")),);
    assert!(meta.completed_at().is_some());
}

#[tokio::test]
async fn metadata_updated_after_resume() {
    const RETRY_WF: WorkflowDef = WorkflowDef::new("retry-wf");

    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&RETRY_WF, move |ctx: Context| {
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

    let inv = engine.invoke(&RETRY_WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let meta = engine.get_metadata(&RETRY_WF, &id).unwrap().unwrap();
    assert!(matches!(meta.status(), MetadataStatus::Failed(_)));

    engine.resume(&RETRY_WF, &id).await.unwrap().wait().await;

    let meta = engine.get_metadata(&RETRY_WF, &id).unwrap().unwrap();
    assert_eq!(*meta.status(), MetadataStatus::Completed(None));
}

#[tokio::test]
async fn list_instances_returns_all() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    engine.invoke(&WF).await.unwrap().wait().await;
    engine.invoke(&WF).await.unwrap().wait().await;
    engine.invoke(&WF).await.unwrap().wait().await;

    let instances = engine.list_instances(&WF).unwrap();
    assert_eq!(instances.len(), 3);
}

#[tokio::test]
async fn list_instances_filters_by_workflow_name() {
    const ALPHA: WorkflowDef = WorkflowDef::new("alpha");
    const BETA: WorkflowDef = WorkflowDef::new("beta");
    const NONEXISTENT: WorkflowDef = WorkflowDef::new("nonexistent");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&ALPHA, wf);
    engine.register(&BETA, wf);
    engine.start().await.unwrap();

    engine.invoke(&ALPHA).await.unwrap().wait().await;
    engine.invoke(&ALPHA).await.unwrap().wait().await;
    engine.invoke(&BETA).await.unwrap().wait().await;

    let alpha_instances = engine.list_instances(&ALPHA).unwrap();
    assert_eq!(alpha_instances.len(), 2);

    let beta_instances = engine.list_instances(&BETA).unwrap();
    assert_eq!(beta_instances.len(), 1);

    let none_instances = engine.list_instances(&NONEXISTENT).unwrap();
    assert!(none_instances.is_empty());
}

#[tokio::test]
async fn get_metadata_returns_none_for_unknown() {
    let mut engine = test_engine();
    engine.start().await.unwrap();

    let meta = engine.get_metadata(&WF, "no-such-id").unwrap();
    assert!(meta.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_consistent_after_wait_multi_thread() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let meta = engine.get_metadata(&WF, &id).unwrap().unwrap();
    assert_eq!(*meta.status(), MetadataStatus::Completed(None));
}

#[tokio::test]
async fn metadata_correct_after_wait_all() {
    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let id1 = engine.invoke(&WF).await.unwrap().instance_id().to_string();
    let id2 = engine.invoke(&WF).await.unwrap().instance_id().to_string();

    engine.wait_all().await;

    let m1 = engine.get_metadata(&WF, &id1).unwrap().unwrap();
    let m2 = engine.get_metadata(&WF, &id2).unwrap().unwrap();
    assert_eq!(*m1.status(), MetadataStatus::Completed(None));
    assert_eq!(*m2.status(), MetadataStatus::Completed(None));
}

#[tokio::test]
async fn list_instances_on_fresh_engine() {
    const ANYTHING: WorkflowDef = WorkflowDef::new("anything");
    let engine = test_engine();
    let instances = engine.list_instances(&ANYTHING).unwrap();
    assert!(instances.is_empty());
}
