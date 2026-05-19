use super::*;
#[tokio::test]
async fn invoke_with_input_delivers_payload() {
    const INPUT_WF: WorkflowDef<i32> = WorkflowDef::new("wf");

    let mut engine = test_engine();
    engine.register(&INPUT_WF, |_ctx: Context, val: i32| async move {
        assert_eq!(val, 42);
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine
        .invoke(&INPUT_WF)
        .input(42_i32)
        .await
        .unwrap()
        .wait()
        .await;
    let _ = state.unwrap_completed();
}

#[tokio::test]
async fn invoke_without_input_backward_compatible() {
    let mut engine = test_engine();
    engine.register(&WF, |ctx: Context| async move {
        let _: String = ctx
            .step("s:v1")
            .run(async || Ok("hello".to_string()))
            .await?;
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    let _ = state.unwrap_completed();
}

#[tokio::test]
async fn input_returns_none_when_not_provided() {
    let mut engine = test_engine();
    engine.register(&WF, |ctx: Context| async move {
        let val = ctx.input::<String>()?;
        assert!(val.is_none());
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    let _ = state.unwrap_completed();
}

#[tokio::test]
async fn input_type_mismatch() {
    // Register a workflow expecting String input, but internally try to
    // deserialize the _input step as i32 to trigger a type mismatch.
    const INPUT_WF: WorkflowDef<String> = WorkflowDef::new("wf");

    let mut engine = test_engine();
    engine.register(&INPUT_WF, |ctx: Context, _val: String| async move {
        // Manually read the raw _input as the wrong type.
        let _wrong = ctx.input::<i32>()?;
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine
        .invoke(&INPUT_WF)
        .input("not an i32".to_string())
        .await
        .unwrap()
        .wait()
        .await;
    assert!(matches!(state, WaitResult::Failed(ref msg) if msg.contains("type mismatch")));
}

#[tokio::test]
async fn input_preserved_across_resume() {
    const INPUT_WF: WorkflowDef<String> = WorkflowDef::new("wf");

    let call_count = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&call_count);

    let mut engine = test_engine();
    engine.register(&INPUT_WF, move |ctx: Context, name: String| {
        let c = Arc::clone(&c);
        async move {
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
        .invoke(&INPUT_WF)
        .input("Alice".to_string())
        .await
        .unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(state.is_failed());

    let state = engine.resume(&INPUT_WF, &id).await.unwrap().wait().await;
    let _ = state.unwrap_completed();
    assert_eq!(call_count.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn input_coexists_with_suspend_and_signal() {
    const INPUT_WF: WorkflowDef<String> = WorkflowDef::new("wf");
    const APPROVE: SuspendPoint<bool> = SuspendPoint::new("approve:v1");

    let mut engine = test_engine();
    engine.register(&INPUT_WF, |ctx: Context, prefix: String| async move {
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
        .invoke(&INPUT_WF)
        .input("request-1".to_string())
        .await
        .unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(state.is_suspended());

    let state = engine
        .signal(&INPUT_WF, &id, &APPROVE, true)
        .await
        .unwrap()
        .wait()
        .await;
    let _ = state.unwrap_completed();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();

    let mut stream = engine.subscribe(&WF, &id).unwrap();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let mut stream = engine.subscribe(&WF, &id).unwrap();
    assert_eq!(stream.next().await, Some(WorkflowState::Completed(None)));
    assert_eq!(stream.next().await, None);
}

#[tokio::test]
async fn subscribe_unknown_instance_returns_not_found() {
    let mut engine = test_engine();
    engine.register(&WF, |_ctx: Context| async { Ok(()) });
    engine.start().await.unwrap();

    let err = engine.subscribe(&WF, "nonexistent").unwrap_err();
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
    engine.register(&WF, |_ctx: Context| async { Ok(()) });
    engine.start().await.unwrap();

    let err = engine.subscribe(&WF, "crashed").unwrap_err();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();

    let mut stream = engine.subscribe(&WF, &id).unwrap();

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
        .signal(&WF, &id, &GATE, true)
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();

    let mut s1 = engine.subscribe(&WF, &id).unwrap();
    let mut s2 = engine.subscribe(&WF, &id).unwrap();

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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Subscribing to a suspended workflow returns a live stream
    // (sender stays in map). First item is the current Suspended state.
    let mut stream = engine.subscribe(&WF, &id).unwrap();
    let state = stream.next().await.unwrap();
    assert!(matches!(state, WorkflowState::Suspended { .. }));
}

#[tokio::test]
async fn subscribe_fallback_completed_metadata() {
    async fn wf(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // After completion the sender is removed from the map.
    // subscribe() falls through to redb metadata and returns a snapshot.
    tokio::task::yield_now().await;
    let mut stream = engine.subscribe(&WF, &id).unwrap();
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
    engine.register(&WF, move |ctx: Context| {
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
    engine.register(&WF, move |ctx: Context| {
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
    engine.register(&WF, move |ctx: Context| {
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let state = engine.state(&WF, &id).unwrap();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let state = engine.state(&WF, &id).unwrap();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let state = engine.state(&WF, &id).unwrap();
    assert!(
        matches!(state, WorkflowState::Failed(_)),
        "expected Failed, got {state:?}"
    );
}

#[tokio::test]
async fn state_returns_not_found_for_missing_instance() {
    let mut engine = test_engine();
    engine.register(&WF, |_ctx: Context| async { Ok(()) });
    engine.start().await.unwrap();

    let err = engine.state(&WF, "no-such-id").unwrap_err();
    assert!(
        matches!(err, StateError::NotFound { .. }),
        "expected NotFound, got {err:?}"
    );
}

#[tokio::test]
async fn state_returns_started_for_stale_running() {
    let mut engine = test_engine();
    engine.register(&WF, |_ctx: Context| async { Ok(()) });
    engine.start().await.unwrap();

    metadata::write_metadata(
        &engine.shared.db,
        "wf",
        "stale-1",
        &WorkflowMetadata::new(MetadataStatus::Running),
    )
    .unwrap();

    let state = engine.state(&WF, "stale-1").unwrap();
    assert_eq!(state, WorkflowState::Started);
}

#[tokio::test]
async fn state_reads_live_sender_while_running() {
    use tokio::sync::Barrier;
    let barrier = Arc::new(Barrier::new(2));

    let b = Arc::clone(&barrier);
    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
        let b = Arc::clone(&b);
        async move {
            ctx.set_status("working");
            b.wait().await;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let state = engine.state(&WF, &id).unwrap();
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

    let _ = engine.invoke(&WF).await.unwrap();
    let _ = engine.invoke(&WF).await.unwrap();

    engine.stop().await;

    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn stop_aborts_after_timeout() {
    const STUCK: WorkflowDef = WorkflowDef::new("stuck");

    let mut engine = Engine::builder()
        .in_memory()
        .shutdown_timeout(Duration::from_millis(100))
        .build();

    engine.register(&STUCK, |_ctx: Context| async move {
        tokio::sync::Notify::new().notified().await;
        Ok(())
    });
    engine.start().await.unwrap();

    let inv = engine.invoke(&STUCK).await.unwrap();
    let instance_id = inv.instance_id().to_string();

    engine.stop().await;

    let meta = engine
        .get_metadata(&STUCK, &instance_id)
        .unwrap()
        .expect("instance exists");
    assert_eq!(*meta.status(), MetadataStatus::Running);
}

#[tokio::test(start_paused = true)]
async fn invoke_after_stop_fails() {
    const NOOP: WorkflowDef = WorkflowDef::new("noop");

    async fn noop(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&NOOP, noop);
    engine.start().await.unwrap();

    engine.stop().await;

    let result = engine.invoke(&NOOP).await;
    assert!(matches!(result, Err(EngineError::NotStarted)));
}

#[tokio::test(start_paused = true)]
async fn stop_with_no_running_workflows() {
    let mut engine = test_engine();
    engine.start().await.unwrap();

    engine.stop().await;

    assert!(
        !engine.shared.running.load(Ordering::Acquire),
        "engine should be stopped"
    );
}

#[tokio::test(start_paused = true)]
async fn stop_preserves_completed_metadata() {
    const FAST: WorkflowDef = WorkflowDef::new("fast");

    async fn fast(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("x").run(async || Ok(42)).await?;
        Ok(())
    }

    let mut engine = test_engine();
    engine.register(&FAST, fast);
    engine.start().await.unwrap();

    let inv = engine.invoke(&FAST).await.unwrap();
    let instance_id = inv.instance_id().to_string();

    engine.stop().await;

    let meta = engine
        .get_metadata(&FAST, &instance_id)
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
    const FAST: WorkflowDef = WorkflowDef::new("fast");
    const STUCK: WorkflowDef = WorkflowDef::new("stuck");

    let completed = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&completed);

    let mut engine = Engine::builder()
        .in_memory()
        .shutdown_timeout(Duration::from_millis(200))
        .build();

    engine.register(&FAST, move |ctx: Context| {
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
    engine.register(&STUCK, |_ctx: Context| async move {
        tokio::sync::Notify::new().notified().await;
        Ok(())
    });
    engine.start().await.unwrap();

    let fast_inv = engine.invoke(&FAST).await.unwrap();
    let fast_id = fast_inv.instance_id().to_string();

    let stuck_inv = engine.invoke(&STUCK).await.unwrap();
    let stuck_id = stuck_inv.instance_id().to_string();

    engine.stop().await;

    assert_eq!(completed.load(Ordering::Relaxed), 1);

    let fast_meta = engine
        .get_metadata(&FAST, &fast_id)
        .unwrap()
        .expect("instance exists");
    assert!(matches!(fast_meta.status(), MetadataStatus::Completed(_)));

    let stuck_meta = engine
        .get_metadata(&STUCK, &stuck_id)
        .unwrap()
        .expect("instance exists");
    assert_eq!(*stuck_meta.status(), MetadataStatus::Running);
}
