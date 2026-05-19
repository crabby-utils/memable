use super::*;

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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(
        state,
        WaitResult::Suspended { ref key, ref status } if key == "wait:v1" && status == "wait:v1"
    ));

    let meta = engine.get_metadata(&WF, &id).unwrap().unwrap();
    assert!(
        matches!(meta.status(), MetadataStatus::Suspended { status, .. } if status == "wait:v1"),
    );

    let state = engine
        .signal(&WF, &id, &WAIT, "hello".to_string())
        .await
        .unwrap()
        .wait()
        .await;
    let _ = state.unwrap_completed();

    let meta = engine.get_metadata(&WF, &id).unwrap().unwrap();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(
        state,
        WaitResult::Suspended { ref key, ref status }
            if key == "approval:v1" && status == "Waiting for manager approval"
    ));

    let meta = engine.get_metadata(&WF, &id).unwrap().unwrap();
    assert!(matches!(
        meta.status(),
        MetadataStatus::Suspended { status, .. } if status == "Waiting for manager approval"
    ));

    let state = engine
        .signal(&WF, &id, &APPROVAL, true)
        .await
        .unwrap()
        .wait()
        .await;
    let _ = state.unwrap_completed();
}

#[tokio::test]
async fn memoised_steps_preserved_across_suspend() {
    const GATE: SuspendPoint<String> = SuspendPoint::new("gate:v1");

    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
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

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    counter.store(0, Ordering::Relaxed);
    let state = engine
        .signal(&WF, &id, &GATE, "go".to_string())
        .await
        .unwrap()
        .wait()
        .await;
    let _ = state.unwrap_completed();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(
        state,
        WaitResult::Suspended { ref key, ref status } if key == "wait:v1" && status == "wait:v1"
    ));

    // Resume without signal — should still be suspended.
    let state = engine.resume(&WF, &id).await.unwrap().wait().await;
    assert!(matches!(
        state,
        WaitResult::Suspended { ref key, ref status } if key == "wait:v1" && status == "wait:v1"
    ));
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    // V1: workflow suspends at "action:v1".
    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(
        state,
        WaitResult::Suspended { ref key, ref status } if key == "action:v1" && status == "action:v1"
    ));

    // V2: same key is now a regular step.
    use_step.store(true, Ordering::Release);
    let state = engine.resume(&WF, &id).await.unwrap().wait().await;
    assert!(
        matches!(state, WaitResult::Failed(ref msg) if msg.contains("suspended entry")),
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(matches!(
        state,
        WaitResult::Suspended { ref key, ref status } if key == "first:v1" && status == "first:v1"
    ));

    // Signal first suspend point.
    let state = engine
        .signal(&WF, &id, &FIRST, 1i32)
        .await
        .unwrap()
        .wait()
        .await;
    assert!(matches!(
        state,
        WaitResult::Suspended { ref key, ref status } if key == "second:v1" && status == "second:v1"
    ));

    // Signal second suspend point.
    let state = engine
        .signal(&WF, &id, &SECOND, 2i32)
        .await
        .unwrap()
        .wait()
        .await;
    let _ = state.unwrap_completed();
}

#[tokio::test]
async fn suspend_after_failed_step_on_resume() {
    const GATE: SuspendPoint<String> = SuspendPoint::new("gate:v1");

    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
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
    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(state.is_failed());

    // Resume — s1 succeeds, hits suspend.
    let state = engine.resume(&WF, &id).await.unwrap().wait().await;
    assert!(matches!(
        state,
        WaitResult::Suspended { ref key, ref status } if key == "gate:v1" && status == "gate:v1"
    ));

    // Signal — completes.
    let state = engine
        .signal(&WF, &id, &GATE, "done".to_string())
        .await
        .unwrap()
        .wait()
        .await;
    let _ = state.unwrap_completed();
}

#[tokio::test]
async fn step_without_status_stores_none() {
    const NO_STATUS: WorkflowDef = WorkflowDef::new("no-status");

    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register(&NO_STATUS, move |ctx: Context| {
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

    let inv = engine.invoke(&NO_STATUS).await.unwrap();
    let instance_id = inv.instance_id().to_string();
    inv.wait().await;

    // Resume — s1 is memoised, execution counter stays at 1.
    counter.store(0, Ordering::Relaxed);
    let state = engine
        .resume(&NO_STATUS, &instance_id)
        .await
        .unwrap()
        .wait()
        .await;
    let _ = state.unwrap_completed();
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_fires_and_completes_workflow() {
    const TIMER_WF: WorkflowDef = WorkflowDef::new("timer-wf");

    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register(&TIMER_WF, move |ctx: Context| {
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

    let inv = engine.invoke(&TIMER_WF).await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(state.is_suspended());
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // Wait for the poller to fire the timer.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let meta = engine.get_metadata(&TIMER_WF, &id).unwrap().unwrap();
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
    const TIMER_MEMO: WorkflowDef = WorkflowDef::new("timer-memo");

    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register(&TIMER_MEMO, move |ctx: Context| {
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

    let inv = engine.invoke(&TIMER_MEMO).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // Wait for timer to fire.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let meta = engine.get_metadata(&TIMER_MEMO, &id).unwrap().unwrap();
        if meta.status().is_terminal() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "timer did not fire");
    }

    // Resume — both s1 and timer are memoised.
    counter.store(0, Ordering::Relaxed);
    let state = engine.resume(&TIMER_MEMO, &id).await.unwrap().wait().await;
    let _ = state.unwrap_completed();
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_skipped_when_workflow_not_suspended() {
    const TIMER_FAIL: WorkflowDef = WorkflowDef::new("timer-fail");

    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&TIMER_FAIL, move |ctx: Context| {
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
    let inv = engine.invoke(&TIMER_FAIL).await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(state.is_failed());

    // Resume — s1 succeeds, hits timer.
    let state = engine.resume(&TIMER_FAIL, &id).await.unwrap().wait().await;
    assert!(state.is_suspended());

    // Wait for timer to fire and complete.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let meta = engine.get_metadata(&TIMER_FAIL, &id).unwrap().unwrap();
        if meta.status().is_terminal() {
            assert_eq!(*meta.status(), MetadataStatus::Completed(None));
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "timer did not fire");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_with_steps_before_and_after() {
    const MULTI_TIMER: WorkflowDef = WorkflowDef::new("multi-timer");

    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register(&MULTI_TIMER, move |ctx: Context| {
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

    let inv = engine.invoke(&MULTI_TIMER).await.unwrap();
    let id = inv.instance_id().to_string();
    // First timer suspends.
    inv.wait().await;
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // Wait for first timer and then second timer to fire.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let meta = engine.get_metadata(&MULTI_TIMER, &id).unwrap().unwrap();
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
