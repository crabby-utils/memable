use super::*;
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    let _ = state.unwrap_completed();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    assert!(matches!(state, WaitResult::Failed(ref msg) if msg.contains("timed out")));
}

#[tokio::test(start_paused = true)]
async fn timed_out_step_not_persisted() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
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

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(state.is_failed());
    assert_eq!(attempts.load(Ordering::Relaxed), 1);

    // Resume — step re-executes (not cached) and succeeds this time.
    let state = engine.resume(&WF, &id).await.unwrap().wait().await;
    let _ = state.unwrap_completed();
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn timeout_skipped_on_cache_hit() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
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

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // Resume with impossibly short timeout — cache hit returns instantly.
    counter.store(0, Ordering::Relaxed);
    let state = engine.resume(&WF, &id).await.unwrap().wait().await;
    let _ = state.unwrap_completed();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    let _ = state.unwrap_completed();
}

// --- Key validation (S1) tests ---

#[test]
#[should_panic(expected = "workflow name must not contain '/'")]
fn register_rejects_slash_in_name() {
    let _ = WorkflowDef::<(), ()>::new("bad/name");
}

#[tokio::test]
async fn resume_rejects_slash_in_instance_id() {
    async fn wf(_ctx: Context) -> Result<(), EngineError> {
        Ok(())
    }
    let mut engine = test_engine();
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let Err(err) = engine.resume(&WF, "bad/id").await else {
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
async fn signal_rejects_slash_in_instance_id() {
    const WAIT: SuspendPoint<bool> = SuspendPoint::new("wait:v1");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: bool = ctx.suspend(&WAIT).await?;
        Ok(())
    }
    let mut engine = test_engine();
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let _id = inv.instance_id().to_string();
    inv.wait().await;

    let Err(err) = engine.signal(&WF, "bad/id", &WAIT, true).await else {
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

    let shared = Arc::new(super::EngineShared {
        db: Arc::new(
            Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .unwrap(),
        ),
        workflows: HashMap::new(),
        running: Arc::new(AtomicBool::new(false)),
        tasks: Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
        timer_serial: Arc::new(AtomicU64::new(0)),
        default_retry: None,
        senders: Arc::new(std::sync::Mutex::new(HashMap::new())),
    });
    let (tx, _rx) = watch::channel(WorkflowState::Started);
    let ctx = Context::new("wf".into(), "id".into(), shared, tx);
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
    engine.register(&WF, |ctx: Context| async move {
        ctx.timer("bad/key", Duration::from_secs(1))?;
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    assert!(state.is_failed());
}

#[test]
#[should_panic(expected = "step keys starting with '_' are reserved")]
fn step_rejects_reserved_prefix() {
    use std::sync::atomic::AtomicU64;

    use redb::Database;
    use redb::backends::InMemoryBackend;

    let shared = Arc::new(super::EngineShared {
        db: Arc::new(
            Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .unwrap(),
        ),
        workflows: HashMap::new(),
        running: Arc::new(AtomicBool::new(false)),
        tasks: Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
        timer_serial: Arc::new(AtomicU64::new(0)),
        default_retry: None,
        senders: Arc::new(std::sync::Mutex::new(HashMap::new())),
    });
    let (tx, _rx) = watch::channel(WorkflowState::Started);
    let ctx = Context::new("wf".into(), "id".into(), shared, tx);
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
    engine.register(&WF, |ctx: Context| async move {
        ctx.timer("_reserved", Duration::from_secs(1))?;
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    assert!(state.is_failed());
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let Err(err) = engine.signal(&WF, &id, &WRONG, true).await else {
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // First signal succeeds — step is Suspended.
    let inv = engine.signal(&WF, &id, &GATE, true).await.unwrap();
    inv.wait().await;

    // Second signal to same step fails — it was already claimed.
    let Err(err) = engine.signal(&WF, &id, &GATE, true).await else {
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Attempt to signal a step the workflow hasn't reached yet.
    let Err(err) = engine.signal(&WF, &id, &SECOND, "sneaky".to_string()).await else {
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Signal with String instead of the expected i32 (simulates cross-version mismatch).
    let inv = engine
        .signal(&WF, &id, &GATE_STRING, "wrong type".to_string())
        .await
        .unwrap();
    let state = inv.wait().await;
    assert!(
        matches!(state, WaitResult::Failed(ref msg) if msg.contains("type mismatch")),
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Signal with u32 instead of i32 — same binary layout, different type name.
    let inv = engine.signal(&WF, &id, &GATE_U32, 42_u32).await.unwrap();
    let state = inv.wait().await;
    assert!(
        matches!(state, WaitResult::Failed(ref msg) if msg.contains("type mismatch")),
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let (r1, r2) = tokio::join!(
        engine.signal(&WF, &id, &GATE, true),
        engine.signal(&WF, &id, &GATE, true),
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Manually signal the timer step before the timer fires.
    let inv = engine.signal(&WF, &id, &WAIT, ()).await.unwrap();
    inv.wait().await;

    engine.wait_all().await;
    assert_eq!(
        COUNTER.load(Ordering::Relaxed),
        1,
        "workflow should run exactly once"
    );

    let meta = engine.get_metadata(&WF, &id).unwrap().unwrap();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
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
    engine.register(&WF, move |ctx: Context| {
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

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    assert!(matches!(state, WaitResult::Failed(ref msg) if msg.contains("3 attempts")));
    assert_eq!(attempts.load(Ordering::Relaxed), 3);
}

#[tokio::test(start_paused = true)]
async fn permanent_error_skips_retry() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
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

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    assert!(matches!(state, WaitResult::Failed(ref msg) if msg.contains("fatal")));
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn step_succeeds_on_second_attempt() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
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

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    let _ = state.unwrap_completed();
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn exponential_backoff_delays() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
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
    engine.invoke(&WF).await.unwrap().wait().await;
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
    engine.register(&WF, move |ctx: Context| {
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

    engine.invoke(&WF).await.unwrap().wait().await;
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
    engine.register(&WF, move |ctx: Context| {
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

    engine.invoke(&WF).await.unwrap().wait().await;
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
    engine.register(&WF, move |ctx: Context| {
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

    engine.invoke(&WF).await.unwrap().wait().await;
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn dead_letter_persisted_and_resume_re_executes() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
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

    // First invoke: 2 attempts (1 + 1 retry), both fail -> RetriesExhausted.
    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    assert!(state.is_failed());
    assert_eq!(attempts.load(Ordering::Relaxed), 2);

    // Resume: fresh retry budget, 2 more attempts, both fail again.
    let state = engine.resume(&WF, &id).await.unwrap().wait().await;
    assert!(state.is_failed());
    assert_eq!(attempts.load(Ordering::Relaxed), 4);

    // Resume again: first attempt succeeds (n=4 >= 4).
    let state = engine.resume(&WF, &id).await.unwrap().wait().await;
    let _ = state.unwrap_completed();
}

#[tokio::test(start_paused = true)]
async fn timeout_applies_per_attempt() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);

    let mut engine = test_engine();
    engine.register(&WF, move |ctx: Context| {
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

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    // First attempt times out (StepTimeout propagates immediately,
    // bypassing retry). Timeout is not retried.
    assert!(matches!(state, WaitResult::Failed(ref msg) if msg.contains("timed out")));
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
    engine.register(&WF, move |ctx: Context| {
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

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Resume — s1 is memoised, closure should NOT run again.
    engine.resume(&WF, &id).await.unwrap().wait().await;
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn retryable_without_policy_behaves_like_step_failed() {
    let mut engine = test_engine();
    engine.register(&WF, |ctx: Context| async move {
        let _: i32 = ctx
            .step("s1")
            .run(async || Err(StepError::retryable("boom")))
            .await?;
        Ok(())
    });
    engine.start().await.unwrap();

    let state = engine.invoke(&WF).await.unwrap().wait().await;
    // No retry policy -> StepFailed (not RetriesExhausted).
    assert!(
        matches!(state, WaitResult::Failed(ref msg) if msg.contains("boom") && !msg.contains("attempts"))
    );
}

// ── input payload tests ──────────────────────────────────────
