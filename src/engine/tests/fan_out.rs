use super::*;
// ---------------------------------------------------------------------------
// Phase 3: Child workflow spawning
// ---------------------------------------------------------------------------

const CHILD_WF: WorkflowDef<String, String> = WorkflowDef::new("child-wf");

async fn child_workflow(_ctx: Context, input: String) -> Result<String, EngineError> {
    Ok(format!("processed:{input}"))
}

#[tokio::test]
async fn child_step_key_isolation() {
    const PARENT: WorkflowDef = WorkflowDef::new("parent");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        let parent_val: String = ctx
            .step("work:v1")
            .run(async || Ok("parent-result".to_string()))
            .await?;
        assert_eq!(parent_val, "parent-result");

        let input_data = crate::context::StepData::Completed {
            result: "child-input".to_string(),
            status: None,
        };
        let input_bytes = crate::context::serialize_step(&input_data, "_input").unwrap();
        let (child_id, mut rx) = ctx
            .spawn_child("child-wf", "child-0", Some(input_bytes))
            .await?;
        assert!(child_id.contains("/child-0"));

        loop {
            let state = rx.borrow().clone();
            if state.is_terminal() {
                break;
            }
            if rx.changed().await.is_err() {
                break;
            }
        }

        let child_output: String = read_output(ctx.db(), "child-wf", &child_id)?;
        assert_eq!(child_output, "processed:child-input");
        Ok(())
    });
    engine.register(&CHILD_WF, child_workflow);
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
}

#[tokio::test]
async fn child_output_readable_via_helper() {
    let mut engine = test_engine();
    engine.register(&CHILD_WF, child_workflow);
    engine.start().await.unwrap();

    let child_instance_id = "test-parent/child-key-0".to_string();
    let input_data = crate::context::StepData::Completed {
        result: "hello".to_string(),
        status: None,
    };
    let input_bytes = crate::context::serialize_step(&input_data, "_input").unwrap();

    let inv = Engine::spawn_workflow::<String>(
        &engine.shared,
        "child-wf",
        child_instance_id.clone(),
        Some(input_bytes),
    )
    .await
    .unwrap();

    let _ = inv.wait().await.unwrap_completed();

    let output: String = read_output(&engine.shared.db, "child-wf", &child_instance_id).unwrap();
    assert_eq!(output, "processed:hello");
}

#[tokio::test]
async fn child_metadata_written() {
    let mut engine = test_engine();
    engine.register(&CHILD_WF, child_workflow);
    engine.start().await.unwrap();

    let child_instance_id = "parent-1/child-0".to_string();
    let input_data = crate::context::StepData::Completed {
        result: "data".to_string(),
        status: None,
    };
    let input_bytes = crate::context::serialize_step(&input_data, "_input").unwrap();

    let inv = Engine::spawn_workflow::<String>(
        &engine.shared,
        "child-wf",
        child_instance_id.clone(),
        Some(input_bytes),
    )
    .await
    .unwrap();

    let _ = inv.wait().await.unwrap_completed();

    let meta = metadata::read_metadata(&engine.shared.db, "child-wf", &child_instance_id)
        .unwrap()
        .expect("child metadata should exist");
    assert!(meta.status().is_terminal());
    assert!(matches!(meta.status(), MetadataStatus::Completed(_)));
}

#[tokio::test]
async fn child_appears_in_list_metadata() {
    let mut engine = test_engine();
    engine.register(&CHILD_WF, child_workflow);
    engine.start().await.unwrap();

    let input_data = crate::context::StepData::Completed {
        result: "x".to_string(),
        status: None,
    };
    let input_bytes = crate::context::serialize_step(&input_data, "_input").unwrap();

    let inv = Engine::spawn_workflow::<String>(
        &engine.shared,
        "child-wf",
        "parent-1/child-0".to_string(),
        Some(input_bytes.clone()),
    )
    .await
    .unwrap();
    let _ = inv.wait().await.unwrap_completed();

    let inv2 = Engine::spawn_workflow::<String>(
        &engine.shared,
        "child-wf",
        "top-level-instance".to_string(),
        Some(input_bytes),
    )
    .await
    .unwrap();
    let _ = inv2.wait().await.unwrap_completed();

    let all = metadata::list_metadata(&engine.shared.db, "child-wf").unwrap();
    assert_eq!(all.len(), 2);

    let child_ids: Vec<&str> = all.iter().map(|(id, _)| id.as_str()).collect();
    assert!(child_ids.contains(&"parent-1/child-0"));
    assert!(child_ids.contains(&"top-level-instance"));
}

#[tokio::test]
async fn child_input_output_round_trip() {
    const ECHO: WorkflowDef<i32, i32> = WorkflowDef::new("echo");

    async fn echo(_ctx: Context, val: i32) -> Result<i32, EngineError> {
        Ok(val * 10)
    }

    let mut engine = test_engine();
    engine.register(&ECHO, echo);
    engine.start().await.unwrap();

    let input_data = crate::context::StepData::Completed {
        result: 42_i32,
        status: None,
    };
    let input_bytes = crate::context::serialize_step(&input_data, "_input").unwrap();

    let inv = Engine::spawn_workflow::<i32>(
        &engine.shared,
        "echo",
        "parent-99/echo-child-0".to_string(),
        Some(input_bytes),
    )
    .await
    .unwrap();

    let _ = inv.wait().await.unwrap_completed();

    let output: i32 = read_output(&engine.shared.db, "echo", "parent-99/echo-child-0").unwrap();
    assert_eq!(output, 420);
}

#[tokio::test]
async fn nested_child_key_isolation() {
    const LEVEL1: WorkflowDef<(), String> = WorkflowDef::new("level1");
    const LEVEL2: WorkflowDef<(), String> = WorkflowDef::new("level2");

    let mut engine = test_engine();

    engine.register(&LEVEL2, |ctx: Context| async move {
        let val: String = ctx
            .step("work:v1")
            .run(async || Ok("leaf-result".to_string()))
            .await?;
        Ok(val)
    });

    engine.register(&LEVEL1, |ctx: Context| async move {
        let val: String = ctx
            .step("work:v1")
            .run(async || Ok("mid-result".to_string()))
            .await?;

        let (grandchild_id, mut rx) = ctx.spawn_child("level2", "grandchild-0", None).await?;

        loop {
            let state = rx.borrow().clone();
            if state.is_terminal() {
                break;
            }
            if rx.changed().await.is_err() {
                break;
            }
        }

        let grandchild_output: String = read_output(ctx.db(), "level2", &grandchild_id)?;
        assert_eq!(grandchild_output, "leaf-result");
        assert_eq!(val, "mid-result");
        Ok(val)
    });

    engine.start().await.unwrap();

    // Spawn level1 as a child of a fake parent
    let inv = Engine::spawn_workflow::<String>(
        &engine.shared,
        "level1",
        "root/level1-child-0".to_string(),
        None,
    )
    .await
    .unwrap();

    let _ = inv.wait().await.unwrap_completed();

    let output: String = read_output(&engine.shared.db, "level1", "root/level1-child-0").unwrap();
    assert_eq!(output, "mid-result");

    // Verify step keys are fully isolated despite both workflows using "work:v1"
    let read_txn = engine.shared.db.begin_read().unwrap();
    let steps_table = read_txn.open_table(STEPS).unwrap();

    let level1_step = "level1/root/level1-child-0/work:v1";
    assert!(steps_table.get(level1_step).unwrap().is_some());

    let level2_step = "level2/root/level1-child-0/grandchild-0/work:v1";
    assert!(steps_table.get(level2_step).unwrap().is_some());
}

#[tokio::test]
async fn spawn_child_from_context() {
    const PARENT: WorkflowDef = WorkflowDef::new("ctx-parent");

    let mut engine = test_engine();
    engine.register(&CHILD_WF, child_workflow);
    engine.register(&PARENT, |ctx: Context| async move {
        let input_data = crate::context::StepData::Completed {
            result: "from-parent".to_string(),
            status: None,
        };
        let input_bytes = crate::context::serialize_step(&input_data, "_input").unwrap();
        let (child_id, mut rx) = ctx
            .spawn_child("child-wf", "my-child-0", Some(input_bytes))
            .await?;

        assert!(child_id.ends_with("/my-child-0"));

        loop {
            let state = rx.borrow().clone();
            if state.is_terminal() {
                break;
            }
            if rx.changed().await.is_err() {
                break;
            }
        }

        let output: String = read_output(ctx.db(), "child-wf", &child_id)?;
        assert_eq!(output, "processed:from-parent");
        Ok(())
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
}

// ── Phase 4: Fan-out tests ──────────────────────────────────────────

#[tokio::test]
async fn fan_out_basic_inline() {
    const PARENT: WorkflowDef<(), Vec<String>> = WorkflowDef::new("fan-parent");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        let items = vec!["hello".to_string(), "world".to_string(), "test".to_string()];
        let results: Vec<String> = ctx
            .fan_out("upper:v1", items)
            .fail_fast()
            .run(|child_ctx, item| async move {
                let upper: String = child_ctx
                    .step("process:v1")
                    .run(async move || Ok(item.to_uppercase()))
                    .await?;
                Ok(upper)
            })
            .await?;
        assert_eq!(results, vec!["HELLO", "WORLD", "TEST"]);
        Ok(results)
    });
    engine.start().await.unwrap();

    let completed = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
    let output: Vec<String> = completed.output().unwrap();
    assert_eq!(output, vec!["HELLO", "WORLD", "TEST"]);
}

#[tokio::test]
async fn fan_out_concurrency_respected() {
    const PARENT: WorkflowDef = WorkflowDef::new("fan-conc");

    let max_concurrent = Arc::new(AtomicU32::new(0));
    let current = Arc::new(AtomicU32::new(0));
    let mc = Arc::clone(&max_concurrent);
    let cc = Arc::clone(&current);

    let mut engine = test_engine();
    engine.register(&PARENT, move |ctx: Context| {
        let mc = Arc::clone(&mc);
        let cc = Arc::clone(&cc);
        async move {
            let items: Vec<i32> = (0..6).collect();
            let _: Vec<i32> = ctx
                .fan_out("work:v1", items)
                .concurrency(2)
                .fail_fast()
                .run(move |child_ctx, item| {
                    let mc = Arc::clone(&mc);
                    let cc = Arc::clone(&cc);
                    async move {
                        let prev = cc.fetch_add(1, Ordering::SeqCst);
                        mc.fetch_max(prev + 1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        cc.fetch_sub(1, Ordering::SeqCst);
                        let val: i32 = child_ctx.step("id:v1").run(async move || Ok(item)).await?;
                        Ok(val)
                    }
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();

    assert!(
        max_concurrent.load(Ordering::SeqCst) <= 2,
        "max concurrent was {}, expected <= 2",
        max_concurrent.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn fan_out_parent_suspended() {
    const PARENT: WorkflowDef = WorkflowDef::new("fan-suspend");

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel::<()>();
    let ready_tx = Arc::new(tokio::sync::Mutex::new(Some(ready_tx)));
    let proceed_rx = Arc::new(tokio::sync::Mutex::new(Some(proceed_rx)));

    let mut engine = test_engine();
    engine.register(&PARENT, move |ctx: Context| {
        let ready_tx = Arc::clone(&ready_tx);
        let proceed_rx = Arc::clone(&proceed_rx);
        async move {
            let items = vec![1];
            let _: Vec<i32> = ctx
                .fan_out("wait:v1", items)
                .fail_fast()
                .run(move |_child_ctx, item| {
                    let ready_tx = Arc::clone(&ready_tx);
                    let proceed_rx = Arc::clone(&proceed_rx);
                    async move {
                        if let Some(tx) = ready_tx.lock().await.take() {
                            let _ = tx.send(());
                        }
                        if let Some(rx) = proceed_rx.lock().await.take() {
                            let _ = rx.await;
                        }
                        Ok(item)
                    }
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let mut inv = engine.invoke(&PARENT).await.unwrap();
    let instance_id = inv.instance_id().to_string();

    // Wait until the child signals it's running.
    ready_rx.await.unwrap();

    // Parent task is still alive while child runs (not Suspended, not terminal).
    let state = engine.state(&PARENT, &instance_id).unwrap();
    assert!(
        !state.is_terminal(),
        "expected non-terminal while child runs, got {state:?}"
    );

    // Let the child finish.
    let _ = proceed_tx.send(());

    // Wait for parent to complete.
    loop {
        let state = inv.status().borrow().clone();
        if state.is_terminal() {
            break;
        }
        if inv.status().changed().await.is_err() {
            break;
        }
    }

    assert!(inv.status().borrow().is_terminal());
}

#[tokio::test]
async fn fan_out_results_memoised() {
    const PARENT: WorkflowDef<(), Vec<i32>> = WorkflowDef::new("fan-memo");

    let call_count = Arc::new(AtomicU32::new(0));
    let cc = Arc::clone(&call_count);

    let mut engine = test_engine();
    engine.register(&PARENT, move |ctx: Context| {
        let cc = Arc::clone(&cc);
        async move {
            let items = vec![10, 20, 30];
            let results: Vec<i32> = ctx
                .fan_out("calc:v1", items)
                .fail_fast()
                .run(move |child_ctx, item| {
                    let cc = Arc::clone(&cc);
                    async move {
                        cc.fetch_add(1, Ordering::Relaxed);
                        let val: i32 = child_ctx
                            .step("double:v1")
                            .run(async move || Ok(item * 2))
                            .await?;
                        Ok(val)
                    }
                })
                .await?;
            Ok(results)
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke(&PARENT).await.unwrap();
    let instance_id = inv.instance_id().to_string();
    let completed = inv.wait().await.unwrap_completed();
    let output: Vec<i32> = completed.output().unwrap();
    assert_eq!(output, vec![20, 40, 60]);
    assert_eq!(call_count.load(Ordering::Relaxed), 3);

    // Resume — fan-out should return cached results.
    call_count.store(0, Ordering::Relaxed);
    let completed = engine
        .resume(&PARENT, &instance_id)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
    let output: Vec<i32> = completed.output().unwrap();
    assert_eq!(output, vec![20, 40, 60]);
    assert_eq!(call_count.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn fan_out_empty_items() {
    const PARENT: WorkflowDef<(), Vec<i32>> = WorkflowDef::new("fan-empty");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        let items: Vec<i32> = vec![];
        let results: Vec<i32> = ctx
            .fan_out("noop:v1", items)
            .fail_fast()
            .run(|_child_ctx, item| async move { Ok(item) })
            .await?;
        assert!(results.is_empty());
        Ok(results)
    });
    engine.start().await.unwrap();

    let completed = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
    let output: Vec<i32> = completed.output().unwrap();
    assert!(output.is_empty());
}

#[tokio::test]
async fn fan_out_child_failure_aborts_remaining() {
    const PARENT: WorkflowDef = WorkflowDef::new("fan-fail");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        let items = vec![1, 2, 3];
        let _: Vec<i32> = ctx
            .fan_out("fail:v1", items)
            .concurrency(1)
            .fail_fast()
            .run(|child_ctx, item| async move {
                if item == 2 {
                    return Err(EngineError::step_failed("boom", "child 2 failed", false));
                }
                let val: i32 = child_ctx.step("id:v1").run(async move || Ok(item)).await?;
                Ok(val)
            })
            .await?;
        Ok(())
    });
    engine.start().await.unwrap();

    let inv = engine.invoke(&PARENT).await.unwrap();
    let result = inv.wait().await;
    assert!(result.is_failed(), "expected Failed, got {result}");
}

#[tokio::test]
async fn fan_out_child_metadata_written() {
    const PARENT: WorkflowDef = WorkflowDef::new("fan-meta");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        let items = vec!["a".to_string(), "b".to_string()];
        let _: Vec<String> = ctx
            .fan_out("meta:v1", items)
            .fail_fast()
            .run(|child_ctx, item| async move {
                let val: String = child_ctx
                    .step("echo:v1")
                    .run(async move || Ok(item.clone()))
                    .await?;
                Ok(val)
            })
            .await?;
        Ok(())
    });
    engine.start().await.unwrap();

    let inv = engine.invoke(&PARENT).await.unwrap();
    let parent_id = inv.instance_id().to_string();
    let _ = inv.wait().await.unwrap_completed();

    // Children should have metadata entries.
    let all = engine.list_instances(&PARENT).unwrap();
    let child_ids: Vec<String> = all
        .iter()
        .filter(|(id, _)| id.starts_with(&format!("{parent_id}/")))
        .map(|(id, _)| id.clone())
        .collect();
    assert_eq!(child_ids.len(), 2);

    for (_, meta) in &all {
        if matches!(meta.status(), MetadataStatus::Running) {
            panic!("child metadata should not be Running after completion");
        }
    }
}

#[tokio::test]
async fn fan_out_registered_workflow() {
    const PARENT: WorkflowDef<(), Vec<String>> = WorkflowDef::new("fan-reg-parent");
    const CHILD: WorkflowDef<String, String> = WorkflowDef::new("fan-reg-child");

    async fn child_wf(_ctx: Context, input: String) -> Result<String, EngineError> {
        Ok(format!("processed:{input}"))
    }

    let mut engine = test_engine();
    engine.register(&CHILD, child_wf);
    engine.register(&PARENT, |ctx: Context| async move {
        let items = vec!["x".to_string(), "y".to_string()];
        let results: Vec<String> = ctx
            .fan_out("reg:v1", items)
            .fail_fast()
            .workflow(&CHILD)
            .await?;
        Ok(results)
    });
    engine.start().await.unwrap();

    let completed = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
    let output: Vec<String> = completed.output().unwrap();
    assert_eq!(output, vec!["processed:x", "processed:y"]);
}

// ── Phase 5: Error mode tests ───────────────────────────────────────

#[tokio::test]
async fn fan_out_collect_all_mixed() {
    const PARENT: WorkflowDef = WorkflowDef::new("fan-ca-mix");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        let items = vec![1, 2, 3];
        let results = ctx
            .fan_out("mix:v1", items)
            .concurrency(1)
            .run(|child_ctx, item| async move {
                if item == 2 {
                    return Err(EngineError::step_failed("boom", "child 2 failed", false));
                }
                let val: i32 = child_ctx.step("id:v1").run(async move || Ok(item)).await?;
                Ok(val)
            })
            .await?;

        assert!(results[0].is_ok());
        assert_eq!(*results[0].as_ref().unwrap(), 1);

        assert!(results[1].is_err());
        let err = results[1].as_ref().unwrap_err();
        assert!(err.message().contains("child 2 failed"));

        assert!(results[2].is_ok());
        assert_eq!(*results[2].as_ref().unwrap(), 3);

        Ok(())
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
}

#[tokio::test]
async fn fan_out_collect_all_all_succeed() {
    const PARENT: WorkflowDef = WorkflowDef::new("fan-ca-ok");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        let items = vec![10, 20, 30];
        let results = ctx
            .fan_out("ok:v1", items)
            .run(|child_ctx, item| async move {
                let val: i32 = child_ctx
                    .step("double:v1")
                    .run(async move || Ok(item * 2))
                    .await?;
                Ok(val)
            })
            .await?;

        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.is_ok());
        }
        let values: Vec<i32> = results.into_iter().map(Result::unwrap).collect();
        assert_eq!(values, vec![20, 40, 60]);
        Ok(())
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
}

#[tokio::test]
async fn fan_out_collect_all_all_fail() {
    const PARENT: WorkflowDef = WorkflowDef::new("fan-ca-fail");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        let items = vec!["a".to_string(), "b".to_string()];
        let results = ctx
            .fan_out("allfail:v1", items)
            .run(|_child_ctx, item| async move {
                Err::<String, _>(EngineError::step_failed(
                    "step",
                    format!("{item} broke"),
                    false,
                ))
            })
            .await?;

        assert_eq!(results.len(), 2);
        for r in &results {
            assert!(r.is_err());
        }

        let err_a = results[0].as_ref().unwrap_err();
        assert!(err_a.instance_id().contains("allfail:v1-0"));
        assert!(err_a.message().contains("a broke"));

        let err_b = results[1].as_ref().unwrap_err();
        assert!(err_b.instance_id().contains("allfail:v1-1"));
        assert!(err_b.message().contains("b broke"));

        Ok(())
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
}

#[tokio::test]
async fn fan_out_collect_all_memoised() {
    const PARENT: WorkflowDef = WorkflowDef::new("fan-ca-memo");

    let call_count = Arc::new(AtomicU32::new(0));
    let cc = Arc::clone(&call_count);

    let mut engine = test_engine();
    engine.register(&PARENT, move |ctx: Context| {
        let cc = Arc::clone(&cc);
        async move {
            let items = vec![1, 2, 3];
            let results = ctx
                .fan_out("memo:v1", items)
                .run(move |child_ctx, item| {
                    let cc = Arc::clone(&cc);
                    async move {
                        cc.fetch_add(1, Ordering::Relaxed);
                        if item == 2 {
                            return Err(EngineError::step_failed("boom", "child 2 failed", false));
                        }
                        let val: i32 = child_ctx
                            .step("id:v1")
                            .run(async move || Ok(item * 10))
                            .await?;
                        Ok(val)
                    }
                })
                .await?;

            assert_eq!(results.len(), 3);
            assert!(results[0].is_ok());
            assert!(results[1].is_err());
            assert!(results[2].is_ok());
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke(&PARENT).await.unwrap();
    let instance_id = inv.instance_id().to_string();
    let _ = inv.wait().await.unwrap_completed();
    assert_eq!(call_count.load(Ordering::Relaxed), 3);

    // Resume — should use cached collect-all results.
    call_count.store(0, Ordering::Relaxed);
    let _ = engine
        .resume(&PARENT, &instance_id)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
    assert_eq!(call_count.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn fan_out_collect_all_registered_workflow() {
    const PARENT: WorkflowDef = WorkflowDef::new("fan-ca-reg-parent");
    const CHILD: WorkflowDef<String, String> = WorkflowDef::new("fan-ca-reg-child");

    async fn child_wf(_ctx: Context, input: String) -> Result<String, EngineError> {
        if input == "bad" {
            return Err(EngineError::step_failed("step", "bad input", false));
        }
        Ok(format!("ok:{input}"))
    }

    let mut engine = test_engine();
    engine.register(&CHILD, child_wf);
    engine.register(&PARENT, |ctx: Context| async move {
        let items = vec!["a".to_string(), "bad".to_string(), "c".to_string()];
        let results = ctx.fan_out("reg:v1", items).workflow(&CHILD).await?;

        assert!(results[0].as_ref().unwrap() == "ok:a");
        assert!(results[1].is_err());
        assert!(results[2].as_ref().unwrap() == "ok:c");
        Ok(())
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
}

#[tokio::test]
async fn fan_out_fail_fast_preserves_error() {
    const PARENT: WorkflowDef = WorkflowDef::new("fan-ff-err");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        let items = vec![1, 2, 3];
        let result = ctx
            .fan_out("ff:v1", items)
            .concurrency(1)
            .fail_fast()
            .run(|_child_ctx, item| async move {
                if item == 2 {
                    return Err(EngineError::step_failed(
                        "specific-key",
                        "specific error message",
                        false,
                    ));
                }
                Ok(item)
            })
            .await;

        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("specific"),
            "expected original error, got: {err_str}"
        );
        Ok(())
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
}

// ---------------------------------------------------------------------------
// Phase 6: Recursive nesting & hardening
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn fan_out_nested_two_levels_inline() {
    const PARENT: WorkflowDef = WorkflowDef::new("nested-2l");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        let outer_results: Vec<Result<Vec<Result<i32, crate::ChildError>>, crate::ChildError>> =
            ctx.fan_out("outer:v1", vec![1, 2])
                .run(|child_ctx, item| async move {
                    let inner_results: Vec<Result<i32, crate::ChildError>> = child_ctx
                        .fan_out("inner:v1", vec![10, 20])
                        .run(move |grandchild_ctx, inner_item| async move {
                            let val: i32 = grandchild_ctx
                                .step("calc:v1")
                                .run(async move || Ok(item * 100 + inner_item))
                                .await?;
                            Ok(val)
                        })
                        .await?;
                    Ok(inner_results)
                })
                .await?;

        assert_eq!(outer_results.len(), 2);
        let first: Vec<i32> = outer_results[0]
            .as_ref()
            .unwrap()
            .iter()
            .map(|r| *r.as_ref().unwrap())
            .collect();
        let second: Vec<i32> = outer_results[1]
            .as_ref()
            .unwrap()
            .iter()
            .map(|r| *r.as_ref().unwrap())
            .collect();
        assert_eq!(first, vec![110, 120]);
        assert_eq!(second, vec![210, 220]);
        Ok(())
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
}

#[tokio::test(start_paused = true)]
async fn fan_out_nested_two_levels_registered() {
    const PARENT: WorkflowDef = WorkflowDef::new("nested-2l-reg-parent");
    const MIDDLE: WorkflowDef<i32, Vec<Result<i32, crate::ChildError>>> =
        WorkflowDef::new("nested-2l-reg-middle");

    let mut engine = test_engine();

    engine.register(&MIDDLE, |ctx: Context, item: i32| async move {
        let results: Vec<Result<i32, crate::ChildError>> = ctx
            .fan_out("inner:v1", vec![10, 20])
            .run(move |grandchild_ctx, inner_item| async move {
                let val: i32 = grandchild_ctx
                    .step("calc:v1")
                    .run(async move || Ok(item * 100 + inner_item))
                    .await?;
                Ok(val)
            })
            .await?;
        Ok(results)
    });

    engine.register(&PARENT, |ctx: Context| async move {
        let outer_results = ctx
            .fan_out("outer:v1", vec![1, 2])
            .workflow(&MIDDLE)
            .await?;

        assert_eq!(outer_results.len(), 2);
        let first: Vec<i32> = outer_results[0]
            .as_ref()
            .unwrap()
            .iter()
            .map(|r| *r.as_ref().unwrap())
            .collect();
        let second: Vec<i32> = outer_results[1]
            .as_ref()
            .unwrap()
            .iter()
            .map(|r| *r.as_ref().unwrap())
            .collect();
        assert_eq!(first, vec![110, 120]);
        assert_eq!(second, vec![210, 220]);
        Ok(())
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
}

#[tokio::test(start_paused = true)]
async fn fan_out_nested_three_levels_mixed_error_modes() {
    const PARENT: WorkflowDef = WorkflowDef::new("nested-3l-mixed");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        // Level 1: collect-all — collects all L1 children including failures
        let results: Vec<Result<String, crate::ChildError>> = ctx
            .fan_out("l1:v1", vec![1, 2])
            .run(|l1_ctx, l1_item| async move {
                // Level 2: fail-fast — aborts remaining L2 children on first error
                let l2_result: Result<Vec<String>, EngineError> = l1_ctx
                    .fan_out("l2:v1", vec![10, 20])
                    .concurrency(1)
                    .fail_fast()
                    .run(move |l2_ctx, l2_item| async move {
                        // Level 3: collect-all — collects grandchild results
                        let l3_results: Vec<Result<i32, crate::ChildError>> = l2_ctx
                            .fan_out("l3:v1", vec![100, 200])
                            .run(move |l3_ctx, l3_item| async move {
                                let combined = l1_item * 1000 + l2_item * 10 + l3_item;
                                if combined == 2_200 {
                                    return Err(EngineError::step_failed(
                                        "boom",
                                        "deliberate failure",
                                        false,
                                    ));
                                }
                                let val: i32 = l3_ctx
                                    .step("calc:v1")
                                    .run(async move || Ok(combined))
                                    .await?;
                                Ok(val)
                            })
                            .await?;

                        // Propagate L3 child errors to trigger L2 fail-fast
                        for r in &l3_results {
                            if let Err(e) = r {
                                return Err(EngineError::step_failed(
                                    "l3-child-failed",
                                    e.message(),
                                    false,
                                ));
                            }
                        }
                        Ok(format!("l2-ok:{l2_item}"))
                    })
                    .await;

                match l2_result {
                    Ok(vals) => Ok(format!("all-ok:{}", vals.join(","))),
                    Err(e) => Ok(format!("l2-failed:{e}")),
                }
            })
            .await?;

        assert_eq!(results.len(), 2);
        // L1 item=1: no failures at any level
        let r0 = results[0].as_ref().unwrap();
        assert!(r0.starts_with("all-ok:"), "expected all-ok, got: {r0}");
        // L1 item=2: grandchild 2200 fails → L3 collects it → L2 closure
        // propagates → L2 fail-fast aborts → L1 collects as success with
        // error message
        let r1 = results[1].as_ref().unwrap();
        assert!(
            r1.starts_with("l2-failed:"),
            "expected l2 failure, got: {r1}"
        );
        Ok(())
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
}

#[tokio::test(start_paused = true)]
async fn fan_out_nested_key_isolation() {
    const PARENT: WorkflowDef = WorkflowDef::new("nested-keys");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        // Parent uses "work:v1"
        let parent_val: String = ctx
            .step("work:v1")
            .run(async || Ok("parent-value".to_string()))
            .await?;
        assert_eq!(parent_val, "parent-value");

        let results: Vec<Result<String, crate::ChildError>> = ctx
            .fan_out("batch:v1", vec!["a".to_string(), "b".to_string()])
            .run(|child_ctx, item| async move {
                // Child also uses "work:v1" — must not collide with parent
                let child_val: String = child_ctx
                    .step("work:v1")
                    .run(async move || Ok(format!("child-{item}")))
                    .await?;

                let inner_results: Vec<Result<String, crate::ChildError>> = child_ctx
                    .fan_out("sub:v1", vec!["x".to_string()])
                    .run(|gc_ctx, gc_item| async move {
                        // Grandchild also uses "work:v1"
                        let gc_val: String = gc_ctx
                            .step("work:v1")
                            .run(async move || Ok(format!("gc-{gc_item}")))
                            .await?;
                        Ok(gc_val)
                    })
                    .await?;

                let gc_val = inner_results[0].as_ref().unwrap().clone();
                Ok(format!("{child_val}+{gc_val}"))
            })
            .await?;

        let r0 = results[0].as_ref().unwrap();
        let r1 = results[1].as_ref().unwrap();
        assert_eq!(r0, "child-a+gc-x");
        assert_eq!(r1, "child-b+gc-x");
        Ok(())
    });
    engine.start().await.unwrap();

    let inv = engine.invoke(&PARENT).await.unwrap();
    let id = inv.instance_id().to_string();
    let _ = inv.wait().await.unwrap_completed();

    // Verify distinct composite keys in redb
    let read_txn = engine.shared.db.begin_read().unwrap();
    let steps_table = read_txn.open_table(STEPS).unwrap();
    let prefix = format!("nested-keys/{id}/");
    let end = format!("nested-keys/{id}0");
    let keys: Vec<String> = steps_table
        .range(prefix.as_str()..end.as_str())
        .unwrap()
        .map(|e| e.unwrap().0.value().to_string())
        .collect();

    // Parent "work:v1" should be distinct from children's "work:v1"
    let parent_key = format!("nested-keys/{id}/work:v1");
    assert!(keys.contains(&parent_key), "parent key missing: {keys:?}");

    let child0_key = format!("nested-keys/{id}/batch:v1-0/work:v1");
    assert!(keys.contains(&child0_key), "child 0 key missing: {keys:?}");

    let gc0_key = format!("nested-keys/{id}/batch:v1-0/sub:v1-0/work:v1");
    assert!(
        keys.contains(&gc0_key),
        "grandchild 0 key missing: {keys:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn fan_out_nested_independent_concurrency() {
    use std::sync::atomic::AtomicUsize;

    const PARENT: WorkflowDef = WorkflowDef::new("nested-conc");

    let outer_max = Arc::new(AtomicUsize::new(0));
    let outer_current = Arc::new(AtomicUsize::new(0));
    let inner_max = Arc::new(AtomicUsize::new(0));
    let inner_current = Arc::new(AtomicUsize::new(0));

    let om = Arc::clone(&outer_max);
    let oc = Arc::clone(&outer_current);
    let im = Arc::clone(&inner_max);
    let ic = Arc::clone(&inner_current);

    let mut engine = test_engine();
    engine.register(&PARENT, move |ctx: Context| {
        let om = Arc::clone(&om);
        let oc = Arc::clone(&oc);
        let im = Arc::clone(&im);
        let ic = Arc::clone(&ic);
        async move {
            let _: Vec<Result<i32, crate::ChildError>> = ctx
                .fan_out("outer:v1", vec![1, 2, 3, 4])
                .concurrency(1)
                .run(move |child_ctx, item| {
                    let om = Arc::clone(&om);
                    let oc = Arc::clone(&oc);
                    let im = Arc::clone(&im);
                    let ic = Arc::clone(&ic);
                    async move {
                        let prev = oc.fetch_add(1, Ordering::SeqCst);
                        om.fetch_max(prev + 1, Ordering::SeqCst);

                        let _: Vec<Result<i32, crate::ChildError>> = child_ctx
                            .fan_out("inner:v1", vec![10, 20])
                            .concurrency(2)
                            .run(move |gc_ctx, inner_item| {
                                let ic = Arc::clone(&ic);
                                let im = Arc::clone(&im);
                                async move {
                                    let prev = ic.fetch_add(1, Ordering::SeqCst);
                                    im.fetch_max(prev + 1, Ordering::SeqCst);

                                    let val: i32 = gc_ctx
                                        .step("calc:v1")
                                        .run(async move || Ok(item * 100 + inner_item))
                                        .await?;

                                    ic.fetch_sub(1, Ordering::SeqCst);
                                    Ok(val)
                                }
                            })
                            .await?;

                        oc.fetch_sub(1, Ordering::SeqCst);
                        Ok(item)
                    }
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();

    assert_eq!(
        outer_max.load(Ordering::SeqCst),
        1,
        "outer concurrency should be limited to 1"
    );
    assert!(
        inner_max.load(Ordering::SeqCst) <= 2,
        "inner concurrency should be limited to 2, got {}",
        inner_max.load(Ordering::SeqCst)
    );
}

#[tokio::test(start_paused = true)]
async fn fan_out_nested_memoisation_across_levels() {
    use std::sync::atomic::AtomicUsize;

    const PARENT: WorkflowDef = WorkflowDef::new("nested-memo");

    let exec_count = Arc::new(AtomicUsize::new(0));
    let ec = Arc::clone(&exec_count);

    let mut engine = test_engine();
    engine.register(&PARENT, move |ctx: Context| {
        let ec = Arc::clone(&ec);
        async move {
            let _: Vec<Result<Vec<Result<i32, crate::ChildError>>, crate::ChildError>> = ctx
                .fan_out("outer:v1", vec![1, 2])
                .run(move |child_ctx, item| {
                    let ec = Arc::clone(&ec);
                    async move {
                        let inner: Vec<Result<i32, crate::ChildError>> = child_ctx
                            .fan_out("inner:v1", vec![10, 20])
                            .run(move |gc_ctx, inner_item| {
                                let ec = Arc::clone(&ec);
                                async move {
                                    ec.fetch_add(1, Ordering::SeqCst);
                                    let val: i32 = gc_ctx
                                        .step("calc:v1")
                                        .run(async move || Ok(item * 100 + inner_item))
                                        .await?;
                                    Ok(val)
                                }
                            })
                            .await?;
                        Ok(inner)
                    }
                })
                .await?;
            Ok(())
        }
    });
    engine.start().await.unwrap();

    let inv = engine.invoke(&PARENT).await.unwrap();
    let id = inv.instance_id().to_string();
    let _ = inv.wait().await.unwrap_completed();

    let first_run_count = exec_count.load(Ordering::SeqCst);
    assert_eq!(first_run_count, 4, "should execute 4 grandchild closures");

    // Resume the same instance — everything should come from cache
    let inv2 = Engine::spawn_workflow::<()>(&engine.shared, "nested-memo", id, None)
        .await
        .unwrap();
    let _ = inv2.wait().await.unwrap_completed();

    assert_eq!(
        exec_count.load(Ordering::SeqCst),
        first_run_count,
        "no additional closures should execute on replay"
    );
}

#[tokio::test(start_paused = true)]
async fn fan_out_nested_empty_inner() {
    const PARENT: WorkflowDef = WorkflowDef::new("nested-empty");

    let mut engine = test_engine();
    engine.register(&PARENT, |ctx: Context| async move {
        let results: Vec<Result<Vec<Result<i32, crate::ChildError>>, crate::ChildError>> = ctx
            .fan_out("outer:v1", vec![1, 2])
            .run(|child_ctx, item| async move {
                let inner_items = if item == 1 { vec![] } else { vec![10, 20] };
                let inner: Vec<Result<i32, crate::ChildError>> = child_ctx
                    .fan_out("inner:v1", inner_items)
                    .run(move |gc_ctx, inner_item| async move {
                        let val: i32 = gc_ctx
                            .step("calc:v1")
                            .run(async move || Ok(item * 100 + inner_item))
                            .await?;
                        Ok(val)
                    })
                    .await?;
                Ok(inner)
            })
            .await?;

        assert_eq!(results.len(), 2);

        let first = results[0].as_ref().unwrap();
        assert!(first.is_empty(), "item=1 should have empty inner results");

        let second: Vec<i32> = results[1]
            .as_ref()
            .unwrap()
            .iter()
            .map(|r| *r.as_ref().unwrap())
            .collect();
        assert_eq!(second, vec![210, 220]);
        Ok(())
    });
    engine.start().await.unwrap();

    let _ = engine
        .invoke(&PARENT)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap_completed();
}

#[tokio::test]
async fn retention_cleanup_nested_inline_steps() {
    const PARENT: WorkflowDef = WorkflowDef::new("ret-nested");

    let mut engine = Engine::builder()
        .retention(Duration::from_secs(1))
        .in_memory()
        .build();

    engine.register(&PARENT, |ctx: Context| async move {
        let _: Vec<Result<Vec<Result<i32, crate::ChildError>>, crate::ChildError>> = ctx
            .fan_out("outer:v1", vec![1, 2])
            .run(|child_ctx, item| async move {
                let inner: Vec<Result<i32, crate::ChildError>> = child_ctx
                    .fan_out("inner:v1", vec![10])
                    .run(move |gc_ctx, inner_item| async move {
                        let val: i32 = gc_ctx
                            .step("calc:v1")
                            .run(async move || Ok(item * 100 + inner_item))
                            .await?;
                        Ok(val)
                    })
                    .await?;
                Ok(inner)
            })
            .await?;
        Ok(())
    });
    engine.start().await.unwrap();

    let inv = engine.invoke(&PARENT).await.unwrap();
    let id = inv.instance_id().to_string();
    let _ = inv.wait().await.unwrap_completed();

    // Count all step entries for this instance (parent + children + grandchildren)
    let read_txn = engine.shared.db.begin_read().unwrap();
    let steps_table = read_txn.open_table(STEPS).unwrap();
    let prefix = format!("ret-nested/{id}/");
    let end = format!("ret-nested/{id}0");
    let step_count = steps_table
        .range(prefix.as_str()..end.as_str())
        .unwrap()
        .count();
    drop(steps_table);
    drop(read_txn);

    // Should have steps at multiple nesting levels
    assert!(
        step_count > 3,
        "expected steps at multiple depths, got {step_count}"
    );

    // Wait past retention and clean up
    tokio::time::sleep(Duration::from_secs(2)).await;
    let cleaned =
        cleanup_expired(&engine.shared.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert!(cleaned >= 1, "should clean at least the parent");

    // All parent+child steps should be gone (they share the step prefix)
    let read_txn = engine.shared.db.begin_read().unwrap();
    let steps_table = read_txn.open_table(STEPS).unwrap();
    let remaining = steps_table
        .range(prefix.as_str()..end.as_str())
        .unwrap()
        .count();
    assert_eq!(remaining, 0, "all nested steps should be cleaned up");
}
