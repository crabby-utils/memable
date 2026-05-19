use super::*;
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Workflow exists before cleanup.
    assert!(engine.get_metadata(&WF, &id).unwrap().is_some());

    // Verify steps exist.
    let read_txn = engine.shared.db.begin_read().unwrap();
    let steps_table = read_txn.open_table(STEPS).unwrap();
    let step_key = format!("wf/{id}/s1");
    assert!(steps_table.get(step_key.as_str()).unwrap().is_some());
    drop(steps_table);
    drop(read_txn);

    // Sleep past retention.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Run cleanup directly.
    let cleaned =
        cleanup_expired(&engine.shared.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 1);

    // Metadata gone.
    assert!(engine.get_metadata(&WF, &id).unwrap().is_none());

    // Steps gone.
    let read_txn = engine.shared.db.begin_read().unwrap();
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
    engine.register(&WF, failing);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let cleaned =
        cleanup_expired(&engine.shared.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 1);
    assert!(engine.get_metadata(&WF, &id).unwrap().is_none());
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
    engine.register(&WF, slow);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();

    // Give the task time to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Cleanup should find nothing to clean.
    let cleaned =
        cleanup_expired(&engine.shared.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 0);
    assert!(engine.get_metadata(&WF, &id).unwrap().is_some());

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
    engine.register(&WF, suspending);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let cleaned =
        cleanup_expired(&engine.shared.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 0);
    assert!(engine.get_metadata(&WF, &id).unwrap().is_some());
}

#[tokio::test]
async fn retention_per_workflow_override() {
    const LONG_LIVED: WorkflowDef = WorkflowDef::new("long-lived");
    const SHORT_LIVED: WorkflowDef = WorkflowDef::new("short-lived");

    async fn wf(ctx: Context) -> Result<(), EngineError> {
        let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
        Ok(())
    }

    let mut engine = Engine::builder()
        .retention(Duration::from_secs(10))
        .in_memory()
        .build();
    engine.register(&LONG_LIVED, wf);
    engine
        .register(&SHORT_LIVED, wf)
        .retention(Duration::from_secs(1));
    engine.start().await.unwrap();

    let long_inv = engine.invoke(&LONG_LIVED).await.unwrap();
    let long_id = long_inv.instance_id().to_string();
    long_inv.wait().await;

    let short_inv = engine.invoke(&SHORT_LIVED).await.unwrap();
    let short_id = short_inv.instance_id().to_string();
    short_inv.wait().await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut overrides = HashMap::new();
    overrides.insert("short-lived".to_string(), Duration::from_secs(1));

    let cleaned = cleanup_expired(&engine.shared.db, Duration::from_secs(10), &overrides).unwrap();
    assert_eq!(cleaned, 1);

    // short-lived is gone, long-lived remains.
    assert!(
        engine
            .get_metadata(&SHORT_LIVED, &short_id)
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .get_metadata(&LONG_LIVED, &long_id)
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    let cleaned =
        cleanup_expired(&engine.shared.db, Duration::from_secs(100), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 0);
    assert!(engine.get_metadata(&WF, &id).unwrap().is_some());
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    let inv1 = engine.invoke(&WF).await.unwrap();
    inv1.wait().await;
    let inv2 = engine.invoke(&WF).await.unwrap();
    inv2.wait().await;
    let inv3 = engine.invoke(&WF).await.unwrap();
    inv3.wait().await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let cleaned =
        cleanup_expired(&engine.shared.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 3);

    let instances = engine.list_instances(&WF).unwrap();
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
    engine.register(&WF, wf);
    engine.start().await.unwrap();

    // Create and complete an instance.
    let old_inv = engine.invoke(&WF).await.unwrap();
    let old_id = old_inv.instance_id().to_string();
    old_inv.wait().await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Invoke a new instance concurrently with cleanup.
    let new_inv = engine.invoke(&WF).await.unwrap();
    let new_id = new_inv.instance_id().to_string();

    let cleaned =
        cleanup_expired(&engine.shared.db, Duration::from_secs(1), &HashMap::new()).unwrap();
    assert_eq!(cleaned, 1);

    new_inv.wait().await;

    // Old is gone, new survived.
    assert!(engine.get_metadata(&WF, &old_id).unwrap().is_none());
    assert!(engine.get_metadata(&WF, &new_id).unwrap().is_some());
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
    engine.register(&WF, multi_step);
    engine.start().await.unwrap();

    let inv = engine.invoke(&WF).await.unwrap();
    let id = inv.instance_id().to_string();
    inv.wait().await;

    // Verify all step entries exist (3 steps + 1 _output).
    let read_txn = engine.shared.db.begin_read().unwrap();
    let steps_table = read_txn.open_table(STEPS).unwrap();
    let prefix = format!("wf/{id}/");
    let end = format!("wf/{id}0");
    let count = steps_table
        .range(prefix.as_str()..end.as_str())
        .unwrap()
        .count();
    assert_eq!(count, 4);
    drop(steps_table);
    drop(read_txn);

    tokio::time::sleep(Duration::from_secs(2)).await;
    cleanup_expired(&engine.shared.db, Duration::from_secs(1), &HashMap::new()).unwrap();

    // All steps gone.
    let read_txn = engine.shared.db.begin_read().unwrap();
    let steps_table = read_txn.open_table(STEPS).unwrap();
    let count = steps_table
        .range(prefix.as_str()..end.as_str())
        .unwrap()
        .count();
    assert_eq!(count, 0);
}
