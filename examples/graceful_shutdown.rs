//! Graceful shutdown — stop the engine with a configurable timeout.
//!
//! `engine.stop()` prevents new invocations, waits up to the configured
//! timeout for running workflows to complete, then aborts any remaining
//! tasks. Aborted workflows keep their `Running` metadata and resume
//! automatically on the next `engine.start()`.
//!
//! Configure the timeout via `EngineBuilder::shutdown_timeout()` or the
//! `MEMABLE_SHUTDOWN_TIMEOUT_SECS` environment variable.

use std::time::Duration;

use memable::{Context, Engine, EngineError, MetadataStatus, WorkflowDef};

/// Define the workflow name as a compile-time constant.
const PIPELINE: WorkflowDef = WorkflowDef::new("pipeline");

/// A pipeline with a slow processing step. When `stop()` is called during
/// execution, the engine waits for the in-progress step to finish (up to
/// the shutdown timeout) rather than killing it immediately.
async fn pipeline(ctx: Context) -> Result<(), EngineError> {
    ctx.set_status("fetching");
    let data: String = ctx
        .step("fetch:v1")
        .run(async || {
            println!("  [step] fetching data");
            Ok("42 records".to_string())
        })
        .await?;

    ctx.set_status("processing");
    let result: String = ctx
        .step("process:v1")
        .run(async move || {
            println!("  [step] processing {data}...");
            // Simulate a step that takes a moment to complete.
            tokio::time::sleep(Duration::from_millis(500)).await;
            println!("  [step] processing complete");
            Ok(format!("processed {data}"))
        })
        .await?;

    ctx.set_status(format!("done: {result}"));
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("memable=info")
        .init();

    // Build an engine with a 3-second shutdown timeout.
    // The timeout can also be set via MEMABLE_SHUTDOWN_TIMEOUT_SECS.
    let mut engine = Engine::builder()
        .in_memory()
        .shutdown_timeout(Duration::from_secs(3))
        .build();

    // Register using the WorkflowDef constant.
    engine.register(&PIPELINE, pipeline);
    engine.start().await?;

    // Invoke two workflows. They'll start executing immediately.
    // invoke() now takes &WorkflowDef instead of a name string.
    println!("=== Invoking workflows ===");
    let inv1 = engine.invoke(&PIPELINE).await?;
    let id1 = inv1.instance_id().to_string();
    let inv2 = engine.invoke(&PIPELINE).await?;
    let id2 = inv2.instance_id().to_string();

    // Give them a moment to start, then initiate graceful shutdown.
    // stop() waits up to the timeout for running workflows to finish.
    tokio::time::sleep(Duration::from_millis(50)).await;
    println!("\n=== Stopping engine (3s timeout) ===");
    engine.stop().await;

    // After stop, check metadata — workflows that completed within the
    // timeout have Completed status; any that were aborted would have
    // Running status (recoverable on next start).
    // get_metadata takes a name string, so use DEF.name().
    println!("\n=== Final state ===");
    for (label, id) in [("workflow 1", &id1), ("workflow 2", &id2)] {
        if let Some(meta) = engine.get_metadata(&PIPELINE, id)? {
            println!("  {label}: {}", meta.status());
            assert!(matches!(meta.status(), MetadataStatus::Completed(_)));
        }
    }

    // After stop, new invocations are rejected.
    let result = engine.invoke(&PIPELINE).await;
    assert!(matches!(result, Err(EngineError::NotStarted)));
    println!("  invoke after stop: correctly rejected");

    println!("\n=== Shutdown complete ===");
    Ok(())
}
