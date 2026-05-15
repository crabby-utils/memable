//! Durable timers — suspend a workflow until a deadline elapses.
//!
//! Demonstrates `ctx.timer()` which suspends the workflow and automatically
//! resumes it after the specified duration. The timer is durable — if the
//! process crashes, the background poller will fire the timer on restart.

use std::time::Duration;

use memable::{Context, Engine, EngineError, MetadataStatus, WorkflowState};

/// A workflow that performs work, waits for a cooldown period, then
/// continues with more work. The timer is automatically signalled by
/// the engine's background poller.
async fn pipeline_with_cooldown(ctx: Context) -> Result<(), EngineError> {
    // Step 1: do some initial work.
    let batch_size: u32 = ctx
        .step("fetch-batch:v1")
        .run(async || {
            println!("  [step] fetching batch from upstream API");
            Ok(50)
        })
        .await?;

    // Timer: wait 2 seconds before continuing. The workflow drops here —
    // no task is held in memory. The engine's background poller detects
    // the expired deadline and auto-signals the workflow to resume.
    println!("  [timer] waiting 2 seconds before processing...");
    ctx.timer("cooldown:v1", Duration::from_secs(2))?;

    // Step 2: this only runs after the timer fires. On resume, step 1
    // returns its memoised result without re-executing.
    let _processed: u32 = ctx
        .step("process-batch:v1")
        .run(async move || {
            println!("  [step] processing {batch_size} records");
            Ok(batch_size)
        })
        .await?;

    println!("  [workflow] done — processed {batch_size} records after cooldown");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::builder().in_memory().build();
    engine.register("pipeline", pipeline_with_cooldown);
    engine.start().await?;

    // Invoke the workflow. It runs step 1, then suspends at the timer.
    println!("=== Invoking workflow ===");
    let inv = engine.invoke("pipeline").await?;
    let instance_id = inv.instance_id().to_string();
    let state = inv.wait().await;
    println!("State: {state}");
    assert!(matches!(state, WorkflowState::Suspended(_)));

    // The metadata table shows the workflow is suspended with timer info.
    let meta = engine
        .get_metadata("pipeline", &instance_id)?
        .expect("instance exists");
    println!("Metadata: {}", meta.status());
    println!();

    // The background poller checks every second for expired timers.
    // After 2 seconds, it will auto-signal this workflow. We poll
    // metadata to observe the transition.
    println!("=== Waiting for timer to fire... ===");
    loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let meta = engine
            .get_metadata("pipeline", &instance_id)?
            .expect("instance exists");
        if meta.status().is_terminal() {
            println!("State: {}", meta.status());
            assert_eq!(*meta.status(), MetadataStatus::Completed);
            break;
        }
    }

    println!();
    println!("=== Timer fired and workflow completed ===");

    // Resuming now would hit all memoised steps (including the timer)
    // and complete instantly without re-executing anything.
    println!("=== Resuming (all steps memoised) ===");
    let state = engine.resume("pipeline", &instance_id).await?.wait().await;
    println!("State: {state}");
    assert_eq!(state, WorkflowState::Completed);

    engine.stop().await;
    Ok(())
}
