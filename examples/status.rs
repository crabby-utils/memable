//! Live status reporting from a running workflow.
//!
//! Demonstrates how an external observer can watch a workflow's progress
//! in real time using `Invocation::into_parts()` and the status channel.

use memable::{Context, Engine, EngineError, WorkflowDef};

/// Define the workflow name as a compile-time constant.
const DATA_PIPELINE: WorkflowDef = WorkflowDef::new("data-pipeline");

/// A multi-step "data pipeline" that reports progress via `ctx.set_status()`.
async fn data_pipeline(ctx: Context) -> Result<(), EngineError> {
    // Report status before each step so observers see what's happening.
    ctx.set_status("fetching records");
    let count: u32 = ctx
        .step("fetch-records:v1")
        .run(async || {
            // Simulate a slow fetch.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            Ok(42)
        })
        .await?;

    ctx.set_status(format!("validating {count} records"));
    let valid: u32 = ctx
        .step("validate:v1")
        .run(async || {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            Ok(38)
        })
        .await?;

    ctx.set_status(format!("transforming {valid} records"));
    let _transformed: Vec<String> = ctx
        .step("transform:v1")
        .run(async || {
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            Ok(vec!["row-a".into(), "row-b".into()])
        })
        .await?;

    ctx.set_status("writing to destination");
    let _rows_written: u32 = ctx
        .step("write:v1")
        .run(async || {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            Ok(38)
        })
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::builder().in_memory().build();

    // Register using the WorkflowDef constant.
    engine.register(&DATA_PIPELINE, data_pipeline);
    engine.start().await?;

    // Split the invocation into an ID and a status receiver so we can
    // monitor progress from a separate task.
    // invoke() now takes &WorkflowDef instead of a name string.
    let (instance_id, mut status_rx) = engine.invoke(&DATA_PIPELINE).await?.into_parts();

    // Spawn a monitoring task that prints every status transition.
    // Use message() to get the raw text passed to set_status(), rather
    // than the Display impl which prefixes it with the state name.
    let monitor = tokio::spawn(async move {
        while status_rx.changed().await.is_ok() {
            let state = status_rx.borrow_and_update().clone();
            match state.message() {
                Some(msg) => println!("[{instance_id}] {msg}"),
                None => println!("[{instance_id}] {state}"),
            }
            if state.is_terminal() {
                break;
            }
        }
    });

    // Wait for the monitor to finish (which happens when the workflow
    // reaches a terminal state).
    monitor.await?;

    engine.stop().await;
    Ok(())
}
