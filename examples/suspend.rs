//! Suspend and resume a workflow with external signals.
//!
//! Demonstrates how a workflow can pause at a suspend point, drop its
//! in-memory state, and later resume when an external signal delivers
//! a payload. Memoised steps before the suspend point are not re-executed.

use memable::{Context, Engine, EngineError, MetadataStatus, SuspendPoint, WorkflowState};

/// Typed suspend point — encodes both the key and payload type at compile time.
/// Both the workflow and the signal call reference this same const, ensuring
/// they can never disagree on the key string or payload type.
const APPROVAL: SuspendPoint<bool> = SuspendPoint::new("approval:v1");

/// A workflow that fetches data, suspends for approval, then processes
/// the approved data.
async fn approval_workflow(ctx: Context) -> Result<(), EngineError> {
    // Step 1: fetch some data. This result is memoised — it won't
    // re-execute on resume after the signal arrives.
    let record_count: u32 = ctx
        .step("fetch-data:v1")
        .run(async || {
            println!("  [step] fetching data from source");
            Ok(142)
        })
        .await?;

    // Suspend the workflow. The closure returns and the workflow task
    // drops — nothing is held in memory. The engine records the
    // suspension in redb.
    //
    // The APPROVAL const carries the payload type (bool). Both suspend
    // and signal reference it, so key typos and type mismatches are
    // caught at compile time.
    let approved: bool = ctx
        .suspend(&APPROVAL)
        .status("Waiting for manager approval")
        .await?;

    if !approved {
        println!("  [workflow] approval denied — aborting");
        return Ok(());
    }

    // Step 2: only runs after the signal delivers `true`.
    let _processed: u32 = ctx
        .step("process-data:v1")
        .run(async move || {
            println!("  [step] processing {record_count} approved records");
            Ok(record_count)
        })
        .await?;

    println!("  [workflow] done — processed {record_count} records");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::builder().in_memory().build();
    engine.register("approval", approval_workflow);
    engine.start().await?;

    // Invoke the workflow. It will run until it hits the suspend point.
    println!("=== Invoking workflow ===");
    let inv = engine.invoke("approval").await?;
    let instance_id = inv.instance_id().to_string();
    let state = inv.wait().await;
    println!("State: {state}");

    // The metadata table shows the workflow is suspended.
    let meta = engine
        .get_metadata("approval", &instance_id)?
        .expect("instance exists");
    println!("Metadata: {}", meta.status());
    assert!(matches!(
        meta.status(),
        MetadataStatus::Suspended { status, .. } if status == "Waiting for manager approval"
    ));
    println!();

    // In a real application, this signal would come from an HTTP handler,
    // a CLI command, a webhook, etc. The engine writes the payload to redb
    // and re-runs the workflow from the top. Memoised steps return cached
    // results, and the suspend step resolves with the signal payload.
    println!("=== Sending approval signal ===");
    let state = engine
        .signal("approval", &instance_id, &APPROVAL, true)
        .await?
        .wait()
        .await;
    println!("State: {state}");
    assert_eq!(state, WorkflowState::Completed(None));

    // After the signal, metadata reflects completion.
    let meta = engine
        .get_metadata("approval", &instance_id)?
        .expect("instance exists");
    println!("Metadata: {}", meta.status());

    engine.stop().await;
    Ok(())
}
