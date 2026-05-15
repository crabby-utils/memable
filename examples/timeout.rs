//! Step timeouts — cancel slow steps without requiring `'static` closures.
//!
//! Demonstrates `ctx.step("key").timeout(Duration).run(closure)`, which
//! wraps the step execution in `tokio::time::timeout`. If the closure
//! doesn't complete in time, the step fails with `EngineError::StepTimeout`
//! and no result is persisted — a resume will retry the step.
//!
//! Closures can still borrow from the workflow scope because the timeout
//! runs inline on the same task (no spawning, no `'static` bound).

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use memable::{Context, Engine, EngineError, MetadataStatus, StepError, WorkflowState};

/// Tracks how many times the slow step has been attempted.
static FETCH_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

/// Simulates a flaky upstream: slow on the first call, fast on retries.
async fn fetch_from_upstream() -> Result<String, StepError> {
    let attempt = FETCH_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
    if attempt == 0 {
        println!("  [upstream] first attempt — hanging...");
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
    println!("  [upstream] responded quickly (attempt {attempt})");
    Ok("payload-from-upstream".to_string())
}

/// A workflow that fetches data with a timeout, then processes it.
/// Uses `pipeline_name` across steps — no `'static` required even
/// with a timeout set.
async fn resilient_pipeline(ctx: Context) -> Result<(), EngineError> {
    let pipeline_name = String::from("customer-sync");

    // This step has a 500ms timeout. On the first invocation the
    // simulated upstream hangs, causing a StepTimeout failure. On
    // resume it re-executes (nothing was persisted) and succeeds.
    let pn = pipeline_name.clone();
    let data: String = ctx
        .step("fetch:v1")
        .timeout(Duration::from_millis(500))
        .run(async move || {
            println!("  [step] fetching data for '{pn}'");
            fetch_from_upstream().await
        })
        .await?;

    let result: String = ctx
        .step("process:v1")
        .run(async move || {
            println!("  [step] processing: {data}");
            Ok(format!("{pipeline_name}: done"))
        })
        .await?;

    println!("  [workflow] {result}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::builder().in_memory().build();
    engine.register("pipeline", resilient_pipeline);
    engine.start().await?;

    // First invocation — the fetch step times out after 500ms.
    println!("=== First invocation (fetch will time out) ===");
    let inv = engine.invoke("pipeline").await?;
    let id = inv.instance_id().to_string();
    let state = inv.wait().await;
    println!("State: {state}");

    let meta = engine
        .get_metadata("pipeline", &id)?
        .expect("instance exists");
    println!("Metadata: {}", meta.status());
    assert!(matches!(meta.status(), MetadataStatus::Failed(msg) if msg.contains("timed out")));
    println!();

    // Resume — the timed-out step was NOT cached, so it re-executes.
    // This time the simulated upstream responds quickly.
    println!("=== Resume (fetch re-executes, succeeds) ===");
    let state = engine.resume("pipeline", &id).await?.wait().await;
    println!("State: {state}");
    assert_eq!(state, WorkflowState::Completed);

    // The fetch step ran twice total (timed out + retry).
    println!("Fetch attempts: {}", FETCH_ATTEMPTS.load(Ordering::Relaxed));

    engine.stop().await;
    println!();
    println!("=== Done ===");
    Ok(())
}
