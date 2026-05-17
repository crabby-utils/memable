//! Retention — automatic cleanup of completed workflow data.
//!
//! By default, completed and failed workflow instances live in the database
//! forever. Enable retention to automatically delete old instances and their
//! associated step data after a configurable time-to-live.
//!
//! Retention is opt-in: configure it on the `EngineBuilder` with
//! `.retention(duration)`. Per-workflow overrides are set via the
//! `Registration` handle returned by `engine.register()`.

use std::time::Duration;

use memable::{Context, Engine, EngineError, WorkflowState};

/// A workflow that processes data in multiple steps.
async fn data_pipeline(ctx: Context) -> Result<(), EngineError> {
    let raw: String = ctx
        .step("fetch:v1")
        .run(async || {
            println!("  [pipeline] fetching raw data");
            Ok("sensor-readings-2024".to_string())
        })
        .await?;

    let _processed: String = ctx
        .step("transform:v1")
        .run(async move || {
            println!("  [pipeline] transforming {raw}");
            Ok(format!("{raw} (cleaned)"))
        })
        .await?;

    println!("  [pipeline] done");
    Ok(())
}

/// A short-lived workflow whose results are only needed briefly.
async fn health_check(ctx: Context) -> Result<(), EngineError> {
    let _status: String = ctx
        .step("ping:v1")
        .run(async || {
            println!("  [health] pinging service");
            Ok("healthy".to_string())
        })
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("memable=info")
        .init();

    // Configure engine-wide retention of 30 days.
    // All completed/failed workflows will be cleaned up after this period.
    let mut engine = Engine::builder()
        .retention(Duration::from_secs(30 * 24 * 60 * 60)) // 30 days
        .in_memory()
        .build();

    // Register the data pipeline with the default engine retention (30 days).
    engine.register("data-pipeline", data_pipeline);

    // Register health checks with a shorter retention — results are
    // ephemeral and only needed for a few hours.
    engine
        .register("health-check", health_check)
        .retention(Duration::from_secs(4 * 60 * 60)); // 4 hours

    engine.start().await?;

    // Run some workflows.
    println!("=== Running workflows ===\n");

    let pipeline_inv = engine.invoke("data-pipeline").await?;
    let pipeline_id = pipeline_inv.instance_id().to_string();
    pipeline_inv.wait().await;

    let health_inv = engine.invoke("health-check").await?;
    let health_id = health_inv.instance_id().to_string();
    health_inv.wait().await;

    // Both workflows are completed and visible in metadata.
    println!("\n=== After completion ===");
    let pipeline_meta = engine.get_metadata("data-pipeline", &pipeline_id)?;
    println!(
        "  data-pipeline: {}",
        pipeline_meta
            .as_ref()
            .map_or("gone".into(), |m| m.status().to_string())
    );

    let health_meta = engine.get_metadata("health-check", &health_id)?;
    println!(
        "  health-check:  {}",
        health_meta
            .as_ref()
            .map_or("gone".into(), |m| m.status().to_string())
    );

    // In a real application, the background retention task runs every 60
    // seconds and removes instances where:
    //
    //   now - completed_at > retention_ttl
    //
    // For data-pipeline instances: cleaned after 30 days.
    // For health-check instances: cleaned after 4 hours.
    //
    // All associated step entries and timer entries are deleted in a single
    // transaction per instance. Running and suspended workflows are never
    // touched — only terminal (completed/failed) instances are eligible.

    println!("\n=== Retention behaviour ===");
    println!("  data-pipeline retention: 30 days (engine default)");
    println!("  health-check retention:  4 hours (per-workflow override)");
    println!("  cleanup interval:        every 60 seconds");
    println!("  cleanup scope:           metadata + steps + timers");

    // Verify workflows are still accessible (they just completed, not yet expired).
    assert_eq!(
        engine.state("data-pipeline", &pipeline_id)?,
        WorkflowState::Completed(None)
    );
    assert_eq!(
        engine.state("health-check", &health_id)?,
        WorkflowState::Completed(None)
    );

    engine.stop().await;
    println!("\n=== Done ===");
    Ok(())
}
