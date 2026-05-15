use std::sync::atomic::{AtomicU32, Ordering};

use memable::{Context, Engine, EngineError, MetadataStatus, StepError};

static STEP_EXECUTIONS: AtomicU32 = AtomicU32::new(0);
static FORMAT_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

async fn greeting_workflow(ctx: Context) -> Result<(), EngineError> {
    let name: String = ctx
        .step("fetch-name:v1")
        .run(async || {
            STEP_EXECUTIONS.fetch_add(1, Ordering::Relaxed);
            println!("  [step] fetching name");
            Ok("Rust".to_string())
        })
        .await?;

    let greeting: String = ctx
        .step("format-greeting:v1")
        .run(async || {
            STEP_EXECUTIONS.fetch_add(1, Ordering::Relaxed);
            if FORMAT_ATTEMPTS.fetch_add(1, Ordering::Relaxed) == 0 {
                println!("  [step] formatting greeting — transient failure!");
                return Err(StepError::retryable("network timeout"));
            }
            println!("  [step] formatting greeting");
            Ok(format!("Hello, {name}!"))
        })
        .await?;

    println!("Result: {greeting}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("memable=info")
        .init();

    let mut engine = Engine::builder().in_memory().build();
    engine.register("greeting", greeting_workflow);
    engine.start().await?;

    // First invocation — step 1 succeeds, step 2 hits a transient failure.
    println!("=== First invocation ===");

    let inv = engine.invoke("greeting").await?;
    let instance_id = inv.instance_id().to_string();
    let state = inv.wait().await;
    println!("Status: {state}");
    println!(
        "Steps executed: {}",
        STEP_EXECUTIONS.load(Ordering::Relaxed)
    );

    // Query metadata — the engine recorded the failure.
    let meta = engine
        .get_metadata("greeting", &instance_id)?
        .expect("instance exists");
    println!("Metadata: {} at {:?}", meta.status(), meta.completed_at());
    println!();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Resume the same instance — step 1 is memoised, step 2 retries.
    println!("=== Resume (step 1 memoised, step 2 retries) ===");
    STEP_EXECUTIONS.store(0, Ordering::Relaxed);

    let state = engine.resume("greeting", &instance_id).await?.wait().await;
    println!("Status: {state}");
    println!(
        "Steps executed: {} (step 1 was memoised!)",
        STEP_EXECUTIONS.load(Ordering::Relaxed)
    );

    // After resume, metadata reflects success.
    let meta = engine
        .get_metadata("greeting", &instance_id)?
        .expect("instance exists");
    println!("Metadata: {} at {:?}", meta.status(), meta.completed_at());
    assert!(matches!(meta.status(), MetadataStatus::Completed));

    // list_instances shows all instances of a workflow definition.
    let instances = engine.list_instances("greeting")?;
    println!("\nAll 'greeting' instances: {}", instances.len());
    for (id, meta) in &instances {
        println!("  {id}: {}", meta.status());
    }

    engine.stop().await;
    Ok(())
}
