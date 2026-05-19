use memable::{Context, Engine, EngineError, WorkflowDef};

/// Define a workflow by name using a compile-time constant.
/// The engine uses this to register, invoke, and look up workflow instances.
const GREET: WorkflowDef<(), String> = WorkflowDef::new("greet");

async fn greet(ctx: Context) -> Result<String, EngineError> {
    let name: String = ctx
        .step("fetch-name:v1")
        .run(async || Ok("world".to_string()))
        .await?;

    let message: String = ctx
        .step("format-greeting:v1")
        .run(async move || Ok(format!("Hello, {name}!")))
        .await?;

    Ok(message)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::builder().in_memory().build();

    // Register the workflow using its WorkflowDef constant.
    engine.register(&GREET, greet);
    engine.start().await?;

    // Invoke the workflow and wait for it to complete.
    // wait() returns a WaitResult — only the Completed variant has output().
    let completed = engine.invoke(&GREET).await?.wait().await.unwrap_completed();
    println!("Result: {greeting}", greeting = completed.output()?);

    engine.wait_all().await;
    Ok(())
}
