// Invoke a workflow with typed input.
//
// Demonstrates passing data to a workflow at invocation time, removing
// the need for an invoke-then-signal pattern for initial parameters.

use memable::{Context, Engine, EngineError, WorkflowState};

async fn greet(ctx: Context) -> Result<(), EngineError> {
    // Read the input provided at invocation. Returns None if no input was given.
    let name: String = ctx.input::<String>()?.unwrap_or("world".into());

    let message: String = ctx
        .step("format-greeting:v1")
        .run(async move || Ok(format!("Hello, {name}!")))
        .await?;

    println!("{message}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::builder().in_memory().build();
    engine.register("greet", greet);
    engine.start().await?;

    // Invoke with input — the workflow receives "Alice" via ctx.input().
    let state = engine
        .invoke("greet")
        .input("Alice".to_string())
        .await?
        .wait()
        .await;
    assert_eq!(state, WorkflowState::Completed);

    // Invoke without input — the workflow falls back to "world".
    let state = engine.invoke("greet").await?.wait().await;
    assert_eq!(state, WorkflowState::Completed);

    engine.stop().await;
    Ok(())
}
