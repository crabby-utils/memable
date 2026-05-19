// Invoke a workflow with typed input.
//
// Demonstrates passing data to a workflow at invocation time. The input
// type is encoded in the WorkflowDef, so the engine enforces type safety
// between the invoke call and the workflow function signature.

use memable::{Context, Engine, EngineError, WorkflowDef};

/// Define the workflow with a typed input parameter.
/// `WorkflowDef<String>` means the workflow expects a `String` input
/// and returns `()` (the output type defaults to `()`).
const GREET: WorkflowDef<String> = WorkflowDef::new("greet");

/// The workflow receives the input as a function parameter instead of
/// reading it from the context. The engine deserialises the invocation
/// payload and passes it directly to this function.
async fn greet(ctx: Context, name: String) -> Result<(), EngineError> {
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

    // Register with the typed WorkflowDef — the compiler checks that
    // `greet` accepts (Context, String), matching WorkflowDef<String>.
    engine.register(&GREET, greet);
    engine.start().await?;

    // Invoke with input — the engine serialises "Alice" and delivers it
    // to the workflow function as the second parameter.
    let _ = engine
        .invoke(&GREET)
        .input("Alice".to_string())
        .await?
        .wait()
        .await
        .unwrap_completed();

    engine.stop().await;
    Ok(())
}
