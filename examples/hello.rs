use memable::{Context, Engine, EngineError};

async fn greet(ctx: Context) -> Result<(), EngineError> {
    let name: String = ctx
        .step("fetch-name:v1")
        .run(async || Ok("world".to_string()))
        .await?;

    let message: String = ctx
        .step("format-greeting:v1")
        .run(async || Ok(format!("Hello, {name}!")))
        .await?;

    println!("{message}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::builder().in_memory().build();
    engine.register("greet", greet);
    engine.start().await?;

    engine.invoke("greet").await?;

    engine.wait_all().await;
    Ok(())
}
