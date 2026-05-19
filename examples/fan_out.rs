use std::sync::atomic::{AtomicU32, Ordering};

use memable::{ChildError, Context, Engine, EngineError, StepError, WorkflowDef};

// ---------------------------------------------------------------------------
// Workflow definitions — typed constants catch name/type mismatches at compile
// time across all register/invoke/resume call sites.
// ---------------------------------------------------------------------------

/// Collect-all mode: each child either succeeds or fails independently.
const COLLECT_ALL: WorkflowDef = WorkflowDef::new("pipeline-collect-all");

/// Fail-fast mode: the first child failure aborts remaining children.
const FAIL_FAST: WorkflowDef = WorkflowDef::new("pipeline-fail-fast");

/// Memoisation demo: resume after failure, completed children are not re-run.
const MEMOISED: WorkflowDef = WorkflowDef::new("pipeline-memoised");

/// Tracks how many child closures actually execute (not replayed from cache).
static CHILD_EXECUTIONS: AtomicU32 = AtomicU32::new(0);

// ---------------------------------------------------------------------------
// Simulated data pipeline: "fetch" a URL, then "transform" the response.
// One URL is deliberately bad to demonstrate error handling.
// ---------------------------------------------------------------------------

fn urls() -> Vec<String> {
    vec![
        "https://example.com/data/users".into(),
        "https://example.com/data/orders".into(),
        "https://bad.invalid/timeout".into(), // this one will fail
        "https://example.com/data/products".into(),
    ]
}

/// Returns true for URLs that should "fail" during the fetch step.
fn is_bad_url(url: &str) -> bool {
    url.contains("bad.invalid")
}

// ---------------------------------------------------------------------------
// 1. Collect-all mode
// ---------------------------------------------------------------------------

async fn collect_all_workflow(ctx: Context) -> Result<(), EngineError> {
    let items = urls();
    println!(
        "  Fan-out {} items (collect-all, concurrency=2)",
        items.len()
    );

    // fan_out returns Vec<Result<T, ChildError>> — every child runs to
    // completion regardless of individual failures.
    let results: Vec<Result<String, ChildError>> = ctx
        .fan_out("fetch-transform:v1", items)
        .concurrency(2)
        .run(move |child_ctx, url| async move {
            CHILD_EXECUTIONS.fetch_add(1, Ordering::Relaxed);

            // Step 1: fetch
            let body: String = child_ctx
                .step("fetch:v1")
                .run(async move || {
                    if is_bad_url(&url) {
                        return Err(StepError::permanent(format!(
                            "DNS resolution failed: {url}"
                        )));
                    }
                    Ok(format!("{{data from {url}}}"))
                })
                .await?;

            // Step 2: transform
            let transformed: String = child_ctx
                .step("transform:v1")
                .run(async move || Ok(body.to_uppercase()))
                .await?;

            Ok(transformed)
        })
        .await?;

    // Inspect results — some Ok, some Err.
    let (ok, err): (Vec<_>, Vec<_>) = results.iter().partition(|r| r.is_ok());
    println!("  Successes: {}, Failures: {}", ok.len(), err.len());

    for result in &results {
        match result {
            Ok(data) => println!("    OK: {data}"),
            Err(e) => println!("    FAIL [{}]: {}", e.instance_id(), e.message()),
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// 2. Fail-fast mode
// ---------------------------------------------------------------------------

async fn fail_fast_workflow(ctx: Context) -> Result<(), EngineError> {
    let items = urls();
    println!(
        "  Fan-out {} items (fail-fast, concurrency=1 to show ordering)",
        items.len()
    );

    // With fail_fast(), the return type is Vec<T> (not Vec<Result>).
    // concurrency(1) ensures children run sequentially so the failure
    // point is deterministic for this demo.
    let result: Result<Vec<String>, EngineError> = ctx
        .fan_out("fetch-transform:v1", items)
        .concurrency(1)
        .fail_fast()
        .run(move |child_ctx, url| async move {
            CHILD_EXECUTIONS.fetch_add(1, Ordering::Relaxed);

            let body: String = child_ctx
                .step("fetch:v1")
                .run(async move || {
                    if is_bad_url(&url) {
                        return Err(StepError::permanent(format!(
                            "DNS resolution failed: {url}"
                        )));
                    }
                    Ok(format!("{{data from {url}}}"))
                })
                .await?;

            let transformed: String = child_ctx
                .step("transform:v1")
                .run(async move || Ok(body.to_uppercase()))
                .await?;

            Ok(transformed)
        })
        .await;

    match &result {
        Ok(data) => println!("  All succeeded: {data:?}"),
        Err(e) => println!("  Aborted early: {e}"),
    }

    let executed = CHILD_EXECUTIONS.load(Ordering::Relaxed);
    println!("  Children executed: {executed} (item 4 was never started)");

    Ok(())
}

// ---------------------------------------------------------------------------
// 3. Memoisation — resume after failure, completed children skip re-execution
// ---------------------------------------------------------------------------

async fn memoised_workflow(ctx: Context) -> Result<(), EngineError> {
    let items = urls();

    let results: Vec<Result<String, ChildError>> = ctx
        .fan_out("fetch-transform:v1", items)
        .concurrency(2)
        .run(move |child_ctx, url| async move {
            CHILD_EXECUTIONS.fetch_add(1, Ordering::Relaxed);

            let body: String = child_ctx
                .step("fetch:v1")
                .run(async move || {
                    if is_bad_url(&url) {
                        return Err(StepError::permanent(format!(
                            "DNS resolution failed: {url}"
                        )));
                    }
                    Ok(format!("{{data from {url}}}"))
                })
                .await?;

            let transformed: String = child_ctx
                .step("transform:v1")
                .run(async move || Ok(body.to_uppercase()))
                .await?;

            Ok(transformed)
        })
        .await?;

    let ok_count = results.iter().filter(|r| r.is_ok()).count();
    let err_count = results.iter().filter(|r| r.is_err()).count();
    println!("  Results: {ok_count} ok, {err_count} failed");
    println!(
        "  Child closures executed: {}",
        CHILD_EXECUTIONS.load(Ordering::Relaxed)
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("memable=info")
        .init();

    // ── 1. Collect-all ──────────────────────────────────────────────
    println!("=== Collect-all mode ===");
    println!("All children run to completion. Failures appear as ChildError in the result vec.\n");

    let mut engine = Engine::builder().in_memory().build();
    engine.register(&COLLECT_ALL, collect_all_workflow);
    engine.start().await?;

    CHILD_EXECUTIONS.store(0, Ordering::Relaxed);
    let _ = engine.invoke(&COLLECT_ALL).await?.wait().await;

    engine.stop().await;
    println!();

    // ── 2. Fail-fast ────────────────────────────────────────────────
    println!("=== Fail-fast mode ===");
    println!("First failure aborts remaining children.\n");

    let mut engine = Engine::builder().in_memory().build();
    engine.register(&FAIL_FAST, fail_fast_workflow);
    engine.start().await?;

    CHILD_EXECUTIONS.store(0, Ordering::Relaxed);
    let _ = engine.invoke(&FAIL_FAST).await?.wait().await;

    engine.stop().await;
    println!();

    // ── 3. Memoisation ──────────────────────────────────────────────
    println!("=== Memoisation ===");
    println!("Resume after failure — completed children are not re-executed.\n");

    let mut engine = Engine::builder().in_memory().build();
    engine.register(&MEMOISED, memoised_workflow);
    engine.start().await?;

    // First run: all 4 children execute.
    CHILD_EXECUTIONS.store(0, Ordering::Relaxed);
    println!("  --- First run ---");
    let inv = engine.invoke(&MEMOISED).await?;
    let instance_id = inv.instance_id().to_string();
    let _ = inv.wait().await;

    // Resume: fan-out result is cached, zero children re-execute.
    CHILD_EXECUTIONS.store(0, Ordering::Relaxed);
    println!("\n  --- Resume (same instance) ---");
    let _ = engine.resume(&MEMOISED, &instance_id).await?.wait().await;

    engine.stop().await;

    Ok(())
}
