use std::future::Future;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::WorkflowFn;
use crate::context::{Context, StepData, serialize_step};
use crate::error::EngineError;

/// Marker for workflows that take no input.
pub struct NoInput;
/// Marker for workflows that take typed input.
pub struct WithInput;

/// Bridges a typed workflow function to the internal type-erased workflow closure.
///
/// Two blanket implementations cover both function signatures:
/// - `Fn(Context) -> Future<Output = Result<O, EngineError>>` (no input)
/// - `Fn(Context, I) -> Future<Output = Result<O, EngineError>>` (with input)
///
/// The `Marker` type parameter resolves impl overlap and is inferred
/// automatically — you never need to specify it.
///
/// You do not implement this trait manually — register any matching async
/// function or closure via [`Engine::register`](super::Engine::register).
pub trait IntoWorkflow<I, O, Marker>: Send + Sync + 'static {
    /// Converts the typed workflow function into a type-erased workflow closure.
    fn into_workflow_fn(self) -> WorkflowFn;
}

fn write_output<O: Serialize>(
    db: &redb::Database,
    workflow_name: &str,
    instance_id: &str,
    output: O,
) -> Result<(), EngineError> {
    let data = StepData::Completed {
        result: output,
        status: None,
    };
    let bytes = serialize_step(&data, "_output")?;
    let composite_key = format!("{workflow_name}/{instance_id}/_output");
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(crate::context::STEPS)?;
        table.insert(composite_key.as_str(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

/// No-input variant: `Fn(Context) -> Future<Output = Result<O, EngineError>>`
impl<O, F, Fut> IntoWorkflow<(), O, NoInput> for F
where
    F: Fn(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O, EngineError>> + Send + 'static,
    O: Serialize + DeserializeOwned + Send + 'static,
{
    fn into_workflow_fn(self) -> WorkflowFn {
        let f = Arc::new(self);
        Arc::new(move |ctx: Context| {
            let f = Arc::clone(&f);
            let db = Arc::clone(ctx.db());
            let wf_name = ctx.workflow_name().to_string();
            let inst_id = ctx.instance_id().to_string();
            let fut = f(ctx);
            Box::pin(async move {
                let output = fut.await?;
                write_output(&db, &wf_name, &inst_id, output)?;
                Ok(())
            })
        })
    }
}

/// With-input variant: `Fn(Context, I) -> Future<Output = Result<O, EngineError>>`
impl<I, O, F, Fut> IntoWorkflow<I, O, WithInput> for F
where
    F: Fn(Context, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O, EngineError>> + Send + 'static,
    I: DeserializeOwned + Send + 'static,
    O: Serialize + DeserializeOwned + Send + 'static,
{
    fn into_workflow_fn(self) -> WorkflowFn {
        let f = Arc::new(self);
        Arc::new(move |ctx: Context| {
            let f = Arc::clone(&f);
            let db = Arc::clone(ctx.db());
            let wf_name = ctx.workflow_name().to_string();
            let inst_id = ctx.instance_id().to_string();
            let input_result = ctx.input::<I>();
            Box::pin(async move {
                let input = input_result?.ok_or_else(|| EngineError::InputMissing {
                    workflow: wf_name.clone(),
                })?;
                let output = f(ctx, input).await?;
                write_output(&db, &wf_name, &inst_id, output)?;
                Ok(())
            })
        })
    }
}
