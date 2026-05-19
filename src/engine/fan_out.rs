use std::future::Future;
use std::sync::Arc;

use redb::ReadableDatabase as _;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task::JoinSet;
use tracing::{Instrument as _, error, info, info_span};

use super::workflow::{read_output, write_output};
use super::{EngineShared, WorkflowState};
use crate::context::{Context, StepData, deserialize_step};
use crate::error::EngineError;
use crate::metadata::{self, MetadataStatus, WorkflowMetadata};

use super::execution::handle_workflow_result;

/// Persisted manifest for crash recovery.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub(crate) struct FanOutManifest {
    pub child_instance_ids: Vec<String>,
    pub count: usize,
}

/// Run an inline-closure fan-out.
///
/// The parent task stays alive while children execute in a local
/// `JoinSet`. A semaphore enforces the concurrency limit. Results
/// are collected in item order.
pub(crate) async fn run_inline<I, T, F, Fut>(
    shared: &Arc<EngineShared>,
    parent_wf_name: &str,
    child_ids: &[String],
    items: Vec<I>,
    closure: Arc<F>,
    concurrency: usize,
) -> Result<Vec<T>, EngineError>
where
    F: Fn(Context, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, EngineError>> + Send + 'static,
    I: Send + 'static,
    T: Serialize + DeserializeOwned + Send + 'static,
{
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut child_tasks: JoinSet<Result<(usize, T), EngineError>> = JoinSet::new();

    for (idx, (child_id, item)) in child_ids.iter().zip(items).enumerate() {
        let shared_c = Arc::clone(shared);
        let wf_name = parent_wf_name.to_string();
        let cid = child_id.clone();
        let f = Arc::clone(&closure);
        let sem = Arc::clone(&semaphore);
        let span = info_span!("child_workflow", name = %wf_name, instance = %cid);

        child_tasks.spawn(
            async move {
                let _permit = sem.acquire().await.expect("semaphore closed");

                let tx = super::Engine::get_or_create_sender(&shared_c.senders, &cid);
                let ctx = Context::new(
                    wf_name.clone(),
                    cid.clone(),
                    Arc::clone(&shared_c),
                    tx.clone(),
                );

                if let Err(e) = metadata::write_metadata(
                    &shared_c.db,
                    &wf_name,
                    &cid,
                    &WorkflowMetadata::new(MetadataStatus::Running),
                ) {
                    error!(error = %e, "failed to write child Running metadata");
                }

                info!("executing");
                let result = f(ctx, item).await;

                if let Ok(ref value) = result {
                    if let Err(e) = write_output(&shared_c.db, &wf_name, &cid, value) {
                        error!(error = %e, "failed to write child _output");
                        handle_workflow_result(Err(e), &shared_c.db, &wf_name, &cid, &tx);
                        shared_c
                            .senders
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&cid);
                        return Err(EngineError::Storage("child output write failed".into()));
                    }
                }

                handle_workflow_result(
                    result
                        .as_ref()
                        .map(|_| ())
                        .map_err(|e| EngineError::StepFailed {
                            key: cid.clone(),
                            source: e.to_string().into(),
                            retryable: false,
                        }),
                    &shared_c.db,
                    &wf_name,
                    &cid,
                    &tx,
                );

                shared_c
                    .senders
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&cid);

                result.map(|val| (idx, val))
            }
            .instrument(span),
        );
    }

    collect_results(child_tasks, child_ids.len()).await
}

/// Run a registered-workflow fan-out.
pub(crate) async fn run_workflow<T>(
    shared: &Arc<EngineShared>,
    child_workflow_name: &str,
    child_ids: &[String],
    items_bytes: Vec<Vec<u8>>,
    concurrency: usize,
) -> Result<Vec<T>, EngineError>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut child_tasks: JoinSet<Result<(usize, T), EngineError>> = JoinSet::new();

    for (idx, (child_id, input_bytes)) in child_ids.iter().zip(items_bytes).enumerate() {
        let shared_c = Arc::clone(shared);
        let wf_name = child_workflow_name.to_string();
        let cid = child_id.clone();
        let sem = Arc::clone(&semaphore);

        child_tasks.spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");

            let invocation = super::Engine::spawn_workflow::<()>(
                &shared_c,
                &wf_name,
                cid.clone(),
                Some(input_bytes),
            )
            .await?;

            let (_id, mut rx) = invocation.into_parts();

            loop {
                {
                    let state = rx.borrow().clone();
                    if state.is_terminal() {
                        return match state {
                            WorkflowState::Completed(_) => {
                                let value: T = read_output(&shared_c.db, &wf_name, &cid)?;
                                Ok((idx, value))
                            }
                            WorkflowState::Failed(msg) => Err(EngineError::StepFailed {
                                key: cid,
                                source: msg.into(),
                                retryable: false,
                            }),
                            _ => unreachable!(),
                        };
                    }
                }
                if rx.changed().await.is_err() {
                    let state = rx.borrow().clone();
                    return match state {
                        WorkflowState::Completed(_) => {
                            let value: T = read_output(&shared_c.db, &wf_name, &cid)?;
                            Ok((idx, value))
                        }
                        other => Err(EngineError::StepFailed {
                            key: cid,
                            source: format!("child ended in state: {other}").into(),
                            retryable: false,
                        }),
                    };
                }
            }
        });
    }

    collect_results(child_tasks, child_ids.len()).await
}

/// Collect results from a `JoinSet`, failing immediately on first error.
async fn collect_results<T: Send + 'static>(
    mut child_tasks: JoinSet<Result<(usize, T), EngineError>>,
    expected: usize,
) -> Result<Vec<T>, EngineError> {
    let mut indexed: Vec<Option<T>> = (0..expected).map(|_| None).collect();
    let mut completed = 0;

    while let Some(join_result) = child_tasks.join_next().await {
        match join_result {
            Ok(Ok((idx, value))) => {
                indexed[idx] = Some(value);
                completed += 1;
            }
            Ok(Err(e)) => {
                info!(
                    error = %e,
                    completed,
                    total = expected,
                    "child failed — aborting remaining children"
                );
                child_tasks.abort_all();
                return Err(e);
            }
            Err(join_err) => {
                error!(error = %join_err, "child task panicked");
                child_tasks.abort_all();
                return Err(EngineError::Storage(
                    format!("child task panicked: {join_err}").into(),
                ));
            }
        }
    }

    Ok(indexed
        .into_iter()
        .map(|v| v.expect("all children completed"))
        .collect())
}

/// Check for a cached fan-out result.
pub(crate) fn check_cached<T: DeserializeOwned>(
    ctx: &Context,
    fan_out_step_key: &str,
) -> Result<Option<Vec<T>>, EngineError> {
    let composite_key = format!(
        "{}/{}/{}",
        ctx.workflow_name(),
        ctx.instance_id(),
        fan_out_step_key
    );
    let read_txn = ctx.db().begin_read()?;
    let table = match read_txn.open_table(crate::context::STEPS) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(EngineError::from(e)),
    };
    let Some(guard) = table.get(composite_key.as_str())? else {
        return Ok(None);
    };
    let data: StepData<Vec<T>> = deserialize_step(guard.value(), fan_out_step_key)?;
    match data {
        StepData::Completed { result, .. } => Ok(Some(result)),
        StepData::Suspended | StepData::Failed { .. } => Ok(None),
    }
}

/// Write the fan-out result to the step table.
///
/// Serializes `results` with the type tag for `Vec<T>` so that
/// [`check_cached`] can deserialize it as `StepData<Vec<T>>`.
pub(crate) fn write_result<T: Serialize>(
    ctx: &Context,
    fan_out_step_key: &str,
    results: &[T],
) -> Result<(), EngineError> {
    let composite_key = format!(
        "{}/{}/{}",
        ctx.workflow_name(),
        ctx.instance_id(),
        fan_out_step_key
    );
    let data = StepData::Completed {
        result: results,
        status: None,
    };
    let inner = postcard::to_allocvec(&data).map_err(|e| EngineError::Serialization {
        key: fan_out_step_key.to_string(),
        source: Box::new(e),
    })?;
    let envelope = crate::context::StepEnvelope {
        state: crate::context::StepState::Completed,
        type_tag: Some(std::any::type_name::<Vec<T>>().to_string()),
        data: inner,
    };
    let bytes = postcard::to_allocvec(&envelope).map_err(|e| EngineError::Serialization {
        key: fan_out_step_key.to_string(),
        source: Box::new(e),
    })?;
    let write_txn = ctx.db().begin_write()?;
    {
        let mut table = write_txn.open_table(crate::context::STEPS)?;
        table.insert(composite_key.as_str(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}
