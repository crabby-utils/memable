use std::collections::HashMap;
use std::hash::{BuildHasher as _, RandomState};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableDatabase as _, ReadableTable as _};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::Instrument as _;
use tracing::{error, info, info_span};

use super::{Senders, WorkflowFn, WorkflowState};
use crate::context::{
    Context, STEPS, StepData, StepEnvelope, StepState, TIMERS, TimerEntry, deserialize_envelope,
    serialize_step,
};
use crate::error::EngineError;
use crate::metadata::{self, MetadataStatus, WORKFLOW_META, WorkflowMetadata};
use crate::retry::RetryPolicy;

pub(super) fn validate_key_component(value: &str, label: &'static str) -> Result<(), EngineError> {
    if value.contains('/') {
        return Err(EngineError::InvalidKey {
            label,
            value: value.to_string(),
        });
    }
    Ok(())
}

pub(super) fn generate_instance_id() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis();
    let rand: u64 = RandomState::new().hash_one(ts);
    format!("{ts}-{rand:x}")
}

pub(super) fn now_unix_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_millis(),
    )
    .expect("system clock overflows u64 millis")
}

pub(super) fn handle_workflow_result(
    result: Result<(), EngineError>,
    db: &Database,
    workflow_name: &str,
    instance_id: &str,
    tx: &watch::Sender<WorkflowState>,
) {
    match result {
        Ok(()) => {
            info!("completed");
            let last_msg = tx.borrow().message().map(ToString::to_string);
            if let Err(e) = metadata::write_metadata(
                db,
                workflow_name,
                instance_id,
                &WorkflowMetadata::new(MetadataStatus::Completed(last_msg.clone())),
            ) {
                error!(error = %e, "failed to write completion metadata");
            }
            let _ = tx.send(WorkflowState::Completed(last_msg));
        }
        Err(EngineError::Suspended { ref key }) => {
            let status = tx.borrow().message().unwrap_or(key).to_string();
            let key = key.clone();
            info!(key, status, "suspended");
            if let Err(e) = metadata::write_metadata(
                db,
                workflow_name,
                instance_id,
                &WorkflowMetadata::new(MetadataStatus::Suspended {
                    key: key.clone(),
                    status: status.clone(),
                }),
            ) {
                error!(error = %e, "failed to write suspension metadata");
            }
            let _ = tx.send(WorkflowState::Suspended { key, status });
        }
        Err(e) => {
            info!(error = %e, "failed");
            let msg = e.to_string();
            if let Err(me) = metadata::write_metadata(
                db,
                workflow_name,
                instance_id,
                &WorkflowMetadata::new(MetadataStatus::Failed(msg.clone())),
            ) {
                error!(error = %me, "failed to write failure metadata");
            }
            let _ = tx.send(WorkflowState::Failed(msg));
        }
    }
}

#[expect(clippy::too_many_arguments)]
pub(super) fn spawn_workflow_task(
    tasks: &mut JoinSet<()>,
    workflow: &WorkflowFn,
    db: &Arc<Database>,
    workflow_name: &str,
    instance_id: &str,
    timer_serial: &Arc<AtomicU64>,
    default_retry: Option<RetryPolicy>,
    tx: &watch::Sender<WorkflowState>,
    senders: &Senders,
) {
    let workflow = Arc::clone(workflow);
    let db = Arc::clone(db);
    let ctx = Context::new(
        workflow_name.to_string(),
        instance_id.to_string(),
        Arc::clone(&db),
        tx.clone(),
        Arc::clone(timer_serial),
        default_retry,
    );

    let wf_name = workflow_name.to_string();
    let inst_id = instance_id.to_string();
    let tx = tx.clone();
    let senders = Arc::clone(senders);
    let span = info_span!("workflow", name = %wf_name, instance = %inst_id);

    tasks.spawn(
        async move {
            info!("executing");
            let result = workflow(ctx).await;
            let terminal = !matches!(&result, Err(EngineError::Suspended { .. }));
            handle_workflow_result(result, &db, &wf_name, &inst_id, &tx);
            if terminal {
                senders
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&inst_id);
            }
        }
        .instrument(span),
    );
}

pub(super) fn claim_suspended_step(
    db: &Database,
    workflow_name: &str,
    instance_id: &str,
    step_key: &str,
    step_bytes: &[u8],
) -> Result<(), EngineError> {
    let composite_key = format!("{workflow_name}/{instance_id}/{step_key}");
    let meta_key = format!("{workflow_name}/{instance_id}");

    let write_txn = db.begin_write()?;
    {
        let mut steps_table = write_txn.open_table(STEPS)?;

        match steps_table.get(composite_key.as_str())? {
            None => {
                return Err(EngineError::SignalRejected {
                    key: step_key.to_string(),
                    reason: "step does not exist".to_string(),
                });
            }
            Some(guard) => {
                let bytes: &[u8] = guard.value();
                let envelope: StepEnvelope = deserialize_envelope(bytes, step_key)?;
                if envelope.state != StepState::Suspended {
                    return Err(EngineError::SignalSuperseded {
                        key: step_key.to_string(),
                    });
                }
            }
        }

        let mut meta_table = write_txn.open_table(WORKFLOW_META)?;

        match meta_table.get(meta_key.as_str())? {
            None => {
                return Err(EngineError::SignalRejected {
                    key: step_key.to_string(),
                    reason: "workflow metadata not found".to_string(),
                });
            }
            Some(guard) => {
                let bytes: &[u8] = guard.value();
                let meta: WorkflowMetadata =
                    postcard::from_bytes(bytes).map_err(|e| EngineError::Serialization {
                        key: meta_key.clone(),
                        source: Box::new(e),
                    })?;
                if !matches!(meta.status(), MetadataStatus::Suspended { .. }) {
                    return Err(EngineError::SignalSuperseded {
                        key: step_key.to_string(),
                    });
                }
            }
        }

        steps_table.insert(composite_key.as_str(), step_bytes)?;

        let running_meta = WorkflowMetadata::new(MetadataStatus::Running);
        let meta_bytes =
            postcard::to_allocvec(&running_meta).map_err(|e| EngineError::Serialization {
                key: meta_key.clone(),
                source: Box::new(e),
            })?;
        meta_table.insert(meta_key.as_str(), meta_bytes.as_slice())?;
    }
    write_txn.commit()?;

    Ok(())
}

pub(super) async fn poll_timers(
    db: &Arc<Database>,
    workflows: &HashMap<String, WorkflowFn>,
    timer_serial: &Arc<AtomicU64>,
    tasks: &Arc<tokio::sync::Mutex<JoinSet<()>>>,
    default_retry: Option<&RetryPolicy>,
    senders: &Senders,
) -> Result<(), EngineError> {
    let now = now_unix_millis();
    let expired = collect_expired_timers(db, now)?;

    for (key, entry) in expired {
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(TIMERS)?;
            table.remove(key)?;
        }
        write_txn.commit()?;

        // Fast-path: skip if workflow is clearly not suspended.
        let meta = metadata::read_metadata(db, &entry.workflow_name, &entry.instance_id)?;
        let is_suspended = meta
            .as_ref()
            .is_some_and(|m| matches!(m.status(), MetadataStatus::Suspended { .. }));
        if !is_suspended {
            info!(
                workflow = entry.workflow_name,
                instance = entry.instance_id,
                step = entry.step_key,
                "timer expired but workflow not suspended — skipping"
            );
            continue;
        }

        info!(
            workflow = entry.workflow_name,
            instance = entry.instance_id,
            step = entry.step_key,
            "timer expired — signalling"
        );

        match signal_timer(
            db,
            workflows,
            timer_serial,
            tasks,
            &entry,
            default_retry,
            senders,
        )
        .await
        {
            Ok(()) => {}
            Err(EngineError::SignalSuperseded { ref key }) => {
                info!(
                    workflow = entry.workflow_name,
                    instance = entry.instance_id,
                    step = key,
                    "timer claim superseded — signal already delivered"
                );
            }
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

type TimerKey = (u64, u64);

fn collect_expired_timers(
    db: &Database,
    now: u64,
) -> Result<Vec<(TimerKey, TimerEntry)>, EngineError> {
    let read_txn = db.begin_read()?;
    let table = match read_txn.open_table(TIMERS) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(e) => return Err(EngineError::from(e)),
    };

    let mut expired = Vec::new();
    for entry in table.range((0, 0)..=(now, u64::MAX))? {
        let (key_guard, value_guard) = entry?;
        let key = key_guard.value();
        let timer_entry: TimerEntry =
            postcard::from_bytes(value_guard.value()).map_err(|e| EngineError::Serialization {
                key: format!("timer({},{})", key.0, key.1),
                source: Box::new(e),
            })?;
        expired.push((key, timer_entry));
    }
    Ok(expired)
}

async fn signal_timer(
    db: &Arc<Database>,
    workflows: &HashMap<String, WorkflowFn>,
    timer_serial: &Arc<AtomicU64>,
    tasks: &Arc<tokio::sync::Mutex<JoinSet<()>>>,
    entry: &TimerEntry,
    default_retry: Option<&RetryPolicy>,
    senders: &Senders,
) -> Result<(), EngineError> {
    let data: StepData<()> = StepData::Completed {
        result: (),
        status: None,
    };
    let step_bytes = serialize_step(&data, &entry.step_key)?;

    claim_suspended_step(
        db,
        &entry.workflow_name,
        &entry.instance_id,
        &entry.step_key,
        &step_bytes,
    )?;

    let workflow = workflows
        .get(&entry.workflow_name)
        .ok_or_else(|| EngineError::WorkflowNotFound(entry.workflow_name.clone()))?;

    let tx = {
        let mut map = senders
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(tx) = map.get(&entry.instance_id) {
            tx.send_if_modified(|state| {
                *state = WorkflowState::Started;
                false
            });
            tx.clone()
        } else {
            let (tx, _) = watch::channel(WorkflowState::Started);
            map.insert(entry.instance_id.clone(), tx.clone());
            tx
        }
    };

    let mut tasks = tasks.lock().await;
    spawn_workflow_task(
        &mut tasks,
        workflow,
        db,
        &entry.workflow_name,
        &entry.instance_id,
        timer_serial,
        default_retry.cloned(),
        &tx,
        senders,
    );

    Ok(())
}
