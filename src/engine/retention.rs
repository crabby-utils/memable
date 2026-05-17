use std::collections::HashMap;
use std::time::Duration;

use redb::{Database, ReadableDatabase as _, ReadableTable as _};
use tracing::{debug, info, warn};

use crate::context::{STEPS, TIMERS, TimerEntry};
use crate::error::EngineError;
use crate::metadata::WORKFLOW_META;

/// Scans the metadata table and deletes all data for terminal workflows
/// that have exceeded their retention period.
///
/// For each expired instance, deletes:
/// 1. All step entries (prefix scan on steps table)
/// 2. Any remaining timer entries referencing this instance
/// 3. The metadata entry itself
///
/// Returns the number of instances cleaned up.
pub(super) fn cleanup_expired(
    db: &Database,
    default_retention: Duration,
    workflow_retentions: &HashMap<String, Duration>,
) -> Result<u32, EngineError> {
    let now = now_unix_secs();
    let expired = collect_expired_metadata(db, now, default_retention, workflow_retentions)?;

    if expired.is_empty() {
        return Ok(0);
    }

    let mut cleaned = 0u32;
    for (workflow_name, instance_id) in &expired {
        match delete_instance_data(db, workflow_name, instance_id) {
            Ok(()) => {
                debug!(
                    workflow = %workflow_name,
                    instance = %instance_id,
                    "cleaned up expired workflow instance"
                );
                cleaned += 1;
            }
            Err(e) => {
                warn!(
                    workflow = %workflow_name,
                    instance = %instance_id,
                    error = %e,
                    "failed to clean up expired instance — will retry next cycle"
                );
            }
        }
    }

    if cleaned > 0 {
        info!(cleaned, "retention cleanup completed");
    }

    Ok(cleaned)
}

/// Collects metadata keys for terminal workflows past their retention period.
fn collect_expired_metadata(
    db: &Database,
    now_secs: u64,
    default_retention: Duration,
    workflow_retentions: &HashMap<String, Duration>,
) -> Result<Vec<(String, String)>, EngineError> {
    let read_txn = db.begin_read()?;
    let table = match read_txn.open_table(WORKFLOW_META) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(e) => return Err(EngineError::from(e)),
    };

    let mut expired: Vec<(String, String)> = Vec::new();
    for entry in table.iter()? {
        let entry = entry?;
        let full_key = entry.0.value();
        let bytes = entry.1.value();

        let Some((workflow_name, instance_id)) = full_key.split_once('/') else {
            continue;
        };

        let meta: crate::metadata::WorkflowMetadata =
            postcard::from_bytes(bytes).map_err(|e| EngineError::Serialization {
                key: full_key.to_string(),
                source: Box::new(e),
            })?;

        let Some(completed_at) = meta.completed_at() else {
            continue;
        };

        let retention = workflow_retentions
            .get(workflow_name)
            .copied()
            .unwrap_or(default_retention);

        let age_secs = now_secs.saturating_sub(completed_at);
        if age_secs >= retention.as_secs() {
            expired.push((workflow_name.to_string(), instance_id.to_string()));
        }
    }

    Ok(expired)
}

/// Deletes all data for a single workflow instance in one transaction.
fn delete_instance_data(
    db: &Database,
    workflow_name: &str,
    instance_id: &str,
) -> Result<(), EngineError> {
    let meta_key = format!("{workflow_name}/{instance_id}");
    let step_prefix = format!("{workflow_name}/{instance_id}/");
    let step_end = format!("{workflow_name}/{instance_id}0");

    let write_txn = db.begin_write()?;
    {
        let mut meta_table = write_txn.open_table(WORKFLOW_META)?;
        meta_table.remove(meta_key.as_str())?;

        let mut steps_table = write_txn.open_table(STEPS)?;
        let step_keys: Vec<String> = steps_table
            .range(step_prefix.as_str()..step_end.as_str())?
            .map(|entry| entry.map(|(k, _)| k.value().to_string()))
            .collect::<Result<_, _>>()?;
        for key in &step_keys {
            steps_table.remove(key.as_str())?;
        }

        let mut timers_table = write_txn.open_table(TIMERS)?;
        let timer_keys: Vec<(u64, u64)> = timers_table
            .iter()?
            .filter_map(|entry| {
                let (key_guard, value_guard) = entry.ok()?;
                let timer: TimerEntry = postcard::from_bytes(value_guard.value()).ok()?;
                if timer.workflow_name == workflow_name && timer.instance_id == instance_id {
                    Some(key_guard.value())
                } else {
                    None
                }
            })
            .collect();
        for key in timer_keys {
            timers_table.remove(key)?;
        }
    }
    write_txn.commit()?;

    Ok(())
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
}
