use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableDatabase as _, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::error::EngineError;

/// redb table for workflow metadata.
/// Key: `"{workflow_name}/{instance_id}"`, Value: postcard-serialized bytes.
pub(crate) const WORKFLOW_META: TableDefinition<&str, &[u8]> =
    TableDefinition::new("workflow_meta");

/// Lifecycle status of a workflow instance.
///
/// Represents the coarse-grained state stored in the metadata table,
/// distinct from [`WorkflowState`](crate::WorkflowState) which includes
/// the finer-grained `InProgress` variant used by the watch channel.
///
/// # Examples
///
/// ```
/// use memable::MetadataStatus;
///
/// let status = MetadataStatus::Failed("connection reset".into());
/// assert!(status.is_terminal());
/// assert!(!MetadataStatus::Running.is_terminal());
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataStatus {
    /// The workflow is currently executing.
    Running,
    /// The workflow is suspended, awaiting an external signal.
    Suspended {
        /// The step key identifying this suspend point.
        key: String,
        /// Human-readable status message.
        status: String,
    },
    /// The workflow completed successfully.
    ///
    /// Contains the last status message at completion time, if any.
    Completed(Option<String>),
    /// The workflow failed with an error message.
    Failed(String),
}

impl MetadataStatus {
    /// Returns `true` if this is a terminal status (`Completed` or `Failed`).
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::MetadataStatus;
    ///
    /// assert!(MetadataStatus::Completed(None).is_terminal());
    /// assert!(MetadataStatus::Failed("err".into()).is_terminal());
    /// assert!(!MetadataStatus::Running.is_terminal());
    /// ```
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed(_) | Self::Failed(_))
    }
}

impl std::fmt::Display for MetadataStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Suspended { status, .. } => write!(f, "suspended: {status}"),
            Self::Completed(None) => write!(f, "completed"),
            Self::Completed(Some(msg)) => write!(f, "completed: {msg}"),
            Self::Failed(msg) => write!(f, "failed: {msg}"),
        }
    }
}

/// Metadata record for a workflow instance.
///
/// Stored in a dedicated redb table keyed by
/// `"{workflow_name}/{instance_id}"`. Provides workflow-level state
/// without scanning the step table.
///
/// # Examples
///
/// ```
/// use memable::{MetadataStatus, WorkflowMetadata};
///
/// let meta = WorkflowMetadata::new(MetadataStatus::Running);
/// assert_eq!(meta.status(), &MetadataStatus::Running);
/// assert!(meta.completed_at().is_none());
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowMetadata {
    status: MetadataStatus,
    completed_at: Option<u64>,
}

impl WorkflowMetadata {
    /// Creates a new metadata record with the given status.
    ///
    /// `completed_at` is set automatically for terminal statuses.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::{MetadataStatus, WorkflowMetadata};
    ///
    /// let meta = WorkflowMetadata::new(MetadataStatus::Completed(None));
    /// assert!(meta.completed_at().is_some());
    /// ```
    #[must_use]
    pub fn new(status: MetadataStatus) -> Self {
        let completed_at = if status.is_terminal() {
            Some(now_unix_secs())
        } else {
            None
        };
        Self {
            status,
            completed_at,
        }
    }

    /// Returns the lifecycle status.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::{MetadataStatus, WorkflowMetadata};
    ///
    /// let meta = WorkflowMetadata::new(MetadataStatus::Running);
    /// assert_eq!(meta.status(), &MetadataStatus::Running);
    /// ```
    #[must_use]
    pub fn status(&self) -> &MetadataStatus {
        &self.status
    }

    /// Returns the Unix timestamp (seconds) when the workflow reached a
    /// terminal state, or `None` if it is still in progress.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::{MetadataStatus, WorkflowMetadata};
    ///
    /// let meta = WorkflowMetadata::new(MetadataStatus::Running);
    /// assert!(meta.completed_at().is_none());
    ///
    /// let meta = WorkflowMetadata::new(MetadataStatus::Completed(None));
    /// assert!(meta.completed_at().is_some());
    /// ```
    #[must_use]
    pub fn completed_at(&self) -> Option<u64> {
        self.completed_at
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
}

pub(crate) fn write_metadata(
    db: &Database,
    workflow_name: &str,
    instance_id: &str,
    metadata: &WorkflowMetadata,
) -> Result<(), EngineError> {
    let key = format!("{workflow_name}/{instance_id}");
    let bytes = postcard::to_allocvec(metadata).map_err(|e| EngineError::Serialization {
        key: key.clone(),
        source: Box::new(e),
    })?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(WORKFLOW_META)?;
        table.insert(key.as_str(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

pub(crate) fn read_metadata(
    db: &Database,
    workflow_name: &str,
    instance_id: &str,
) -> Result<Option<WorkflowMetadata>, EngineError> {
    let key = format!("{workflow_name}/{instance_id}");
    let read_txn = db.begin_read()?;
    let table = match read_txn.open_table(WORKFLOW_META) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(EngineError::from(e)),
    };
    match table.get(key.as_str())? {
        Some(guard) => {
            let meta: WorkflowMetadata =
                postcard::from_bytes(guard.value()).map_err(|e| EngineError::Serialization {
                    key,
                    source: Box::new(e),
                })?;
            Ok(Some(meta))
        }
        None => Ok(None),
    }
}

/// Lists all instances for a workflow name via prefix range scan.
///
/// Returns `(instance_id, metadata)` pairs.
pub(crate) fn list_metadata(
    db: &Database,
    workflow_name: &str,
) -> Result<Vec<(String, WorkflowMetadata)>, EngineError> {
    let prefix = format!("{workflow_name}/");
    // '/' is 0x2F, '0' is 0x30 — this captures all keys with the prefix.
    let end = format!("{workflow_name}0");

    let read_txn = db.begin_read()?;
    let table = match read_txn.open_table(WORKFLOW_META) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(e) => return Err(EngineError::from(e)),
    };

    let mut results = Vec::new();
    for entry in table.range(prefix.as_str()..end.as_str())? {
        let (key_guard, value_guard) = entry?;
        let full_key = key_guard.value();
        let instance_id = full_key
            .strip_prefix(&prefix)
            .expect("range scan should only yield keys with the prefix")
            .to_string();
        let meta: WorkflowMetadata =
            postcard::from_bytes(value_guard.value()).map_err(|e| EngineError::Serialization {
                key: full_key.to_string(),
                source: Box::new(e),
            })?;
        results.push((instance_id, meta));
    }
    Ok(results)
}
