use std::fmt;
use std::time::Duration;

/// Errors originating from the engine itself.
///
/// Covers storage failures (redb), serialization failures (postcard), and
/// step execution failures propagated from user closures.
///
/// # Examples
///
/// ```
/// use memable::EngineError;
///
/// let err = EngineError::step_failed("my-step", "connection reset");
/// assert!(err.to_string().contains("my-step"));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// A storage operation failed.
    #[error("storage error: {0}")]
    Storage(Box<dyn std::error::Error + Send + Sync>),

    /// Serialization or deserialization of a step result failed.
    #[error("serialization error for step '{key}': {source}")]
    Serialization {
        /// The step key that failed.
        key: String,
        /// The underlying serialization error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A step closure returned an error.
    #[error("step '{key}' failed: {source}")]
    StepFailed {
        /// The step key that failed.
        key: String,
        /// The underlying error from the step closure.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The engine has not been started.
    #[error("engine has not been started")]
    NotStarted,

    /// No workflow is registered under the given name.
    #[error("workflow '{0}' is not registered")]
    WorkflowNotFound(String),

    /// A step exceeded its configured timeout.
    #[error("step '{key}' timed out after {duration:?}")]
    StepTimeout {
        /// The step key that timed out.
        key: String,
        /// The timeout duration that was exceeded.
        duration: Duration,
    },

    /// The workflow suspended at a step, awaiting an external signal.
    #[error("workflow suspended at step '{key}'")]
    Suspended {
        /// The step key where the workflow suspended.
        key: String,
    },
}

impl EngineError {
    /// Creates a [`StepFailed`](EngineError::StepFailed) error.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::EngineError;
    ///
    /// let err = EngineError::step_failed("my-step", "boom");
    /// assert!(matches!(err, EngineError::StepFailed { .. }));
    /// ```
    pub fn step_failed(
        key: impl Into<String>,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self::StepFailed {
            key: key.into(),
            source: source.into(),
        }
    }
}

impl From<redb::DatabaseError> for EngineError {
    fn from(err: redb::DatabaseError) -> Self {
        Self::Storage(Box::new(err))
    }
}

impl From<redb::TransactionError> for EngineError {
    fn from(err: redb::TransactionError) -> Self {
        Self::Storage(Box::new(err))
    }
}

impl From<redb::TableError> for EngineError {
    fn from(err: redb::TableError) -> Self {
        Self::Storage(Box::new(err))
    }
}

impl From<redb::StorageError> for EngineError {
    fn from(err: redb::StorageError) -> Self {
        Self::Storage(Box::new(err))
    }
}

impl From<redb::CommitError> for EngineError {
    fn from(err: redb::CommitError) -> Self {
        Self::Storage(Box::new(err))
    }
}

/// Error returned by step closures.
///
/// The variant communicates retry intent to the engine:
/// - [`Retryable`](StepError::Retryable) — transient failure (retry support
///   is planned but not yet implemented).
/// - [`Permanent`](StepError::Permanent) — unrecoverable failure, propagated
///   immediately.
///
/// There is intentionally no blanket [`From`] implementation. Callers must
/// choose explicitly whether a given error is retryable or permanent.
///
/// # Examples
///
/// ```
/// use memable::StepError;
///
/// let err = StepError::permanent("invalid input");
/// assert!(err.to_string().contains("permanent"));
/// ```
#[derive(Debug)]
pub enum StepError {
    /// A transient failure that may succeed on retry.
    Retryable(Box<dyn std::error::Error + Send + Sync>),
    /// An unrecoverable failure.
    Permanent(Box<dyn std::error::Error + Send + Sync>),
}

impl StepError {
    /// Creates a [`Retryable`](StepError::Retryable) error.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::StepError;
    ///
    /// let err = StepError::retryable("timeout");
    /// assert!(matches!(err, StepError::Retryable(_)));
    /// ```
    pub fn retryable(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Retryable(source.into())
    }

    /// Creates a [`Permanent`](StepError::Permanent) error.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::StepError;
    ///
    /// let err = StepError::permanent("bad input");
    /// assert!(matches!(err, StepError::Permanent(_)));
    /// ```
    pub fn permanent(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Permanent(source.into())
    }
}

impl fmt::Display for StepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retryable(e) => write!(f, "retryable: {e}"),
            Self::Permanent(e) => write!(f, "permanent: {e}"),
        }
    }
}

impl std::error::Error for StepError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Retryable(e) | Self::Permanent(e) => Some(e.as_ref()),
        }
    }
}
