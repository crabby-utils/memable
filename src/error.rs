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
/// let err = EngineError::step_failed("my-step", "connection reset", false);
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
        /// Whether the original error was marked as retryable.
        retryable: bool,
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

    /// A key component contains the `/` delimiter.
    #[error("invalid key component ({label}): '{value}' must not contain '/'")]
    InvalidKey {
        /// Which component was invalid (e.g. `"workflow_name"`, `"instance_id"`, `"step_key"`).
        label: &'static str,
        /// The invalid value.
        value: String,
    },

    /// The stored payload type does not match the expected type.
    ///
    /// This occurs when [`Engine::signal`](crate::Engine::signal) delivers a
    /// payload of type `T` but the workflow's suspend point expects type `U`.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::EngineError;
    ///
    /// let err = EngineError::TypeMismatch {
    ///     key: "approval:v1".into(),
    ///     expected: "i32".into(),
    ///     found: "alloc::string::String".into(),
    /// };
    /// assert!(err.to_string().contains("type mismatch"));
    /// ```
    #[error("type mismatch for step '{key}': expected `{expected}`, found `{found}`")]
    TypeMismatch {
        /// The step key where the mismatch occurred.
        key: String,
        /// The type name expected by the deserializing code.
        expected: String,
        /// The type name found in the stored envelope.
        found: String,
    },

    /// A signal was rejected because the target step is not suspended.
    #[error("signal rejected for step '{key}': {reason}")]
    SignalRejected {
        /// The step key the signal targeted.
        key: String,
        /// Why the signal was rejected.
        reason: String,
    },

    /// A signal was superseded because another caller already claimed the
    /// suspended step.
    ///
    /// This is distinct from [`SignalRejected`](EngineError::SignalRejected),
    /// which means the step was never suspended or does not exist.
    /// `SignalSuperseded` means the step *was* suspended but another signal
    /// or timer already claimed it.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::EngineError;
    ///
    /// let err = EngineError::SignalSuperseded {
    ///     key: "approval:v1".into(),
    /// };
    /// assert!(err.to_string().contains("superseded"));
    /// ```
    #[error("signal superseded for step '{key}': another caller already claimed it")]
    SignalSuperseded {
        /// The step key that was already claimed.
        key: String,
    },

    /// A regular step found a `Suspended` entry in the step table.
    ///
    /// This means the step key was previously used as a suspend or timer
    /// point, and the workflow code has since changed to use the same key
    /// for a regular step. The engine refuses to silently re-execute the
    /// step because it would bypass the original suspend gate.
    ///
    /// To resolve this, either:
    /// - Use a new step key (e.g. `"review:v2"`) in the updated workflow
    /// - Drain or signal suspended instances before deploying the change
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::EngineError;
    ///
    /// let err = EngineError::SuspendedStepConflict {
    ///     key: "approval:v1".into(),
    /// };
    /// assert!(err.to_string().contains("suspended entry"));
    /// ```
    #[error(
        "step '{key}' has a suspended entry but was called as a regular step \
         — the workflow code likely changed while an instance was suspended"
    )]
    SuspendedStepConflict {
        /// The step key with the conflicting entry.
        key: String,
    },

    /// All retry attempts for a step were exhausted.
    ///
    /// The step's last error is preserved as the [`source`](std::error::Error::source).
    /// The failed result is persisted as a dead-letter entry. Calling
    /// [`Engine::resume`](crate::Engine::resume) re-attempts the step with
    /// a fresh retry budget.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::EngineError;
    ///
    /// let err = EngineError::retries_exhausted("fetch:v1", 3, "connection refused");
    /// assert!(err.to_string().contains("3 attempts"));
    /// ```
    #[error("step '{key}' failed after {attempts} attempts: {source}")]
    RetriesExhausted {
        /// The step key that exhausted its retries.
        key: String,
        /// Total number of attempts (initial + retries).
        attempts: u32,
        /// The last error from the step closure.
        source: Box<dyn std::error::Error + Send + Sync>,
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
    /// let err = EngineError::step_failed("my-step", "boom", false);
    /// assert!(matches!(err, EngineError::StepFailed { .. }));
    /// ```
    pub fn step_failed(
        key: impl Into<String>,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
        retryable: bool,
    ) -> Self {
        Self::StepFailed {
            key: key.into(),
            source: source.into(),
            retryable,
        }
    }

    /// Creates a [`RetriesExhausted`](EngineError::RetriesExhausted) error.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::EngineError;
    ///
    /// let err = EngineError::retries_exhausted("fetch:v1", 3, "timeout");
    /// assert!(matches!(err, EngineError::RetriesExhausted { .. }));
    /// ```
    pub fn retries_exhausted(
        key: impl Into<String>,
        attempts: u32,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self::RetriesExhausted {
            key: key.into(),
            attempts,
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

/// Error returned by [`Engine::subscribe`](crate::Engine::subscribe).
///
/// Distinguishes between an instance that never existed and one that has
/// stale `Running` metadata from a crash (no live task backing it).
///
/// # Examples
///
/// ```
/// use memable::SubscribeError;
///
/// let err = SubscribeError::NotFound {
///     workflow_name: "etl".into(),
///     instance_id: "run-42".into(),
/// };
/// assert!(err.to_string().contains("etl"));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    /// No instance with this workflow name and ID exists.
    #[error("no instance found for workflow '{workflow_name}' with id '{instance_id}'")]
    NotFound {
        /// The workflow name that was queried.
        workflow_name: String,
        /// The instance ID that was queried.
        instance_id: String,
    },

    /// The instance has persisted `Running` metadata but no live task.
    ///
    /// This typically indicates the process crashed while the workflow was
    /// executing. The instance may be recoverable via
    /// [`Engine::resume`](crate::Engine::resume).
    #[error(
        "instance '{instance_id}' of workflow '{workflow_name}' has stale Running metadata \
         (likely crashed) — consider calling resume()"
    )]
    StaleRunning {
        /// The workflow name.
        workflow_name: String,
        /// The instance ID with stale state.
        instance_id: String,
    },

    /// A storage operation failed while reading metadata.
    #[error("storage error: {0}")]
    Storage(Box<dyn std::error::Error + Send + Sync>),
}

/// Error returned by step closures.
///
/// The variant communicates retry intent to the engine:
/// - [`Retryable`](StepError::Retryable) — transient failure that the engine
///   will retry according to the step's [`RetryPolicy`](crate::RetryPolicy).
/// - [`Permanent`](StepError::Permanent) — unrecoverable failure, propagated
///   immediately without retrying.
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
