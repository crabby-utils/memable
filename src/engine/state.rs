/// Observable state of a workflow instance.
///
/// Returned via an [`Invocation`](super::Invocation) from [`Engine::invoke`](super::Engine::invoke). Callers
/// observe state transitions or drop the handle for fire-and-forget
/// execution.
///
/// # Examples
///
/// ```
/// use memable::WorkflowState;
///
/// let state = WorkflowState::InProgress("syncing records".into());
/// assert_eq!(state.to_string(), "in progress: syncing records");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowState {
    /// The workflow has been submitted but has not yet started executing.
    Started,
    /// The workflow is executing and has reported a status message.
    InProgress(String),
    /// The workflow is suspended, awaiting an external signal.
    ///
    /// `key` is the step key where the workflow suspended (e.g. `"approval:v1"`).
    /// `status` is the human-readable message set via
    /// [`SuspendBuilder::status`](crate::SuspendBuilder::status), or the key
    /// itself if no custom status was provided.
    Suspended {
        /// The step key identifying this suspend point.
        key: String,
        /// Human-readable status message.
        status: String,
    },
    /// The workflow completed successfully.
    ///
    /// Contains the last status message set via
    /// [`Context::set_status`](crate::Context::set_status) before the
    /// workflow returned, or `None` if no status was ever set.
    Completed(Option<String>),
    /// The workflow failed with an error message.
    Failed(String),
}

impl WorkflowState {
    /// Returns `true` if this is a terminal state (`Completed` or `Failed`).
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::WorkflowState;
    ///
    /// assert!(WorkflowState::Completed(None).is_terminal());
    /// assert!(WorkflowState::Failed("err".into()).is_terminal());
    /// assert!(!WorkflowState::Started.is_terminal());
    /// ```
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed(_) | Self::Failed(_))
    }

    /// Returns the message carried by this state, if any.
    ///
    /// [`Started`](Self::Started) carries no message.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::WorkflowState;
    ///
    /// let state = WorkflowState::InProgress("loading".into());
    /// assert_eq!(state.message(), Some("loading"));
    ///
    /// assert_eq!(WorkflowState::Completed(None).message(), None);
    /// ```
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        match self {
            Self::InProgress(msg) | Self::Failed(msg) => Some(msg),
            Self::Suspended { status, .. } => Some(status),
            Self::Completed(msg) => msg.as_deref(),
            Self::Started => None,
        }
    }
}

impl std::fmt::Display for WorkflowState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started => write!(f, "started"),
            Self::InProgress(msg) => write!(f, "in progress: {msg}"),
            Self::Suspended { key, status } => write!(f, "suspended ({key}): {status}"),
            Self::Completed(None) => write!(f, "completed"),
            Self::Completed(Some(msg)) => write!(f, "completed: {msg}"),
            Self::Failed(msg) => write!(f, "failed: {msg}"),
        }
    }
}
