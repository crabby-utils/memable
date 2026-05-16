use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};

use redb::Database;
use redb::backends::InMemoryBackend;
use tokio::task::JoinSet;

use crate::error::EngineError;
use crate::retry::RetryPolicy;

/// Builder for configuring and constructing an [`Engine`](super::Engine).
///
/// Uses a typestate pattern: [`build`](EngineBuilder::build) is only available
/// after a storage backend has been configured via [`in_memory`](EngineBuilder::in_memory)
/// or [`open`](EngineBuilder::open).
///
/// # Examples
///
/// ```
/// use memable::Engine;
///
/// let engine = Engine::builder().in_memory().build();
/// ```
pub struct EngineBuilder<S = NoStore> {
    pub(super) store: S,
    pub(super) default_retry: Option<RetryPolicy>,
    pub(super) resume_on_start: bool,
}

/// Typestate: no storage backend configured yet.
///
/// Call [`EngineBuilder::in_memory`] or [`EngineBuilder::open`] to proceed.
#[doc(hidden)]
pub struct NoStore;

/// Typestate: storage backend has been configured.
#[doc(hidden)]
pub struct HasStore(pub(super) Database);

impl EngineBuilder<NoStore> {
    /// Uses an in-memory store (no data survives process exit).
    ///
    /// Ideal for tests and short-lived workflows.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::Engine;
    ///
    /// let engine = Engine::builder().in_memory().build();
    /// ```
    #[must_use]
    #[expect(clippy::missing_panics_doc)]
    pub fn in_memory(self) -> EngineBuilder<HasStore> {
        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .expect("in-memory database creation should not fail");
        EngineBuilder {
            store: HasStore(db),
            default_retry: self.default_retry,
            resume_on_start: self.resume_on_start,
        }
    }

    /// Uses a file-backed store at `path` for durable persistence.
    ///
    /// Creates the database file if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Storage`] if the database cannot be opened
    /// or created at the given path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use memable::Engine;
    ///
    /// let engine = Engine::builder()
    ///     .open("workflows.redb")
    ///     .expect("failed to open database")
    ///     .build();
    /// ```
    pub fn open(self, path: impl AsRef<Path>) -> Result<EngineBuilder<HasStore>, EngineError> {
        let db = Database::create(path)?;
        Ok(EngineBuilder {
            store: HasStore(db),
            default_retry: self.default_retry,
            resume_on_start: self.resume_on_start,
        })
    }

    /// Sets a default retry policy for all steps.
    ///
    /// Individual steps can override this with
    /// [`StepBuilder::retry`](crate::StepBuilder::retry) or disable it
    /// with [`StepBuilder::no_retry`](crate::StepBuilder::no_retry).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use memable::{Engine, RetryPolicy};
    ///
    /// let engine = Engine::builder()
    ///     .in_memory()
    ///     .default_retry(RetryPolicy::exponential(3, Duration::from_secs(1)))
    ///     .build();
    /// ```
    #[must_use]
    pub fn default_retry(mut self, policy: RetryPolicy) -> Self {
        self.default_retry = Some(policy);
        self
    }

    /// Controls whether `Running` workflow instances are automatically
    /// resumed when the engine starts.
    ///
    /// Defaults to `true`. When enabled, [`Engine::start`](super::Engine::start) scans the
    /// metadata table for instances that were `Running` when the process
    /// last exited and resumes them. `Suspended` instances are not
    /// affected — they continue to wait for their signal or timer.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::Engine;
    ///
    /// let engine = Engine::builder()
    ///     .in_memory()
    ///     .resume_on_start(false)
    ///     .build();
    /// ```
    #[must_use]
    pub fn resume_on_start(mut self, enabled: bool) -> Self {
        self.resume_on_start = enabled;
        self
    }
}

impl EngineBuilder<HasStore> {
    /// Sets a default retry policy for all steps.
    ///
    /// Individual steps can override this with
    /// [`StepBuilder::retry`](crate::StepBuilder::retry) or disable it
    /// with [`StepBuilder::no_retry`](crate::StepBuilder::no_retry).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use memable::{Engine, RetryPolicy};
    ///
    /// let engine = Engine::builder()
    ///     .in_memory()
    ///     .default_retry(RetryPolicy::exponential(3, Duration::from_secs(1)))
    ///     .build();
    /// ```
    #[must_use]
    pub fn default_retry(mut self, policy: RetryPolicy) -> Self {
        self.default_retry = Some(policy);
        self
    }

    /// Controls whether `Running` workflow instances are automatically
    /// resumed when the engine starts.
    ///
    /// Defaults to `true`. When enabled, [`Engine::start`](super::Engine::start) scans the
    /// metadata table for instances that were `Running` when the process
    /// last exited and resumes them. `Suspended` instances are not
    /// affected — they continue to wait for their signal or timer.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::Engine;
    ///
    /// let engine = Engine::builder()
    ///     .in_memory()
    ///     .resume_on_start(false)
    ///     .build();
    /// ```
    #[must_use]
    pub fn resume_on_start(mut self, enabled: bool) -> Self {
        self.resume_on_start = enabled;
        self
    }

    /// Builds the engine.
    ///
    /// This method is only available after a storage backend has been
    /// configured via [`in_memory`](EngineBuilder::in_memory) or
    /// [`open`](EngineBuilder::open).
    #[must_use]
    pub fn build(self) -> super::Engine {
        super::Engine {
            db: Arc::new(self.store.0),
            workflows: HashMap::new(),
            running: Arc::new(AtomicBool::new(false)),
            tasks: Arc::new(tokio::sync::Mutex::new(JoinSet::new())),
            timer_serial: Arc::new(AtomicU64::new(0)),
            default_retry: self.default_retry,
            resume_on_start: self.resume_on_start,
            senders: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
