use std::collections::HashMap;
use std::future::Future;
use std::hash::{BuildHasher as _, RandomState};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redb::backends::InMemoryBackend;
use redb::{Database, ReadableDatabase as _, ReadableTable as _};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::Instrument as _;
use tracing::{error, info, info_span};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::context::{
    Context, STEPS, StepData, StepEnvelope, TIMERS, TimerEntry, deserialize_envelope,
    serialize_step,
};
use crate::error::EngineError;
use crate::metadata::{self, MetadataStatus, WORKFLOW_META, WorkflowMetadata};
use crate::retry::RetryPolicy;

/// Observable state of a workflow instance.
///
/// Returned via an [`Invocation`] from [`Engine::invoke`]. Callers
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
    Suspended(String),
    /// The workflow completed successfully.
    Completed,
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
    /// assert!(WorkflowState::Completed.is_terminal());
    /// assert!(WorkflowState::Failed("err".into()).is_terminal());
    /// assert!(!WorkflowState::Started.is_terminal());
    /// ```
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed(_))
    }

    /// Returns the message carried by this state, if any.
    ///
    /// [`Started`](Self::Started) and [`Completed`](Self::Completed) carry
    /// no message and return `None`. All other variants return the inner
    /// string.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::WorkflowState;
    ///
    /// let state = WorkflowState::InProgress("loading".into());
    /// assert_eq!(state.message(), Some("loading"));
    ///
    /// assert_eq!(WorkflowState::Completed.message(), None);
    /// ```
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        match self {
            Self::InProgress(msg) | Self::Suspended(msg) | Self::Failed(msg) => Some(msg),
            Self::Started | Self::Completed => None,
        }
    }
}

impl std::fmt::Display for WorkflowState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started => write!(f, "started"),
            Self::InProgress(msg) => write!(f, "in progress: {msg}"),
            Self::Suspended(msg) => write!(f, "suspended: {msg}"),
            Self::Completed => write!(f, "completed"),
            Self::Failed(msg) => write!(f, "failed: {msg}"),
        }
    }
}

/// Handle for a running workflow instance.
///
/// Returned by [`Engine::invoke`] and [`Engine::resume`]. Provides the
/// instance ID and a status channel for observing [`WorkflowState`]
/// transitions.
///
/// # Examples
///
/// ```
/// # use memable::{Engine, Context, EngineError, WorkflowState};
/// # async fn greet(ctx: Context) -> Result<(), EngineError> { Ok(()) }
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut engine = Engine::builder().in_memory().build();
/// # engine.register("greet", greet);
/// # engine.start().await?;
/// let state = engine.invoke("greet").await?.wait().await;
/// assert_eq!(state, WorkflowState::Completed);
/// # Ok(())
/// # }
/// ```
pub struct Invocation {
    instance_id: String,
    status: watch::Receiver<WorkflowState>,
}

impl Invocation {
    /// Returns the instance ID for this invocation.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register("wf", wf);
    /// # engine.start().await?;
    /// let invocation = engine.invoke("wf").await?;
    /// println!("Instance: {}", invocation.instance_id());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Returns a mutable reference to the status receiver.
    ///
    /// For simple cases, prefer [`wait`](Invocation::wait). Use this
    /// method when you need to observe intermediate state transitions.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowState};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register("wf", wf);
    /// # engine.start().await?;
    /// let mut invocation = engine.invoke("wf").await?;
    /// while invocation.status().changed().await.is_ok() {
    ///     if invocation.status().borrow().is_terminal() { break; }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn status(&mut self) -> &mut watch::Receiver<WorkflowState> {
        &mut self.status
    }

    /// Waits until the workflow reaches a terminal state.
    ///
    /// Returns the final [`WorkflowState`] (`Completed` or `Failed`).
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowState};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register("wf", wf);
    /// # engine.start().await?;
    /// let state = engine.invoke("wf").await?.wait().await;
    /// assert_eq!(state, WorkflowState::Completed);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait(mut self) -> WorkflowState {
        loop {
            if self.status.borrow().is_terminal() {
                return self.status.borrow().clone();
            }
            if self.status.changed().await.is_err() {
                return self.status.borrow().clone();
            }
        }
    }

    /// Decomposes into the instance ID and status receiver.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register("wf", wf);
    /// # engine.start().await?;
    /// let (id, mut status) = engine.invoke("wf").await?.into_parts();
    /// println!("Started instance {id}");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn into_parts(self) -> (String, watch::Receiver<WorkflowState>) {
        (self.instance_id, self.status)
    }
}

/// Type-erased workflow function.
type WorkflowFn = Arc<
    dyn Fn(Context) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send>> + Send + Sync,
>;

fn validate_key_component(value: &str, label: &'static str) -> Result<(), EngineError> {
    if value.contains('/') {
        return Err(EngineError::InvalidKey {
            label,
            value: value.to_string(),
        });
    }
    Ok(())
}

fn generate_instance_id() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis();
    let rand: u64 = RandomState::new().hash_one(ts);
    format!("{ts}-{rand:x}")
}

/// Builder for configuring and constructing an [`Engine`].
///
/// # Examples
///
/// ```
/// use memable::Engine;
///
/// let engine = Engine::builder().in_memory().build();
/// ```
pub struct EngineBuilder {
    db: Option<Database>,
    default_retry: Option<RetryPolicy>,
}

impl EngineBuilder {
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
    pub fn in_memory(mut self) -> Self {
        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .expect("in-memory database creation should not fail");
        self.db = Some(db);
        self
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
    pub fn open(mut self, path: impl AsRef<Path>) -> Result<Self, EngineError> {
        let db = Database::create(path)?;
        self.db = Some(db);
        Ok(self)
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

    /// Builds the engine.
    ///
    /// # Panics
    ///
    /// Panics if no store was configured (call [`in_memory`](EngineBuilder::in_memory)
    /// or [`open`](EngineBuilder::open) first).
    #[must_use]
    pub fn build(self) -> Engine {
        let db = self
            .db
            .expect("Engine::builder() requires .in_memory() or .open(path) before .build()");
        Engine {
            db: Arc::new(db),
            workflows: HashMap::new(),
            running: Arc::new(AtomicBool::new(false)),
            tasks: Arc::new(tokio::sync::Mutex::new(JoinSet::new())),
            timer_serial: Arc::new(AtomicU64::new(0)),
            default_retry: self.default_retry,
        }
    }
}

/// The durable execution engine.
///
/// Workflows are registered as named definitions, then invoked by name.
/// Each invocation auto-generates a unique instance ID and returns an
/// [`Invocation`] handle for observing the workflow's [`WorkflowState`].
/// Failed instances can be retried with [`Engine::resume`].
///
/// # Lifecycle
///
/// 1. Build with [`Engine::builder`]
/// 2. Register workflows with [`Engine::register`]
/// 3. Start with [`Engine::start`]
/// 4. Invoke workflows with [`Engine::invoke`]
/// 5. Stop with [`Engine::stop`]
///
/// # Examples
///
/// ```
/// use memable::{Engine, Context, EngineError, WorkflowState};
///
/// async fn greet(ctx: Context) -> Result<(), EngineError> {
///     let name: String = ctx.step("fetch-name:v1").run(async || {
///         Ok("world".to_string())
///     }).await?;
///     assert_eq!(name, "world");
///     Ok(())
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut engine = Engine::builder().in_memory().build();
/// engine.register("greet", greet);
/// engine.start().await?;
///
/// let state = engine.invoke("greet").await?.wait().await;
/// assert_eq!(state, WorkflowState::Completed);
/// # Ok(())
/// # }
/// ```
pub struct Engine {
    db: Arc<Database>,
    workflows: HashMap<String, WorkflowFn>,
    running: Arc<AtomicBool>,
    tasks: Arc<tokio::sync::Mutex<JoinSet<()>>>,
    timer_serial: Arc<AtomicU64>,
    default_retry: Option<RetryPolicy>,
}

impl Engine {
    /// Returns a new [`EngineBuilder`].
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::Engine;
    ///
    /// let engine = Engine::builder().in_memory().build();
    /// ```
    #[must_use]
    pub fn builder() -> EngineBuilder {
        EngineBuilder {
            db: None,
            default_retry: None,
        }
    }

    /// Registers a workflow definition by name.
    ///
    /// The workflow function receives a [`Context`] and returns
    /// `Result<(), EngineError>`. Durable results are communicated via
    /// [`Context::step`].
    ///
    /// # Panics
    ///
    /// Panics if the engine has already been started, or if `name`
    /// contains the `/` delimiter.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError};
    /// async fn my_workflow(ctx: Context) -> Result<(), EngineError> {
    ///     Ok(())
    /// }
    ///
    /// let mut engine = Engine::builder().in_memory().build();
    /// engine.register("my-workflow", my_workflow);
    /// ```
    pub fn register<F, Fut>(&mut self, name: impl Into<String>, workflow: F)
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
    {
        assert!(
            !self.running.load(Ordering::Acquire),
            "cannot register workflows after the engine has started"
        );
        let name = name.into();
        assert!(
            !name.contains('/'),
            "workflow name must not contain '/': '{name}'"
        );
        info!(name, "registered workflow");
        self.workflows
            .insert(name, Arc::new(move |ctx| Box::pin(workflow(ctx))));
    }

    /// Starts the engine, allowing workflow invocations.
    ///
    /// Spawns a background timer poller that checks for expired durable
    /// timers every second. Timer deadlines have millisecond precision, but
    /// actual firing latency is up to ~1 second after the deadline.
    /// Must be called after all workflows are registered.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError`] if startup fails.
    ///
    /// # Panics
    ///
    /// Panics if the engine has already been started.
    pub async fn start(&mut self) -> Result<(), EngineError> {
        assert!(
            !self.running.load(Ordering::Acquire),
            "engine has already been started"
        );
        self.running.store(true, Ordering::Release);

        let db = Arc::clone(&self.db);
        let running = Arc::clone(&self.running);
        let workflows = self.workflows.clone();
        let timer_serial = Arc::clone(&self.timer_serial);
        let poller_tasks = Arc::clone(&self.tasks);
        let default_retry = self.default_retry.clone();
        let mut tasks = self.tasks.lock().await;
        tasks.spawn(
            async move {
                info!("timer poller started");
                while running.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if let Err(e) = poll_timers(
                        &db,
                        &workflows,
                        &timer_serial,
                        &poller_tasks,
                        default_retry.as_ref(),
                    )
                    .await
                    {
                        error!(error = %e, "timer poll failed");
                    }
                }
                info!("timer poller stopped");
            }
            .instrument(info_span!("timer_poller")),
        );

        info!("engine started");
        Ok(())
    }

    /// Invokes a registered workflow, creating a new instance.
    ///
    /// A unique instance ID is generated automatically. The workflow runs
    /// in a spawned Tokio task. Returns an [`Invocation`] handle with the
    /// instance ID and a status channel.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::NotStarted`] if the engine has not been started,
    /// [`EngineError::WorkflowNotFound`] if no workflow is registered
    /// under the given name, or [`EngineError::InvalidKey`] if the name
    /// contains `/`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowState};
    /// # async fn greet(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register("greet", greet);
    /// # engine.start().await?;
    /// let invocation = engine.invoke("greet").await?;
    /// println!("Started instance {}", invocation.instance_id());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn invoke(
        &self,
        workflow_name: impl Into<String>,
    ) -> Result<Invocation, EngineError> {
        let instance_id = generate_instance_id();
        let workflow_name = workflow_name.into();
        validate_key_component(&workflow_name, "workflow_name")?;
        self.spawn_workflow(&workflow_name, instance_id).await
    }

    /// Resumes a previously failed or incomplete workflow instance.
    ///
    /// Re-runs the workflow with the same instance ID. Completed steps
    /// return their memoised results; only incomplete or failed steps
    /// re-execute.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::NotStarted`] if the engine has not been started,
    /// [`EngineError::WorkflowNotFound`] if no workflow is registered
    /// under the given name, or [`EngineError::InvalidKey`] if `workflow_name`
    /// or `instance_id` contains `/`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowState};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register("wf", wf);
    /// # engine.start().await?;
    /// # let invocation = engine.invoke("wf").await?;
    /// # let instance_id = invocation.instance_id().to_string();
    /// // Resume a failed instance — memoised steps are skipped.
    /// let invocation = engine.resume("wf", &instance_id).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn resume(
        &self,
        workflow_name: impl Into<String>,
        instance_id: impl Into<String>,
    ) -> Result<Invocation, EngineError> {
        let workflow_name = workflow_name.into();
        let instance_id = instance_id.into();
        validate_key_component(&workflow_name, "workflow_name")?;
        validate_key_component(&instance_id, "instance_id")?;
        self.spawn_workflow(&workflow_name, instance_id).await
    }

    /// Delivers a signal payload to a suspended workflow step.
    ///
    /// Writes the payload as a completed step entry, then resumes the
    /// workflow. The resumed workflow replays memoised steps, finds the
    /// now-completed suspend entry, and continues execution.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::NotStarted`] if the engine has not been started,
    /// [`EngineError::WorkflowNotFound`] if no workflow is registered under
    /// the given name, [`EngineError::InvalidKey`] if any component contains
    /// `/`, [`EngineError::SignalRejected`] if the step does not exist or is
    /// not currently suspended, [`EngineError::SignalSuperseded`] if another
    /// caller already claimed the step, or [`EngineError::Storage`] /
    /// [`EngineError::Serialization`] if the write fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowState};
    /// async fn approval(ctx: Context) -> Result<(), EngineError> {
    ///     let approved: bool = ctx.suspend("approval:v1").await?;
    ///     assert!(approved);
    ///     Ok(())
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut engine = Engine::builder().in_memory().build();
    /// engine.register("approval", approval);
    /// engine.start().await?;
    ///
    /// let inv = engine.invoke("approval").await?;
    /// let id = inv.instance_id().to_string();
    /// inv.wait().await;
    ///
    /// let state = engine.signal("approval", &id, "approval:v1", true).await?.wait().await;
    /// assert_eq!(state, WorkflowState::Completed);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn signal<T>(
        &self,
        workflow_name: &str,
        instance_id: &str,
        step_key: &str,
        payload: T,
    ) -> Result<Invocation, EngineError>
    where
        T: Serialize + DeserializeOwned + Send,
    {
        validate_key_component(workflow_name, "workflow_name")?;
        validate_key_component(instance_id, "instance_id")?;
        validate_key_component(step_key, "step_key")?;

        let data: StepData<T> = StepData::Completed {
            result: payload,
            status: None,
        };
        let step_bytes = serialize_step(&data, step_key)?;

        claim_suspended_step(&self.db, workflow_name, instance_id, step_key, &step_bytes)?;

        info!(
            workflow = workflow_name,
            instance = instance_id,
            step = step_key,
            "signal delivered"
        );

        let mut tasks = self.tasks.lock().await;

        if !self.running.load(Ordering::Acquire) {
            return Err(EngineError::NotStarted);
        }

        let workflow = self
            .workflows
            .get(workflow_name)
            .ok_or_else(|| EngineError::WorkflowNotFound(workflow_name.to_string()))?;

        Ok(spawn_workflow_task(
            &mut tasks,
            workflow,
            &self.db,
            workflow_name,
            instance_id,
            &self.timer_serial,
            self.default_retry.clone(),
        ))
    }

    /// Returns the metadata for a specific workflow instance.
    ///
    /// Returns `None` if no instance with the given name and ID exists.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Storage`] or [`EngineError::Serialization`]
    /// if the database read fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, MetadataStatus};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register("wf", wf);
    /// # engine.start().await?;
    /// let inv = engine.invoke("wf").await?;
    /// let id = inv.instance_id().to_string();
    /// inv.wait().await;
    ///
    /// let meta = engine.get_metadata("wf", &id)?.expect("instance exists");
    /// assert_eq!(*meta.status(), MetadataStatus::Completed);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_metadata(
        &self,
        workflow_name: &str,
        instance_id: &str,
    ) -> Result<Option<WorkflowMetadata>, EngineError> {
        metadata::read_metadata(&self.db, workflow_name, instance_id)
    }

    /// Lists all instances of a workflow definition.
    ///
    /// Returns `(instance_id, metadata)` pairs for every instance whose
    /// composite key starts with `"{workflow_name}/"`.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Storage`] or [`EngineError::Serialization`]
    /// if the database read fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register("wf", wf);
    /// # engine.start().await?;
    /// engine.invoke("wf").await?.wait().await;
    /// engine.invoke("wf").await?.wait().await;
    ///
    /// let instances = engine.list_instances("wf")?;
    /// assert_eq!(instances.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn list_instances(
        &self,
        workflow_name: &str,
    ) -> Result<Vec<(String, WorkflowMetadata)>, EngineError> {
        metadata::list_metadata(&self.db, workflow_name)
    }

    /// Stops the engine, preventing new workflow invocations.
    ///
    /// Running workflows continue to completion. To wait for all running
    /// workflows to finish, use [`wait_all`](Engine::wait_all) instead.
    #[expect(clippy::unused_async)]
    pub async fn stop(&self) {
        self.running.store(false, Ordering::Release);
        info!("engine stopped");
    }

    /// Waits for all running workflows to complete and prevents new
    /// invocations.
    ///
    /// Sets the engine to a stopped state, then drains all spawned
    /// workflow tasks. Any call to [`invoke`](Engine::invoke) or
    /// [`resume`](Engine::resume) after `wait_all` is called will return
    /// [`EngineError::NotStarted`].
    ///
    /// Returns immediately if no workflows are running.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowState};
    /// # async fn greet(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register("greet", greet);
    /// # engine.start().await?;
    /// // Fire-and-forget — no need to hold the Invocation handle.
    /// let _ = engine.invoke("greet").await?;
    /// let _ = engine.invoke("greet").await?;
    ///
    /// // All workflows will have completed when this returns.
    /// engine.wait_all().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait_all(&self) {
        self.running.store(false, Ordering::Release);
        info!("waiting for all workflows to complete");
        let mut tasks = self.tasks.lock().await;
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                if e.is_panic() {
                    error!("workflow task panicked: {e}");
                }
            }
        }
        info!("all workflows completed");
    }

    async fn spawn_workflow(
        &self,
        workflow_name: &str,
        instance_id: String,
    ) -> Result<Invocation, EngineError> {
        let mut tasks = self.tasks.lock().await;

        if !self.running.load(Ordering::Acquire) {
            return Err(EngineError::NotStarted);
        }

        let workflow = self
            .workflows
            .get(workflow_name)
            .ok_or_else(|| EngineError::WorkflowNotFound(workflow_name.to_string()))?;

        metadata::write_metadata(
            &self.db,
            workflow_name,
            &instance_id,
            &WorkflowMetadata::new(MetadataStatus::Running),
        )?;

        Ok(spawn_workflow_task(
            &mut tasks,
            workflow,
            &self.db,
            workflow_name,
            &instance_id,
            &self.timer_serial,
            self.default_retry.clone(),
        ))
    }
}

fn now_unix_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_millis(),
    )
    .expect("system clock overflows u64 millis")
}

fn handle_workflow_result(
    result: Result<(), EngineError>,
    db: &Database,
    workflow_name: &str,
    instance_id: &str,
    tx: &watch::Sender<WorkflowState>,
) {
    match result {
        Ok(()) => {
            info!("completed");
            if let Err(e) = metadata::write_metadata(
                db,
                workflow_name,
                instance_id,
                &WorkflowMetadata::new(MetadataStatus::Completed),
            ) {
                error!(error = %e, "failed to write completion metadata");
            }
            let _ = tx.send(WorkflowState::Completed);
        }
        Err(EngineError::Suspended { ref key }) => {
            let status = tx.borrow().message().unwrap_or(key).to_string();
            info!(key, status, "suspended");
            if let Err(e) = metadata::write_metadata(
                db,
                workflow_name,
                instance_id,
                &WorkflowMetadata::new(MetadataStatus::Suspended(status.clone())),
            ) {
                error!(error = %e, "failed to write suspension metadata");
            }
            let _ = tx.send(WorkflowState::Suspended(status));
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

fn spawn_workflow_task(
    tasks: &mut JoinSet<()>,
    workflow: &WorkflowFn,
    db: &Arc<Database>,
    workflow_name: &str,
    instance_id: &str,
    timer_serial: &Arc<AtomicU64>,
    default_retry: Option<RetryPolicy>,
) -> Invocation {
    let workflow = Arc::clone(workflow);
    let (tx, rx) = watch::channel(WorkflowState::Started);
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
    let span = info_span!("workflow", name = %wf_name, instance = %inst_id);

    tasks.spawn(
        async move {
            info!("executing");
            let result = workflow(ctx).await;
            handle_workflow_result(result, &db, &wf_name, &inst_id, &tx);
        }
        .instrument(span),
    );

    Invocation {
        instance_id: instance_id.to_string(),
        status: rx,
    }
}

fn claim_suspended_step(
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
                if envelope.type_tag.is_some() {
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
                if !matches!(meta.status(), MetadataStatus::Suspended(_)) {
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

async fn poll_timers(
    db: &Arc<Database>,
    workflows: &HashMap<String, WorkflowFn>,
    timer_serial: &Arc<AtomicU64>,
    tasks: &Arc<tokio::sync::Mutex<JoinSet<()>>>,
    default_retry: Option<&RetryPolicy>,
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
            .is_some_and(|m| matches!(m.status(), MetadataStatus::Suspended(_)));
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

        match signal_timer(db, workflows, timer_serial, tasks, &entry, default_retry).await {
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

    let mut tasks = tasks.lock().await;
    spawn_workflow_task(
        &mut tasks,
        workflow,
        db,
        &entry.workflow_name,
        &entry.instance_id,
        timer_serial,
        default_retry.cloned(),
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::StepError;
    use crate::context::SuspendBuilder;

    fn test_engine() -> Engine {
        Engine::builder().in_memory().build()
    }

    #[tokio::test]
    async fn simple_workflow_completes() {
        async fn add(ctx: Context) -> Result<(), EngineError> {
            let a: i32 = ctx.step("a").run(async || Ok(1)).await?;
            let b: i32 = ctx.step("b").run(async || Ok(2)).await?;
            assert_eq!(a + b, 3);
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("add", add);
        engine.start().await.unwrap();

        let state = engine.invoke("add").await.unwrap().wait().await;
        assert_eq!(state, WorkflowState::Completed);
    }

    #[tokio::test]
    async fn memoisation_on_resume() {
        let counter = Arc::new(AtomicU32::new(0));
        let attempts = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("memo", move |ctx: Context| {
            let c = Arc::clone(&c);
            let a = Arc::clone(&a);
            async move {
                let c2 = Arc::clone(&c);
                let _: String = ctx
                    .step("s1")
                    .run(async move || {
                        c2.fetch_add(1, Ordering::Relaxed);
                        Ok("hello".to_string())
                    })
                    .await?;
                let _: String = ctx
                    .step("s2")
                    .run(async move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        if a.fetch_add(1, Ordering::Relaxed) == 0 {
                            return Err(StepError::retryable("transient"));
                        }
                        Ok("world".to_string())
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        // First invoke — s1 succeeds, s2 fails.
        let inv = engine.invoke("memo").await.unwrap();
        let instance_id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert!(matches!(state, WorkflowState::Failed(_)));
        assert_eq!(counter.load(Ordering::Relaxed), 2);

        // Resume — s1 is memoised, s2 retries and succeeds.
        counter.store(0, Ordering::Relaxed);
        let state = engine
            .resume("memo", &instance_id)
            .await
            .unwrap()
            .wait()
            .await;
        assert_eq!(state, WorkflowState::Completed);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn step_error_produces_failed_state() {
        async fn failing(ctx: Context) -> Result<(), EngineError> {
            let _: String = ctx
                .step("fail")
                .run(async || Err(StepError::permanent("boom")))
                .await?;
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("fail", failing);
        engine.start().await.unwrap();

        let state = engine.invoke("fail").await.unwrap().wait().await;
        assert!(matches!(state, WorkflowState::Failed(_)));
    }

    #[tokio::test]
    async fn different_instances_have_separate_caches() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);

        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let c = Arc::clone(&c);
            async move {
                let _: i32 = ctx
                    .step("x")
                    .run(async move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        Ok(1)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        engine.invoke("wf").await.unwrap().wait().await;
        engine.invoke("wf").await.unwrap().wait().await;

        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn invoke_before_start_fails() {
        async fn noop(_ctx: Context) -> Result<(), EngineError> {
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("noop", noop);

        let result = engine.invoke("noop").await;
        assert!(matches!(result, Err(EngineError::NotStarted)));
    }

    #[tokio::test]
    async fn invoke_unknown_workflow_fails() {
        let mut engine = test_engine();
        engine.start().await.unwrap();

        let result = engine.invoke("nonexistent").await;
        assert!(matches!(result, Err(EngineError::WorkflowNotFound(_))));
    }

    #[tokio::test]
    async fn wait_all_waits_for_completion() {
        let counter = Arc::new(AtomicU32::new(0));

        let c = Arc::clone(&counter);
        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let c = Arc::clone(&c);
            async move {
                let _: i32 = ctx
                    .step("x")
                    .run(async move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        Ok(1)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        drop(engine.invoke("wf").await.unwrap());
        drop(engine.invoke("wf").await.unwrap());
        drop(engine.invoke("wf").await.unwrap());

        engine.wait_all().await;
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn invoke_after_wait_all_fails() {
        async fn noop(_ctx: Context) -> Result<(), EngineError> {
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("noop", noop);
        engine.start().await.unwrap();

        engine.wait_all().await;

        let result = engine.invoke("noop").await;
        assert!(matches!(result, Err(EngineError::NotStarted)));
    }

    #[tokio::test]
    async fn wait_all_with_no_active_tasks() {
        let mut engine = test_engine();
        engine.start().await.unwrap();

        engine.wait_all().await;
    }

    #[tokio::test]
    async fn status_persisted_and_restored_on_resume() {
        let step_counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&step_counter);
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("status-wf", move |ctx: Context| {
            let c = Arc::clone(&c);
            let a = Arc::clone(&a);
            async move {
                ctx.set_status("step-one");
                let c2 = Arc::clone(&c);
                let _: String = ctx
                    .step("s1")
                    .run(async move || {
                        c2.fetch_add(1, Ordering::Relaxed);
                        Ok("one".to_string())
                    })
                    .await?;

                ctx.set_status("step-two");
                let _: String = ctx
                    .step("s2")
                    .run(async move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        if a.fetch_add(1, Ordering::Relaxed) == 0 {
                            return Err(StepError::retryable("transient"));
                        }
                        Ok("two".to_string())
                    })
                    .await?;

                Ok(())
            }
        });
        engine.start().await.unwrap();

        // First run — s1 succeeds, s2 fails.
        let inv = engine.invoke("status-wf").await.unwrap();
        let instance_id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert!(matches!(state, WorkflowState::Failed(_)));
        assert_eq!(step_counter.load(Ordering::Relaxed), 2);

        // Resume — s1 is memoised, s2 retries and succeeds.
        step_counter.store(0, Ordering::Relaxed);
        let state = engine
            .resume("status-wf", &instance_id)
            .await
            .unwrap()
            .wait()
            .await;
        assert_eq!(state, WorkflowState::Completed);
        // Only s2 re-executed; s1 was served from cache (with its persisted status).
        assert_eq!(step_counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn subscribers_not_notified_during_replay() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("silent-replay", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                ctx.set_status("phase-1");
                let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;

                ctx.set_status("phase-2");
                let _: i32 = ctx
                    .step("s2")
                    .run(async move || {
                        if a.fetch_add(1, Ordering::Relaxed) == 0 {
                            return Err(StepError::retryable("fail first time"));
                        }
                        Ok(2)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        // First invoke — s1 ok, s2 fails.
        let inv = engine.invoke("silent-replay").await.unwrap();
        let instance_id = inv.instance_id().to_string();
        inv.wait().await;

        // Resume and watch for notifications.
        let mut inv = engine.resume("silent-replay", &instance_id).await.unwrap();
        let rx = inv.status();

        // Mark the current value as seen so changed() only fires on new updates.
        rx.borrow_and_update();

        let mut notifications = Vec::new();
        loop {
            if rx.changed().await.is_err() {
                break;
            }
            let state = rx.borrow_and_update().clone();
            let terminal = state.is_terminal();
            notifications.push(state);
            if terminal {
                break;
            }
        }

        // Replayed statuses ("phase-1", "phase-2") should NOT appear as
        // notifications. We expect only the live "phase-2" set_status (after
        // replay ends at the s2 cache miss) and the terminal Completed.
        assert!(
            !notifications
                .iter()
                .any(|s| s == &WorkflowState::InProgress("phase-1".into())),
            "replayed status 'phase-1' should not have notified subscribers, got: {notifications:?}"
        );
    }

    #[tokio::test]
    async fn metadata_completed_on_success() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
        assert_eq!(*meta.status(), MetadataStatus::Completed);
        assert!(meta.completed_at().is_some());
    }

    #[tokio::test]
    async fn metadata_failed_on_error() {
        async fn failing(ctx: Context) -> Result<(), EngineError> {
            let _: String = ctx
                .step("fail")
                .run(async || Err(StepError::permanent("boom")))
                .await?;
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("fail", failing);
        engine.start().await.unwrap();

        let inv = engine.invoke("fail").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        let meta = engine.get_metadata("fail", &id).unwrap().unwrap();
        assert!(matches!(meta.status(), MetadataStatus::Failed(msg) if msg.contains("boom")),);
        assert!(meta.completed_at().is_some());
    }

    #[tokio::test]
    async fn metadata_updated_after_resume() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("retry-wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .run(async move || {
                        if a.fetch_add(1, Ordering::Relaxed) == 0 {
                            return Err(StepError::retryable("transient"));
                        }
                        Ok(1)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let inv = engine.invoke("retry-wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        let meta = engine.get_metadata("retry-wf", &id).unwrap().unwrap();
        assert!(matches!(meta.status(), MetadataStatus::Failed(_)));

        engine.resume("retry-wf", &id).await.unwrap().wait().await;

        let meta = engine.get_metadata("retry-wf", &id).unwrap().unwrap();
        assert_eq!(*meta.status(), MetadataStatus::Completed);
    }

    #[tokio::test]
    async fn list_instances_returns_all() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        engine.invoke("wf").await.unwrap().wait().await;
        engine.invoke("wf").await.unwrap().wait().await;
        engine.invoke("wf").await.unwrap().wait().await;

        let instances = engine.list_instances("wf").unwrap();
        assert_eq!(instances.len(), 3);
    }

    #[tokio::test]
    async fn list_instances_filters_by_workflow_name() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("alpha", wf);
        engine.register("beta", wf);
        engine.start().await.unwrap();

        engine.invoke("alpha").await.unwrap().wait().await;
        engine.invoke("alpha").await.unwrap().wait().await;
        engine.invoke("beta").await.unwrap().wait().await;

        let alpha_instances = engine.list_instances("alpha").unwrap();
        assert_eq!(alpha_instances.len(), 2);

        let beta_instances = engine.list_instances("beta").unwrap();
        assert_eq!(beta_instances.len(), 1);

        let none_instances = engine.list_instances("nonexistent").unwrap();
        assert!(none_instances.is_empty());
    }

    #[tokio::test]
    async fn get_metadata_returns_none_for_unknown() {
        let mut engine = test_engine();
        engine.start().await.unwrap();

        let meta = engine.get_metadata("wf", "no-such-id").unwrap();
        assert!(meta.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn metadata_consistent_after_wait_multi_thread() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
        assert_eq!(*meta.status(), MetadataStatus::Completed);
    }

    #[tokio::test]
    async fn metadata_correct_after_wait_all() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let id1 = engine.invoke("wf").await.unwrap().instance_id().to_string();
        let id2 = engine.invoke("wf").await.unwrap().instance_id().to_string();

        engine.wait_all().await;

        let m1 = engine.get_metadata("wf", &id1).unwrap().unwrap();
        let m2 = engine.get_metadata("wf", &id2).unwrap().unwrap();
        assert_eq!(*m1.status(), MetadataStatus::Completed);
        assert_eq!(*m2.status(), MetadataStatus::Completed);
    }

    #[tokio::test]
    async fn list_instances_on_fresh_engine() {
        let engine = test_engine();
        let instances = engine.list_instances("anything").unwrap();
        assert!(instances.is_empty());
    }

    #[tokio::test]
    async fn suspend_then_signal_completes() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: i32 = ctx.step("s1").run(async || Ok(1)).await?;
            let payload: String = ctx.suspend("wait:v1").await?;
            assert_eq!(payload, "hello");
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert_eq!(state, WorkflowState::Suspended("wait:v1".into()));

        let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
        assert!(matches!(meta.status(), MetadataStatus::Suspended(msg) if msg == "wait:v1"),);

        let state = engine
            .signal("wf", &id, "wait:v1", "hello".to_string())
            .await
            .unwrap()
            .wait()
            .await;
        assert_eq!(state, WorkflowState::Completed);

        let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
        assert_eq!(*meta.status(), MetadataStatus::Completed);
    }

    #[tokio::test]
    async fn suspend_with_custom_status() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: bool = ctx
                .suspend("approval:v1")
                .status("Waiting for manager approval")
                .await?;
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert_eq!(
            state,
            WorkflowState::Suspended("Waiting for manager approval".into())
        );

        let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
        assert!(matches!(
            meta.status(),
            MetadataStatus::Suspended(msg) if msg == "Waiting for manager approval"
        ));

        let state = engine
            .signal("wf", &id, "approval:v1", true)
            .await
            .unwrap()
            .wait()
            .await;
        assert_eq!(state, WorkflowState::Completed);
    }

    #[tokio::test]
    async fn memoised_steps_preserved_across_suspend() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);

        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let c = Arc::clone(&c);
            async move {
                let c2 = Arc::clone(&c);
                let _: i32 = ctx
                    .step("s1")
                    .run(async move || {
                        c2.fetch_add(1, Ordering::Relaxed);
                        Ok(42)
                    })
                    .await?;
                let _: String = ctx.suspend("gate:v1").await?;
                let _: i32 = ctx
                    .step("s2")
                    .run(async move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        Ok(99)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        counter.store(0, Ordering::Relaxed);
        let state = engine
            .signal("wf", &id, "gate:v1", "go".to_string())
            .await
            .unwrap()
            .wait()
            .await;
        assert_eq!(state, WorkflowState::Completed);
        // s1 memoised (0 executions), s2 runs (1 execution)
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn resume_without_signal_stays_suspended() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: String = ctx.suspend("wait:v1").await?;
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert_eq!(state, WorkflowState::Suspended("wait:v1".into()));

        // Resume without signal — should still be suspended.
        let state = engine.resume("wf", &id).await.unwrap().wait().await;
        assert_eq!(state, WorkflowState::Suspended("wait:v1".into()));
    }

    #[tokio::test]
    async fn step_rejects_suspended_entry() {
        let use_step = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let use_step2 = Arc::clone(&use_step);

        let wf = move |ctx: Context| {
            let use_step = Arc::clone(&use_step2);
            async move {
                if use_step.load(Ordering::Acquire) {
                    let _: String = ctx.step("action:v1").run(async || Ok("x".into())).await?;
                } else {
                    let _: String = ctx.suspend("action:v1").await?;
                }
                Ok(())
            }
        };

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        // V1: workflow suspends at "action:v1".
        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert_eq!(state, WorkflowState::Suspended("action:v1".into()));

        // V2: same key is now a regular step.
        use_step.store(true, Ordering::Release);
        let state = engine.resume("wf", &id).await.unwrap().wait().await;
        assert!(
            matches!(state, WorkflowState::Failed(ref msg) if msg.contains("suspended entry")),
            "expected SuspendedStepConflict, got: {state:?}"
        );
    }

    #[tokio::test]
    async fn multiple_suspend_points() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let a: i32 = ctx.suspend("first:v1").await?;
            let b: i32 = ctx.suspend("second:v1").await?;
            assert_eq!(a + b, 3);
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert_eq!(state, WorkflowState::Suspended("first:v1".into()));

        // Signal first suspend point.
        let state = engine
            .signal("wf", &id, "first:v1", 1i32)
            .await
            .unwrap()
            .wait()
            .await;
        assert_eq!(state, WorkflowState::Suspended("second:v1".into()));

        // Signal second suspend point.
        let state = engine
            .signal("wf", &id, "second:v1", 2i32)
            .await
            .unwrap()
            .wait()
            .await;
        assert_eq!(state, WorkflowState::Completed);
    }

    #[tokio::test]
    async fn suspend_after_failed_step_on_resume() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .run(async move || {
                        if a.fetch_add(1, Ordering::Relaxed) == 0 {
                            return Err(StepError::retryable("transient"));
                        }
                        Ok(1)
                    })
                    .await?;
                let _: String = ctx.suspend("gate:v1").await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        // First invoke — s1 fails.
        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert!(matches!(state, WorkflowState::Failed(_)));

        // Resume — s1 succeeds, hits suspend.
        let state = engine.resume("wf", &id).await.unwrap().wait().await;
        assert_eq!(state, WorkflowState::Suspended("gate:v1".into()));

        // Signal — completes.
        let state = engine
            .signal("wf", &id, "gate:v1", "done".to_string())
            .await
            .unwrap()
            .wait()
            .await;
        assert_eq!(state, WorkflowState::Completed);
    }

    #[tokio::test]
    async fn step_without_status_stores_none() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);

        let mut engine = test_engine();
        engine.register("no-status", move |ctx: Context| {
            let c = Arc::clone(&c);
            async move {
                // No set_status call — status should be None in the record.
                let _: i32 = ctx
                    .step("s1")
                    .run(async move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        Ok(42)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let inv = engine.invoke("no-status").await.unwrap();
        let instance_id = inv.instance_id().to_string();
        inv.wait().await;

        // Resume — s1 is memoised, execution counter stays at 1.
        counter.store(0, Ordering::Relaxed);
        let state = engine
            .resume("no-status", &instance_id)
            .await
            .unwrap()
            .wait()
            .await;
        assert_eq!(state, WorkflowState::Completed);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn timer_fires_and_completes_workflow() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);

        let mut engine = test_engine();
        engine.register("timer-wf", move |ctx: Context| {
            let c = Arc::clone(&c);
            async move {
                let c2 = Arc::clone(&c);
                let _: i32 = ctx
                    .step("s1")
                    .run(async move || {
                        c2.fetch_add(1, Ordering::Relaxed);
                        Ok(1)
                    })
                    .await?;
                ctx.timer("wait:v1", Duration::ZERO)?;
                let _: i32 = ctx
                    .step("s2")
                    .run(async move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        Ok(2)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let inv = engine.invoke("timer-wf").await.unwrap();
        let id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert!(matches!(state, WorkflowState::Suspended(_)));
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        // Wait for the poller to fire the timer.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let meta = engine.get_metadata("timer-wf", &id).unwrap().unwrap();
            if meta.status().is_terminal() {
                assert_eq!(*meta.status(), MetadataStatus::Completed);
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timer did not fire within 5 seconds"
            );
        }
        // s1 ran on first invoke, s2 ran after timer — s1 memoised on resume.
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn timer_memoised_on_resume() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);

        let mut engine = test_engine();
        engine.register("timer-memo", move |ctx: Context| {
            let c = Arc::clone(&c);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .run(async move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        Ok(1)
                    })
                    .await?;
                ctx.timer("delay:v1", Duration::ZERO)?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let inv = engine.invoke("timer-memo").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        // Wait for timer to fire.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let meta = engine.get_metadata("timer-memo", &id).unwrap().unwrap();
            if meta.status().is_terminal() {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "timer did not fire");
        }

        // Resume — both s1 and timer are memoised.
        counter.store(0, Ordering::Relaxed);
        let state = engine.resume("timer-memo", &id).await.unwrap().wait().await;
        assert_eq!(state, WorkflowState::Completed);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn timer_skipped_when_workflow_not_suspended() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("timer-fail", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .run(async move || {
                        if a.fetch_add(1, Ordering::Relaxed) == 0 {
                            return Err(StepError::retryable("transient"));
                        }
                        Ok(1)
                    })
                    .await?;
                ctx.timer("delay:v1", Duration::ZERO)?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        // First invoke — s1 fails before reaching timer.
        let inv = engine.invoke("timer-fail").await.unwrap();
        let id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert!(matches!(state, WorkflowState::Failed(_)));

        // Resume — s1 succeeds, hits timer.
        let state = engine.resume("timer-fail", &id).await.unwrap().wait().await;
        assert!(matches!(state, WorkflowState::Suspended(_)));

        // Wait for timer to fire and complete.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let meta = engine.get_metadata("timer-fail", &id).unwrap().unwrap();
            if meta.status().is_terminal() {
                assert_eq!(*meta.status(), MetadataStatus::Completed);
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "timer did not fire");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn timer_with_steps_before_and_after() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);

        let mut engine = test_engine();
        engine.register("multi-timer", move |ctx: Context| {
            let c = Arc::clone(&c);
            async move {
                let c2 = Arc::clone(&c);
                let _: i32 = ctx
                    .step("before")
                    .run(async move || {
                        c2.fetch_add(1, Ordering::Relaxed);
                        Ok(1)
                    })
                    .await?;
                ctx.timer("t1:v1", Duration::ZERO)?;
                let c3 = Arc::clone(&c);
                let _: i32 = ctx
                    .step("between")
                    .run(async move || {
                        c3.fetch_add(1, Ordering::Relaxed);
                        Ok(2)
                    })
                    .await?;
                ctx.timer("t2:v1", Duration::ZERO)?;
                let _: i32 = ctx
                    .step("after")
                    .run(async move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        Ok(3)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let inv = engine.invoke("multi-timer").await.unwrap();
        let id = inv.instance_id().to_string();
        // First timer suspends.
        inv.wait().await;
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        // Wait for first timer and then second timer to fire.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let meta = engine.get_metadata("multi-timer", &id).unwrap().unwrap();
            if meta.status().is_terminal() {
                assert_eq!(*meta.status(), MetadataStatus::Completed);
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timers did not complete"
            );
        }
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn step_with_timeout_completes() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let v: i32 = ctx
                .step("s1")
                .timeout(Duration::from_secs(5))
                .run(async || Ok(42))
                .await?;
            assert_eq!(v, 42);
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let state = engine.invoke("wf").await.unwrap().wait().await;
        assert_eq!(state, WorkflowState::Completed);
    }

    #[tokio::test(start_paused = true)]
    async fn step_timeout_exceeds_deadline() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: i32 = ctx
                .step("slow")
                .timeout(Duration::from_millis(100))
                .run(async || {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    Ok(1)
                })
                .await?;
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let state = engine.invoke("wf").await.unwrap().wait().await;
        assert!(matches!(state, WorkflowState::Failed(msg) if msg.contains("timed out")));
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_step_not_persisted() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("flaky")
                    .timeout(Duration::from_millis(100))
                    .run(async move || {
                        let n = a.fetch_add(1, Ordering::Relaxed);
                        if n == 0 {
                            tokio::time::sleep(Duration::from_secs(10)).await;
                        }
                        Ok(42)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert!(matches!(state, WorkflowState::Failed(_)));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);

        // Resume — step re-executes (not cached) and succeeds this time.
        let state = engine.resume("wf", &id).await.unwrap().wait().await;
        assert_eq!(state, WorkflowState::Completed);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_skipped_on_cache_hit() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);

        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let c = Arc::clone(&c);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .timeout(Duration::from_nanos(1))
                    .run(async move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        Ok(1)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        // Resume with impossibly short timeout — cache hit returns instantly.
        counter.store(0, Ordering::Relaxed);
        let state = engine.resume("wf", &id).await.unwrap().wait().await;
        assert_eq!(state, WorkflowState::Completed);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_with_borrowing_closure() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let local_data = String::from("borrowed");
            let local_data_clone = local_data.clone();
            let v: String = ctx
                .step("borrow")
                .timeout(Duration::from_secs(5))
                .run(async move || Ok(local_data_clone.clone()))
                .await?;
            assert_eq!(v, "borrowed");
            assert_eq!(local_data, "borrowed");
            Ok(())
        }

        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let state = engine.invoke("wf").await.unwrap().wait().await;
        assert_eq!(state, WorkflowState::Completed);
    }

    // --- Key validation (S1) tests ---

    #[test]
    #[should_panic(expected = "workflow name must not contain '/'")]
    fn register_rejects_slash_in_name() {
        let mut engine = test_engine();
        engine.register("bad/name", |_ctx: Context| async { Ok(()) });
    }

    #[tokio::test]
    async fn invoke_rejects_slash_in_name() {
        async fn wf(_ctx: Context) -> Result<(), EngineError> {
            Ok(())
        }
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let Err(err) = engine.invoke("bad/name").await else {
            panic!("expected InvalidKey error");
        };
        assert!(matches!(
            err,
            EngineError::InvalidKey {
                label: "workflow_name",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn resume_rejects_slash_in_workflow_name() {
        async fn wf(_ctx: Context) -> Result<(), EngineError> {
            Ok(())
        }
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let Err(err) = engine.resume("bad/name", "id-1").await else {
            panic!("expected InvalidKey error");
        };
        assert!(matches!(
            err,
            EngineError::InvalidKey {
                label: "workflow_name",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn resume_rejects_slash_in_instance_id() {
        async fn wf(_ctx: Context) -> Result<(), EngineError> {
            Ok(())
        }
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let Err(err) = engine.resume("wf", "bad/id").await else {
            panic!("expected InvalidKey error");
        };
        assert!(matches!(
            err,
            EngineError::InvalidKey {
                label: "instance_id",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn signal_rejects_slash_in_any_component() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: bool = ctx.suspend("wait:v1").await?;
            Ok(())
        }
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        let Err(err) = engine.signal("bad/name", &id, "wait:v1", true).await else {
            panic!("expected InvalidKey error");
        };
        assert!(matches!(
            err,
            EngineError::InvalidKey {
                label: "workflow_name",
                ..
            }
        ));

        let Err(err) = engine.signal("wf", "bad/id", "wait:v1", true).await else {
            panic!("expected InvalidKey error");
        };
        assert!(matches!(
            err,
            EngineError::InvalidKey {
                label: "instance_id",
                ..
            }
        ));

        let Err(err) = engine.signal("wf", &id, "bad/key", true).await else {
            panic!("expected InvalidKey error");
        };
        assert!(matches!(
            err,
            EngineError::InvalidKey {
                label: "step_key",
                ..
            }
        ));
    }

    #[test]
    #[should_panic(expected = "step key must not contain '/'")]
    fn step_rejects_slash_in_key() {
        let db = Arc::new(
            Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .unwrap(),
        );
        let (tx, _rx) = watch::channel(WorkflowState::Started);
        let ctx = Context::new(
            "wf".into(),
            "id".into(),
            db,
            tx,
            Arc::new(AtomicU64::new(0)),
            None,
        );
        let _ = ctx.step("bad/key");
    }

    #[test]
    #[should_panic(expected = "suspend key must not contain '/'")]
    fn suspend_rejects_slash_in_key() {
        let db = Arc::new(
            Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .unwrap(),
        );
        let (tx, _rx) = watch::channel(WorkflowState::Started);
        let ctx = Context::new(
            "wf".into(),
            "id".into(),
            db,
            tx,
            Arc::new(AtomicU64::new(0)),
            None,
        );
        let _: SuspendBuilder<'_, bool> = ctx.suspend("bad/key");
    }

    #[tokio::test]
    async fn timer_rejects_slash_in_key() {
        let mut engine = test_engine();
        engine.register("wf", |ctx: Context| async move {
            ctx.timer("bad/key", Duration::from_secs(1))?;
            Ok(())
        });
        engine.start().await.unwrap();

        let state = engine.invoke("wf").await.unwrap().wait().await;
        assert!(matches!(state, WorkflowState::Failed(_)));
    }

    #[tokio::test]
    async fn signal_rejects_when_step_does_not_exist() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: bool = ctx.suspend("wait:v1").await?;
            Ok(())
        }
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        let Err(err) = engine.signal("wf", &id, "wrong-key:v1", true).await else {
            panic!("expected SignalRejected error");
        };
        assert!(
            matches!(err, EngineError::SignalRejected { ref key, .. } if key == "wrong-key:v1"),
            "expected SignalRejected, got {err:?}"
        );
    }

    #[tokio::test]
    async fn signal_rejects_already_completed_step() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: bool = ctx.suspend("gate:v1").await?;
            let _: bool = ctx.suspend("gate2:v1").await?;
            Ok(())
        }
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        // First signal succeeds — step is Suspended.
        let inv = engine.signal("wf", &id, "gate:v1", true).await.unwrap();
        inv.wait().await;

        // Second signal to same step fails — it was already claimed.
        let Err(err) = engine.signal("wf", &id, "gate:v1", true).await else {
            panic!("expected SignalSuperseded error");
        };
        assert!(
            matches!(err, EngineError::SignalSuperseded { ref key } if key == "gate:v1"),
            "expected SignalSuperseded for completed step, got {err:?}"
        );
    }

    #[tokio::test]
    async fn signal_rejects_pre_completing_future_step() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: bool = ctx.suspend("first:v1").await?;
            let _: String = ctx.suspend("second:v1").await?;
            Ok(())
        }
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        // Attempt to signal a step the workflow hasn't reached yet.
        let Err(err) = engine
            .signal("wf", &id, "second:v1", "sneaky".to_string())
            .await
        else {
            panic!("expected SignalRejected error");
        };
        assert!(
            matches!(err, EngineError::SignalRejected { ref key, .. } if key == "second:v1"),
            "expected SignalRejected for future step, got {err:?}"
        );
    }

    #[tokio::test]
    async fn signal_type_mismatch_returns_error() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: i32 = ctx.suspend("gate:v1").await?;
            Ok(())
        }
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        // Signal with String instead of the expected i32.
        let inv = engine
            .signal("wf", &id, "gate:v1", "wrong type".to_string())
            .await
            .unwrap();
        let state = inv.wait().await;
        assert!(
            matches!(state, WorkflowState::Failed(ref msg) if msg.contains("type mismatch")),
            "expected TypeMismatch failure, got {state:?}"
        );
    }

    #[tokio::test]
    async fn signal_type_mismatch_caught_for_binary_compatible_types() {
        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: i32 = ctx.suspend("gate:v1").await?;
            Ok(())
        }
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        // Signal with u32 instead of i32 — same binary layout, different type name.
        let inv = engine.signal("wf", &id, "gate:v1", 42_u32).await.unwrap();
        let state = inv.wait().await;
        assert!(
            matches!(state, WorkflowState::Failed(ref msg) if msg.contains("type mismatch")),
            "expected TypeMismatch failure for u32 vs i32, got {state:?}"
        );
    }

    #[tokio::test]
    async fn serialize_deserialize_step_round_trip() {
        use crate::context::{StepData, deserialize_step, serialize_step};

        // Completed round-trip.
        let data: StepData<String> = StepData::Completed {
            result: "hello".to_string(),
            status: Some("done".to_string()),
        };
        let bytes = serialize_step(&data, "test-key").unwrap();
        let recovered: StepData<String> = deserialize_step(&bytes, "test-key").unwrap();
        match recovered {
            StepData::Completed { result, status } => {
                assert_eq!(result, "hello");
                assert_eq!(status.as_deref(), Some("done"));
            }
            StepData::Suspended | StepData::Failed { .. } => panic!("expected Completed"),
        }

        // Suspended round-trip.
        let data = StepData::<u64>::Suspended;
        let bytes = serialize_step(&data, "test-key").unwrap();
        let recovered: StepData<u64> = deserialize_step(&bytes, "test-key").unwrap();
        assert!(matches!(recovered, StepData::Suspended));
    }

    #[tokio::test]
    async fn type_mismatch_error_contains_type_names() {
        use crate::context::{StepData, deserialize_step, serialize_step};

        let data: StepData<String> = StepData::Completed {
            result: "hello".to_string(),
            status: None,
        };
        let bytes = serialize_step(&data, "k").unwrap();

        let err = deserialize_step::<i32>(&bytes, "k").unwrap_err();
        match err {
            EngineError::TypeMismatch {
                key,
                expected,
                found,
            } => {
                assert_eq!(key, "k");
                assert!(
                    expected.contains("i32"),
                    "expected contains i32, got {expected}"
                );
                assert!(
                    found.contains("String"),
                    "found contains String, got {found}"
                );
            }
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn double_signal_same_step_second_superseded() {
        static COUNTER: AtomicU32 = AtomicU32::new(0);

        async fn wf(ctx: Context) -> Result<(), EngineError> {
            let _: bool = ctx.suspend("gate:v1").await?;
            COUNTER.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        COUNTER.store(0, Ordering::Relaxed);
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        let (r1, r2) = tokio::join!(
            engine.signal("wf", &id, "gate:v1", true),
            engine.signal("wf", &id, "gate:v1", true),
        );

        let mut successes = 0u32;
        let mut superseded = 0u32;
        for r in [r1, r2] {
            match r {
                Ok(inv) => {
                    inv.wait().await;
                    successes += 1;
                }
                Err(EngineError::SignalSuperseded { .. }) => superseded += 1,
                Err(e) => panic!("unexpected error: {e:?}"),
            }
        }

        assert_eq!(successes, 1, "exactly one signal should succeed");
        assert_eq!(superseded, 1, "exactly one signal should be superseded");

        engine.wait_all().await;
        assert_eq!(
            COUNTER.load(Ordering::Relaxed),
            1,
            "workflow should run exactly once after signal"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn timer_after_signal_already_claimed() {
        static COUNTER: AtomicU32 = AtomicU32::new(0);

        async fn wf(ctx: Context) -> Result<(), EngineError> {
            ctx.timer("wait:v1", Duration::from_secs(60))?;
            COUNTER.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        COUNTER.store(0, Ordering::Relaxed);
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        // Manually signal the timer step before the timer fires.
        let inv = engine.signal("wf", &id, "wait:v1", ()).await.unwrap();
        inv.wait().await;

        engine.wait_all().await;
        assert_eq!(
            COUNTER.load(Ordering::Relaxed),
            1,
            "workflow should run exactly once"
        );

        let meta = engine.get_metadata("wf", &id).unwrap().unwrap();
        assert_eq!(*meta.status(), MetadataStatus::Completed);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn signal_timer_tracked_by_wait_all() {
        static COUNTER: AtomicU32 = AtomicU32::new(0);

        async fn wf(ctx: Context) -> Result<(), EngineError> {
            ctx.timer("tick:v1", Duration::from_secs(0))?;
            COUNTER.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        COUNTER.store(0, Ordering::Relaxed);
        let mut engine = test_engine();
        engine.register("wf", wf);
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let _id = inv.instance_id().to_string();
        inv.wait().await;

        // Let the timer fire and resume. wait_all should wait for the
        // timer-resumed workflow (C1 fix verification).
        tokio::time::sleep(Duration::from_secs(2)).await;
        engine.wait_all().await;

        assert_eq!(
            COUNTER.load(Ordering::Relaxed),
            1,
            "timer-resumed workflow tracked by wait_all"
        );
    }

    // ── Retry tests ──────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn retryable_error_retries_then_exhausts() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .retry(crate::RetryPolicy::fixed(2, Duration::from_millis(10)))
                    .run(async move || {
                        a.fetch_add(1, Ordering::Relaxed);
                        Err(StepError::retryable("boom"))
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let state = engine.invoke("wf").await.unwrap().wait().await;
        assert!(matches!(state, WorkflowState::Failed(msg) if msg.contains("3 attempts")));
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_error_skips_retry() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .retry(crate::RetryPolicy::fixed(3, Duration::from_millis(10)))
                    .run(async move || {
                        a.fetch_add(1, Ordering::Relaxed);
                        Err(StepError::permanent("fatal"))
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let state = engine.invoke("wf").await.unwrap().wait().await;
        assert!(matches!(state, WorkflowState::Failed(msg) if msg.contains("fatal")));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn step_succeeds_on_second_attempt() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let v: i32 = ctx
                    .step("s1")
                    .retry(crate::RetryPolicy::fixed(3, Duration::from_millis(10)))
                    .run(async move || {
                        if a.fetch_add(1, Ordering::Relaxed) == 0 {
                            return Err(StepError::retryable("transient"));
                        }
                        Ok(42)
                    })
                    .await?;
                assert_eq!(v, 42);
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let state = engine.invoke("wf").await.unwrap().wait().await;
        assert_eq!(state, WorkflowState::Completed);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn exponential_backoff_delays() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .retry(crate::RetryPolicy::exponential(3, Duration::from_secs(1)))
                    .run(async move || {
                        a.fetch_add(1, Ordering::Relaxed);
                        Err(StepError::retryable("fail"))
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let start = tokio::time::Instant::now();
        engine.invoke("wf").await.unwrap().wait().await;
        let elapsed = start.elapsed();

        assert_eq!(attempts.load(Ordering::Relaxed), 4);
        // 1s + 2s + 4s = 7s total backoff
        assert!(elapsed >= Duration::from_secs(7));
    }

    #[tokio::test(start_paused = true)]
    async fn engine_default_retry_applies() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = Engine::builder()
            .in_memory()
            .default_retry(crate::RetryPolicy::fixed(2, Duration::from_millis(10)))
            .build();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .run(async move || {
                        a.fetch_add(1, Ordering::Relaxed);
                        Err(StepError::retryable("boom"))
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        engine.invoke("wf").await.unwrap().wait().await;
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn per_step_retry_overrides_default() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = Engine::builder()
            .in_memory()
            .default_retry(crate::RetryPolicy::fixed(5, Duration::from_millis(10)))
            .build();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .retry(crate::RetryPolicy::fixed(1, Duration::from_millis(10)))
                    .run(async move || {
                        a.fetch_add(1, Ordering::Relaxed);
                        Err(StepError::retryable("boom"))
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        engine.invoke("wf").await.unwrap().wait().await;
        // Per-step policy: 1 retry = 2 total attempts (not 6 from the default).
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn no_retry_overrides_default() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = Engine::builder()
            .in_memory()
            .default_retry(crate::RetryPolicy::fixed(3, Duration::from_millis(10)))
            .build();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .no_retry()
                    .run(async move || {
                        a.fetch_add(1, Ordering::Relaxed);
                        Err(StepError::retryable("boom"))
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        engine.invoke("wf").await.unwrap().wait().await;
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn dead_letter_persisted_and_resume_re_executes() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .retry(crate::RetryPolicy::fixed(1, Duration::from_millis(10)))
                    .run(async move || {
                        let n = a.fetch_add(1, Ordering::Relaxed);
                        if n < 4 {
                            return Err(StepError::retryable("not yet"));
                        }
                        Ok(100)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        // First invoke: 2 attempts (1 + 1 retry), both fail → RetriesExhausted.
        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        let state = inv.wait().await;
        assert!(matches!(state, WorkflowState::Failed(_)));
        assert_eq!(attempts.load(Ordering::Relaxed), 2);

        // Resume: fresh retry budget, 2 more attempts, both fail again.
        let state = engine.resume("wf", &id).await.unwrap().wait().await;
        assert!(matches!(state, WorkflowState::Failed(_)));
        assert_eq!(attempts.load(Ordering::Relaxed), 4);

        // Resume again: first attempt succeeds (n=4 >= 4).
        let state = engine.resume("wf", &id).await.unwrap().wait().await;
        assert_eq!(state, WorkflowState::Completed);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_applies_per_attempt() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = test_engine();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .timeout(Duration::from_millis(50))
                    .retry(crate::RetryPolicy::fixed(2, Duration::from_millis(10)))
                    .run(async move || {
                        let n = a.fetch_add(1, Ordering::Relaxed);
                        if n == 0 {
                            tokio::time::sleep(Duration::from_secs(60)).await;
                        }
                        Ok(1)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let state = engine.invoke("wf").await.unwrap().wait().await;
        // First attempt times out (StepTimeout propagates immediately,
        // bypassing retry). Timeout is not retried.
        assert!(matches!(state, WorkflowState::Failed(msg) if msg.contains("timed out")));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn memoised_step_skips_retry() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);

        let mut engine = Engine::builder()
            .in_memory()
            .default_retry(crate::RetryPolicy::fixed(3, Duration::from_millis(1)))
            .build();
        engine.register("wf", move |ctx: Context| {
            let a = Arc::clone(&a);
            async move {
                let _: i32 = ctx
                    .step("s1")
                    .run(async move || {
                        a.fetch_add(1, Ordering::Relaxed);
                        Ok(1)
                    })
                    .await?;
                Ok(())
            }
        });
        engine.start().await.unwrap();

        let inv = engine.invoke("wf").await.unwrap();
        let id = inv.instance_id().to_string();
        inv.wait().await;

        // Resume — s1 is memoised, closure should NOT run again.
        engine.resume("wf", &id).await.unwrap().wait().await;
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_without_policy_behaves_like_step_failed() {
        let mut engine = test_engine();
        engine.register("wf", |ctx: Context| async move {
            let _: i32 = ctx
                .step("s1")
                .run(async || Err(StepError::retryable("boom")))
                .await?;
            Ok(())
        });
        engine.start().await.unwrap();

        let state = engine.invoke("wf").await.unwrap().wait().await;
        // No retry policy → StepFailed (not RetriesExhausted).
        assert!(
            matches!(state, WorkflowState::Failed(msg) if msg.contains("boom") && !msg.contains("attempts"))
        );
    }
}
