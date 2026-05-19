use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use redb::Database;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::Instrument as _;
use tracing::{error, info, info_span, warn};

use super::builder::{EngineBuilder, NoStore};
use super::execution::{
    claim_suspended_step, poll_timers, spawn_workflow_task, validate_key_component,
};
use super::invocation::{Invocation, InvocationBuilder};
use super::retention::cleanup_expired;
use super::{Senders, WorkflowFn, WorkflowState};
use crate::context::{STEPS, StepData, SuspendPoint, WorkflowDef, serialize_step};
use crate::error::{EngineError, StateError, SubscribeError};
use crate::metadata::{self, MetadataStatus, WORKFLOW_META, WorkflowMetadata};
use crate::retry::RetryPolicy;
use crate::stream::StatusStream;

/// The durable execution engine.
///
/// Workflows are registered via [`WorkflowDef`] constants, then invoked
/// by reference. Each invocation auto-generates a unique instance ID and
/// returns an [`Invocation`] handle for observing the workflow's
/// [`WorkflowState`] and reading typed output.
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
/// use memable::{Engine, Context, EngineError, WorkflowDef};
///
/// const GREET: WorkflowDef = WorkflowDef::new("greet");
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
/// engine.register(&GREET, greet);
/// engine.start().await?;
///
/// engine.invoke(&GREET).await?.wait().await.unwrap_completed();
/// # Ok(())
/// # }
/// ```
pub struct Engine {
    pub(super) db: Arc<Database>,
    pub(super) workflows: HashMap<String, WorkflowFn>,
    pub(super) running: Arc<AtomicBool>,
    pub(super) tasks: Arc<tokio::sync::Mutex<JoinSet<()>>>,
    pub(super) timer_serial: Arc<AtomicU64>,
    pub(super) default_retry: Option<RetryPolicy>,
    pub(super) resume_on_start: bool,
    pub(super) shutdown_timeout: Duration,
    pub(super) retention: Option<Duration>,
    pub(super) workflow_retentions: HashMap<String, Duration>,
    pub(super) senders: Senders,
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
    pub fn builder() -> EngineBuilder<NoStore> {
        EngineBuilder {
            store: NoStore,
            default_retry: None,
            resume_on_start: true,
            shutdown_timeout: None,
            retention: None,
        }
    }

    /// Registers a workflow definition.
    ///
    /// The workflow function signature is determined by the [`WorkflowDef`]
    /// type parameters. Functions with no input take `(Context)`, while
    /// functions with input take `(Context, I)`. Both return
    /// `Result<O, EngineError>`.
    ///
    /// # Panics
    ///
    /// Panics if the engine has already been started.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::{Engine, Context, EngineError, WorkflowDef};
    ///
    /// const MY_WF: WorkflowDef = WorkflowDef::new("my-workflow");
    ///
    /// async fn my_workflow(ctx: Context) -> Result<(), EngineError> {
    ///     Ok(())
    /// }
    ///
    /// let mut engine = Engine::builder().in_memory().build();
    /// engine.register(&MY_WF, my_workflow);
    /// ```
    pub fn register<I, O, M, W>(
        &mut self,
        def: &crate::context::WorkflowDef<I, O>,
        workflow: W,
    ) -> Registration<'_>
    where
        W: super::workflow::IntoWorkflow<I, O, M>,
    {
        assert!(
            !self.running.load(Ordering::Acquire),
            "cannot register workflows after the engine has started"
        );
        let name = def.name().to_string();
        info!(%name, "registered workflow");
        self.workflows
            .insert(name.clone(), workflow.into_workflow_fn());
        Registration { engine: self, name }
    }

    /// Starts the engine, allowing workflow invocations.
    ///
    /// Spawns a background timer poller that checks for expired durable
    /// timers every second. Timer deadlines have millisecond precision, but
    /// actual firing latency is up to ~1 second after the deadline.
    /// Must be called after all workflows are registered.
    ///
    /// When [`EngineBuilder::resume_on_start`] is enabled (the default),
    /// scans the metadata table and resumes any instances that were
    /// `Running` when the process last exited. `Suspended` instances are
    /// not resumed — they continue to wait for their signal or timer.
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
        let poller_senders = Arc::clone(&self.senders);
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
                        &poller_senders,
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

        if let Some(retention) = self.retention {
            let db = Arc::clone(&self.db);
            let running = Arc::clone(&self.running);
            let workflow_retentions = self.workflow_retentions.clone();
            tasks.spawn(
                async move {
                    info!(?retention, "retention cleanup started");
                    while running.load(Ordering::Acquire) {
                        for _ in 0..60 {
                            if !running.load(Ordering::Acquire) {
                                break;
                            }
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        if !running.load(Ordering::Acquire) {
                            break;
                        }
                        if let Err(e) = cleanup_expired(&db, retention, &workflow_retentions) {
                            error!(error = %e, "retention cleanup failed");
                        }
                    }
                    info!("retention cleanup stopped");
                }
                .instrument(info_span!("retention_cleanup")),
            );
        }

        drop(tasks);

        if self.resume_on_start {
            self.resume_running_instances().await?;
        }

        info!("engine started");
        Ok(())
    }

    async fn resume_running_instances(&self) -> Result<(), EngineError> {
        let workflow_names: Vec<String> = self.workflows.keys().cloned().collect();
        for workflow_name in &workflow_names {
            let instances = metadata::list_metadata(&self.db, workflow_name)?;
            for (instance_id, meta) in instances {
                if *meta.status() == MetadataStatus::Running {
                    info!(
                        workflow = %workflow_name,
                        instance_id = %instance_id,
                        "auto-resuming workflow instance"
                    );
                    match self
                        .spawn_workflow::<()>(workflow_name, instance_id.clone(), None)
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            warn!(
                                workflow = %workflow_name,
                                instance_id = %instance_id,
                                error = %e,
                                "failed to auto-resume workflow instance, skipping"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Invokes a registered workflow, creating a new instance.
    ///
    /// Returns an [`InvocationBuilder`] that carries the input and output
    /// types from the [`WorkflowDef`]. For workflows that require input,
    /// call [`.input(payload)`](InvocationBuilder::input) before `.await`
    /// — the compiler enforces this.
    ///
    /// A unique instance ID is generated automatically. The workflow runs
    /// in a spawned Tokio task.
    ///
    /// # Errors
    ///
    /// The returned builder yields [`EngineError::NotStarted`] if the engine
    /// has not been started, [`EngineError::WorkflowNotFound`] if no workflow
    /// is registered under the given name, or [`EngineError::InvalidKey`] if
    /// the name contains `/`.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::{Engine, Context, EngineError, WorkflowDef};
    ///
    /// const GREET: WorkflowDef = WorkflowDef::new("greet");
    ///
    /// async fn greet(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut engine = Engine::builder().in_memory().build();
    /// engine.register(&GREET, greet);
    /// engine.start().await?;
    ///
    /// engine.invoke(&GREET).await?.wait().await.unwrap_completed();
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn invoke<I, O>(&self, def: &WorkflowDef<I, O>) -> InvocationBuilder<'_, I, O> {
        InvocationBuilder {
            engine: self,
            workflow_name: def.name().to_string(),
            input_payload: Ok(None),
            _marker: PhantomData,
        }
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
    /// under the given name, or [`EngineError::InvalidKey`] if
    /// `instance_id` contains `/`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowDef};
    /// # const WF: WorkflowDef = WorkflowDef::new("wf");
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register(&WF, wf);
    /// # engine.start().await?;
    /// # let invocation = engine.invoke(&WF).await?;
    /// # let instance_id = invocation.instance_id().to_string();
    /// # invocation.wait().await;
    /// // Resume a failed instance — memoised steps are skipped.
    /// let invocation = engine.resume(&WF, &instance_id).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn resume<I, O>(
        &self,
        def: &WorkflowDef<I, O>,
        instance_id: impl Into<String>,
    ) -> Result<Invocation<O>, EngineError> {
        let instance_id = instance_id.into();
        validate_key_component(&instance_id, "instance_id")?;
        self.spawn_workflow(def.name(), instance_id, None).await
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
    /// the given name, [`EngineError::InvalidKey`] if `instance_id` contains
    /// `/`, [`EngineError::SignalRejected`] if the step does not exist or is
    /// not currently suspended, [`EngineError::SignalSuperseded`] if another
    /// caller already claimed the step, or [`EngineError::Storage`] /
    /// [`EngineError::Serialization`] if the write fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, SuspendPoint, WorkflowDef};
    /// const WF: WorkflowDef = WorkflowDef::new("approval");
    /// const APPROVAL: SuspendPoint<bool> = SuspendPoint::new("approval:v1");
    ///
    /// async fn approval(ctx: Context) -> Result<(), EngineError> {
    ///     let approved: bool = ctx.suspend(&APPROVAL).await?;
    ///     assert!(approved);
    ///     Ok(())
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut engine = Engine::builder().in_memory().build();
    /// engine.register(&WF, approval);
    /// engine.start().await?;
    ///
    /// let inv = engine.invoke(&WF).await?;
    /// let id = inv.instance_id().to_string();
    /// inv.wait().await;
    ///
    /// engine.signal(&WF, &id, &APPROVAL, true).await?.wait().await.unwrap_completed();
    /// # Ok(())
    /// # }
    /// ```
    pub async fn signal<I, O, T>(
        &self,
        def: &WorkflowDef<I, O>,
        instance_id: &str,
        point: &SuspendPoint<T>,
        payload: T,
    ) -> Result<Invocation<O>, EngineError>
    where
        T: Serialize + DeserializeOwned + Send,
    {
        let workflow_name = def.name();
        validate_key_component(instance_id, "instance_id")?;

        let step_key = point.key();
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

        let tx = self.get_or_create_sender(instance_id);
        let rx = tx.subscribe();

        spawn_workflow_task(
            &mut tasks,
            workflow,
            &self.db,
            workflow_name,
            instance_id,
            &self.timer_serial,
            self.default_retry.clone(),
            &tx,
            &self.senders,
        );

        Ok(Invocation {
            instance_id: instance_id.to_string(),
            status: rx,
            db: Arc::clone(&self.db),
            workflow_name: workflow_name.to_string(),
            _marker: PhantomData,
        })
    }

    /// Subscribes to status updates for a workflow instance.
    ///
    /// Returns a [`StatusStream`] that immediately yields the current state,
    /// then yields each subsequent state change. The stream ends when the
    /// workflow completes, fails, or is replaced by a new run (e.g. after
    /// [`signal`](Engine::signal)).
    ///
    /// When no live sender exists (e.g. after a server restart), falls back
    /// to persisted metadata and returns a single-item stream with the last
    /// known state.
    ///
    /// # Errors
    ///
    /// Returns [`SubscribeError::NotFound`] if no instance with the given
    /// name and ID exists, [`SubscribeError::StaleRunning`] if the instance
    /// has `Running` metadata but no live task (crash recovery scenario),
    /// or [`SubscribeError::Storage`] if the database read fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowState, SubscribeError, WorkflowDef};
    /// # const WF: WorkflowDef = WorkflowDef::new("wf");
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register(&WF, wf);
    /// # engine.start().await?;
    /// let inv = engine.invoke(&WF).await?;
    /// let id = inv.instance_id().to_string();
    ///
    /// let stream = engine.subscribe(&WF, &id)?;
    /// // Use with StreamExt::next(), .map(), .take_while(), etc.
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe<I, O>(
        &self,
        def: &WorkflowDef<I, O>,
        instance_id: &str,
    ) -> Result<StatusStream, SubscribeError> {
        let workflow_name = def.name();
        let senders = self
            .senders
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(tx) = senders.get(instance_id) {
            return Ok(StatusStream::live(tx.subscribe()));
        }
        drop(senders);

        let meta = metadata::read_metadata(&self.db, workflow_name, instance_id)
            .map_err(|e| SubscribeError::Storage(e.to_string().into()))?;

        match meta {
            None => Err(SubscribeError::NotFound {
                workflow_name: workflow_name.to_string(),
                instance_id: instance_id.to_string(),
            }),
            Some(meta) => match meta.status() {
                MetadataStatus::Suspended { key, status } => {
                    Ok(StatusStream::snapshot(WorkflowState::Suspended {
                        key: key.clone(),
                        status: status.clone(),
                    }))
                }
                MetadataStatus::Completed(msg) => Ok(StatusStream::snapshot(
                    WorkflowState::Completed(msg.clone()),
                )),
                MetadataStatus::Failed(msg) => {
                    Ok(StatusStream::snapshot(WorkflowState::Failed(msg.clone())))
                }
                MetadataStatus::Running => Err(SubscribeError::StaleRunning {
                    workflow_name: workflow_name.to_string(),
                    instance_id: instance_id.to_string(),
                }),
            },
        }
    }

    /// Returns the current state of a workflow instance.
    ///
    /// Checks the live watch channel first, falling back to persisted
    /// metadata. This is a single point-in-time read — no stream is
    /// allocated.
    ///
    /// If the instance has stale `Running` metadata (no live task, e.g.
    /// after a crash), returns [`WorkflowState::Started`] rather than an
    /// error — the caller sees "was running" rather than needing to handle
    /// a crash-recovery variant.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::NotFound`] if no instance with the given name
    /// and ID exists, or [`StateError::Storage`] if the database read fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowState, WorkflowDef};
    /// # const WF: WorkflowDef = WorkflowDef::new("wf");
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register(&WF, wf);
    /// # engine.start().await?;
    /// let inv = engine.invoke(&WF).await?;
    /// let id = inv.instance_id().to_string();
    /// inv.wait().await;
    ///
    /// let state = engine.state(&WF, &id)?;
    /// assert_eq!(state, WorkflowState::Completed(None));
    /// # Ok(())
    /// # }
    /// ```
    pub fn state<I, O>(
        &self,
        def: &WorkflowDef<I, O>,
        instance_id: &str,
    ) -> Result<WorkflowState, StateError> {
        let workflow_name = def.name();
        let senders = self
            .senders
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(tx) = senders.get(instance_id) {
            return Ok(tx.borrow().clone());
        }
        drop(senders);

        let meta = metadata::read_metadata(&self.db, workflow_name, instance_id)
            .map_err(|e| StateError::Storage(e.to_string().into()))?;

        match meta {
            None => Err(StateError::NotFound {
                workflow_name: workflow_name.to_string(),
                instance_id: instance_id.to_string(),
            }),
            Some(meta) => match meta.status() {
                MetadataStatus::Running => Ok(WorkflowState::Started),
                MetadataStatus::Suspended { key, status } => Ok(WorkflowState::Suspended {
                    key: key.clone(),
                    status: status.clone(),
                }),
                MetadataStatus::Completed(msg) => Ok(WorkflowState::Completed(msg.clone())),
                MetadataStatus::Failed(msg) => Ok(WorkflowState::Failed(msg.clone())),
            },
        }
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
    /// # use memable::{Engine, Context, EngineError, MetadataStatus, WorkflowDef};
    /// # const WF: WorkflowDef = WorkflowDef::new("wf");
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register(&WF, wf);
    /// # engine.start().await?;
    /// let inv = engine.invoke(&WF).await?;
    /// let id = inv.instance_id().to_string();
    /// inv.wait().await;
    ///
    /// let meta = engine.get_metadata(&WF, &id)?.expect("instance exists");
    /// assert_eq!(*meta.status(), MetadataStatus::Completed(None));
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_metadata<I, O>(
        &self,
        def: &WorkflowDef<I, O>,
        instance_id: &str,
    ) -> Result<Option<WorkflowMetadata>, EngineError> {
        metadata::read_metadata(&self.db, def.name(), instance_id)
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
    /// # use memable::{Engine, Context, EngineError, WorkflowDef};
    /// # const WF: WorkflowDef = WorkflowDef::new("wf");
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register(&WF, wf);
    /// # engine.start().await?;
    /// engine.invoke(&WF).await?.wait().await;
    /// engine.invoke(&WF).await?.wait().await;
    ///
    /// let instances = engine.list_instances(&WF)?;
    /// assert_eq!(instances.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn list_instances<I, O>(
        &self,
        def: &WorkflowDef<I, O>,
    ) -> Result<Vec<(String, WorkflowMetadata)>, EngineError> {
        metadata::list_metadata(&self.db, def.name())
    }

    /// Gracefully stops the engine.
    ///
    /// Prevents new workflow invocations, then waits up to the configured
    /// [`shutdown_timeout`](crate::EngineBuilder::shutdown_timeout) for
    /// running workflows to complete. If the timeout expires, remaining
    /// tasks are aborted — their metadata stays as `Running` so they
    /// resume automatically on the next [`Engine::start`].
    ///
    /// The timeout defaults to 30 seconds and can be overridden via
    /// [`EngineBuilder::shutdown_timeout`](crate::EngineBuilder::shutdown_timeout)
    /// or the `MEMABLE_SHUTDOWN_TIMEOUT_SECS` environment variable.
    ///
    /// To wait indefinitely for all workflows to finish, use
    /// [`wait_all`](Engine::wait_all) instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowDef};
    /// # const WF: WorkflowDef = WorkflowDef::new("wf");
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut engine = Engine::builder().in_memory().build();
    /// engine.register(&WF, wf);
    /// engine.start().await?;
    ///
    /// let _ = engine.invoke(&WF).await?;
    /// engine.stop().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stop(&self) {
        self.running.store(false, Ordering::Release);
        info!(
            timeout_secs = self.shutdown_timeout.as_secs_f64(),
            "engine stopping — waiting for running workflows"
        );

        let mut tasks = self.tasks.lock().await;
        let drain_result = tokio::time::timeout(self.shutdown_timeout, async {
            while let Some(result) = tasks.join_next().await {
                if let Err(e) = result {
                    if e.is_panic() {
                        error!("workflow task panicked during shutdown: {e}");
                    }
                }
            }
        })
        .await;

        if drain_result.is_err() {
            let remaining = tasks.len();
            warn!(
                remaining,
                "shutdown timeout exceeded — aborting remaining workflows"
            );
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }

        info!("engine stopped");
    }

    /// Waits indefinitely for all running workflows to complete and
    /// prevents new invocations.
    ///
    /// Sets the engine to a stopped state, then drains all spawned
    /// workflow tasks. Any call to [`invoke`](Engine::invoke) or
    /// [`resume`](Engine::resume) after `wait_all` is called will return
    /// [`EngineError::NotStarted`].
    ///
    /// Returns immediately if no workflows are running.
    ///
    /// For a timeout-based shutdown that aborts remaining workflows,
    /// use [`stop`](Engine::stop) instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowDef};
    /// # const GREET: WorkflowDef = WorkflowDef::new("greet");
    /// # async fn greet(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register(&GREET, greet);
    /// # engine.start().await?;
    /// // Fire-and-forget — no need to hold the Invocation handle.
    /// let _ = engine.invoke(&GREET).await?;
    /// let _ = engine.invoke(&GREET).await?;
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

    pub(super) async fn spawn_workflow<O>(
        &self,
        workflow_name: &str,
        instance_id: String,
        input_bytes: Option<Vec<u8>>,
    ) -> Result<Invocation<O>, EngineError> {
        validate_key_component(workflow_name, "workflow_name")?;
        let mut tasks = self.tasks.lock().await;

        if !self.running.load(Ordering::Acquire) {
            return Err(EngineError::NotStarted);
        }

        let workflow = self
            .workflows
            .get(workflow_name)
            .ok_or_else(|| EngineError::WorkflowNotFound(workflow_name.to_string()))?;

        let meta = WorkflowMetadata::new(MetadataStatus::Running);
        let meta_key = format!("{workflow_name}/{instance_id}");
        let meta_bytes = postcard::to_allocvec(&meta).map_err(|e| EngineError::Serialization {
            key: meta_key.clone(),
            source: Box::new(e),
        })?;

        let write_txn = self.db.begin_write()?;
        {
            let mut meta_table = write_txn.open_table(WORKFLOW_META)?;
            meta_table.insert(meta_key.as_str(), meta_bytes.as_slice())?;

            if let Some(ref input) = input_bytes {
                let mut steps_table = write_txn.open_table(STEPS)?;
                let input_key = format!("{workflow_name}/{instance_id}/_input");
                steps_table.insert(input_key.as_str(), input.as_slice())?;
            }
        }
        write_txn.commit()?;

        let tx = self.get_or_create_sender(&instance_id);
        let rx = tx.subscribe();

        spawn_workflow_task(
            &mut tasks,
            workflow,
            &self.db,
            workflow_name,
            &instance_id,
            &self.timer_serial,
            self.default_retry.clone(),
            &tx,
            &self.senders,
        );

        Ok(Invocation {
            instance_id,
            status: rx,
            db: Arc::clone(&self.db),
            workflow_name: workflow_name.to_string(),
            _marker: PhantomData,
        })
    }

    fn get_or_create_sender(&self, instance_id: &str) -> watch::Sender<WorkflowState> {
        let mut senders = self
            .senders
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(tx) = senders.get(instance_id) {
            tx.send_if_modified(|state| {
                *state = WorkflowState::Started;
                false
            });
            return tx.clone();
        }
        let (tx, _) = watch::channel(WorkflowState::Started);
        senders.insert(instance_id.to_string(), tx.clone());
        tx
    }
}

/// Handle returned by [`Engine::register`] for per-workflow configuration.
///
/// The registration is complete even if no methods are called — this type
/// exists solely to allow optional chaining of per-workflow settings.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use memable::{Engine, Context, EngineError, WorkflowDef};
///
/// const EPHEMERAL: WorkflowDef = WorkflowDef::new("ephemeral");
///
/// async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
///
/// let mut engine = Engine::builder()
///     .retention(Duration::from_secs(86400))
///     .in_memory()
///     .build();
///
/// // Override retention for this specific workflow:
/// engine.register(&EPHEMERAL, wf).retention(Duration::from_secs(3600));
/// ```
pub struct Registration<'a> {
    engine: &'a mut Engine,
    name: String,
}

impl Registration<'_> {
    /// Sets a per-workflow retention override.
    ///
    /// This duration takes precedence over the engine-level default for
    /// instances of this workflow definition.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use memable::{Engine, Context, EngineError, WorkflowDef};
    ///
    /// const SHORT: WorkflowDef = WorkflowDef::new("short-lived");
    ///
    /// async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    ///
    /// let mut engine = Engine::builder()
    ///     .retention(Duration::from_secs(30 * 24 * 60 * 60))
    ///     .in_memory()
    ///     .build();
    ///
    /// engine.register(&SHORT, wf).retention(Duration::from_secs(3600));
    /// ```
    #[expect(clippy::must_use_candidate, clippy::return_self_not_must_use)]
    pub fn retention(self, ttl: Duration) -> Self {
        self.engine
            .workflow_retentions
            .insert(self.name.clone(), ttl);
        self
    }
}
