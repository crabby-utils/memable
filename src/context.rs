use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redb::{ReadableDatabase as _, TableDefinition};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;
use tracing::{info, info_span};

use crate::engine::{EngineShared, WorkflowState};
use crate::error::{EngineError, StepError};
use crate::retry::RetryPolicy;

/// A typed workflow definition that encodes the input and output types.
///
/// Define as a `const` and pass to [`Engine::register`](crate::Engine::register),
/// [`Engine::invoke`](crate::Engine::invoke), [`Engine::resume`](crate::Engine::resume),
/// and [`Engine::signal`](crate::Engine::signal) to get compile-time guarantees
/// that all call sites agree on the workflow name and payload types.
///
/// # Examples
///
/// ```
/// use memable::WorkflowDef;
///
/// const CLEANUP: WorkflowDef = WorkflowDef::new("cleanup");
/// const GREETING: WorkflowDef<String, String> = WorkflowDef::new("greeting");
/// ```
pub struct WorkflowDef<I = (), O = ()> {
    name: &'static str,
    _marker: PhantomData<(I, O)>,
}

impl<I, O> WorkflowDef<I, O> {
    /// Creates a new workflow definition with the given name.
    ///
    /// # Panics
    ///
    /// Panics at compile time (or runtime if not called in a const context)
    /// if `name` contains `/` or starts with `_`.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::WorkflowDef;
    ///
    /// const GREET: WorkflowDef<String, String> = WorkflowDef::new("greet");
    /// assert_eq!(GREET.name(), "greet");
    /// ```
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        let bytes = name.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            assert!(bytes[i] != b'/', "workflow name must not contain '/'");
            i += 1;
        }
        assert!(
            bytes.is_empty() || bytes[0] != b'_',
            "workflow name must not start with '_'"
        );
        Self {
            name,
            _marker: PhantomData,
        }
    }

    /// Returns the string name for this workflow definition.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::WorkflowDef;
    ///
    /// const WF: WorkflowDef = WorkflowDef::new("my-workflow");
    /// assert_eq!(WF.name(), "my-workflow");
    /// ```
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

/// A typed suspend point that encodes both the step key and payload type.
///
/// Define as a `const` and share between the workflow (via [`Context::suspend`])
/// and the signal call (via [`Engine::signal`](crate::Engine::signal)) to get
/// compile-time guarantees that both sides agree on the key and payload type.
///
/// # Examples
///
/// ```
/// use memable::SuspendPoint;
///
/// const APPROVAL: SuspendPoint<bool> = SuspendPoint::new("approval:v1");
/// ```
pub struct SuspendPoint<T> {
    key: &'static str,
    _marker: PhantomData<T>,
}

impl<T> SuspendPoint<T> {
    /// Creates a new suspend point with the given key.
    ///
    /// # Panics
    ///
    /// Panics at compile time (or runtime if not called in a const context)
    /// if `key` contains `/` or starts with `_`.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::SuspendPoint;
    ///
    /// const APPROVAL: SuspendPoint<bool> = SuspendPoint::new("approval:v1");
    /// assert_eq!(APPROVAL.key(), "approval:v1");
    /// ```
    #[must_use]
    pub const fn new(key: &'static str) -> Self {
        let bytes = key.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            assert!(bytes[i] != b'/', "suspend point key must not contain '/'");
            i += 1;
        }
        assert!(
            bytes.is_empty() || bytes[0] != b'_',
            "suspend point key must not start with '_'"
        );
        Self {
            key,
            _marker: PhantomData,
        }
    }

    /// Returns the string key for this suspend point.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::SuspendPoint;
    ///
    /// const POINT: SuspendPoint<String> = SuspendPoint::new("step:v1");
    /// assert_eq!(POINT.key(), "step:v1");
    /// ```
    #[must_use]
    pub const fn key(&self) -> &'static str {
        self.key
    }
}

/// redb table for step results.
/// Key: `"{workflow_name}/{instance_id}/{step_key}"`, Value: postcard-serialized bytes.
pub(crate) const STEPS: TableDefinition<&str, &[u8]> = TableDefinition::new("steps");

/// redb table for pending timers.
/// Key: `(deadline_unix_millis, serial)`, Value: postcard-serialized [`TimerEntry`].
pub(crate) const TIMERS: TableDefinition<(u64, u64), &[u8]> = TableDefinition::new("timers");

/// Value stored in the timer table.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub(crate) struct TimerEntry {
    pub workflow_name: String,
    pub instance_id: String,
    pub step_key: String,
}

#[derive(Debug, Clone)]
enum RetryOverride {
    Disabled,
    Custom(RetryPolicy),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum StepData<T> {
    Completed { result: T, status: Option<String> },
    Suspended,
    Failed { error: String },
}

/// Discriminant for the step's lifecycle state, stored explicitly in the
/// envelope so that state checks don't rely on the type-tag encoding.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StepState {
    Suspended,
    Completed,
    Failed,
}

/// Storage envelope that wraps serialized [`StepData<T>`] with a type tag.
///
/// The type tag enables detection of payload type mismatches between
/// [`Engine::signal`](crate::Engine::signal) and the workflow's suspend point
/// before deserialization is attempted.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct StepEnvelope {
    pub state: StepState,
    /// `Some(type_name)` for `Completed` entries, `None` otherwise.
    pub type_tag: Option<String>,
    /// Postcard-serialized `StepData<T>` bytes.
    pub(crate) data: Vec<u8>,
}

/// Serialize a [`StepData<T>`] into an envelope with a type tag.
pub(crate) fn serialize_step<T: Serialize>(
    data: &StepData<T>,
    key: &str,
) -> Result<Vec<u8>, EngineError> {
    let (state, type_tag) = match data {
        StepData::Completed { .. } => (
            StepState::Completed,
            Some(std::any::type_name::<T>().to_string()),
        ),
        StepData::Suspended => (StepState::Suspended, None),
        StepData::Failed { .. } => (StepState::Failed, None),
    };
    let inner = postcard::to_allocvec(data).map_err(|e| EngineError::Serialization {
        key: key.to_string(),
        source: Box::new(e),
    })?;
    let envelope = StepEnvelope {
        state,
        type_tag,
        data: inner,
    };
    postcard::to_allocvec(&envelope).map_err(|e| EngineError::Serialization {
        key: key.to_string(),
        source: Box::new(e),
    })
}

/// Deserialize a [`StepData<T>`] from an envelope, validating the type tag.
///
/// Returns [`EngineError::TypeMismatch`] if the stored type tag does not
/// match `std::any::type_name::<T>()`. Suspended entries have no type tag
/// and always succeed.
pub(crate) fn deserialize_step<T: DeserializeOwned>(
    bytes: &[u8],
    key: &str,
) -> Result<StepData<T>, EngineError> {
    let envelope: StepEnvelope =
        postcard::from_bytes(bytes).map_err(|e| EngineError::Serialization {
            key: key.to_string(),
            source: Box::new(e),
        })?;
    if let Some(ref stored) = envelope.type_tag {
        let expected = std::any::type_name::<T>();
        if stored != expected {
            return Err(EngineError::TypeMismatch {
                key: key.to_string(),
                expected: expected.to_string(),
                found: stored.clone(),
            });
        }
    }
    postcard::from_bytes(&envelope.data).map_err(|e| EngineError::Serialization {
        key: key.to_string(),
        source: Box::new(e),
    })
}

/// Deserialize only the [`StepEnvelope`] without parsing the inner payload.
pub(crate) fn deserialize_envelope(bytes: &[u8], key: &str) -> Result<StepEnvelope, EngineError> {
    postcard::from_bytes(bytes).map_err(|e| EngineError::Serialization {
        key: key.to_string(),
        source: Box::new(e),
    })
}

/// Workflow execution context.
///
/// Provides the [`step`](Context::step) method for performing durable,
/// memoised operations within a workflow.
///
/// A `Context` is created by the [`Engine`](crate::Engine) and passed to
/// the workflow function. It is not constructed directly.
pub struct Context {
    workflow_name: String,
    instance_id: String,
    shared: Arc<EngineShared>,
    status_tx: watch::Sender<WorkflowState>,
    replaying: AtomicBool,
}

impl Context {
    pub(crate) fn new(
        workflow_name: String,
        instance_id: String,
        shared: Arc<EngineShared>,
        status_tx: watch::Sender<WorkflowState>,
    ) -> Self {
        Self {
            workflow_name,
            instance_id,
            shared,
            status_tx,
            replaying: AtomicBool::new(true),
        }
    }

    /// Returns the registered workflow definition name.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowDef};
    /// # async fn check(ctx: Context) -> Result<(), EngineError> {
    ///     assert_eq!(ctx.workflow_name(), "my-workflow");
    /// #   Ok(())
    /// # }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # const MY_WF: WorkflowDef = WorkflowDef::new("my-workflow");
    /// # engine.register(&MY_WF, check);
    /// # engine.start().await?;
    /// # engine.invoke(&MY_WF).await?.wait().await;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    /// Returns the unique instance ID for this workflow invocation.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowState, WorkflowDef};
    /// # async fn check(ctx: Context) -> Result<(), EngineError> {
    ///     println!("Instance: {}", ctx.instance_id());
    /// #   Ok(())
    /// # }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # const CHECK: WorkflowDef = WorkflowDef::new("check");
    /// # engine.register(&CHECK, check);
    /// # engine.start().await?;
    /// # engine.invoke(&CHECK).await?.wait().await;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Returns a reference to the database handle.
    pub(crate) fn db(&self) -> &Arc<redb::Database> {
        &self.shared.db
    }

    /// Reads the typed input payload provided at invocation time.
    pub(crate) fn input<T: DeserializeOwned>(&self) -> Result<Option<T>, EngineError> {
        let composite_key = format!("{}/{}/_input", self.workflow_name, self.instance_id);
        let Some(bytes) = self.read_step(&composite_key)? else {
            return Ok(None);
        };
        let data: StepData<T> = deserialize_step(&bytes, "_input")?;
        match data {
            StepData::Completed { result, .. } => Ok(Some(result)),
            StepData::Suspended | StepData::Failed { .. } => Ok(None),
        }
    }

    /// Updates the workflow's observable status.
    ///
    /// Sets the [`WorkflowState`] to [`InProgress`](WorkflowState::InProgress)
    /// with the given message. Subscribers observing the workflow via
    /// [`Invocation::status`](crate::Invocation::status) will see the update.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::{Engine, Context, EngineError, WorkflowDef};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// const PIPELINE: WorkflowDef = WorkflowDef::new("pipeline");
    /// let mut engine = Engine::builder().in_memory().build();
    /// engine.register(&PIPELINE, |ctx: Context| async move {
    ///     ctx.set_status("loading data");
    ///     let _: String = ctx.step("load:v1").run(async || {
    ///         Ok("data".to_string())
    ///     }).await?;
    ///     ctx.set_status("processing");
    ///     Ok(())
    /// });
    /// engine.start().await?;
    ///
    /// let c = engine.invoke(&PIPELINE).await?.wait().await.unwrap_completed();
    /// assert_eq!(c.status(), Some("processing"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_status(&self, msg: impl fmt::Display) {
        let value = WorkflowState::InProgress(msg.to_string());
        if self.replaying.load(Ordering::Acquire) {
            self.status_tx.send_if_modified(|state| {
                *state = value;
                false
            });
        } else {
            let _ = self.status_tx.send(value);
        }
    }

    /// Creates a durable step builder.
    ///
    /// Returns a [`StepBuilder`] that configures and executes the step.
    /// Call [`.run(closure)`](StepBuilder::run) to execute, optionally
    /// setting a [`.timeout()`](StepBuilder::timeout) first.
    ///
    /// If a result for this step key already exists in the journal, the
    /// cached result is returned without executing the closure. Otherwise
    /// the closure runs, its result is serialized with postcard, persisted
    /// to redb, and returned.
    ///
    /// # Panics
    ///
    /// Panics if `key` contains `/` or starts with `_` (reserved for
    /// engine use).
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::{Engine, Context, EngineError, WorkflowDef};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// const EXAMPLE: WorkflowDef = WorkflowDef::new("example");
    /// let mut engine = Engine::builder().in_memory().build();
    /// engine.register(&EXAMPLE, |ctx: Context| async move {
    ///     // Simple step — no timeout.
    ///     let value: String = ctx.step("greet:v1").run(async || {
    ///         Ok("Hello, world!".to_string())
    ///     }).await?;
    ///     assert_eq!(value, "Hello, world!");
    ///
    ///     // Step with timeout — clone data for the closure.
    ///     let value_clone = value.clone();
    ///     let loud: String = ctx.step("shout:v1")
    ///         .timeout(std::time::Duration::from_secs(5))
    ///         .run(async move || {
    ///             Ok(value_clone.to_uppercase())
    ///         }).await?;
    ///     assert_eq!(loud, "HELLO, WORLD!");
    ///     Ok(())
    /// });
    /// engine.start().await?;
    /// engine.invoke(&EXAMPLE).await?.wait().await.unwrap_completed();
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn step<'a>(&'a self, key: &'a str) -> StepBuilder<'a> {
        assert!(!key.contains('/'), "step key must not contain '/': '{key}'");
        assert!(
            !key.starts_with('_'),
            "step keys starting with '_' are reserved: '{key}'"
        );
        StepBuilder {
            ctx: self,
            key,
            timeout: None,
            retry_override: None,
        }
    }

    /// Suspends the workflow, awaiting an external signal.
    ///
    /// On first execution, writes a `Suspended` entry to the step table
    /// and returns [`EngineError::Suspended`], which short-circuits the
    /// workflow. The workflow future completes and drops — no task is
    /// held in memory.
    ///
    /// After [`Engine::signal`](crate::Engine::signal) delivers a payload,
    /// the workflow is re-run. The suspend step finds the completed entry
    /// and returns the deserialized payload, allowing execution to continue.
    ///
    /// Use [`.status()`](SuspendBuilder::status) to set a custom status
    /// message (defaults to the step key).
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Suspended`] when the workflow should suspend.
    /// Returns [`EngineError::Storage`] or [`EngineError::Serialization`]
    /// on storage failures.
    ///
    /// # Examples
    ///
    /// ```
    /// use memable::{Context, EngineError, SuspendPoint};
    ///
    /// const APPROVAL: SuspendPoint<bool> = SuspendPoint::new("approval:v1");
    ///
    /// async fn approval_workflow(ctx: Context) -> Result<(), EngineError> {
    ///     let approved: bool = ctx.suspend(&APPROVAL)
    ///         .status("Waiting for manager approval")
    ///         .await?;
    ///     if approved {
    ///         // continue with approved path
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn suspend<'a, T>(&'a self, point: &'a SuspendPoint<T>) -> SuspendBuilder<'a, T>
    where
        T: Serialize + DeserializeOwned + Send,
    {
        SuspendBuilder {
            ctx: self,
            key: point.key(),
            status_msg: None,
            _marker: PhantomData,
        }
    }

    /// Suspends the workflow until a deadline elapses.
    ///
    /// On first execution, writes a `Suspended` entry to the step table
    /// and a row to the timer table, then returns [`EngineError::Suspended`].
    /// Deadlines are stored with millisecond precision. A background poller
    /// in the [`Engine`](crate::Engine) checks every second for expired
    /// timers, so actual wake-up latency is up to ~1 second after the
    /// deadline.
    ///
    /// On replay (after the timer has fired), the step finds its completed
    /// entry and returns immediately.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::InvalidKey`] if `key` contains `/` or starts
    /// with `_` (reserved for engine use).
    /// Returns [`EngineError::Suspended`] when the timer is first set.
    /// Returns [`EngineError::Storage`] or [`EngineError::Serialization`]
    /// on storage failures.
    ///
    /// # Panics
    ///
    /// Panics if the system clock is before the Unix epoch.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use memable::{Context, EngineError};
    ///
    /// async fn delayed_workflow(ctx: Context) -> Result<(), EngineError> {
    ///     // Wait 30 seconds before continuing.
    ///     ctx.timer("cooldown:v1", Duration::from_secs(30))?;
    ///     // Execution resumes here after the timer fires.
    ///     Ok(())
    /// }
    /// ```
    pub fn timer(&self, key: &str, duration: Duration) -> Result<(), EngineError> {
        if key.contains('/') || key.starts_with('_') {
            return Err(EngineError::InvalidKey {
                label: "step_key",
                value: key.to_string(),
            });
        }
        let composite_key = format!("{}/{}/{key}", self.workflow_name, self.instance_id);
        let span = info_span!("timer", key, composite_key = %composite_key);

        // Check for completed entry (timer already fired and signalled).
        if let Some(bytes) = self.read_step(&composite_key)? {
            let data: StepData<()> = deserialize_step(&bytes, key)?;
            match data {
                StepData::Completed { .. } => {
                    span.in_scope(|| info!("timer already fired — resuming"));
                    return Ok(());
                }
                StepData::Suspended => {
                    span.in_scope(|| info!("timer still pending — re-suspending"));
                    return Err(EngineError::Suspended {
                        key: key.to_string(),
                    });
                }
                StepData::Failed { .. } => {
                    // Dead-lettered — treat as cache miss for re-execution.
                }
            }
        }

        // First execution — compute absolute deadline.
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_millis();
        let deadline = u64::try_from(now_ms).expect("system clock overflows u64 millis")
            + u64::try_from(duration.as_millis()).expect("duration overflows u64 millis");

        // Serialize step and timer data, then write both in a single transaction
        // so a crash can't leave a Suspended step with no timer to wake it.
        let data = StepData::<()>::Suspended;
        let step_bytes = serialize_step(&data, key)?;

        let serial = self.shared.timer_serial.fetch_add(1, Ordering::Relaxed);
        let entry = TimerEntry {
            workflow_name: self.workflow_name.clone(),
            instance_id: self.instance_id.clone(),
            step_key: key.to_string(),
        };
        let timer_bytes =
            postcard::to_allocvec(&entry).map_err(|e| EngineError::Serialization {
                key: key.to_string(),
                source: Box::new(e),
            })?;

        let write_txn = self.shared.db.begin_write()?;
        {
            let mut steps = write_txn.open_table(STEPS)?;
            steps.insert(composite_key.as_str(), step_bytes.as_slice())?;
            let mut timers = write_txn.open_table(TIMERS)?;
            timers.insert((deadline, serial), timer_bytes.as_slice())?;
        }
        write_txn.commit()?;

        let msg = format!("timer {key} (deadline {deadline})");
        span.in_scope(|| info!(deadline, "timer set — suspending"));
        self.status_tx.send_if_modified(|state| {
            *state = WorkflowState::Suspended {
                key: key.to_string(),
                status: msg.clone(),
            };
            false
        });

        Err(EngineError::Suspended {
            key: key.to_string(),
        })
    }

    fn execute_suspend<T>(&self, key: &str, status_msg: Option<&str>) -> Result<T, EngineError>
    where
        T: Serialize + DeserializeOwned + Send,
    {
        let composite_key = format!("{}/{}/{key}", self.workflow_name, self.instance_id);
        let span = info_span!("suspend", key, composite_key = %composite_key);

        if let Some(bytes) = self.read_step(&composite_key)? {
            let data: StepData<T> = deserialize_step(&bytes, key)?;
            match data {
                StepData::Completed { result, status } => {
                    span.in_scope(|| info!("signal received — resuming"));
                    self.replaying.store(false, Ordering::Release);
                    if let Some(status) = status {
                        self.status_tx.send_if_modified(|state| {
                            *state = WorkflowState::InProgress(status);
                            false
                        });
                    }
                    return Ok(result);
                }
                StepData::Suspended => {
                    span.in_scope(|| info!("still suspended — awaiting signal"));
                    return Err(EngineError::Suspended {
                        key: key.to_string(),
                    });
                }
                StepData::Failed { .. } => {
                    // Dead-lettered — treat as cache miss for re-execution.
                }
            }
        }

        // Cache miss — write Suspended entry and short-circuit.
        let data = StepData::<T>::Suspended;
        let bytes = serialize_step(&data, key)?;
        self.write_step(&composite_key, &bytes)?;

        let msg = status_msg.unwrap_or(key).to_string();
        span.in_scope(|| info!(status = %msg, "suspending"));
        self.status_tx.send_if_modified(|state| {
            *state = WorkflowState::Suspended {
                key: key.to_string(),
                status: msg.clone(),
            };
            false
        });

        Err(EngineError::Suspended {
            key: key.to_string(),
        })
    }

    fn current_status_string(&self) -> Option<String> {
        match &*self.status_tx.borrow() {
            WorkflowState::InProgress(msg) => Some(msg.clone()),
            _ => None,
        }
    }

    #[cfg_attr(not(test), expect(dead_code, reason = "used by tests and future API"))]
    pub(crate) async fn spawn_child(
        &self,
        workflow_name: &str,
        child_key: &str,
        input_bytes: Option<Vec<u8>>,
    ) -> Result<(String, tokio::sync::watch::Receiver<WorkflowState>), EngineError> {
        let child_instance_id = format!("{}/{child_key}", self.instance_id);
        let invocation = crate::engine::Engine::spawn_workflow::<()>(
            &self.shared,
            workflow_name,
            child_instance_id,
            input_bytes,
        )
        .await?;
        Ok(invocation.into_parts())
    }

    /// Creates a fan-out builder that spawns child workflows for each item.
    ///
    /// Children execute concurrently (up to an optional limit set via
    /// [`FanOutBuilder::concurrency`]) while the parent workflow suspends.
    /// When all children complete, the parent resumes with the collected
    /// results.
    ///
    /// Use [`.run(closure)`](FanOutBuilder::run) for inline closures or
    /// [`.workflow(&DEF)`](FanOutBuilder::workflow) for registered workflows.
    ///
    /// # Panics
    ///
    /// Panics if `key` contains `/`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use memable::{Context, EngineError};
    ///
    /// async fn pipeline(ctx: Context) -> Result<(), EngineError> {
    ///     let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    ///     let results: Vec<String> = ctx.fan_out("process:v1", items)
    ///         .concurrency(2)
    ///         .run(|child_ctx, item| async move {
    ///             let upper: String = child_ctx.step("upper:v1")
    ///                 .run(async move || Ok(item.to_uppercase()))
    ///                 .await?;
    ///             Ok(upper)
    ///         }).await?;
    ///     assert_eq!(results, vec!["A", "B", "C"]);
    ///     Ok(())
    /// }
    /// ```
    #[must_use]
    pub fn fan_out<'a, I>(&'a self, key: &'a str, items: Vec<I>) -> FanOutBuilder<'a, I> {
        assert!(
            !key.contains('/'),
            "fan_out key must not contain '/': '{key}'"
        );
        FanOutBuilder {
            ctx: self,
            key,
            items,
            concurrency: None,
        }
    }

    /// Reads a step result from redb. Returns `None` on cache miss.
    fn read_step(&self, composite_key: &str) -> Result<Option<Vec<u8>>, EngineError> {
        let read_txn = self.shared.db.begin_read()?;
        let table = match read_txn.open_table(STEPS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(EngineError::from(e)),
        };
        match table.get(composite_key)? {
            Some(guard) => Ok(Some(guard.value().to_vec())),
            None => Ok(None),
        }
    }

    /// Writes a step result to redb.
    fn write_step(&self, composite_key: &str, value: &[u8]) -> Result<(), EngineError> {
        let write_txn = self.shared.db.begin_write()?;
        {
            let mut table = write_txn.open_table(STEPS)?;
            table.insert(composite_key, value)?;
        }
        write_txn.commit()?;
        Ok(())
    }
}

/// Builder for a suspend point with optional status message.
///
/// Created by [`Context::suspend`]. Implements [`IntoFuture`] so it can
/// be `.await`ed directly or chained with [`.status()`](SuspendBuilder::status).
///
/// # Examples
///
/// ```
/// use memable::{Context, EngineError, SuspendPoint};
///
/// const WAIT: SuspendPoint<String> = SuspendPoint::new("wait:v1");
/// const APPROVAL: SuspendPoint<bool> = SuspendPoint::new("approval:v1");
///
/// async fn workflow(ctx: Context) -> Result<(), EngineError> {
///     // Simple suspend — status defaults to the step key.
///     let payload: String = ctx.suspend(&WAIT).await?;
///
///     // Suspend with custom status message.
///     let approved: bool = ctx.suspend(&APPROVAL)
///         .status("Waiting for manager approval")
///         .await?;
///     Ok(())
/// }
/// ```
pub struct SuspendBuilder<'a, T> {
    ctx: &'a Context,
    key: &'a str,
    status_msg: Option<&'a str>,
    _marker: PhantomData<T>,
}

impl<'a, T> SuspendBuilder<'a, T>
where
    T: Serialize + DeserializeOwned + Send,
{
    /// Sets a custom status message for the suspended state.
    ///
    /// If not called, the status defaults to the step key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Context, EngineError, SuspendPoint};
    /// # const APPROVAL: SuspendPoint<bool> = SuspendPoint::new("approval:v1");
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> {
    /// let approved: bool = ctx.suspend(&APPROVAL)
    ///     .status("Waiting for manager approval")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn status(mut self, msg: &'a str) -> Self {
        self.status_msg = Some(msg);
        self
    }
}

impl<'a, T> IntoFuture for SuspendBuilder<'a, T>
where
    T: Serialize + DeserializeOwned + Send + 'a,
{
    type Output = Result<T, EngineError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.ctx.execute_suspend(self.key, self.status_msg) })
    }
}

/// Builder for a durable step with optional timeout.
///
/// Created by [`Context::step`]. Chain [`.timeout()`](StepBuilder::timeout)
/// to set a deadline, then call [`.run(closure)`](StepBuilder::run) to
/// execute.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use memable::{Context, EngineError};
///
/// async fn workflow(ctx: Context) -> Result<(), EngineError> {
///     // Simple step.
///     let v: String = ctx.step("fetch:v1").run(async || {
///         Ok("data".to_string())
///     }).await?;
///
///     // Step with timeout — clone data for the closure.
///     let v_clone = v.clone();
///     let processed: String = ctx.step("process:v1")
///         .timeout(Duration::from_secs(30))
///         .run(async move || {
///             Ok(v_clone.to_uppercase())
///         }).await?;
///     Ok(())
/// }
/// ```
pub struct StepBuilder<'a> {
    ctx: &'a Context,
    key: &'a str,
    timeout: Option<Duration>,
    retry_override: Option<RetryOverride>,
}

impl StepBuilder<'_> {
    /// Sets a timeout for the step execution.
    ///
    /// If the closure does not complete within `duration`, the step
    /// fails with [`EngineError::StepTimeout`] and no result is
    /// persisted. The workflow can be resumed to retry.
    ///
    /// The timeout applies only to the closure execution, not to
    /// cache lookups — a memoised result returns immediately
    /// regardless of timeout.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use memable::{Context, EngineError};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> {
    /// let v: String = ctx.step("query:v1")
    ///     .timeout(Duration::from_secs(10))
    ///     .run(async || {
    ///         Ok("result".to_string())
    ///     }).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Sets a per-step retry policy, overriding any engine-level default.
    ///
    /// When the closure returns [`StepError::Retryable`], the engine
    /// retries according to this policy. Permanent errors are never retried.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use memable::{Context, EngineError, RetryPolicy};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> {
    /// let v: String = ctx.step("fetch:v1")
    ///     .retry(RetryPolicy::exponential(3, Duration::from_secs(1)))
    ///     .run(async || {
    ///         Ok("data".to_string())
    ///     }).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_override = Some(RetryOverride::Custom(policy));
        self
    }

    /// Disables retry for this step, overriding any engine-level default.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Context, EngineError};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> {
    /// let v: i32 = ctx.step("once:v1")
    ///     .no_retry()
    ///     .run(async || Ok(42))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn no_retry(mut self) -> Self {
        self.retry_override = Some(RetryOverride::Disabled);
        self
    }

    /// Resolves the effective retry policy: per-step override > engine default > none.
    fn effective_retry(&self) -> Option<&RetryPolicy> {
        match &self.retry_override {
            Some(RetryOverride::Disabled) => None,
            Some(RetryOverride::Custom(policy)) => Some(policy),
            None => self.ctx.shared.default_retry.as_ref(),
        }
    }

    /// Executes the step with the given closure.
    ///
    /// If a cached result exists for this step key, it is returned
    /// without running the closure. Otherwise the closure executes
    /// (subject to any configured [`timeout`](StepBuilder::timeout)),
    /// and its result is persisted.
    ///
    /// When a [`RetryPolicy`] is active (via [`retry`](StepBuilder::retry)
    /// or [`EngineBuilder::default_retry`](crate::EngineBuilder::default_retry)),
    /// [`StepError::Retryable`] errors are retried with backoff.
    /// [`StepError::Permanent`] errors fail immediately.
    ///
    /// The closure must satisfy `AsyncFnMut` (not `AsyncFnOnce`) so it can
    /// be called multiple times during retry. Closures that capture from
    /// the workflow scope need `async move ||` with owned values — clone
    /// shared state (e.g. `Arc`) before each closure.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError`] if:
    /// - A storage operation fails ([`EngineError::Storage`])
    /// - Serialization or deserialization fails ([`EngineError::Serialization`])
    /// - The step closure returns a permanent error ([`EngineError::StepFailed`])
    /// - All retry attempts are exhausted ([`EngineError::RetriesExhausted`])
    /// - The step exceeds its timeout ([`EngineError::StepTimeout`])
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowDef};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// const EXAMPLE: WorkflowDef = WorkflowDef::new("example");
    /// let mut engine = Engine::builder().in_memory().build();
    /// engine.register(&EXAMPLE, |ctx: Context| async move {
    ///     let value: i32 = ctx.step("add:v1").run(async || {
    ///         Ok(1 + 2)
    ///     }).await?;
    ///     assert_eq!(value, 3);
    ///     Ok(())
    /// });
    /// engine.start().await?;
    /// engine.invoke(&EXAMPLE).await?.wait().await.unwrap_completed();
    /// # Ok(())
    /// # }
    /// ```
    #[expect(clippy::too_many_lines)]
    pub async fn run<F, T>(self, mut f: F) -> Result<T, EngineError>
    where
        F: AsyncFnMut() -> Result<T, StepError> + Send,
        T: Serialize + DeserializeOwned + Send,
    {
        let composite_key = format!(
            "{}/{}/{}",
            self.ctx.workflow_name, self.ctx.instance_id, self.key
        );
        let span = info_span!("step", key = self.key, composite_key = %composite_key);

        // Check for cached result.
        if let Some(bytes) = self.ctx.read_step(&composite_key)? {
            span.in_scope(|| info!("cache hit"));
            let data: StepData<T> = deserialize_step(&bytes, self.key)?;
            match data {
                StepData::Completed { result, status } => {
                    if let Some(status) = status {
                        self.ctx.status_tx.send_if_modified(|state| {
                            *state = WorkflowState::InProgress(status);
                            false
                        });
                    }
                    return Ok(result);
                }
                StepData::Suspended => {
                    return Err(EngineError::SuspendedStepConflict {
                        key: self.key.to_string(),
                    });
                }
                StepData::Failed { .. } => {
                    span.in_scope(|| info!("dead-letter found — re-executing"));
                }
            }
        }

        // Cache miss (or dead-letter cleared) — replay is over.
        self.ctx.replaying.store(false, Ordering::Release);

        let max_retries = self.effective_retry().map_or(0, |p| p.max_retries);
        let total_attempts = max_retries + 1;

        for attempt in 0..total_attempts {
            span.in_scope(|| {
                if attempt > 0 {
                    tracing::warn!(attempt = attempt + 1, max = total_attempts, "retrying step");
                } else {
                    info!("executing");
                }
            });

            let step_result = if let Some(duration) = self.timeout {
                tokio::time::timeout(duration, f())
                    .await
                    .map_err(|_| EngineError::StepTimeout {
                        key: self.key.to_string(),
                        duration,
                    })?
            } else {
                f().await
            };

            match step_result {
                Ok(result) => {
                    let data = StepData::Completed {
                        result,
                        status: self.ctx.current_status_string(),
                    };
                    let bytes = serialize_step(&data, self.key)?;
                    let StepData::Completed { result, .. } = data else {
                        unreachable!()
                    };
                    self.ctx.write_step(&composite_key, &bytes)?;
                    span.in_scope(|| info!("persisted"));
                    return Ok(result);
                }
                Err(StepError::Permanent(inner)) => {
                    let failed = StepData::<T>::Failed {
                        error: inner.to_string(),
                    };
                    let bytes = serialize_step(&failed, self.key)?;
                    self.ctx.write_step(&composite_key, &bytes)?;
                    return Err(EngineError::StepFailed {
                        key: self.key.to_string(),
                        source: inner,
                        retryable: false,
                    });
                }
                Err(StepError::Retryable(inner)) => {
                    let is_last = attempt + 1 >= total_attempts;
                    if is_last {
                        let failed = StepData::<T>::Failed {
                            error: inner.to_string(),
                        };
                        let bytes = serialize_step(&failed, self.key)?;
                        self.ctx.write_step(&composite_key, &bytes)?;
                        if max_retries == 0 {
                            return Err(EngineError::StepFailed {
                                key: self.key.to_string(),
                                source: inner,
                                retryable: true,
                            });
                        }
                        return Err(EngineError::RetriesExhausted {
                            key: self.key.to_string(),
                            attempts: total_attempts,
                            source: inner,
                        });
                    }
                    if let Some(policy) = self.effective_retry() {
                        let delay = policy.delay_for(attempt);
                        span.in_scope(|| {
                            tracing::warn!(
                                attempt = attempt + 1,
                                max = total_attempts,
                                error = %inner,
                                delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                                "retryable error — backing off"
                            );
                        });
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        unreachable!("loop should return before exhausting iterations")
    }
}

/// Builder for a fan-out operation that spawns child workflows.
///
/// Created by [`Context::fan_out`]. Chain
/// [`.concurrency(n)`](FanOutBuilder::concurrency) to limit parallelism,
/// then call [`.run(closure)`](FanOutBuilder::run) for inline closures or
/// [`.workflow(&DEF)`](FanOutBuilder::workflow) for registered workflows.
///
/// # Examples
///
/// ```no_run
/// use memable::{Context, EngineError};
///
/// async fn pipeline(ctx: Context) -> Result<(), EngineError> {
///     let items = vec![1, 2, 3];
///     let results: Vec<i32> = ctx.fan_out("double:v1", items)
///         .concurrency(2)
///         .run(|child_ctx, item| async move {
///             let doubled: i32 = child_ctx.step("calc:v1")
///                 .run(async move || Ok(item * 2))
///                 .await?;
///             Ok(doubled)
///         }).await?;
///     assert_eq!(results, vec![2, 4, 6]);
///     Ok(())
/// }
/// ```
pub struct FanOutBuilder<'a, I> {
    ctx: &'a Context,
    key: &'a str,
    items: Vec<I>,
    concurrency: Option<usize>,
}

impl<I> FanOutBuilder<'_, I> {
    /// Limits the number of children executing concurrently.
    ///
    /// If not set, all children run in parallel.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use memable::{Context, EngineError};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> {
    /// let results: Vec<String> = ctx.fan_out("batch:v1", vec!["a".into(), "b".into()])
    ///     .concurrency(1)
    ///     .run(|child_ctx, item: String| async move { Ok(item) })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = Some(n);
        self
    }

    /// Executes the fan-out with an inline closure.
    ///
    /// Each item is paired with a fresh [`Context`] and passed to the
    /// closure. The closure's return value is written as the child's
    /// output. Results are collected in item order.
    ///
    /// The parent task stays alive while children execute concurrently.
    /// If any child fails, remaining children are cancelled and the
    /// error is propagated.
    ///
    /// # Errors
    ///
    /// Returns the child's error if any child fails. Returns
    /// [`EngineError::Storage`] or [`EngineError::Serialization`] on
    /// persistence failures.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use memable::{Context, EngineError};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> {
    /// let results: Vec<String> = ctx.fan_out("shout:v1", vec!["hello".into()])
    ///     .run(|child_ctx, item: String| async move {
    ///         let upper: String = child_ctx.step("upper:v1")
    ///             .run(async move || Ok(item.to_uppercase()))
    ///             .await?;
    ///         Ok(upper)
    ///     }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run<F, T, Fut>(self, f: F) -> Result<Vec<T>, EngineError>
    where
        F: Fn(Context, I) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<T, EngineError>> + Send + 'static,
        I: Serialize + DeserializeOwned + Send + 'static,
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        use crate::engine::fan_out;

        let fan_out_step_key = format!("_fanout:{}", self.key);

        // Check for cached result (replay path).
        if let Some(cached) = fan_out::check_cached::<T>(self.ctx, &fan_out_step_key)? {
            return Ok(cached);
        }

        // No longer replaying.
        self.ctx
            .replaying
            .store(false, std::sync::atomic::Ordering::Release);

        let child_ids: Vec<String> = (0..self.items.len())
            .map(|idx| format!("{}/{}-{idx}", self.ctx.instance_id, self.key))
            .collect();

        let concurrency = self.concurrency.unwrap_or(self.items.len().max(1));

        // Write manifest for crash recovery.
        self.write_manifest(&fan_out_step_key, &child_ids)?;

        let closure = std::sync::Arc::new(f);
        let results = fan_out::run_inline(
            &self.ctx.shared,
            &self.ctx.workflow_name,
            &child_ids,
            self.items,
            closure,
            concurrency,
        )
        .await?;

        // Persist the collected results for memoisation.
        fan_out::write_result(self.ctx, &fan_out_step_key, &results)?;

        Ok(results)
    }

    /// Executes the fan-out using a registered workflow definition.
    ///
    /// Each item is serialized as the child workflow's input. The child's
    /// output type is determined by the [`WorkflowDef`]. Results are
    /// collected in item order.
    ///
    /// # Errors
    ///
    /// Returns the child's error if any child fails. Returns
    /// [`EngineError::WorkflowNotFound`] if the workflow is not registered.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use memable::{Context, EngineError, WorkflowDef};
    /// # const PROCESS: WorkflowDef<String, String> = WorkflowDef::new("process");
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> {
    /// let results: Vec<String> = ctx.fan_out("batch:v1", vec!["a".into(), "b".into()])
    ///     .workflow(&PROCESS)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn workflow<O>(self, def: &WorkflowDef<I, O>) -> Result<Vec<O>, EngineError>
    where
        I: Serialize + DeserializeOwned + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        use crate::engine::fan_out;

        let fan_out_step_key = format!("_fanout:{}", self.key);

        if let Some(cached) = fan_out::check_cached::<O>(self.ctx, &fan_out_step_key)? {
            return Ok(cached);
        }

        self.ctx
            .replaying
            .store(false, std::sync::atomic::Ordering::Release);

        let child_workflow_name = def.name().to_string();

        let child_ids: Vec<String> = (0..self.items.len())
            .map(|idx| format!("{}/{}-{idx}", self.ctx.instance_id, self.key))
            .collect();

        let concurrency = self.concurrency.unwrap_or(self.items.len().max(1));

        self.write_manifest(&fan_out_step_key, &child_ids)?;

        // Serialize each item as input bytes.
        let mut items_bytes = Vec::with_capacity(self.items.len());
        for item in self.items {
            let data = StepData::Completed {
                result: item,
                status: None,
            };
            let bytes = serialize_step(&data, "_input")?;
            items_bytes.push(bytes);
        }

        let results = fan_out::run_workflow::<O>(
            &self.ctx.shared,
            &child_workflow_name,
            &child_ids,
            items_bytes,
            concurrency,
        )
        .await?;

        fan_out::write_result(self.ctx, &fan_out_step_key, &results)?;

        Ok(results)
    }

    fn write_manifest(
        &self,
        fan_out_step_key: &str,
        child_ids: &[String],
    ) -> Result<(), EngineError> {
        let manifest_step_key = fan_out_step_key.replace("_fanout:", "_fanout_manifest:");
        let manifest = crate::engine::fan_out::FanOutManifest {
            child_instance_ids: child_ids.to_vec(),
            count: child_ids.len(),
        };
        let manifest_data = StepData::Completed {
            result: manifest,
            status: None,
        };
        let manifest_composite = format!(
            "{}/{}/{}",
            self.ctx.workflow_name, self.ctx.instance_id, manifest_step_key
        );
        let manifest_bytes = serialize_step(&manifest_data, &manifest_step_key)?;
        self.ctx.write_step(&manifest_composite, &manifest_bytes)
    }
}
