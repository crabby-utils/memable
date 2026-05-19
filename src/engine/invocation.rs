use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use redb::Database;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;

use super::WorkflowState;
use super::execution::generate_instance_id;
use super::workflow::read_output;
use crate::context::{StepData, serialize_step};
use crate::error::EngineError;

/// Handle for a running workflow instance.
///
/// Returned by [`Engine::invoke`](super::Engine::invoke),
/// [`Engine::resume`](super::Engine::resume), and
/// [`Engine::signal`](super::Engine::signal). Provides the instance ID
/// and a status channel for observing [`WorkflowState`] transitions.
///
/// Call [`wait`](Invocation::wait) to consume the handle and obtain a
/// [`WaitResult`] whose [`Completed`] variant provides typed
/// [`output`](Completed::output) access. This prevents reading the
/// output before the workflow has finished.
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
/// engine.invoke(&GREET).await?.wait().await.unwrap_completed();
/// # Ok(())
/// # }
/// ```
pub struct Invocation<O = ()> {
    pub(super) instance_id: String,
    pub(super) status: watch::Receiver<WorkflowState>,
    pub(super) db: Arc<Database>,
    pub(super) workflow_name: String,
    pub(super) _marker: PhantomData<O>,
}

impl<O> Invocation<O> {
    /// Returns the instance ID for this invocation.
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
    /// let invocation = engine.invoke(&WF).await?;
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
    /// # use memable::{Engine, Context, EngineError, WorkflowState, WorkflowDef};
    /// # const WF: WorkflowDef = WorkflowDef::new("wf");
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register(&WF, wf);
    /// # engine.start().await?;
    /// let mut invocation = engine.invoke(&WF).await?;
    /// while invocation.status().changed().await.is_ok() {
    ///     if invocation.status().borrow().is_terminal() { break; }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn status(&mut self) -> &mut watch::Receiver<WorkflowState> {
        &mut self.status
    }

    /// Waits until the workflow reaches a terminal or suspended state,
    /// consuming the handle.
    ///
    /// Returns a [`WaitResult`] whose variants carry the terminal state.
    /// The [`Completed`] variant provides typed [`output`](Completed::output)
    /// access — this compile-time guarantee prevents reading output before
    /// the workflow has finished.
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
    /// engine.invoke(&WF).await?.wait().await.unwrap_completed();
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait(mut self) -> WaitResult<O> {
        let ws = loop {
            let ws = self.status.borrow().clone();
            if ws.is_terminal() || matches!(ws, WorkflowState::Suspended { .. }) {
                break ws;
            }
            if self.status.changed().await.is_err() {
                break self.status.borrow().clone();
            }
        };
        match ws {
            WorkflowState::Completed(status) => WaitResult::Completed(Completed {
                instance_id: self.instance_id,
                status,
                db: self.db,
                workflow_name: self.workflow_name,
                _marker: PhantomData,
            }),
            WorkflowState::Suspended { key, status } => WaitResult::Suspended { key, status },
            WorkflowState::Failed(msg) => WaitResult::Failed(msg),
            WorkflowState::Started | WorkflowState::InProgress(_) => {
                unreachable!("wait loops until terminal or suspended")
            }
        }
    }

    /// Decomposes into the instance ID and status receiver.
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
    /// let (id, mut status) = engine.invoke(&WF).await?.into_parts();
    /// println!("Started instance {id}");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn into_parts(self) -> (String, watch::Receiver<WorkflowState>) {
        (self.instance_id, self.status)
    }
}

/// Result of waiting for a workflow to reach a terminal or suspended state.
///
/// Returned by [`Invocation::wait`]. The [`Completed`] variant provides
/// typed [`output`](Completed::output) access, enforcing at compile time
/// that the output is only read after the workflow finishes.
///
/// # Examples
///
/// ```
/// use memable::{Engine, Context, EngineError, WorkflowDef, WaitResult};
///
/// const GREET: WorkflowDef = WorkflowDef::new("greet");
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut engine = Engine::builder().in_memory().build();
/// engine.register(&GREET, |ctx: Context| async move { Ok(()) });
/// engine.start().await?;
///
/// match engine.invoke(&GREET).await?.wait().await {
///     WaitResult::Completed(c) => println!("done, status: {:?}", c.status()),
///     WaitResult::Suspended { key, .. } => println!("waiting on {key}"),
///     WaitResult::Failed(msg) => println!("error: {msg}"),
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub enum WaitResult<O = ()> {
    /// The workflow completed successfully.
    Completed(Completed<O>),
    /// The workflow suspended, awaiting an external signal.
    Suspended {
        /// The step key identifying this suspend point.
        key: String,
        /// Human-readable status message.
        status: String,
    },
    /// The workflow failed with an error message.
    Failed(String),
}

impl<O> std::fmt::Display for WaitResult<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Completed(c) => match &c.status {
                None => write!(f, "completed"),
                Some(msg) => write!(f, "completed: {msg}"),
            },
            Self::Suspended { key, status } => write!(f, "suspended ({key}): {status}"),
            Self::Failed(msg) => write!(f, "failed: {msg}"),
        }
    }
}

impl<O> WaitResult<O> {
    /// Returns `true` if the workflow completed successfully.
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
    /// assert!(engine.invoke(&WF).await?.wait().await.is_completed());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }

    /// Returns `true` if the workflow is suspended.
    #[must_use]
    pub fn is_suspended(&self) -> bool {
        matches!(self, Self::Suspended { .. })
    }

    /// Returns `true` if the workflow failed.
    #[must_use]
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    /// Unwraps the [`Completed`] variant.
    ///
    /// # Panics
    ///
    /// Panics if the workflow did not complete successfully.
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
    /// let completed = engine.invoke(&WF).await?.wait().await.unwrap_completed();
    /// assert_eq!(completed.status(), None);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn unwrap_completed(self) -> Completed<O> {
        match self {
            Self::Completed(c) => c,
            Self::Suspended { key, status } => {
                panic!(
                    "called unwrap_completed on Suspended {{ key: {key:?}, status: {status:?} }}"
                )
            }
            Self::Failed(msg) => panic!("called unwrap_completed on Failed({msg:?})"),
        }
    }
}

/// A completed workflow invocation with typed output access.
///
/// Obtained from [`WaitResult::Completed`] after calling
/// [`Invocation::wait`]. For workflows with a non-`()` output type,
/// call [`output`](Completed::output) to read the result.
///
/// # Examples
///
/// ```
/// # use memable::{Engine, Context, EngineError, WorkflowDef};
/// const GREETING: WorkflowDef<String, String> = WorkflowDef::new("greeting");
///
/// async fn greeting(ctx: Context, name: String) -> Result<String, EngineError> {
///     Ok(format!("Hello, {name}!"))
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut engine = Engine::builder().in_memory().build();
/// engine.register(&GREETING, greeting);
/// engine.start().await?;
///
/// let completed = engine.invoke(&GREETING)
///     .input("Alice".to_string())
///     .await?
///     .wait().await
///     .unwrap_completed();
/// assert_eq!(completed.output()?, "Hello, Alice!");
/// # Ok(())
/// # }
/// ```
pub struct Completed<O = ()> {
    instance_id: String,
    status: Option<String>,
    db: Arc<Database>,
    workflow_name: String,
    _marker: PhantomData<O>,
}

impl<O> Completed<O> {
    /// Returns the instance ID for this invocation.
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
    /// let completed = engine.invoke(&WF).await?.wait().await.unwrap_completed();
    /// println!("Instance: {}", completed.instance_id());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Returns the last status message set before the workflow completed.
    ///
    /// Returns `None` if [`Context::set_status`](crate::Context::set_status)
    /// was never called.
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
    /// let completed = engine.invoke(&WF).await?.wait().await.unwrap_completed();
    /// assert_eq!(completed.status(), None);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn status(&self) -> Option<&str> {
        self.status.as_deref()
    }
}

impl<O> std::fmt::Debug for Completed<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Completed")
            .field("instance_id", &self.instance_id)
            .field("status", &self.status)
            .field("workflow_name", &self.workflow_name)
            .finish_non_exhaustive()
    }
}

impl<O: DeserializeOwned> Completed<O> {
    /// Reads the workflow output from the store.
    ///
    /// The output is written automatically by the engine when the workflow
    /// function returns `Ok(value)`.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Storage`] or [`EngineError::Serialization`]
    /// if the read fails, or [`EngineError::TypeMismatch`] if the stored
    /// type does not match `O`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowDef};
    /// const GREETING: WorkflowDef<String, String> = WorkflowDef::new("greeting");
    ///
    /// async fn greeting(ctx: Context, name: String) -> Result<String, EngineError> {
    ///     Ok(format!("Hello, {name}!"))
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut engine = Engine::builder().in_memory().build();
    /// engine.register(&GREETING, greeting);
    /// engine.start().await?;
    ///
    /// let completed = engine.invoke(&GREETING)
    ///     .input("Alice".to_string())
    ///     .await?
    ///     .wait().await
    ///     .unwrap_completed();
    /// assert_eq!(completed.output()?, "Hello, Alice!");
    /// # Ok(())
    /// # }
    /// ```
    pub fn output(&self) -> Result<O, EngineError> {
        read_output(&self.db, &self.workflow_name, &self.instance_id)
    }
}

/// Builder for invoking a workflow with optional typed input.
///
/// Created by [`Engine::invoke`](super::Engine::invoke). For workflows
/// that require input (`WorkflowDef<I, O>` where `I` is not `()`),
/// call [`.input(payload)`](Self::input) before `.await`ing the builder.
/// The compiler enforces this — `.await` is only available after input
/// is provided.
///
/// # Examples
///
/// ```
/// use memable::{Engine, Context, EngineError, WorkflowDef};
///
/// const GREET: WorkflowDef<String, String> = WorkflowDef::new("greet");
///
/// async fn greet(ctx: Context, name: String) -> Result<String, EngineError> {
///     Ok(format!("Hello, {name}!"))
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut engine = Engine::builder().in_memory().build();
/// engine.register(&GREET, greet);
/// engine.start().await?;
///
/// let result = engine.invoke(&GREET)
///     .input("Alice".to_string())
///     .await?
///     .wait().await
///     .unwrap_completed()
///     .output()?;
/// assert_eq!(result, "Hello, Alice!");
/// # Ok(())
/// # }
/// ```
pub struct InvocationBuilder<'a, I = (), O = ()> {
    pub(super) engine: &'a super::Engine,
    pub(super) workflow_name: String,
    pub(super) input_payload: Result<Option<Vec<u8>>, EngineError>,
    pub(super) _marker: PhantomData<(I, O)>,
}

impl<'a, I: Serialize, O> InvocationBuilder<'a, I, O> {
    /// Attaches a typed input payload to the workflow invocation.
    ///
    /// The payload is serialized immediately and stored as a step entry
    /// with the reserved key `_input` before the workflow task spawns.
    /// The workflow receives it as a function parameter.
    ///
    /// For `WorkflowDef<I, O>` where `I` is not `()`, this method must
    /// be called before `.await` — the compiler enforces this.
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowState, WorkflowDef};
    /// # const WF: WorkflowDef<i32> = WorkflowDef::new("wf");
    /// # async fn wf(ctx: Context, val: i32) -> Result<(), EngineError> {
    /// #     assert_eq!(val, 42);
    /// #     Ok(())
    /// # }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register(&WF, wf);
    /// # engine.start().await?;
    /// let mut inv = engine.invoke(&WF).input(42_i32).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn input(self, payload: I) -> InvocationBuilder<'a, (), O> {
        let data: StepData<I> = StepData::Completed {
            result: payload,
            status: None,
        };
        InvocationBuilder {
            engine: self.engine,
            workflow_name: self.workflow_name,
            input_payload: serialize_step(&data, "_input").map(Some),
            _marker: PhantomData,
        }
    }
}

impl<'a, O: Send + 'a> IntoFuture for InvocationBuilder<'a, (), O> {
    type Output = Result<Invocation<O>, EngineError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let input_bytes = self.input_payload?;
            super::Engine::spawn_workflow(
                &self.engine.shared,
                &self.workflow_name,
                generate_instance_id(),
                input_bytes,
            )
            .await
        })
    }
}
