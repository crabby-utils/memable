use std::future::{Future, IntoFuture};
use std::pin::Pin;

use serde::Serialize;
use tokio::sync::watch;

use super::WorkflowState;
use super::execution::generate_instance_id;
use crate::context::{StepData, serialize_step};
use crate::error::EngineError;

/// Handle for a running workflow instance.
///
/// Returned by [`Engine::invoke`](super::Engine::invoke) and [`Engine::resume`](super::Engine::resume). Provides the
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
/// assert_eq!(state, WorkflowState::Completed(None));
/// # Ok(())
/// # }
/// ```
pub struct Invocation {
    pub(super) instance_id: String,
    pub(super) status: watch::Receiver<WorkflowState>,
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
    /// assert_eq!(state, WorkflowState::Completed(None));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait(mut self) -> WorkflowState {
        loop {
            let ws = self.status.borrow().clone();
            if ws.is_terminal() || matches!(ws, WorkflowState::Suspended { .. }) {
                return ws;
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

/// Builder for invoking a workflow with optional typed input.
///
/// Created by [`Engine::invoke`](super::Engine::invoke). Call [`.input(payload)`](Self::input) to
/// attach data the workflow can read via [`Context::input`](crate::Context::input), then `.await`
/// the builder to start the workflow.
///
/// # Examples
///
/// ```
/// use memable::{Engine, Context, EngineError, WorkflowState};
///
/// async fn greet(ctx: Context) -> Result<(), EngineError> {
///     let name: String = ctx.input::<String>()?.unwrap_or("world".into());
///     Ok(())
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut engine = Engine::builder().in_memory().build();
/// engine.register("greet", greet);
/// engine.start().await?;
///
/// // Without input:
/// let inv = engine.invoke("greet").await?;
///
/// // With typed input:
/// let inv = engine.invoke("greet").input("Alice".to_string()).await?;
/// # Ok(())
/// # }
/// ```
pub struct InvocationBuilder<'a> {
    pub(super) engine: &'a super::Engine,
    pub(super) workflow_name: String,
    pub(super) input_payload: Result<Option<Vec<u8>>, EngineError>,
}

impl InvocationBuilder<'_> {
    /// Attaches a typed input payload to the workflow invocation.
    ///
    /// The payload is serialized immediately and stored as a step entry
    /// with the reserved key `_input` before the workflow task spawns.
    /// The workflow reads it via [`Context::input`](crate::Context::input).
    ///
    /// # Examples
    ///
    /// ```
    /// # use memable::{Engine, Context, EngineError, WorkflowState};
    /// # async fn wf(ctx: Context) -> Result<(), EngineError> {
    /// #     let val: i32 = ctx.input::<i32>()?.unwrap();
    /// #     assert_eq!(val, 42);
    /// #     Ok(())
    /// # }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut engine = Engine::builder().in_memory().build();
    /// # engine.register("wf", wf);
    /// # engine.start().await?;
    /// let inv = engine.invoke("wf").input(42_i32).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn input<T: Serialize>(mut self, payload: T) -> Self {
        let data: StepData<T> = StepData::Completed {
            result: payload,
            status: None,
        };
        self.input_payload = serialize_step(&data, "_input").map(Some);
        self
    }
}

impl<'a> IntoFuture for InvocationBuilder<'a> {
    type Output = Result<Invocation, EngineError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let input_bytes = self.input_payload?;
            self.engine
                .spawn_workflow(&self.workflow_name, generate_instance_id(), input_bytes)
                .await
        })
    }
}
