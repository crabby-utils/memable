//! # memable
//!
//! An embeddable durable execution engine for Rust.
//!
//! Unlike platforms such as Temporal or Restate, memable is a **library** — it
//! runs inside your process with zero external dependencies. Workflows recover
//! via **key-based memoisation** rather than positional replay, so you can
//! deploy new code, fix bugs, and selectively re-execute steps without breaking
//! in-flight workflows.
//!
//! ## Quick start
//!
//! ```
//! use memable::{Engine, Context, EngineError, WorkflowState};
//!
//! async fn greet(ctx: Context) -> Result<(), EngineError> {
//!     let name: String = ctx.step("fetch-name:v1").run(async || {
//!         Ok("world".to_string())
//!     }).await?;
//!     let message: String = ctx.step("format-greeting:v1").run(async move || {
//!         Ok(format!("Hello, {name}!"))
//!     }).await?;
//!     assert_eq!(message, "Hello, world!");
//!     Ok(())
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut engine = Engine::builder().in_memory().build();
//! engine.register("greet", greet);
//! engine.start().await?;
//!
//! let state = engine.invoke("greet").await?.wait().await;
//! assert_eq!(state, WorkflowState::Completed(None));
//! # Ok(())
//! # }
//! ```
//!
//! ## Automatic retry
//!
//! Steps that return [`StepError::Retryable`] are retried automatically
//! when a [`RetryPolicy`] is configured. Permanent errors bypass retries.
//!
//! ```
//! use std::time::Duration;
//! use memable::{Engine, Context, EngineError, StepError, RetryPolicy, WorkflowState};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut engine = Engine::builder()
//!     .in_memory()
//!     .default_retry(RetryPolicy::exponential(3, Duration::from_secs(1)))
//!     .build();
//!
//! engine.register("fetch", |ctx: Context| async move {
//!     let data: String = ctx.step("call-api:v1")
//!         .run(async || {
//!             // Returning Retryable triggers automatic retry with backoff.
//!             // Returning Permanent would fail immediately.
//!             Ok("response".to_string())
//!         }).await?;
//!     Ok(())
//! });
//! engine.start().await?;
//!
//! let state = engine.invoke("fetch").await?.wait().await;
//! assert_eq!(state, WorkflowState::Completed(None));
//! # Ok(())
//! # }
//! ```
//!
//! ## Closure captures
//!
//! Step closures use `AsyncFnMut` bounds to support retry. Closures that
//! capture variables from the workflow scope need `async move ||` with
//! owned values — clone shared state before each closure:
//!
//! ```
//! # use memable::{Context, EngineError};
//! # use std::sync::{Arc, atomic::{AtomicU32, Ordering}};
//! # async fn wf(ctx: Context) -> Result<(), EngineError> {
//! let counter = Arc::new(AtomicU32::new(0));
//!
//! let c = Arc::clone(&counter);
//! let _: u32 = ctx.step("count:v1").run(async move || {
//!     Ok(c.fetch_add(1, Ordering::Relaxed))
//! }).await?;
//! # Ok(())
//! # }
//! ```

mod context;
mod engine;
mod error;
mod metadata;
mod retry;
mod stream;

pub use context::{Context, StepBuilder, SuspendBuilder, SuspendPoint};
pub use engine::{
    Engine, EngineBuilder, HasStore, Invocation, InvocationBuilder, NoStore, WorkflowState,
};
pub use error::{EngineError, StepError, SubscribeError};
pub use metadata::{MetadataStatus, WorkflowMetadata};
pub use retry::RetryPolicy;
pub use stream::StatusStream;
