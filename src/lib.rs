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
//!     let message: String = ctx.step("format-greeting:v1").run(async || {
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
//! assert_eq!(state, WorkflowState::Completed);
//! # Ok(())
//! # }
//! ```

mod context;
mod engine;
mod error;
mod metadata;

pub use context::{Context, StepBuilder, SuspendBuilder};
pub use engine::{Engine, EngineBuilder, Invocation, WorkflowState};
pub use error::{EngineError, StepError};
pub use metadata::{MetadataStatus, WorkflowMetadata};
