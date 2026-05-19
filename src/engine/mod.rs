mod builder;
mod engine_impl;
mod execution;
mod invocation;
mod retention;
mod state;
mod workflow;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::context::Context;
use crate::error::EngineError;

/// Type-erased workflow function.
pub(crate) type WorkflowFn = Arc<
    dyn Fn(Context) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send>> + Send + Sync,
>;

pub(crate) type Senders = Arc<Mutex<HashMap<String, watch::Sender<state::WorkflowState>>>>;

pub use self::builder::{EngineBuilder, HasStore, NoStore};
pub use self::engine_impl::{Engine, Registration};
pub use self::invocation::{Completed, Invocation, InvocationBuilder, WaitResult};
pub use self::state::WorkflowState;
pub use self::workflow::IntoWorkflow;
