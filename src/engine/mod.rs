mod builder;
mod engine_impl;
mod execution;
pub(crate) mod fan_out;
mod invocation;
mod retention;
mod state;
mod workflow;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};

use redb::Database;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::context::Context;
use crate::error::EngineError;
use crate::retry::RetryPolicy;

/// Type-erased workflow function.
pub(crate) type WorkflowFn = Arc<
    dyn Fn(Context) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send>> + Send + Sync,
>;

pub(crate) type Senders = Arc<Mutex<HashMap<String, watch::Sender<state::WorkflowState>>>>;

/// Shared engine state accessible from both [`Engine`] and [`Context`].
pub(crate) struct EngineShared {
    pub(crate) db: Arc<Database>,
    pub(crate) workflows: HashMap<String, WorkflowFn>,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) tasks: Arc<tokio::sync::Mutex<JoinSet<()>>>,
    pub(crate) timer_serial: Arc<AtomicU64>,
    pub(crate) default_retry: Option<RetryPolicy>,
    pub(crate) senders: Senders,
}

pub use self::builder::{EngineBuilder, HasStore, NoStore};
pub use self::engine_impl::{Engine, Registration};
pub use self::invocation::{Completed, Invocation, InvocationBuilder, WaitResult};
pub use self::state::WorkflowState;
pub use self::workflow::IntoWorkflow;
