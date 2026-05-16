use std::pin::Pin;
use std::task::{self, Poll};

use futures_core::Stream;
use tokio::sync::watch;

use crate::engine::WorkflowState;

/// A stream of [`WorkflowState`] updates for a workflow instance.
///
/// Created by [`Engine::subscribe`](crate::Engine::subscribe). The stream
/// immediately yields the current state, then yields each subsequent change.
/// It ends when the workflow completes, fails, or is replaced by a new run
/// (e.g. after [`Engine::signal`](crate::Engine::signal)).
///
/// # Examples
///
/// ```
/// # use memable::{Engine, Context, EngineError, WorkflowState, StatusStream};
/// use futures_core::Stream;
///
/// # async fn wf(ctx: Context) -> Result<(), EngineError> { Ok(()) }
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut engine = Engine::builder().in_memory().build();
/// engine.register("wf", wf);
/// engine.start().await?;
///
/// let inv = engine.invoke("wf").await?;
/// let id = inv.instance_id().to_string();
///
/// // Subscribe returns a Stream<Item = WorkflowState>
/// let stream: StatusStream = engine.subscribe("wf", &id).unwrap();
/// # Ok(())
/// # }
/// ```
pub struct StatusStream {
    inner: Inner,
}

enum Inner {
    Live {
        rx: watch::Receiver<WorkflowState>,
        initial_pending: bool,
    },
    Snapshot(Option<WorkflowState>),
}

impl StatusStream {
    pub(crate) fn live(rx: watch::Receiver<WorkflowState>) -> Self {
        Self {
            inner: Inner::Live {
                rx,
                initial_pending: true,
            },
        }
    }

    pub(crate) fn snapshot(state: WorkflowState) -> Self {
        Self {
            inner: Inner::Snapshot(Some(state)),
        }
    }

    /// Returns the next state change, or `None` when the stream ends.
    ///
    /// Equivalent to `StreamExt::next()` but available without extra
    /// dependencies.
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
    /// # let inv = engine.invoke("wf").await?;
    /// # let id = inv.instance_id().to_string();
    /// # inv.wait().await;
    /// let mut stream = engine.subscribe("wf", &id).unwrap();
    /// let state = stream.next().await;
    /// assert_eq!(state, Some(WorkflowState::Completed));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next(&mut self) -> Option<WorkflowState> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }
}

impl Stream for StatusStream {
    type Item = WorkflowState;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match &mut this.inner {
            Inner::Live {
                rx,
                initial_pending,
            } => {
                if *initial_pending {
                    *initial_pending = false;
                    return Poll::Ready(Some(rx.borrow_and_update().clone()));
                }
                let result = {
                    let changed = rx.changed();
                    tokio::pin!(changed);
                    changed.poll(cx)
                };
                match result {
                    Poll::Ready(Ok(())) => Poll::Ready(Some(rx.borrow_and_update().clone())),
                    Poll::Ready(Err(_)) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                }
            }
            Inner::Snapshot(state) => Poll::Ready(state.take()),
        }
    }
}
