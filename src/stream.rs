use std::fmt;
use std::future::Future;
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

type ChangedFuture = Pin<
    Box<
        dyn Future<
                Output = (
                    Result<(), watch::error::RecvError>,
                    watch::Receiver<WorkflowState>,
                ),
            > + Send,
    >,
>;

enum Inner {
    Live(LiveState),
    Snapshot(Option<WorkflowState>),
}

enum LiveState {
    Initial(watch::Receiver<WorkflowState>),
    Idle(watch::Receiver<WorkflowState>),
    Waiting(ChangedFuture),
    Transitioning,
}

impl fmt::Debug for StatusStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Inner::Live(_) => f.debug_struct("StatusStream").field("kind", &"live").finish(),
            Inner::Snapshot(s) => {
                f.debug_struct("StatusStream").field("snapshot", s).finish()
            }
        }
    }
}

impl StatusStream {
    pub(crate) fn live(rx: watch::Receiver<WorkflowState>) -> Self {
        Self {
            inner: Inner::Live(LiveState::Initial(rx)),
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
    /// assert_eq!(state, Some(WorkflowState::Completed(None)));
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
            Inner::Live(state) => loop {
                match std::mem::replace(state, LiveState::Transitioning) {
                    LiveState::Initial(mut rx) => {
                        let value = rx.borrow_and_update().clone();
                        *state = LiveState::Idle(rx);
                        return Poll::Ready(Some(value));
                    }
                    LiveState::Idle(mut rx) => {
                        let fut = Box::pin(async move {
                            let result = rx.changed().await;
                            (result, rx)
                        });
                        *state = LiveState::Waiting(fut);
                    }
                    LiveState::Waiting(mut fut) => match fut.as_mut().poll(cx) {
                        Poll::Ready((Ok(()), mut rx)) => {
                            let value = rx.borrow_and_update().clone();
                            *state = LiveState::Idle(rx);
                            return Poll::Ready(Some(value));
                        }
                        Poll::Ready((Err(_), _)) => {
                            return Poll::Ready(None);
                        }
                        Poll::Pending => {
                            *state = LiveState::Waiting(fut);
                            return Poll::Pending;
                        }
                    },
                    LiveState::Transitioning => unreachable!(),
                }
            },
            Inner::Snapshot(state) => Poll::Ready(state.take()),
        }
    }
}
