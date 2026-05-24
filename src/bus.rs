use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use crate::errors::BusError;

/// Unified stream returned by `subscribe` and `consume` for any bus backend.
pub struct BusStream<M: Clone + Send + 'static> {
    inner: ReceiverStream<M>,
}

impl<M: Clone + Send + 'static> BusStream<M> {
    pub fn new(rx: mpsc::Receiver<M>) -> Self {
        Self { inner: ReceiverStream::new(rx) }
    }
}

impl<M: Clone + Send + 'static> Stream for BusStream<M> {
    type Item = M;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<M>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Unified trait for all bus backends.
///
/// `Message` is the native payload carrier for a given backend:
/// - `InternalBus` → `AnyMessage`  (typed, zero-copy, no serde)
/// - `NngBus<C>` / `RedisBus<C>` → `RawMessage`  (bytes, decoded with `C`)
///
/// Typed `publish` lives as an inherent method on each concrete type because the
/// required bounds differ (`Any + Send + Sync` vs `Serialize`).
#[allow(async_fn_in_trait)]
pub trait Bus: Send + Sync {
    type Message: Clone + Send + 'static;

    /// Route and deliver an already-constructed message to local subscribers.
    async fn dispatch(
        &self,
        subject: &str,
        msg: Self::Message,
    ) -> Result<(), BusError>;

    async fn subscribe(&self, pattern: &str) -> Result<BusStream<Self::Message>, BusError>;
    async fn bind_queue(&self, pattern: &str, queue: &str) -> Result<(), BusError>;
    async fn consume(&self, queue: &str) -> Result<BusStream<Self::Message>, BusError>;
}
