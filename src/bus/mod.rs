use crate::errors::BusError;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub mod codec;
pub mod internal_bus;
pub(crate) mod internal_router;
#[cfg(any(feature = "nng", feature = "redis"))]
pub(crate) mod local_router;
pub(crate) mod message;
#[cfg(feature = "nats")]
pub mod nats_bus;
#[cfg(feature = "nng")]
pub mod nng_bus;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq_bus;
pub mod raw_message;
#[cfg(feature = "redis")]
pub mod redis_bus;
pub(crate) mod routing;
#[cfg(any(feature = "nng", feature = "redis"))]
pub(crate) mod wire;

/// Channel-backed stream used by local-routed buses.
pub struct BusStream<M: Clone + Send + 'static> {
    inner: ReceiverStream<M>,
}

impl<M: Clone + Send + 'static> BusStream<M> {
    pub fn new(rx: mpsc::Receiver<M>) -> Self {
        Self {
            inner: ReceiverStream::new(rx),
        }
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
/// - external buses → `RawMessage`  (bytes, decoded with `C`)
///
/// Typed `publish` lives as an inherent method on each concrete type because the
/// required bounds differ (`Any + Send + Sync` vs `Serialize`).
#[allow(async_fn_in_trait)]
pub trait Bus: Send + Sync {
    type Message: Clone + Send + 'static;
    type Subscription: Stream<Item = Self::Message> + Send + Unpin + 'static;

    /// Route and deliver an already-constructed message to local subscribers.
    async fn dispatch(&self, subject: &str, msg: Self::Message) -> Result<(), BusError>;

    async fn subscribe(&self, pattern: &str) -> Result<Self::Subscription, BusError>;
    async fn subscribe_group(
        &self,
        pattern: &str,
        group: &str,
    ) -> Result<Self::Subscription, BusError>;
}
