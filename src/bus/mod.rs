use crate::codec::Codec;
use crate::errors::BusError;
use crate::raw_message::RawMessage;
use bytes::Bytes;
use futures::Stream;
use serde::Serialize;
use std::collections::HashMap;
use std::future::Future;
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
    fn dispatch<'a>(
        &'a self,
        subject: &'a str,
        msg: Self::Message,
    ) -> impl Future<Output = Result<(), BusError>> + Send + 'a;

    async fn subscribe(&self, pattern: &str) -> Result<Self::Subscription, BusError>;
    async fn subscribe_group(
        &self,
        pattern: &str,
        group: &str,
    ) -> Result<Self::Subscription, BusError>;
}

/// Implementation capability for byte-oriented external bus backends.
///
/// Implementors provide transport-specific byte publishing and subscriptions.
/// The blanket [`Bus`] implementation supplies `RawMessage` dispatch.
#[allow(async_fn_in_trait)]
pub trait RawBus: Send + Sync {
    type Codec: crate::codec::Codec;
    type RawSubscription: Stream<Item = RawMessage> + Send + Unpin + 'static;

    fn codec(&self) -> &Self::Codec;

    fn publish<'a, T: Serialize>(
        &'a self,
        subject: &'a str,
        value: &T,
        headers: Option<HashMap<String, String>>,
    ) -> impl Future<Output = Result<(), BusError>> + Send + 'a {
        let payload = self.codec().encode(value);

        async move { self.publish_bytes(subject, payload?, headers).await }
    }

    fn publish_bytes<'a>(
        &'a self,
        subject: &'a str,
        payload: Bytes,
        headers: Option<HashMap<String, String>>,
    ) -> impl Future<Output = Result<(), BusError>> + Send + 'a;

    fn dispatch_raw<'a>(
        &'a self,
        subject: &'a str,
        message: RawMessage,
    ) -> impl Future<Output = Result<(), BusError>> + Send + 'a {
        async move {
            self.publish_bytes(subject, message.payload, message.envelope.headers)
                .await
        }
    }

    async fn subscribe_raw(&self, pattern: &str) -> Result<Self::RawSubscription, BusError>;
    async fn subscribe_group_raw(
        &self,
        pattern: &str,
        group: &str,
    ) -> Result<Self::RawSubscription, BusError>;
}

impl<T> Bus for T
where
    T: RawBus,
{
    type Message = RawMessage;
    type Subscription = T::RawSubscription;

    fn dispatch<'a>(
        &'a self,
        subject: &'a str,
        message: RawMessage,
    ) -> impl Future<Output = Result<(), BusError>> + Send + 'a {
        self.dispatch_raw(subject, message)
    }

    async fn subscribe(&self, pattern: &str) -> Result<Self::Subscription, BusError> {
        self.subscribe_raw(pattern).await
    }

    async fn subscribe_group(
        &self,
        pattern: &str,
        group: &str,
    ) -> Result<Self::Subscription, BusError> {
        self.subscribe_group_raw(pattern, group).await
    }
}
