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

pub mod adapter;
pub mod codec;
pub mod in_memory_bus;
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

pub use adapter::{BrokerSource, BrokerTarget};

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
/// - `InMemoryBus` → `AnyMessage`  (typed, zero-copy, no serde)
/// - external buses → `RawMessage`  (bytes, decoded with `C`)
///
/// `InMemoryBus` publishes Rust objects directly. [`ExternalBus`] supplies the
/// serialized publish operation for byte-oriented backends.
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
pub trait ExternalBus: Send + Sync {
    type Codec: crate::codec::Codec;
    type Subscription: Stream<Item = RawMessage> + Send + Unpin + 'static;

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

    async fn subscribe_raw(&self, pattern: &str) -> Result<Self::Subscription, BusError>;
    async fn subscribe_group_raw(
        &self,
        pattern: &str,
        group: &str,
    ) -> Result<Self::Subscription, BusError>;
}

impl<T> Bus for T
where
    T: ExternalBus,
{
    type Message = RawMessage;
    type Subscription = <T as ExternalBus>::Subscription;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonCodec;
    use crate::message::Envelope;
    use futures::stream;
    use serde::de::DeserializeOwned;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    struct Published {
        subject: String,
        payload: Bytes,
        headers: Option<HashMap<String, String>>,
    }

    struct RecordingExternalBus<C> {
        codec: C,
        published: mpsc::UnboundedSender<Published>,
        subscriptions: Arc<AtomicUsize>,
        group_subscriptions: Arc<AtomicUsize>,
    }

    impl<C: Codec> ExternalBus for RecordingExternalBus<C> {
        type Codec = C;
        type Subscription = stream::Empty<RawMessage>;

        fn codec(&self) -> &Self::Codec {
            &self.codec
        }

        fn publish_bytes<'a>(
            &'a self,
            subject: &'a str,
            payload: Bytes,
            headers: Option<HashMap<String, String>>,
        ) -> impl Future<Output = Result<(), BusError>> + Send + 'a {
            let published = self.published.clone();
            async move {
                published
                    .send(Published {
                        subject: subject.to_string(),
                        payload,
                        headers,
                    })
                    .map_err(|_| BusError::Internal("test publisher stopped".into()))
            }
        }

        async fn subscribe_raw(&self, _pattern: &str) -> Result<Self::Subscription, BusError> {
            self.subscriptions.fetch_add(1, Ordering::Relaxed);
            Ok(stream::empty())
        }

        async fn subscribe_group_raw(
            &self,
            _pattern: &str,
            _group: &str,
        ) -> Result<Self::Subscription, BusError> {
            self.group_subscriptions.fetch_add(1, Ordering::Relaxed);
            Ok(stream::empty())
        }
    }

    fn recording_bus<C>(codec: C) -> (RecordingExternalBus<C>, mpsc::UnboundedReceiver<Published>) {
        let (published, receiver) = mpsc::unbounded_channel();
        (
            RecordingExternalBus {
                codec,
                published,
                subscriptions: Arc::new(AtomicUsize::new(0)),
                group_subscriptions: Arc::new(AtomicUsize::new(0)),
            },
            receiver,
        )
    }

    #[tokio::test]
    async fn external_bus_default_publish_encodes_payload_and_preserves_headers() {
        let (bus, mut published) = recording_bus(JsonCodec);
        let headers = HashMap::from([("trace_id".to_string(), "trace-1".to_string())]);

        bus.publish("orders.created", &42u32, Some(headers.clone()))
            .await
            .unwrap();

        let message = published.recv().await.unwrap();
        assert_eq!(message.subject, "orders.created");
        assert_eq!(message.headers, Some(headers));
        assert_eq!(JsonCodec.decode::<u32>(&message.payload).unwrap(), 42);
    }

    #[tokio::test]
    async fn bus_blanket_dispatch_forwards_raw_payload_without_encoding() {
        let (bus, mut published) = recording_bus(JsonCodec);
        let payload = Bytes::from_static(b"already encoded");
        let headers = HashMap::from([("trace_id".to_string(), "trace-2".to_string())]);
        let message = RawMessage {
            envelope: Envelope {
                subject: "ignored.envelope.subject".to_string(),
                timestamp: Instant::now(),
                id: 7,
                headers: Some(headers.clone()),
                attempts: 0,
            },
            payload: payload.clone(),
        };

        Bus::dispatch(&bus, "orders.created", message)
            .await
            .unwrap();

        let message = published.recv().await.unwrap();
        assert_eq!(message.subject, "orders.created");
        assert_eq!(message.payload, payload);
        assert_eq!(message.headers, Some(headers));
    }

    #[tokio::test]
    async fn bus_blanket_implementation_forwards_subscriptions() {
        let (bus, _published) = recording_bus(JsonCodec);
        let subscriptions = Arc::clone(&bus.subscriptions);
        let group_subscriptions = Arc::clone(&bus.group_subscriptions);

        let _ = Bus::subscribe(&bus, "orders.*").await.unwrap();
        let _ = Bus::subscribe_group(&bus, "jobs.*", "workers")
            .await
            .unwrap();

        assert_eq!(subscriptions.load(Ordering::Relaxed), 1);
        assert_eq!(group_subscriptions.load(Ordering::Relaxed), 1);
    }

    struct FailingCodec;

    impl Codec for FailingCodec {
        fn encode<T: Serialize>(&self, _value: &T) -> Result<Bytes, BusError> {
            Err(BusError::Serialization("intentional failure".into()))
        }

        fn decode<T: DeserializeOwned>(&self, _bytes: &[u8]) -> Result<T, BusError> {
            Err(BusError::Serialization("intentional failure".into()))
        }
    }

    #[tokio::test]
    async fn external_bus_publish_does_not_call_transport_when_encoding_fails() {
        let (bus, mut published) = recording_bus(FailingCodec);

        let result = bus.publish("orders.created", &42u32, None).await;

        assert!(matches!(result, Err(BusError::Serialization(_))));
        assert!(matches!(
            published.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}
