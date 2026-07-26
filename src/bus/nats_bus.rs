use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use futures::Stream;

use crate::bus::ExternalBus;
use crate::codec::{Codec, JsonCodec};
use crate::errors::{BackendError, BusError};
use crate::message::Envelope;
use crate::raw_message::RawMessage;

// ── NatsBus ───────────────────────────────────────────────────────────────────
// NATS server handles all wildcard matching, fanout, and queue groups natively.
// SubjectRouter is NOT used here.

pub struct NatsBus<C: Codec = JsonCodec> {
    client: async_nats::Client,
    codec: C,
    next_msg_id: Arc<AtomicU64>,
}

impl<C: Codec> NatsBus<C> {
    pub async fn connect(codec: C, url: &str) -> Result<Self, BusError> {
        let client = async_nats::connect(url)
            .await
            .map_err(|e| BusError::Connection(e.to_string()))?;
        Ok(Self {
            client,
            codec,
            next_msg_id: Arc::new(AtomicU64::new(1)),
        })
    }
}

fn headers_to_nats(
    headers: HashMap<String, String>,
) -> Result<async_nats::header::HeaderMap, BusError> {
    let mut header_map = async_nats::header::HeaderMap::new();
    for (k, v) in headers {
        let name: async_nats::header::HeaderName =
            k.parse()
                .map_err(|e: async_nats::header::ParseHeaderNameError| {
                    BusError::Internal(e.to_string())
                })?;
        let value: async_nats::header::HeaderValue =
            v.parse()
                .map_err(|e: async_nats::header::ParseHeaderValueError| {
                    BusError::Internal(e.to_string())
                })?;
        header_map.insert(name, value);
    }
    Ok(header_map)
}

fn nats_msg_to_raw(msg: async_nats::Message, id: u64) -> RawMessage {
    let headers = msg.headers.map(|h| {
        h.iter()
            .map(|(k, vs)| {
                (
                    k.to_string(),
                    vs.first().map(|v| v.to_string()).unwrap_or_default(),
                )
            })
            .collect::<HashMap<String, String>>()
    });
    RawMessage {
        envelope: Envelope {
            id,
            subject: msg.subject.to_string(),
            timestamp: Instant::now(),
            headers,
            attempts: 0,
        },
        payload: msg.payload,
    }
}

pub struct NatsStream {
    sub: async_nats::Subscriber,
    next_id: Arc<AtomicU64>,
}

impl NatsStream {
    fn new(sub: async_nats::Subscriber, next_id: Arc<AtomicU64>) -> Self {
        Self { sub, next_id }
    }
}

impl Stream for NatsStream {
    type Item = RawMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<RawMessage>> {
        match Pin::new(&mut self.sub).poll_next(cx) {
            Poll::Ready(Some(msg)) => {
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                Poll::Ready(Some(nats_msg_to_raw(msg, id)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<C: Codec + 'static> ExternalBus for NatsBus<C> {
    type Codec = C;
    type Subscription = NatsStream;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn publish_bytes<'a>(
        &'a self,
        subject: &'a str,
        payload: bytes::Bytes,
        headers: Option<HashMap<String, String>>,
    ) -> impl std::future::Future<Output = Result<(), BusError>> + Send + 'a {
        async move {
            if let Some(my_headers) = headers {
                let header_map = headers_to_nats(my_headers)?;
                self.client
                    .publish_with_headers(subject.to_string(), header_map, payload)
                    .await
                    .map_err(|e| BusError::Backend(BackendError::NatsPublish(e)))?;
            } else {
                self.client
                    .publish(subject.to_string(), payload)
                    .await
                    .map_err(|e| BusError::Backend(BackendError::NatsPublish(e)))?;
            }
            Ok(())
        }
    }

    async fn subscribe_raw(&self, pattern: &str) -> Result<Self::Subscription, BusError> {
        let sub = self
            .client
            .subscribe(pattern.to_string())
            .await
            .map_err(|e| BusError::Backend(BackendError::NatsSubscribe(e)))?;
        self.client
            .flush()
            .await
            .map_err(|e| BusError::Backend(BackendError::NatsFlush(e)))?;
        Ok(NatsStream::new(sub, Arc::clone(&self.next_msg_id)))
    }

    async fn subscribe_group_raw(
        &self,
        pattern: &str,
        group: &str,
    ) -> Result<Self::Subscription, BusError> {
        let sub = self
            .client
            .queue_subscribe(pattern.to_string(), group.to_string())
            .await
            .map_err(|e| BusError::Backend(BackendError::NatsSubscribe(e)))?;
        self.client
            .flush()
            .await
            .map_err(|e| BusError::Backend(BackendError::NatsFlush(e)))?;
        Ok(NatsStream::new(sub, Arc::clone(&self.next_msg_id)))
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::Bus;
    use crate::codec::JsonCodec;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::timeout;

    const NATS_URL: &str = "nats://localhost:4222";
    const DELIVERY_TIMEOUT: Duration = Duration::from_secs(2);

    /// Try to connect; return `None` (and print a notice) if NATS is not up.
    async fn try_bus() -> Option<NatsBus<JsonCodec>> {
        match NatsBus::connect(JsonCodec, NATS_URL).await {
            Ok(bus) => Some(bus),
            Err(_) => {
                eprintln!("skipping: NATS not available at {NATS_URL}");
                None
            }
        }
    }

    macro_rules! nats_bus {
        () => {
            match try_bus().await {
                Some(b) => b,
                None => return,
            }
        };
    }

    #[tokio::test]
    async fn test_nats_pub_sub() {
        let bus = nats_bus!();
        let mut sub = bus.subscribe("events.login").await.unwrap();

        bus.publish("events.login", &42u32, None).await.unwrap();

        let msg = timeout(DELIVERY_TIMEOUT, sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<u32>().unwrap(), 42);
    }

    #[tokio::test]
    async fn test_nats_wildcard_star() {
        let bus = nats_bus!();
        let mut sub = bus.subscribe("foo.*").await.unwrap();

        bus.publish("foo.bar", &1u32, None).await.unwrap();

        let msg = timeout(DELIVERY_TIMEOUT, sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<u32>().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_nats_wildcard_gt() {
        let bus = nats_bus!();
        let mut sub = bus.subscribe("orders.>").await.unwrap();

        bus.publish("orders.placed.eu", &"order-1", None)
            .await
            .unwrap();

        let msg = timeout(DELIVERY_TIMEOUT, sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<String>().unwrap(), "order-1");
    }

    #[tokio::test]
    async fn test_nats_queue_group_delivers_each_message_once() {
        let bus = nats_bus!();
        let c1 = bus.subscribe_group("jobs.*", "workers").await.unwrap();
        let c2 = bus.subscribe_group("jobs.*", "workers").await.unwrap();

        bus.publish("jobs.a", &1u32, None).await.unwrap();
        bus.publish("jobs.b", &2u32, None).await.unwrap();

        let mut messages = timeout(
            DELIVERY_TIMEOUT,
            futures::stream::select(c1, c2)
                .take(2)
                .map(|message| message.decode_json::<u32>().unwrap())
                .collect::<Vec<_>>(),
        )
        .await
        .expect("timed out");
        messages.sort_unstable();
        assert_eq!(messages, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_nats_subscribe_group_same_pattern_is_allowed() {
        let bus = nats_bus!();
        let _c1 = bus.subscribe_group("jobs.*", "workers").await.unwrap();
        assert!(bus.subscribe_group("jobs.*", "workers").await.is_ok());
    }

    #[tokio::test]
    async fn test_nats_same_group_can_subscribe_to_different_patterns() {
        let bus = nats_bus!();
        let _c1 = bus.subscribe_group("jobs.*", "workers").await.unwrap();
        assert!(bus.subscribe_group("tasks.*", "workers").await.is_ok());
    }

    #[tokio::test]
    async fn test_nats_dispatch_routes_via_server() {
        let bus = nats_bus!();
        let mut sub = bus.subscribe("internal.event").await.unwrap();

        let raw = RawMessage {
            envelope: Envelope {
                id: 1,
                subject: "internal.event".to_string(),
                timestamp: Instant::now(),
                headers: None,
                attempts: 0,
            },
            payload: bytes::Bytes::from_static(b"\"hello\""),
        };
        bus.dispatch("internal.event", raw).await.unwrap();

        let msg = timeout(DELIVERY_TIMEOUT, sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<String>().unwrap(), "hello");
    }
}
