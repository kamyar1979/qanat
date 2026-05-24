use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use futures::Stream;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::bus::Bus;
use crate::codec::Codec;
use crate::errors::BusError;
use crate::message::Envelope;
use crate::raw_message::RawMessage;

// ── NatsBus ───────────────────────────────────────────────────────────────────
// NATS server handles all wildcard matching, fanout, and queue groups natively.
// SubjectRouter is NOT used here.

pub struct NatsBus<C: Codec> {
    client: async_nats::Client,
    codec: C,
    /// Maps queue name → subject pattern; needed so `consume` can call
    /// `queue_subscribe(pattern, queue)` when the caller only knows the queue name.
    queues: Mutex<HashMap<String, String>>,
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
            queues: Mutex::new(HashMap::new()),
            next_msg_id: Arc::new(AtomicU64::new(1)),
        })
    }

    pub async fn publish<T: Serialize>(
        &self,
        subject: &str,
        payload: &T,
        headers: Option<HashMap<String, String>>,
    ) -> Result<(), BusError> {
        let payload_bytes = self.codec.encode(payload)?;

        if let Some(hdrs) = headers {
            let mut header_map = async_nats::header::HeaderMap::new();
            for (k, v) in hdrs {
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
            self.client
                .publish_with_headers(subject.to_string(), header_map, payload_bytes)
                .await
                .map_err(|e| BusError::Backend(Box::new(e)))?;
        } else {
            self.client
                .publish(subject.to_string(), payload_bytes)
                .await
                .map_err(|e| BusError::Backend(Box::new(e)))?;
        }

        Ok(())
    }
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

impl<C: Codec + 'static> Bus for NatsBus<C> {
    type Message = RawMessage;
    type Subscription = NatsStream;

    /// Forward the raw bytes directly into NATS — the server routes from there.
    async fn dispatch(&self, subject: &str, msg: RawMessage) -> Result<(), BusError> {
        self.client
            .publish(subject.to_string(), msg.payload)
            .await
            .map_err(|e| BusError::Backend(Box::new(e)))?;
        Ok(())
    }

    async fn subscribe(&self, pattern: &str) -> Result<Self::Subscription, BusError> {
        let sub = self
            .client
            .subscribe(pattern.to_string())
            .await
            .map_err(|e| BusError::Backend(Box::new(e)))?;
        Ok(NatsStream::new(sub, Arc::clone(&self.next_msg_id)))
    }

    async fn bind_queue(&self, pattern: &str, queue: &str) -> Result<(), BusError> {
        let mut queues = self.queues.lock().await;
        match queues.get(queue) {
            Some(existing) if existing.as_str() == pattern => Ok(()),
            Some(existing) => Err(BusError::Internal(format!(
                "queue '{}' already bound to pattern '{}', cannot rebind to '{}'",
                queue, existing, pattern
            ))),
            None => {
                queues.insert(queue.to_string(), pattern.to_string());
                Ok(())
            }
        }
    }

    async fn consume(&self, queue: &str) -> Result<Self::Subscription, BusError> {
        let pattern = {
            let queues = self.queues.lock().await;
            queues
                .get(queue)
                .ok_or_else(|| BusError::QueueNotFound(queue.to_string()))?
                .clone()
        };
        let sub = self
            .client
            .queue_subscribe(pattern, queue.to_string())
            .await
            .map_err(|e| BusError::Backend(Box::new(e)))?;
        Ok(NatsStream::new(sub, Arc::clone(&self.next_msg_id)))
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonCodec;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::timeout;

    const NATS_URL: &str = "nats://localhost:4222";

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

        let msg = timeout(Duration::from_millis(500), sub.next())
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

        let msg = timeout(Duration::from_millis(500), sub.next())
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

        let msg = timeout(Duration::from_millis(500), sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<String>().unwrap(), "order-1");
    }

    #[tokio::test]
    async fn test_nats_queue_group_round_robin() {
        let bus = nats_bus!();
        bus.bind_queue("jobs.*", "workers").await.unwrap();
        let mut c1 = bus.consume("workers").await.unwrap();
        let mut c2 = bus.consume("workers").await.unwrap();

        bus.publish("jobs.a", &1u32, None).await.unwrap();
        bus.publish("jobs.b", &2u32, None).await.unwrap();

        let m1 = timeout(Duration::from_millis(500), c1.next())
            .await
            .expect("timed out")
            .unwrap()
            .decode_json::<u32>()
            .unwrap();
        let m2 = timeout(Duration::from_millis(500), c2.next())
            .await
            .expect("timed out")
            .unwrap()
            .decode_json::<u32>()
            .unwrap();

        assert!(m1 == 1 || m1 == 2);
        assert!(m2 == 1 || m2 == 2);
        assert_ne!(m1, m2);
    }

    #[tokio::test]
    async fn test_nats_bind_queue_idempotent() {
        let bus = nats_bus!();
        bus.bind_queue("jobs.*", "workers").await.unwrap();
        assert!(bus.bind_queue("jobs.*", "workers").await.is_ok());
    }

    #[tokio::test]
    async fn test_nats_bind_queue_conflict_returns_error() {
        let bus = nats_bus!();
        bus.bind_queue("jobs.*", "workers").await.unwrap();
        assert!(bus.bind_queue("tasks.*", "workers").await.is_err());
    }

    #[tokio::test]
    async fn test_nats_consume_nonexistent_queue_returns_error() {
        let bus = nats_bus!();
        assert!(matches!(
            bus.consume("ghost").await,
            Err(BusError::QueueNotFound(_))
        ));
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

        let msg = timeout(Duration::from_millis(500), sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<String>().unwrap(), "hello");
    }
}
