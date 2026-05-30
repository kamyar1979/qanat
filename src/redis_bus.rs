use crate::bus::{Bus, BusStream};
use crate::codec::{Codec, JsonCodec};
use crate::errors::{BackendError, BusError};
use crate::local_router::LocalRouter;
use crate::message::Envelope;
use crate::raw_message::RawMessage;
use crate::wire;
use bytes::Bytes;
use futures::StreamExt;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

const DEFAULT_CHANNEL: &str = "qanat:bus";

struct RedisState<C: Codec = JsonCodec> {
    local: LocalRouter<C>,
    channel: String,
    publisher: Mutex<redis::aio::MultiplexedConnection>,
}

pub struct RedisBus<C: Codec = JsonCodec> {
    inner: Arc<RedisState<C>>,
}

impl<C: Codec + 'static> RedisBus<C> {
    pub async fn connect(codec: C, url: &str) -> Result<Self, BusError> {
        Self::connect_with_channel(codec, url, DEFAULT_CHANNEL).await
    }

    pub async fn connect_with_channel(
        codec: C,
        url: &str,
        channel: &str,
    ) -> Result<Self, BusError> {
        let client = redis::Client::open(url).map_err(|e| BusError::Connection(e.to_string()))?;
        let publisher = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BusError::Connection(e.to_string()))?;

        let mut pubsub = client
            .get_async_pubsub()
            .await
            .map_err(|e| BusError::Connection(e.to_string()))?;
        pubsub
            .subscribe(channel)
            .await
            .map_err(|e| BusError::Backend(BackendError::Redis(e)))?;

        let inner = Arc::new(RedisState {
            local: LocalRouter::new(codec),
            channel: channel.to_string(),
            publisher: Mutex::new(publisher),
        });

        Self::start_receive_loop(Arc::clone(&inner), pubsub);

        Ok(Self { inner })
    }

    fn start_receive_loop(inner: Arc<RedisState<C>>, pubsub: redis::aio::PubSub) {
        tokio::spawn(async move {
            let mut messages = pubsub.into_on_message();
            while let Some(redis_msg) = messages.next().await {
                if let Some((subject, payload)) = wire::decode(redis_msg.get_payload_bytes()) {
                    let msg = RawMessage {
                        envelope: Envelope {
                            id: inner.local.next_message_id(),
                            subject: subject.to_string(),
                            timestamp: Instant::now(),
                            headers: None,
                            attempts: 0,
                        },
                        payload: Bytes::copy_from_slice(payload),
                    };
                    let _ = inner.local.dispatch_local(msg).await;
                }
            }
        });
    }

    pub async fn publish<T: Serialize>(
        &self,
        subject: &str,
        payload: &T,
        _headers: Option<HashMap<String, String>>,
    ) -> Result<(), BusError> {
        let payload_bytes = self.inner.local.codec.encode(payload)?;
        self.publish_bytes(subject, payload_bytes).await
    }

    async fn publish_bytes(&self, subject: &str, payload: Bytes) -> Result<(), BusError> {
        let wire = wire::encode(subject, &payload);
        let mut publisher = self.inner.publisher.lock().await;
        redis::cmd("PUBLISH")
            .arg(&self.inner.channel)
            .arg(wire)
            .query_async::<()>(&mut *publisher)
            .await
            .map_err(|e| BusError::Backend(BackendError::Redis(e)))
    }
}

impl<C: Codec> Bus for RedisBus<C> {
    type Message = RawMessage;
    type Subscription = BusStream<RawMessage>;

    /// Publish raw bytes through Redis. The Redis subscriber loop receives the
    /// frame and applies local Qanat routing.
    async fn dispatch(&self, subject: &str, msg: RawMessage) -> Result<(), BusError> {
        self.publish_bytes(subject, msg.payload).await
    }

    async fn subscribe(&self, pattern: &str) -> Result<Self::Subscription, BusError> {
        Ok(self.inner.local.subscribe(pattern).await)
    }

    async fn bind_queue(&self, pattern: &str, queue: &str) -> Result<(), BusError> {
        self.inner.local.bind_queue(pattern, queue).await
    }

    async fn consume(&self, queue: &str) -> Result<Self::Subscription, BusError> {
        self.inner.local.consume(queue).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonCodec;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::time::timeout;

    const REDIS_URL: &str = "redis://127.0.0.1/";
    static CHANNEL_COUNTER: AtomicU64 = AtomicU64::new(1);

    fn redis_channel() -> String {
        format!(
            "qanat:test:{}",
            CHANNEL_COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    async fn try_bus() -> Option<RedisBus<JsonCodec>> {
        match RedisBus::connect_with_channel(JsonCodec, REDIS_URL, &redis_channel()).await {
            Ok(bus) => Some(bus),
            Err(_) => {
                eprintln!("skipping: Redis not available at {REDIS_URL}");
                None
            }
        }
    }

    macro_rules! redis_bus {
        () => {
            match try_bus().await {
                Some(b) => b,
                None => return,
            }
        };
    }

    #[tokio::test]
    async fn test_redis_pub_sub() {
        let bus = redis_bus!();
        let mut sub = bus.subscribe("events.login").await.unwrap();

        bus.publish("events.login", &42u32, None).await.unwrap();

        let msg = timeout(Duration::from_millis(500), sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<u32>().unwrap(), 42);
    }

    #[tokio::test]
    async fn test_redis_wildcard_routing() {
        let bus = redis_bus!();
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
    async fn test_redis_queue_group_round_robin() {
        let bus = redis_bus!();
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
    async fn test_redis_dispatch_routes_locally_after_pubsub_round_trip() {
        let bus = redis_bus!();
        let mut sub = bus.subscribe("internal.event").await.unwrap();

        let raw = RawMessage {
            envelope: Envelope {
                id: 1,
                subject: "internal.event".to_string(),
                timestamp: Instant::now(),
                headers: None,
                attempts: 0,
            },
            payload: Bytes::from_static(b"\"hello\""),
        };
        bus.dispatch("internal.event", raw).await.unwrap();

        let msg = timeout(Duration::from_millis(500), sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<String>().unwrap(), "hello");
    }
}
