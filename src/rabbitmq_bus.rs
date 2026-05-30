use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures::Stream;
use lapin::options::{
    BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions,
    QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer, ExchangeKind, Queue,
};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::bus::Bus;
use crate::codec::{Codec, JsonCodec};
use crate::errors::BusError;
use crate::message::Envelope;
use crate::raw_message::RawMessage;

/// RabbitMQ-backed bus using a caller-provided topic exchange.
///
/// RabbitMQ handles wildcard routing and work queue delivery server-side, so
/// this backend deliberately does not use Qanat's local `SubjectRouter`.
pub struct RabbitMqBus<C: Codec = JsonCodec> {
    _connection: Connection,
    channel: Channel,
    exchange: String,
    codec: C,
    queues: Mutex<HashMap<String, String>>,
    next_msg_id: Arc<AtomicU64>,
}

impl<C: Codec> RabbitMqBus<C> {
    pub async fn connect(codec: C, url: &str, exchange: &str) -> Result<Self, BusError> {
        let connection = Connection::connect(url, ConnectionProperties::default())
            .await
            .map_err(|e| BusError::Connection(e.to_string()))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| BusError::Connection(e.to_string()))?;

        channel
            .exchange_declare(
                exchange.into(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| BusError::Backend(Box::new(e)))?;

        Ok(Self {
            _connection: connection,
            channel,
            exchange: exchange.to_string(),
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
        self.publish_bytes(subject, payload_bytes, headers).await
    }

    async fn publish_bytes(
        &self,
        subject: &str,
        payload: Bytes,
        headers: Option<HashMap<String, String>>,
    ) -> Result<(), BusError> {
        self.channel
            .basic_publish(
                self.exchange.clone().into(),
                subject.into(),
                BasicPublishOptions::default(),
                &payload,
                headers_to_properties(headers),
            )
            .await
            .map_err(|e| BusError::Backend(Box::new(e)))?
            .await
            .map_err(|e| BusError::Backend(Box::new(e)))?;
        Ok(())
    }

    async fn declare_subscription_queue(&self) -> Result<Queue, BusError> {
        self.channel
            .queue_declare(
                "".into(),
                QueueDeclareOptions::exclusive().auto_delete(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BusError::Backend(Box::new(e)))
    }

    async fn consume_queue(&self, queue: &str) -> Result<RabbitMqStream, BusError> {
        let consumer = self
            .channel
            .basic_consume(
                queue.into(),
                "".into(),
                BasicConsumeOptions {
                    no_ack: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| BusError::Backend(Box::new(e)))?;
        Ok(RabbitMqStream::new(consumer, Arc::clone(&self.next_msg_id)))
    }
}

impl<C: Codec + 'static> Bus for RabbitMqBus<C> {
    type Message = RawMessage;
    type Subscription = RabbitMqStream;

    /// Publish raw bytes to RabbitMQ. The exchange routes from there.
    async fn dispatch(&self, subject: &str, msg: RawMessage) -> Result<(), BusError> {
        self.publish_bytes(subject, msg.payload, msg.envelope.headers)
            .await
    }

    async fn subscribe(&self, pattern: &str) -> Result<Self::Subscription, BusError> {
        let queue = self.declare_subscription_queue().await?;
        let binding_key = rabbit_binding_key(pattern)?;
        self.channel
            .queue_bind(
                queue.name().clone(),
                self.exchange.clone().into(),
                binding_key.into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BusError::Backend(Box::new(e)))?;

        self.consume_queue(queue.name().as_str()).await
    }

    async fn bind_queue(&self, pattern: &str, queue: &str) -> Result<(), BusError> {
        let binding_key = rabbit_binding_key(pattern)?;

        {
            let mut queues = self.queues.lock().await;
            match queues.get(queue) {
                Some(existing) if existing.as_str() == pattern => return Ok(()),
                Some(existing) => {
                    return Err(BusError::Internal(format!(
                        "queue '{}' already bound to pattern '{}', cannot rebind to '{}'",
                        queue, existing, pattern
                    )));
                }
                None => {
                    queues.insert(queue.to_string(), pattern.to_string());
                }
            }
        }

        if let Err(err) = self
            .channel
            .queue_declare(
                queue.into(),
                QueueDeclareOptions::durable(),
                FieldTable::default(),
            )
            .await
        {
            self.queues.lock().await.remove(queue);
            return Err(BusError::Backend(Box::new(err)));
        }

        if let Err(err) = self
            .channel
            .queue_bind(
                queue.into(),
                self.exchange.clone().into(),
                binding_key.into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
        {
            self.queues.lock().await.remove(queue);
            return Err(BusError::Backend(Box::new(err)));
        }

        Ok(())
    }

    async fn consume(&self, queue: &str) -> Result<Self::Subscription, BusError> {
        {
            let queues = self.queues.lock().await;
            if !queues.contains_key(queue) {
                return Err(BusError::QueueNotFound(queue.to_string()));
            }
        }

        self.consume_queue(queue).await
    }
}

fn rabbit_binding_key(pattern: &str) -> Result<String, BusError> {
    if pattern.is_empty() {
        return Err(BusError::Internal("subject pattern cannot be empty".into()));
    }

    let mut tokens = pattern.split('.').collect::<Vec<_>>();
    if tokens.iter().any(|token| token.is_empty()) {
        return Err(BusError::Internal(format!(
            "subject pattern '{}' contains an empty token",
            pattern
        )));
    }

    if let Some(index) = tokens.iter().position(|token| *token == ">") {
        if index != tokens.len() - 1 {
            return Err(BusError::Internal(format!(
                "'>' wildcard must be the final token in pattern '{}'",
                pattern
            )));
        }

        if tokens.len() == 1 {
            return Ok("#".to_string());
        }

        tokens.pop();
        tokens.push("*");
        tokens.push("#");
        return Ok(tokens.join("."));
    }

    Ok(pattern.to_string())
}

fn headers_to_properties(headers: Option<HashMap<String, String>>) -> BasicProperties {
    let Some(headers) = headers else {
        return BasicProperties::default();
    };

    let mut table = FieldTable::default();
    for (key, value) in headers {
        table.insert(
            ShortString::from(key),
            AMQPValue::LongString(LongString::from(value)),
        );
    }

    BasicProperties::default().with_headers(table)
}

fn properties_to_headers(properties: &BasicProperties) -> Option<HashMap<String, String>> {
    let table = properties.headers().as_ref()?;
    Some(
        table
            .into_iter()
            .filter_map(|(key, value)| {
                value.as_long_string().map(|value| {
                    (
                        key.to_string(),
                        String::from_utf8_lossy(value.as_bytes()).into_owned(),
                    )
                })
            })
            .collect(),
    )
}

fn delivery_to_raw(delivery: lapin::message::Delivery, id: u64) -> RawMessage {
    RawMessage {
        envelope: Envelope {
            id,
            subject: delivery.routing_key.to_string(),
            timestamp: Instant::now(),
            headers: properties_to_headers(&delivery.properties),
            attempts: u32::from(delivery.redelivered),
        },
        payload: Bytes::from(delivery.data),
    }
}

pub struct RabbitMqStream {
    consumer: Consumer,
    next_id: Arc<AtomicU64>,
}

impl RabbitMqStream {
    fn new(consumer: Consumer, next_id: Arc<AtomicU64>) -> Self {
        Self { consumer, next_id }
    }
}

impl Stream for RabbitMqStream {
    type Item = RawMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<RawMessage>> {
        match Pin::new(&mut self.consumer).poll_next(cx) {
            Poll::Ready(Some(Ok(delivery))) => {
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                Poll::Ready(Some(delivery_to_raw(delivery, id)))
            }
            Poll::Ready(Some(Err(_))) | Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonCodec;
    use futures::StreamExt;
    use lapin::options::{ExchangeDeleteOptions, QueueDeleteOptions};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::time::timeout;

    const RABBITMQ_URL: &str = "amqp://guest:guest@127.0.0.1:5672/%2f";
    static NAME_COUNTER: AtomicU64 = AtomicU64::new(1);

    fn unique_name(kind: &str) -> String {
        format!(
            "qanat.test.{}.{}",
            kind,
            NAME_COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    async fn try_bus() -> Option<RabbitMqBus<JsonCodec>> {
        let exchange = unique_name("exchange");
        match RabbitMqBus::connect(JsonCodec, RABBITMQ_URL, &exchange).await {
            Ok(bus) => Some(bus),
            Err(_) => {
                eprintln!("skipping: RabbitMQ not available at {RABBITMQ_URL}");
                None
            }
        }
    }

    async fn cleanup_exchange(bus: &RabbitMqBus<JsonCodec>) {
        let _ = bus
            .channel
            .exchange_delete(
                bus.exchange.clone().into(),
                ExchangeDeleteOptions::default(),
            )
            .await;
    }

    async fn cleanup_queue(bus: &RabbitMqBus<JsonCodec>, queue: &str) {
        let _ = bus
            .channel
            .queue_delete(queue.into(), QueueDeleteOptions::default())
            .await;
    }

    macro_rules! rabbit_bus {
        () => {
            match try_bus().await {
                Some(b) => b,
                None => return,
            }
        };
    }

    #[test]
    fn rabbit_binding_key_keeps_exact_and_star_patterns() {
        assert_eq!(rabbit_binding_key("events.login").unwrap(), "events.login");
        assert_eq!(rabbit_binding_key("events.*").unwrap(), "events.*");
    }

    #[test]
    fn rabbit_binding_key_translates_gt_to_one_or_more_tokens() {
        assert_eq!(rabbit_binding_key(">").unwrap(), "#");
        assert_eq!(rabbit_binding_key("orders.>").unwrap(), "orders.*.#");
        assert_eq!(rabbit_binding_key("a.b.>").unwrap(), "a.b.*.#");
    }

    #[test]
    fn rabbit_binding_key_rejects_non_terminal_gt() {
        assert!(rabbit_binding_key("a.>.b").is_err());
    }

    #[tokio::test]
    async fn test_rabbitmq_pub_sub() {
        let bus = rabbit_bus!();
        let mut sub = bus.subscribe("events.login").await.unwrap();

        bus.publish("events.login", &42u32, None).await.unwrap();

        let msg = timeout(Duration::from_secs(2), sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<u32>().unwrap(), 42);

        cleanup_exchange(&bus).await;
    }

    #[tokio::test]
    async fn test_rabbitmq_wildcard_star() {
        let bus = rabbit_bus!();
        let mut sub = bus.subscribe("foo.*").await.unwrap();

        bus.publish("foo.bar", &1u32, None).await.unwrap();

        let msg = timeout(Duration::from_secs(2), sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<u32>().unwrap(), 1);

        cleanup_exchange(&bus).await;
    }

    #[tokio::test]
    async fn test_rabbitmq_wildcard_gt_matches_one_or_more_trailing_tokens() {
        let bus = rabbit_bus!();
        let mut sub = bus.subscribe("orders.>").await.unwrap();

        bus.publish("orders", &"bare", None).await.unwrap();
        assert!(
            timeout(Duration::from_millis(150), sub.next())
                .await
                .is_err(),
            "orders.> must not match orders"
        );

        bus.publish("orders.placed.eu", &"order-1", None)
            .await
            .unwrap();

        let msg = timeout(Duration::from_secs(2), sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<String>().unwrap(), "order-1");

        cleanup_exchange(&bus).await;
    }

    #[tokio::test]
    async fn test_rabbitmq_queue_group_round_robin() {
        let bus = rabbit_bus!();
        let queue = unique_name("queue");
        bus.bind_queue("jobs.*", &queue).await.unwrap();
        let mut c1 = bus.consume(&queue).await.unwrap();
        let mut c2 = bus.consume(&queue).await.unwrap();

        bus.publish("jobs.a", &1u32, None).await.unwrap();
        bus.publish("jobs.b", &2u32, None).await.unwrap();

        let m1 = timeout(Duration::from_secs(2), c1.next())
            .await
            .expect("timed out")
            .unwrap()
            .decode_json::<u32>()
            .unwrap();
        let m2 = timeout(Duration::from_secs(2), c2.next())
            .await
            .expect("timed out")
            .unwrap()
            .decode_json::<u32>()
            .unwrap();

        assert!(m1 == 1 || m1 == 2);
        assert!(m2 == 1 || m2 == 2);
        assert_ne!(m1, m2);

        cleanup_queue(&bus, &queue).await;
        cleanup_exchange(&bus).await;
    }

    #[tokio::test]
    async fn test_rabbitmq_bind_queue_idempotent() {
        let bus = rabbit_bus!();
        let queue = unique_name("queue");

        bus.bind_queue("jobs.*", &queue).await.unwrap();
        assert!(bus.bind_queue("jobs.*", &queue).await.is_ok());

        cleanup_queue(&bus, &queue).await;
        cleanup_exchange(&bus).await;
    }

    #[tokio::test]
    async fn test_rabbitmq_bind_queue_conflict_returns_error() {
        let bus = rabbit_bus!();
        let queue = unique_name("queue");

        bus.bind_queue("jobs.*", &queue).await.unwrap();
        assert!(bus.bind_queue("tasks.*", &queue).await.is_err());

        cleanup_queue(&bus, &queue).await;
        cleanup_exchange(&bus).await;
    }

    #[tokio::test]
    async fn test_rabbitmq_consume_nonexistent_queue_returns_error() {
        let bus = rabbit_bus!();

        assert!(matches!(
            bus.consume("ghost").await,
            Err(BusError::QueueNotFound(_))
        ));

        cleanup_exchange(&bus).await;
    }

    #[tokio::test]
    async fn test_rabbitmq_dispatch_routes_via_exchange() {
        let bus = rabbit_bus!();
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

        let msg = timeout(Duration::from_secs(2), sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(msg.decode_json::<String>().unwrap(), "hello");

        cleanup_exchange(&bus).await;
    }
}
