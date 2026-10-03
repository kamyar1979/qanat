use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures::Stream;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
    ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::{
    Acker, BasicProperties, Channel, Connection, ConnectionProperties, Consumer, ExchangeKind,
    Queue,
};
use tokio::sync::{mpsc, oneshot};

use crate::bus::ExternalBus;
use crate::codec::{Codec, JsonCodec};
use crate::errors::{BackendError, BusError};
use crate::message::Envelope;
use crate::raw_message::RawMessage;
use crate::{DeliveryDecision, MESSAGE_ID_HEADER};

/// RabbitMQ-backed bus using a caller-provided topic exchange.
///
/// RabbitMQ handles wildcard routing and work queue delivery server-side, so
/// this backend deliberately does not use Qanat's local `SubjectRouter`.
pub struct RabbitMqBus<C: Codec = JsonCodec> {
    _connection: Connection,
    channel: Channel,
    exchange: String,
    codec: C,
    next_msg_id: Arc<AtomicU64>,
    settlements: mpsc::UnboundedSender<SettlementCommand>,
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
            .map_err(|e| BusError::Backend(BackendError::RabbitMq(e)))?;

        let (settlements, settlement_rx) = mpsc::unbounded_channel();
        tokio::spawn(settlement_actor(settlement_rx));

        Ok(Self {
            _connection: connection,
            channel,
            exchange: exchange.to_string(),
            codec,
            next_msg_id: Arc::new(AtomicU64::new(1)),
            settlements,
        })
    }

    async fn declare_subscription_queue(&self) -> Result<Queue, BusError> {
        self.channel
            .queue_declare(
                "".into(),
                QueueDeclareOptions::exclusive().auto_delete(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BusError::Backend(BackendError::RabbitMq(e)))
    }

    async fn consume_queue(&self, queue: &str) -> Result<RabbitMqStream, BusError> {
        let consumer = self
            .channel
            .basic_consume(
                queue.into(),
                "".into(),
                BasicConsumeOptions {
                    no_ack: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| BusError::Backend(BackendError::RabbitMq(e)))?;
        Ok(RabbitMqStream::new(
            consumer,
            Arc::clone(&self.next_msg_id),
            self.settlements.clone(),
        ))
    }
}

impl<C: Codec + 'static> ExternalBus for RabbitMqBus<C> {
    type Codec = C;
    type Subscription = RabbitMqStream;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn supports_content_type_headers(&self) -> bool {
        true
    }

    fn publish_bytes<'a>(
        &'a self,
        subject: &'a str,
        payload: Bytes,
        headers: Option<HashMap<String, String>>,
    ) -> impl std::future::Future<Output = Result<(), BusError>> + Send + 'a {
        let mut headers = headers.unwrap_or_default();
        crate::message::ensure_message_id(&mut headers);
        async move {
            self.channel
                .basic_publish(
                    self.exchange.clone().into(),
                    subject.into(),
                    BasicPublishOptions::default(),
                    &payload,
                    headers_to_properties(Some(headers)),
                )
                .await
                .map_err(|e| BusError::Backend(BackendError::RabbitMq(e)))?
                .await
                .map_err(|e| BusError::Backend(BackendError::RabbitMq(e)))?;
            Ok(())
        }
    }

    async fn subscribe_raw(&self, pattern: &str) -> Result<Self::Subscription, BusError> {
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
            .map_err(|e| BusError::Backend(BackendError::RabbitMq(e)))?;

        self.consume_queue(queue.name().as_str()).await
    }

    async fn subscribe_group_raw(
        &self,
        pattern: &str,
        group: &str,
    ) -> Result<Self::Subscription, BusError> {
        let binding_key = rabbit_binding_key(pattern)?;

        self.channel
            .queue_declare(
                group.into(),
                QueueDeclareOptions::durable(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BusError::Backend(BackendError::RabbitMq(e)))?;

        self.channel
            .queue_bind(
                group.into(),
                self.exchange.clone().into(),
                binding_key.into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BusError::Backend(BackendError::RabbitMq(e)))?;

        self.consume_queue(group).await
    }

    fn settle_raw(
        &self,
        delivery_id: u64,
        decision: DeliveryDecision,
    ) -> impl std::future::Future<Output = Result<(), BusError>> + Send + '_ {
        async move {
            let (reply, result) = oneshot::channel();
            self.settlements
                .send(SettlementCommand::Settle {
                    delivery_id,
                    decision,
                    reply,
                })
                .map_err(|_| BusError::Internal("RabbitMQ settlement actor stopped".into()))?;
            result
                .await
                .map_err(|_| BusError::Internal("RabbitMQ settlement reply was dropped".into()))?
        }
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

    let mut properties = BasicProperties::default();
    if let Some(content_type) = crate::codec::content_type(&headers) {
        properties = properties.with_content_type(content_type.into());
    }
    if let Some(message_id) = crate::message::message_id_from_headers(&headers) {
        properties = properties.with_message_id(message_id.into());
    }
    let mut table = FieldTable::default();
    for (key, value) in headers {
        table.insert(
            ShortString::from(key),
            AMQPValue::LongString(LongString::from(value)),
        );
    }

    properties.with_headers(table)
}

fn properties_to_headers(properties: &BasicProperties) -> Option<HashMap<String, String>> {
    let mut headers: HashMap<String, String> = properties
        .headers()
        .as_ref()
        .into_iter()
        .flat_map(|table| table.into_iter())
        .filter_map(|(key, value)| {
            value.as_long_string().map(|value| {
                (
                    key.to_string(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
        })
        .collect();
    if let Some(content_type) = properties.content_type().as_ref() {
        crate::codec::set_content_type(&mut headers, content_type.as_str());
    }
    if let Some(message_id) = properties.message_id().as_ref() {
        headers.insert(MESSAGE_ID_HEADER.into(), message_id.to_string());
    }
    if headers.is_empty() && properties.headers().is_none() {
        None
    } else {
        Some(headers)
    }
}

fn delivery_to_raw(delivery: &lapin::message::Delivery, id: u64) -> Option<RawMessage> {
    let headers = properties_to_headers(&delivery.properties);
    let message_id = headers
        .as_ref()
        .and_then(crate::message::message_id_from_headers)?;
    Some(RawMessage {
        envelope: Envelope {
            id,
            subject: delivery.routing_key.to_string(),
            timestamp: Instant::now(),
            message_id,
            headers,
            attempts: u32::from(delivery.redelivered),
        },
        payload: Bytes::copy_from_slice(&delivery.data),
    })
}

enum SettlementCommand {
    Register {
        delivery_id: u64,
        acker: Acker,
    },
    RejectUnidentified {
        acker: Acker,
    },
    Settle {
        delivery_id: u64,
        decision: DeliveryDecision,
        reply: oneshot::Sender<Result<(), BusError>>,
    },
}

async fn settlement_actor(mut commands: mpsc::UnboundedReceiver<SettlementCommand>) {
    let mut ackers = HashMap::new();
    while let Some(command) = commands.recv().await {
        match command {
            SettlementCommand::Register { delivery_id, acker } => {
                ackers.insert(delivery_id, acker);
            }
            SettlementCommand::RejectUnidentified { acker } => {
                if let Err(error) = acker
                    .nack(BasicNackOptions {
                        requeue: false,
                        ..Default::default()
                    })
                    .await
                {
                    tracing::error!(error = %error, "failed to reject RabbitMQ delivery without message_id");
                }
            }
            SettlementCommand::Settle {
                delivery_id,
                decision,
                reply,
            } => {
                let result = match ackers.remove(&delivery_id) {
                    Some(acker) => settle_delivery(&acker, decision).await,
                    None => Err(BusError::Internal(format!(
                        "RabbitMQ delivery {delivery_id} is not pending settlement"
                    ))),
                };
                let _ = reply.send(result);
            }
        }
    }
}

async fn settle_delivery(acker: &Acker, decision: DeliveryDecision) -> Result<(), BusError> {
    let accepted = match decision {
        DeliveryDecision::Ack => acker.ack(BasicAckOptions::default()).await,
        DeliveryDecision::Retry => {
            acker
                .nack(BasicNackOptions {
                    requeue: true,
                    ..Default::default()
                })
                .await
        }
        DeliveryDecision::Reject => {
            acker
                .nack(BasicNackOptions {
                    requeue: false,
                    ..Default::default()
                })
                .await
        }
    }
    .map_err(|error| BusError::Backend(BackendError::RabbitMq(error)))?;
    if accepted {
        Ok(())
    } else {
        Err(BusError::Internal(
            "RabbitMQ delivery was already settled".into(),
        ))
    }
}

pub struct RabbitMqStream {
    consumer: Consumer,
    next_id: Arc<AtomicU64>,
    settlements: mpsc::UnboundedSender<SettlementCommand>,
}

impl RabbitMqStream {
    fn new(
        consumer: Consumer,
        next_id: Arc<AtomicU64>,
        settlements: mpsc::UnboundedSender<SettlementCommand>,
    ) -> Self {
        Self {
            consumer,
            next_id,
            settlements,
        }
    }
}

impl Stream for RabbitMqStream {
    type Item = RawMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<RawMessage>> {
        loop {
            match Pin::new(&mut self.consumer).poll_next(cx) {
                Poll::Ready(Some(Ok(delivery))) => {
                    let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                    let Some(message) = delivery_to_raw(&delivery, id) else {
                        tracing::error!(
                            routing_key = %delivery.routing_key,
                            "rejecting RabbitMQ delivery without a stable message_id"
                        );
                        if self
                            .settlements
                            .send(SettlementCommand::RejectUnidentified {
                                acker: delivery.acker,
                            })
                            .is_err()
                        {
                            return Poll::Ready(None);
                        }
                        continue;
                    };
                    if self
                        .settlements
                        .send(SettlementCommand::Register {
                            delivery_id: id,
                            acker: delivery.acker,
                        })
                        .is_err()
                    {
                        return Poll::Ready(None);
                    }
                    return Poll::Ready(Some(message));
                }
                Poll::Ready(Some(Err(error))) => {
                    tracing::error!(error = %error, "RabbitMQ consumer failed");
                    return Poll::Ready(None);
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::Bus;
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

    #[test]
    fn rabbitmq_properties_round_trip_headers() {
        let headers = HashMap::from([
            ("correlation_id".to_string(), "request-1".to_string()),
            (MESSAGE_ID_HEADER.to_string(), "message-1".to_string()),
        ]);

        let properties = headers_to_properties(Some(headers.clone()));

        assert_eq!(
            properties.message_id().as_ref().map(|id| id.as_str()),
            Some("message-1")
        );
        assert_eq!(properties_to_headers(&properties), Some(headers));
    }

    #[test]
    fn native_content_type_is_mapped_without_application_headers() {
        let properties = BasicProperties::default().with_content_type("application/cbor".into());
        assert_eq!(
            properties_to_headers(&properties).unwrap()["content-type"],
            "application/cbor"
        );
        let properties = headers_to_properties(Some(HashMap::from([(
            "Content-Type".into(),
            "application/msgpack".into(),
        )])));
        assert_eq!(
            properties.content_type().as_ref().unwrap().as_str(),
            "application/msgpack"
        );
        let headers = properties_to_headers(&properties).unwrap();
        assert_eq!(headers["content-type"], "application/msgpack");
        assert!(!headers.contains_key("Content-Type"));
    }

    async fn try_bus() -> Option<RabbitMqBus<JsonCodec>> {
        let exchange = unique_name("exchange");
        match RabbitMqBus::connect(JsonCodec, RABBITMQ_URL, &exchange).await {
            Ok(bus) => Some(bus),
            Err(error) if std::env::var_os("QANAT_REQUIRE_BROKERS").is_some() => {
                panic!("RabbitMQ is required at {RABBITMQ_URL}, but connection failed: {error}")
            }
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
        bus.settle(msg.envelope.id, DeliveryDecision::Ack)
            .await
            .unwrap();

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
        bus.settle(msg.envelope.id, DeliveryDecision::Ack)
            .await
            .unwrap();

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
        bus.settle(msg.envelope.id, DeliveryDecision::Ack)
            .await
            .unwrap();

        cleanup_exchange(&bus).await;
    }

    #[tokio::test]
    async fn test_rabbitmq_queue_group_round_robin() {
        let bus = rabbit_bus!();
        let queue = unique_name("queue");
        let mut c1 = bus.subscribe_group("jobs.*", &queue).await.unwrap();
        let mut c2 = bus.subscribe_group("jobs.*", &queue).await.unwrap();

        bus.publish("jobs.a", &1u32, None).await.unwrap();
        bus.publish("jobs.b", &2u32, None).await.unwrap();

        let message1 = timeout(Duration::from_secs(2), c1.next())
            .await
            .expect("timed out")
            .unwrap();
        let message2 = timeout(Duration::from_secs(2), c2.next())
            .await
            .expect("timed out")
            .unwrap();
        let m1 = message1.decode_json::<u32>().unwrap();
        let m2 = message2.decode_json::<u32>().unwrap();

        assert!(m1 == 1 || m1 == 2);
        assert!(m2 == 1 || m2 == 2);
        assert_ne!(m1, m2);
        bus.settle(message1.envelope.id, DeliveryDecision::Ack)
            .await
            .unwrap();
        bus.settle(message2.envelope.id, DeliveryDecision::Ack)
            .await
            .unwrap();

        cleanup_queue(&bus, &queue).await;
        cleanup_exchange(&bus).await;
    }

    #[tokio::test]
    async fn test_rabbitmq_subscribe_group_same_pattern_is_allowed() {
        let bus = rabbit_bus!();
        let queue = unique_name("queue");

        let _c1 = bus.subscribe_group("jobs.*", &queue).await.unwrap();
        assert!(bus.subscribe_group("jobs.*", &queue).await.is_ok());

        cleanup_queue(&bus, &queue).await;
        cleanup_exchange(&bus).await;
    }

    #[tokio::test]
    async fn test_rabbitmq_same_group_can_bind_multiple_patterns() {
        let bus = rabbit_bus!();
        let queue = unique_name("queue");

        let _c1 = bus.subscribe_group("jobs.*", &queue).await.unwrap();
        assert!(bus.subscribe_group("tasks.*", &queue).await.is_ok());

        cleanup_queue(&bus, &queue).await;
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
                message_id: "message-1".to_string(),
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
        bus.settle(msg.envelope.id, DeliveryDecision::Ack)
            .await
            .unwrap();

        cleanup_exchange(&bus).await;
    }

    #[tokio::test]
    async fn test_rabbitmq_retry_requeues_with_stable_message_id() {
        let bus = rabbit_bus!();
        let queue = unique_name("retry.queue");
        let mut sub = bus.subscribe_group("retry.events", &queue).await.unwrap();

        bus.publish("retry.events", &42u32, None).await.unwrap();

        let first = timeout(Duration::from_secs(2), sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        assert_eq!(first.envelope.attempts, 0);
        let message_id = first.envelope.message_id.clone();
        bus.settle(first.envelope.id, DeliveryDecision::Retry)
            .await
            .unwrap();

        let redelivered = timeout(Duration::from_secs(2), sub.next())
            .await
            .expect("timed out waiting for redelivery")
            .expect("stream ended");
        assert_eq!(redelivered.envelope.message_id, message_id);
        assert_eq!(redelivered.envelope.attempts, 1);
        assert_eq!(redelivered.decode_json::<u32>().unwrap(), 42);
        bus.settle(redelivered.envelope.id, DeliveryDecision::Ack)
            .await
            .unwrap();

        cleanup_queue(&bus, &queue).await;
        cleanup_exchange(&bus).await;
    }
}
