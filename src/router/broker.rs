use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::bus::Bus;
use crate::codec::{Codec, JsonCodec};
use crate::errors::BusError;
use crate::message::Envelope;
use crate::raw_message::RawMessage;
use crate::router::core::{
    FromRouteMessage, RouteHeader, RouteHeaders, RouteMessage, RouteSource, RouteStream,
    RouteTarget,
};
use futures::StreamExt;
use futures::future::{BoxFuture, LocalBoxFuture};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{OnceCell, mpsc, oneshot};
use uuid::Uuid;

pub const DEFAULT_REDELIVERY_MESSAGE_DELAY: Duration = Duration::from_secs(5);
pub const DEFAULT_MESSAGE_RETRIES: usize = 3;
pub const DEFAULT_PROXY_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_REPLY_TOPIC_PREFIX: &str = "_qanat.reply";
pub const CORRELATION_ID_HEADER: &str = "correlation_id";
pub const REPLY_TO_HEADER: &str = "reply_to";

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BrokerSubject {
    pub subject: String,
}

impl BrokerSubject {
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BrokerRoute {
    pub pattern: String,
    pub group: String,
    pub reply_to: Option<String>,
    pub reply_topic_prefix: Option<String>,
}

impl BrokerRoute {
    pub fn new(pattern: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            group: group.into(),
            reply_to: None,
            reply_topic_prefix: None,
        }
    }

    pub fn with_reply_to(mut self, reply_to: impl Into<String>) -> Self {
        self.reply_to = Some(reply_to.into());
        self.reply_topic_prefix = None;
        self
    }

    pub fn with_reply_topic_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.reply_to = None;
        self.reply_topic_prefix = Some(prefix.into());
        self
    }

    pub fn without_reply_topic_prefix(mut self) -> Self {
        self.reply_topic_prefix = None;
        self
    }
}

pub struct BrokerSource<B: Bus> {
    bus: Arc<B>,
    route: BrokerRoute,
}

impl<B: Bus> BrokerSource<B> {
    pub fn new(bus: B, pattern: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            bus: Arc::new(bus),
            route: BrokerRoute::new(pattern, group),
        }
    }

    pub fn from_shared(bus: Arc<B>, pattern: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            bus,
            route: BrokerRoute::new(pattern, group),
        }
    }

    pub fn bus(&self) -> &B {
        self.bus.as_ref()
    }

    pub fn route(&self) -> &BrokerRoute {
        &self.route
    }

    pub fn shared_bus(&self) -> Arc<B> {
        Arc::clone(&self.bus)
    }
}

impl<B> RouteSource for BrokerSource<B>
where
    B: Bus<Message = RawMessage> + 'static,
{
    fn into_stream(self: Box<Self>) -> LocalBoxFuture<'static, Result<RouteStream, BusError>> {
        Box::pin(async move {
            let stream = self
                .bus
                .subscribe_group(&self.route.pattern, &self.route.group)
                .await?;
            Ok(Box::pin(stream.map(route_message_from_broker)) as RouteStream)
        })
    }
}

fn route_message_from_broker(message: RawMessage) -> RouteMessage {
    RouteMessage {
        address: message.envelope.subject,
        timestamp: message.envelope.timestamp,
        id: message.envelope.id,
        headers: message.envelope.headers.unwrap_or_default(),
        attempts: message.envelope.attempts,
        payload: message.payload,
    }
}

fn broker_message_from_route(message: RouteMessage) -> RawMessage {
    RawMessage {
        envelope: Envelope {
            subject: message.address,
            timestamp: message.timestamp,
            id: message.id,
            headers: (!message.headers.is_empty()).then_some(message.headers),
            attempts: message.attempts,
        },
        payload: message.payload,
    }
}

enum BrokerDestination {
    Subject(String),
    ReplyTo,
}

pub struct BrokerTarget<B: Bus> {
    bus: Arc<B>,
    destination: BrokerDestination,
}

impl<B: Bus> BrokerTarget<B> {
    pub fn new(bus: B, subject: impl Into<String>) -> Self {
        Self::from_shared(Arc::new(bus), subject)
    }

    pub fn from_shared(bus: Arc<B>, subject: impl Into<String>) -> Self {
        Self {
            bus,
            destination: BrokerDestination::Subject(subject.into()),
        }
    }

    pub fn reply_to(bus: B) -> Self {
        Self::reply_to_shared(Arc::new(bus))
    }

    pub fn reply_to_shared(bus: Arc<B>) -> Self {
        Self {
            bus,
            destination: BrokerDestination::ReplyTo,
        }
    }

    pub fn bus(&self) -> &B {
        self.bus.as_ref()
    }
}

#[derive(Clone, Debug)]
pub struct BrokerEnvelope(pub Envelope);

impl<C: Codec> FromRouteMessage<C> for BrokerEnvelope {
    fn from_message(message: &RouteMessage, _codec: &C) -> Result<Self, BusError> {
        Ok(Self(broker_message_from_route(message.clone()).envelope))
    }
}

pub type BrokerHeaders = RouteHeaders;
pub type BrokerHeader<T> = RouteHeader<T>;
pub use crate::router::core::FromRouteHeader as FromBrokerHeader;

#[derive(Clone, Debug)]
pub struct BrokerRawMessage(pub RawMessage);

impl<C: Codec> FromRouteMessage<C> for BrokerRawMessage {
    fn from_message(message: &RouteMessage, _codec: &C) -> Result<Self, BusError> {
        Ok(Self(broker_message_from_route(message.clone())))
    }
}

impl<B> RouteTarget for BrokerTarget<B>
where
    B: Bus<Message = RawMessage> + 'static,
{
    fn accepts(&self, message: &RouteMessage) -> bool {
        match self.destination {
            BrokerDestination::Subject(_) => true,
            BrokerDestination::ReplyTo => message.headers.contains_key(REPLY_TO_HEADER),
        }
    }

    fn deliver(&self, mut output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>> {
        Box::pin(async move {
            let subject = match &self.destination {
                BrokerDestination::Subject(subject) => subject.clone(),
                BrokerDestination::ReplyTo => output
                    .headers
                    .get(REPLY_TO_HEADER)
                    .cloned()
                    .ok_or_else(|| {
                        BusError::Internal("broker reply target requires a reply_to header".into())
                    })?,
            };
            output.headers.remove(REPLY_TO_HEADER);
            output.address = subject.clone();
            self.bus
                .dispatch(&subject, broker_message_from_route(output))
                .await
        })
    }
}

enum ProxyCommand {
    Register {
        correlation_id: String,
        reply: oneshot::Sender<RawMessage>,
        registered: oneshot::Sender<()>,
    },
    Cancel {
        correlation_id: String,
    },
}

struct ProxyRuntime {
    reply_subject: String,
    commands: mpsc::Sender<ProxyCommand>,
    task: tokio::task::JoinHandle<()>,
}

pub struct BrokerProxy<B: Bus, C: Codec = JsonCodec> {
    bus: Arc<B>,
    codec: Arc<C>,
    route: BrokerRoute,
    timeout: Duration,
    runtime: OnceCell<ProxyRuntime>,
}

impl<B: Bus> BrokerProxy<B, JsonCodec> {
    pub fn new(bus: B, route: BrokerRoute) -> Self {
        Self::with_codec(bus, JsonCodec, route)
    }
}

impl<B, C> BrokerProxy<B, C>
where
    B: Bus,
    C: Codec,
{
    pub fn with_codec(bus: B, codec: C, route: BrokerRoute) -> Self {
        Self::from_shared(Arc::new(bus), Arc::new(codec), route)
    }

    fn from_shared(bus: Arc<B>, codec: Arc<C>, mut route: BrokerRoute) -> Self {
        if route.reply_to.is_none() && route.reply_topic_prefix.is_none() {
            route.reply_topic_prefix = Some(DEFAULT_REPLY_TOPIC_PREFIX.to_string());
        }

        Self {
            bus,
            codec,
            route,
            timeout: DEFAULT_PROXY_TIMEOUT,
            runtime: OnceCell::new(),
        }
    }

    pub fn reply_to(mut self, reply_to: impl Into<String>) -> Self {
        self.route.reply_to = Some(reply_to.into());
        self.route.reply_topic_prefix = None;
        self
    }

    pub fn reply_topic_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.route.reply_to = None;
        self.route.reply_topic_prefix = Some(prefix.into());
        self
    }

    pub fn without_reply_topic_prefix(mut self) -> Self {
        self.route.reply_topic_prefix = None;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn bus(&self) -> &B {
        self.bus.as_ref()
    }

    pub fn codec(&self) -> &C {
        &self.codec
    }

    pub fn route(&self) -> &BrokerRoute {
        &self.route
    }

    pub fn reply_subject(&self) -> Option<&str> {
        self.runtime
            .get()
            .map(|runtime| runtime.reply_subject.as_str())
    }
}

impl<B, C> BrokerProxy<B, C>
where
    B: Bus<Message = RawMessage> + 'static,
    C: Codec,
{
    async fn initialize(&self) -> Result<ProxyRuntime, BusError> {
        let reply_subject = match &self.route.reply_to {
            Some(reply_to) => reply_to.clone(),
            None => {
                let prefix = self.route.reply_topic_prefix.as_deref().ok_or_else(|| {
                    BusError::Internal(
                        "broker proxy requires either reply_to or reply_topic_prefix".into(),
                    )
                })?;
                format!("{prefix}.{}", Uuid::new_v4().simple())
            }
        };
        let mut subscription = self.bus.subscribe(&reply_subject).await?;
        let (commands, mut command_rx) = mpsc::channel::<ProxyCommand>(64);

        let task = tokio::spawn(async move {
            let mut pending = HashMap::<String, oneshot::Sender<RawMessage>>::new();

            loop {
                tokio::select! {
                    command = command_rx.recv() => match command {
                        Some(ProxyCommand::Register {
                            correlation_id,
                            reply,
                            registered,
                        }) => {
                            pending.insert(correlation_id, reply);
                            let _ = registered.send(());
                        }
                        Some(ProxyCommand::Cancel { correlation_id }) => {
                            pending.remove(&correlation_id);
                        }
                        None => break,
                    },
                    message = subscription.next() => {
                        let Some(message) = message else {
                            break;
                        };
                        let correlation_id = message
                            .envelope
                            .headers
                            .as_ref()
                            .and_then(|headers| headers.get(CORRELATION_ID_HEADER))
                            .cloned();
                        if let Some(reply) =
                            correlation_id.and_then(|id| pending.remove(&id))
                        {
                            let _ = reply.send(message);
                        }
                    }
                }
            }
        });

        Ok(ProxyRuntime {
            reply_subject,
            commands,
            task,
        })
    }

    pub async fn call<I, O>(&self, input: &I) -> Result<O, BusError>
    where
        I: Serialize + Sync,
        O: DeserializeOwned,
    {
        self.call_with_headers(input, HashMap::new()).await
    }

    pub async fn call_with_headers<I, O>(
        &self,
        input: &I,
        mut headers: HashMap<String, String>,
    ) -> Result<O, BusError>
    where
        I: Serialize + Sync,
        O: DeserializeOwned,
    {
        let runtime = self.runtime.get_or_try_init(|| self.initialize()).await?;
        let commands = &runtime.commands;
        let reply_subject = &runtime.reply_subject;
        let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let correlation_id = Uuid::new_v4().to_string();
        let payload = self.codec.encode(input)?;

        headers.insert(CORRELATION_ID_HEADER.to_string(), correlation_id.clone());
        headers.insert(REPLY_TO_HEADER.to_string(), reply_subject.clone());

        let (reply_tx, reply_rx) = oneshot::channel();
        let (registered_tx, registered_rx) = oneshot::channel();
        commands
            .send(ProxyCommand::Register {
                correlation_id: correlation_id.clone(),
                reply: reply_tx,
                registered: registered_tx,
            })
            .await
            .map_err(|_| BusError::Internal("broker proxy reply task stopped".into()))?;
        registered_rx
            .await
            .map_err(|_| BusError::Internal("broker proxy reply task stopped".into()))?;

        let request = RawMessage {
            envelope: Envelope {
                subject: self.route.pattern.clone(),
                timestamp: std::time::Instant::now(),
                id: request_id,
                headers: Some(headers),
                attempts: 0,
            },
            payload,
        };
        if let Err(error) = self.bus.dispatch(&self.route.pattern, request).await {
            let _ = commands.try_send(ProxyCommand::Cancel { correlation_id });
            return Err(error);
        }

        let reply = match tokio::time::timeout(self.timeout, reply_rx).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => {
                return Err(BusError::Internal(
                    "broker proxy reply task stopped before delivering a response".into(),
                ));
            }
            Err(_) => {
                let _ = commands.try_send(ProxyCommand::Cancel {
                    correlation_id: correlation_id.clone(),
                });
                return Err(BusError::Timeout(format!(
                    "broker request '{correlation_id}' timed out after {:?}",
                    self.timeout
                )));
            }
        };

        self.codec.decode(&reply.payload)
    }
}

impl<B: Bus, C: Codec> Drop for BrokerProxy<B, C> {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.get() {
            let task = &runtime.task;
            task.abort();
        }
    }
}

/// Configuration for a broker-backed router/server layer.
///
/// This intentionally does not choose a serialization format. Qanat external
/// buses already make that a type-level choice through `Codec`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerConfiguration {
    pub broker_url: String,
    pub redelivery_message_delay: Duration,
    pub max_redelivery_retries: Option<usize>,
    pub durability: bool,
}

impl BrokerConfiguration {
    pub fn new(broker_url: impl Into<String>) -> Self {
        Self {
            broker_url: broker_url.into(),
            redelivery_message_delay: DEFAULT_REDELIVERY_MESSAGE_DELAY,
            max_redelivery_retries: Some(DEFAULT_MESSAGE_RETRIES),
            durability: false,
        }
    }

    pub fn with_redelivery_message_delay(mut self, delay: Duration) -> Self {
        self.redelivery_message_delay = delay;
        self
    }

    pub fn with_max_redelivery_retries(mut self, retries: usize) -> Self {
        self.max_redelivery_retries = Some(retries);
        self
    }

    pub fn without_redelivery_limit(mut self) -> Self {
        self.max_redelivery_retries = None;
        self
    }

    pub fn durable(mut self) -> Self {
        self.durability = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::Bus;
    use crate::codec::Codec;
    use crate::http::HttpTarget;
    use crate::message::Envelope;
    use futures::stream;
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use tokio_stream::wrappers::ReceiverStream;

    #[derive(Clone)]
    struct FakeBus {
        group_subscriptions: Arc<AtomicUsize>,
    }

    impl FakeBus {
        fn new() -> Self {
            Self {
                group_subscriptions: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn group_subscription_count(&self) -> usize {
            self.group_subscriptions.load(Ordering::Relaxed)
        }
    }

    impl Bus for FakeBus {
        type Message = RawMessage;
        type Subscription = stream::Empty<RawMessage>;

        fn dispatch<'a>(
            &'a self,
            _subject: &'a str,
            _msg: RawMessage,
        ) -> impl std::future::Future<Output = Result<(), BusError>> + Send + 'a {
            async { Ok(()) }
        }

        async fn subscribe(&self, _pattern: &str) -> Result<Self::Subscription, BusError> {
            Ok(stream::empty())
        }

        async fn subscribe_group(
            &self,
            _pattern: &str,
            _group: &str,
        ) -> Result<Self::Subscription, BusError> {
            self.group_subscriptions.fetch_add(1, Ordering::Relaxed);
            Ok(stream::empty())
        }
    }

    enum LoopbackCommand {
        Dispatch {
            subject: String,
            message: RawMessage,
            completed: oneshot::Sender<()>,
        },
        Subscribe {
            pattern: String,
            subscriber: mpsc::Sender<RawMessage>,
            completed: oneshot::Sender<()>,
        },
    }

    #[derive(Clone)]
    struct LoopbackBus {
        commands: mpsc::Sender<LoopbackCommand>,
    }

    impl Default for LoopbackBus {
        fn default() -> Self {
            let (commands, mut command_rx) = mpsc::channel::<LoopbackCommand>(32);

            tokio::spawn(async move {
                let mut subscribers = HashMap::<String, Vec<mpsc::Sender<RawMessage>>>::new();

                while let Some(command) = command_rx.recv().await {
                    match command {
                        LoopbackCommand::Dispatch {
                            subject,
                            message,
                            completed,
                        } => {
                            if let Some(subject_subscribers) = subscribers.get_mut(&subject) {
                                for subscriber in subject_subscribers.iter() {
                                    let _ = subscriber.send(message.clone()).await;
                                }
                                subject_subscribers.retain(|subscriber| !subscriber.is_closed());
                            }
                            let _ = completed.send(());
                        }
                        LoopbackCommand::Subscribe {
                            pattern,
                            subscriber,
                            completed,
                        } => {
                            subscribers.entry(pattern).or_default().push(subscriber);
                            let _ = completed.send(());
                        }
                    }
                }
            });

            Self { commands }
        }
    }

    impl Bus for LoopbackBus {
        type Message = RawMessage;
        type Subscription = ReceiverStream<RawMessage>;

        fn dispatch<'a>(
            &'a self,
            subject: &'a str,
            message: RawMessage,
        ) -> impl Future<Output = Result<(), BusError>> + Send + 'a {
            async move {
                let (completed, completion) = oneshot::channel();
                self.commands
                    .send(LoopbackCommand::Dispatch {
                        subject: subject.to_string(),
                        message,
                        completed,
                    })
                    .await
                    .map_err(|_| BusError::Internal("loopback bus actor stopped".into()))?;
                completion
                    .await
                    .map_err(|_| BusError::Internal("loopback bus actor stopped".into()))
            }
        }

        async fn subscribe(&self, pattern: &str) -> Result<Self::Subscription, BusError> {
            let (subscriber, receiver) = mpsc::channel(16);
            let (completed, completion) = oneshot::channel();
            self.commands
                .send(LoopbackCommand::Subscribe {
                    pattern: pattern.to_string(),
                    subscriber,
                    completed,
                })
                .await
                .map_err(|_| BusError::Internal("loopback bus actor stopped".into()))?;
            completion
                .await
                .map_err(|_| BusError::Internal("loopback bus actor stopped".into()))?;
            Ok(ReceiverStream::new(receiver))
        }

        async fn subscribe_group(
            &self,
            pattern: &str,
            _group: &str,
        ) -> Result<Self::Subscription, BusError> {
            self.subscribe(pattern).await
        }
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    struct TestMessage {
        id: u64,
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    struct TestReply {
        id: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CorrelationId(String);

    impl crate::router::FromRouteHeader for CorrelationId {
        const NAME: &'static str = "correlation_id";

        fn from_header(value: &str) -> Result<Self, BusError> {
            Ok(Self(value.to_string()))
        }
    }

    async fn handle_test_message(message: TestMessage) {
        let _ = message.id;
    }

    fn raw_message(
        subject: &str,
        payload: bytes::Bytes,
        headers: Option<HashMap<String, String>>,
    ) -> RawMessage {
        RawMessage {
            envelope: Envelope {
                subject: subject.to_string(),
                timestamp: Instant::now(),
                id: 1,
                headers,
                attempts: 0,
            },
            payload,
        }
    }

    struct CaptureTarget {
        sender: mpsc::Sender<RouteMessage>,
    }

    impl RouteTarget for CaptureTarget {
        fn deliver(&self, output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>> {
            Box::pin(async move {
                self.sender
                    .send(output)
                    .await
                    .map_err(|_| BusError::Internal("capture target closed".into()))
            })
        }
    }

    #[test]
    fn broker_configuration_uses_python_equivalent_defaults() {
        let config = BrokerConfiguration::new("amqp://guest:guest@localhost:5672/%2f");

        assert_eq!(config.broker_url, "amqp://guest:guest@localhost:5672/%2f");
        assert_eq!(
            config.redelivery_message_delay,
            DEFAULT_REDELIVERY_MESSAGE_DELAY
        );
        assert_eq!(config.max_redelivery_retries, Some(DEFAULT_MESSAGE_RETRIES));
        assert!(!config.durability);
    }

    #[test]
    fn broker_configuration_builder_sets_operational_options() {
        let config = BrokerConfiguration::new("redis://127.0.0.1/")
            .with_redelivery_message_delay(Duration::from_secs(10))
            .without_redelivery_limit()
            .durable();

        assert_eq!(config.redelivery_message_delay, Duration::from_secs(10));
        assert_eq!(config.max_redelivery_retries, None);
        assert!(config.durability);
    }

    #[test]
    fn broker_route_can_set_reply_topic_prefix() {
        let route = BrokerRoute::new("orders.created", "orders.in")
            .with_reply_to("orders.processed")
            .with_reply_topic_prefix("_qanat.proxy.reply");

        assert_eq!(route.reply_to, None);
        assert_eq!(
            route.reply_topic_prefix.as_deref(),
            Some("_qanat.proxy.reply")
        );
    }

    #[test]
    fn broker_source_contains_bus_pattern_and_group() {
        let bus = FakeBus::new();
        let source = BrokerSource::new(bus, "orders.created", "orders.in");

        assert_eq!(source.route().pattern, "orders.created");
        assert_eq!(source.route().group, "orders.in");
        assert_eq!(source.bus().group_subscription_count(), 0);
    }

    #[test]
    fn neutral_router_owns_the_codec() {
        let bus = FakeBus::new();
        let router = crate::router::Router::with_codec(crate::codec::JsonCodec)
            .bind(handle_test_message)
            .from(BrokerSource::new(
                bus.clone(),
                "orders.created",
                "orders.in",
            ))
            .to(BrokerTarget::new(bus, "orders.processed"));

        let _: &crate::codec::JsonCodec = router.codec();
        assert_eq!(router.route_count(), 1);
    }

    #[test]
    fn broker_target_can_use_fixed_subject() {
        let bus = FakeBus::new();
        let target = BrokerTarget::new(bus, "orders.processed");

        assert!(target.accepts(&route_message_from_broker(raw_message(
            "orders.created",
            bytes::Bytes::new(),
            None,
        ))));
    }

    #[test]
    fn broker_reply_target_requires_reply_header() {
        let bus = FakeBus::new();
        let target = BrokerTarget::reply_to(bus);

        assert!(!target.accepts(&route_message_from_broker(raw_message(
            "orders.created",
            bytes::Bytes::new(),
            None,
        ))));
    }

    #[test]
    fn broker_proxy_uses_broker_route_shape() {
        let bus = FakeBus::new();
        let proxy = BrokerProxy::new(
            bus.clone(),
            BrokerRoute::new("orders.process", "orders.rpc"),
        )
        .reply_topic_prefix(DEFAULT_REPLY_TOPIC_PREFIX)
        .reply_to("orders.reply");

        assert_eq!(proxy.route().pattern, "orders.process");
        assert_eq!(proxy.route().group, "orders.rpc");
        assert_eq!(proxy.route().reply_to.as_deref(), Some("orders.reply"));
        assert_eq!(proxy.route().reply_topic_prefix, None);
        assert_eq!(proxy.bus().group_subscription_count(), 0);
        let _: &crate::codec::JsonCodec = proxy.codec();
    }

    #[tokio::test]
    async fn broker_proxies_get_distinct_instance_reply_subjects() {
        let bus = LoopbackBus::default();
        let first = BrokerProxy::new(
            bus.clone(),
            BrokerRoute::new("orders.process", "orders.rpc"),
        )
        .timeout(Duration::from_millis(1));
        let second = BrokerProxy::new(bus, BrokerRoute::new("orders.process", "orders.rpc"))
            .timeout(Duration::from_millis(1));

        let _: Result<TestReply, _> = first.call(&TestMessage { id: 1 }).await;
        let _: Result<TestReply, _> = second.call(&TestMessage { id: 2 }).await;

        assert_ne!(first.reply_subject(), second.reply_subject());
    }

    #[tokio::test]
    async fn broker_proxy_calls_bound_function_and_decodes_reply() {
        let bus = LoopbackBus::default();
        let called = Arc::new(AtomicUsize::new(0));
        let called_by_handler = Arc::clone(&called);
        let mut router = crate::router::Router::new()
            .bind(move |message: TestMessage| {
                let called = Arc::clone(&called_by_handler);
                async move {
                    called.fetch_add(1, Ordering::Relaxed);
                    TestReply { id: message.id + 1 }
                }
            })
            .from(BrokerSource::new(
                bus.clone(),
                "orders.process",
                "orders.rpc",
            ))
            .to(BrokerTarget::reply_to(bus.clone()));
        router.install().await.unwrap();
        let proxy = BrokerProxy::new(bus, BrokerRoute::new("orders.process", "orders.rpc"))
            .timeout(Duration::from_secs(1));

        let (first, second) = tokio::join!(
            proxy.call::<_, TestReply>(&TestMessage { id: 41 }),
            proxy.call::<_, TestReply>(&TestMessage { id: 99 }),
        );
        let first = first.unwrap_or_else(|error| {
            panic!(
                "first proxy call failed after handler was called {} time(s): {error}",
                called.load(Ordering::Relaxed)
            )
        });
        let second = second.unwrap_or_else(|error| {
            panic!(
                "second proxy call failed after handler was called {} time(s): {error}",
                called.load(Ordering::Relaxed)
            )
        });

        assert_eq!(first.id, 42);
        assert_eq!(second.id, 100);
        assert_eq!(called.load(Ordering::Relaxed), 2);
        assert!(
            proxy
                .reply_subject()
                .unwrap()
                .starts_with(DEFAULT_REPLY_TOPIC_PREFIX)
        );
    }

    #[tokio::test]
    async fn broker_proxy_times_out_when_no_service_replies() {
        let proxy = BrokerProxy::new(
            LoopbackBus::default(),
            BrokerRoute::new("orders.missing", "orders.rpc"),
        )
        .timeout(Duration::from_millis(10));

        let result = proxy.call::<_, TestReply>(&TestMessage { id: 41 }).await;

        assert!(matches!(result, Err(BusError::Timeout(_))));
    }

    #[tokio::test]
    async fn neutral_router_decodes_broker_message_and_calls_function() {
        let bus = LoopbackBus::default();
        let seen = Arc::new(AtomicUsize::new(0));
        let seen_by_handler = Arc::clone(&seen);
        let (output_tx, mut output_rx) = mpsc::channel(1);
        let mut router = crate::router::Router::new()
            .bind(move |message: TestMessage| {
                let seen = Arc::clone(&seen_by_handler);
                async move {
                    seen.fetch_add(message.id as usize, Ordering::Relaxed);
                }
            })
            .from(BrokerSource::new(
                bus.clone(),
                "orders.created",
                "orders.in",
            ))
            .to(CaptureTarget { sender: output_tx });
        router.install().await.unwrap();

        bus.dispatch(
            "orders.created",
            raw_message(
                "orders.created",
                router.codec().encode(&TestMessage { id: 42 }).unwrap(),
                None,
            ),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), output_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(seen.load(Ordering::Relaxed), 42);
    }

    #[tokio::test]
    async fn broker_target_dispatches_encoded_output_to_configured_reply() {
        let bus = LoopbackBus::default();
        let mut replies = bus.subscribe("orders.processed").await.unwrap();
        let mut router = crate::router::Router::new()
            .bind(|message: TestMessage| async move { TestReply { id: message.id + 1 } })
            .from(BrokerSource::new(
                bus.clone(),
                "orders.created",
                "orders.in",
            ))
            .to(BrokerTarget::new(bus.clone(), "orders.processed"));
        router.install().await.unwrap();

        bus.dispatch(
            "orders.created",
            raw_message(
                "orders.created",
                router.codec().encode(&TestMessage { id: 42 }).unwrap(),
                None,
            ),
        )
        .await
        .unwrap();
        let reply = replies.next().await.unwrap();
        let decoded: TestReply = router.codec().decode(&reply.payload).unwrap();

        assert_eq!(reply.envelope.subject, "orders.processed");
        assert_eq!(decoded.id, 43);
    }

    #[tokio::test]
    async fn broker_handler_preserves_correlation_id_in_dynamic_reply() {
        let bus = LoopbackBus::default();
        let mut replies = bus.subscribe("instance.reply.7").await.unwrap();
        let mut router = crate::router::Router::new()
            .bind(|message: TestMessage| async move { TestReply { id: message.id + 1 } })
            .from(BrokerSource::new(
                bus.clone(),
                "orders.created",
                "orders.in",
            ))
            .to(BrokerTarget::reply_to(bus.clone()));
        router.install().await.unwrap();
        let message = raw_message(
            "orders.created",
            router.codec().encode(&TestMessage { id: 42 }).unwrap(),
            Some(HashMap::from([
                (CORRELATION_ID_HEADER.to_string(), "request-42".to_string()),
                (REPLY_TO_HEADER.to_string(), "instance.reply.7".to_string()),
            ])),
        );
        bus.dispatch("orders.created", message).await.unwrap();
        let reply = replies.next().await.unwrap();

        assert_eq!(reply.envelope.subject, "instance.reply.7");
        assert_eq!(
            reply
                .envelope
                .headers
                .as_ref()
                .and_then(|headers| headers.get(CORRELATION_ID_HEADER))
                .map(String::as_str),
            Some("request-42")
        );
        assert!(
            reply
                .envelope
                .headers
                .as_ref()
                .is_some_and(|headers| !headers.contains_key(REPLY_TO_HEADER))
        );
    }

    #[tokio::test]
    async fn broker_handler_extracts_header_and_body_arguments() {
        let bus = LoopbackBus::default();
        let (seen, mut seen_rx) = mpsc::channel::<(String, u64)>(1);
        let (output_tx, mut output_rx) = mpsc::channel(1);
        let mut router = crate::router::Router::new()
            .bind(
                move |RouteHeader(correlation): RouteHeader<CorrelationId>,
                      message: TestMessage| {
                    let seen = seen.clone();
                    async move {
                        seen.send((correlation.0, message.id)).await.unwrap();
                    }
                },
            )
            .from(BrokerSource::new(
                bus.clone(),
                "orders.created",
                "orders.in",
            ))
            .to(CaptureTarget { sender: output_tx });
        router.install().await.unwrap();
        let message = raw_message(
            "orders.created",
            router.codec().encode(&TestMessage { id: 42 }).unwrap(),
            Some(HashMap::from([(
                "correlation_id".to_string(),
                "request-1".to_string(),
            )])),
        );
        bus.dispatch("orders.created", message).await.unwrap();
        output_rx.recv().await.unwrap();

        assert_eq!(seen_rx.recv().await, Some(("request-1".to_string(), 42)));
    }

    #[test]
    fn route_header_extractor_returns_error_when_required_header_is_missing() {
        let message =
            route_message_from_broker(raw_message("orders.created", bytes::Bytes::new(), None));

        let err = RouteHeader::<CorrelationId>::from_message(&message, &crate::codec::JsonCodec)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("missing route header 'correlation_id'")
        );
    }

    #[tokio::test]
    async fn neutral_router_installs_multiple_broker_sources() {
        let bus = FakeBus::new();
        let mut router = crate::router::Router::new()
            .bind(handle_test_message)
            .from(BrokerSource::new(
                bus.clone(),
                "orders.created",
                "orders.in",
            ))
            .to(BrokerTarget::new(bus.clone(), "orders.processed"))
            .bind(handle_test_message)
            .from(BrokerSource::new(
                bus.clone(),
                "payments.created",
                "payments.in",
            ))
            .to(BrokerTarget::new(bus.clone(), "payments.processed"));

        router.install().await.unwrap();

        assert_eq!(router.task_count(), 2);
        assert_eq!(bus.group_subscription_count(), 2);
    }

    #[tokio::test]
    async fn neutral_router_sends_broker_input_to_http_target() {
        let bus = LoopbackBus::default();
        let input_bus = bus.clone();
        let (requests, mut request_rx) = mpsc::channel(1);
        let target = HttpTarget::post("http://orders.test/events", move |request| {
            let requests = requests.clone();
            async move {
                requests.send(request).await.unwrap();
                Ok(crate::http::HttpResponse::new(202))
            }
        });
        let mut router = crate::router::Router::new()
            .bind(
                |mut headers: crate::router::RouteHeaders, message: TestMessage| async move {
                    headers.remove("x-internal");
                    headers.insert("x-processed-by".into(), "orders-service".into());
                    (headers, TestReply { id: message.id + 1 })
                },
            )
            .from(BrokerSource::new(bus, "orders.created", "orders.in"))
            .to(target);
        router.install().await.unwrap();

        input_bus
            .dispatch(
                "orders.created",
                raw_message(
                    "orders.created",
                    router.codec().encode(&TestMessage { id: 41 }).unwrap(),
                    Some(HashMap::from([
                        (CORRELATION_ID_HEADER.to_string(), "request-41".to_string()),
                        ("x-internal".to_string(), "secret".to_string()),
                    ])),
                ),
            )
            .await
            .unwrap();

        let request = tokio::time::timeout(Duration::from_secs(1), request_rx.recv())
            .await
            .expect("HTTP target was not invoked")
            .expect("HTTP target channel closed");
        let reply: TestReply = router.codec().decode(&request.body).unwrap();

        assert_eq!(request.method, crate::http::HttpMethod::Post);
        assert_eq!(request.url, "http://orders.test/events");
        assert_eq!(reply.id, 42);
        assert_eq!(
            request
                .headers
                .get(CORRELATION_ID_HEADER)
                .map(String::as_str),
            Some("request-41")
        );
        assert_eq!(
            request.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
        assert_eq!(
            request.headers.get("x-processed-by").map(String::as_str),
            Some("orders-service")
        );
        assert!(!request.headers.contains_key("x-internal"));
    }
}
