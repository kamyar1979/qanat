use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::bus::Bus;
use crate::codec::{Codec, JsonCodec};
use crate::errors::BusError;
use crate::message::Envelope;
use crate::raw_message::RawMessage;
use futures::StreamExt;
use futures::future::BoxFuture;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{OnceCell, mpsc, oneshot};

pub const DEFAULT_REDELIVERY_MESSAGE_DELAY: Duration = Duration::from_secs(5);
pub const DEFAULT_MESSAGE_RETRIES: usize = 3;
pub const DEFAULT_PROXY_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_REPLY_TOPIC_PREFIX: &str = "_qanat.reply";
pub const CORRELATION_ID_HEADER: &str = "correlation_id";
pub const REPLY_TO_HEADER: &str = "reply_to";

static NEXT_PROXY_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static PROCESS_UNIQUE_ID: LazyLock<u64> =
    LazyLock::new(|| RandomState::new().build_hasher().finish());

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

struct BrokerBinding<C: Codec> {
    route: BrokerRoute,
    handler: Arc<dyn BrokerHandler<C>>,
}

pub struct BrokerRouter<B: Bus, C: Codec = JsonCodec> {
    bus: Arc<B>,
    codec: Arc<C>,
    bindings: Vec<BrokerBinding<C>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl<B: Bus> BrokerRouter<B, JsonCodec> {
    pub fn new(bus: B) -> Self {
        Self::with_codec(bus, JsonCodec)
    }
}

impl<B, C> BrokerRouter<B, C>
where
    B: Bus,
    C: Codec,
{
    pub fn with_codec(bus: B, codec: C) -> Self {
        Self {
            bus: Arc::new(bus),
            codec: Arc::new(codec),
            bindings: Vec::new(),
            tasks: Vec::new(),
        }
    }

    pub fn bind<H, Args>(
        mut self,
        pattern: impl Into<String>,
        group: impl Into<String>,
        handler: H,
    ) -> Self
    where
        H: IntoBrokerHandler<C, Args>,
        Args: Send + Sync + 'static,
    {
        self.bindings.push(BrokerBinding {
            route: BrokerRoute::new(pattern, group),
            handler: handler.into_handler(),
        });
        self
    }

    pub fn reply_to(mut self, reply_to: impl Into<String>) -> Self {
        if let Some(binding) = self.bindings.last_mut() {
            binding.route.reply_to = Some(reply_to.into());
            binding.route.reply_topic_prefix = None;
        }
        self
    }

    pub fn reply_topic_prefix(mut self, prefix: impl Into<String>) -> Self {
        if let Some(binding) = self.bindings.last_mut() {
            binding.route.reply_to = None;
            binding.route.reply_topic_prefix = Some(prefix.into());
        }
        self
    }

    pub fn bus(&self) -> &B {
        self.bus.as_ref()
    }

    pub fn codec(&self) -> &C {
        &self.codec
    }

    pub fn route_count(&self) -> usize {
        self.bindings.len()
    }

    pub fn routes(&self) -> impl ExactSizeIterator<Item = &BrokerRoute> {
        self.bindings.iter().map(|binding| &binding.route)
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn proxy(&self, route: BrokerRoute) -> BrokerProxy<B, C> {
        BrokerProxy::from_shared(Arc::clone(&self.bus), Arc::clone(&self.codec), route)
    }

    pub async fn install(&mut self) -> Result<(), BusError>
    where
        B: Bus<Message = RawMessage> + 'static,
    {
        for binding in self.bindings.iter() {
            let route = binding.route.clone();
            let handler = Arc::clone(&binding.handler);
            let mut subscription = self
                .bus
                .subscribe_group(&route.pattern, &route.group)
                .await?;
            let codec = Arc::clone(&self.codec);
            let bus = Arc::clone(&self.bus);

            let task = tokio::spawn(async move {
                while let Some(message) = subscription.next().await {
                    if let Ok(Some((reply_subject, reply))) =
                        handler.call(message, &route, codec.as_ref()).await
                    {
                        let _ = bus.dispatch(&reply_subject, reply).await;
                    }
                }
            });
            self.tasks.push(task);
        }
        Ok(())
    }
}

pub trait BrokerHandler<C: Codec>: Send + Sync {
    fn call<'a>(
        &'a self,
        message: RawMessage,
        route: &'a BrokerRoute,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<Option<(String, RawMessage)>, BusError>>;
}

pub trait IntoBrokerHandler<C: Codec, Args>: Send + Sync + 'static {
    fn into_handler(self) -> Arc<dyn BrokerHandler<C>>;
}

pub struct TypedBrokerHandler<Args, O, F> {
    handler: F,
    _types: PhantomData<fn(Args) -> O>,
}

fn encode_handler_reply<O: Serialize>(
    output: &O,
    mut message: RawMessage,
    route: &BrokerRoute,
    codec: &impl Codec,
) -> Result<Option<(String, RawMessage)>, BusError> {
    let reply_to = message
        .envelope
        .headers
        .as_ref()
        .and_then(|headers| headers.get(REPLY_TO_HEADER))
        .cloned()
        .or_else(|| route.reply_to.clone());
    let Some(reply_to) = reply_to else {
        return Ok(None);
    };

    if let Some(headers) = message.envelope.headers.as_mut() {
        headers.remove(REPLY_TO_HEADER);
    }

    let reply = RawMessage {
        envelope: Envelope {
            subject: reply_to.clone(),
            timestamp: std::time::Instant::now(),
            id: message.envelope.id,
            headers: message.envelope.headers,
            attempts: 0,
        },
        payload: codec.encode(output)?,
    };
    Ok(Some((reply_to, reply)))
}

impl<Args, O, F> TypedBrokerHandler<Args, O, F> {
    pub fn handler(&self) -> &F {
        &self.handler
    }
}

impl<C, A, O, F, Fut> IntoBrokerHandler<C, (A,)> for F
where
    C: Codec,
    A: FromBrokerMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    fn into_handler(self) -> Arc<dyn BrokerHandler<C>> {
        Arc::new(TypedBrokerHandler {
            handler: self,
            _types: PhantomData::<fn((A,)) -> O>,
        })
    }
}

impl<C, A, B, O, F, Fut> IntoBrokerHandler<C, (A, B)> for F
where
    C: Codec,
    A: FromBrokerMessage<C> + Send + Sync + 'static,
    B: FromBrokerMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    fn into_handler(self) -> Arc<dyn BrokerHandler<C>> {
        Arc::new(TypedBrokerHandler {
            handler: self,
            _types: PhantomData::<fn((A, B)) -> O>,
        })
    }
}

impl<C, A, B, D, O, F, Fut> IntoBrokerHandler<C, (A, B, D)> for F
where
    C: Codec,
    A: FromBrokerMessage<C> + Send + Sync + 'static,
    B: FromBrokerMessage<C> + Send + Sync + 'static,
    D: FromBrokerMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B, D) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    fn into_handler(self) -> Arc<dyn BrokerHandler<C>> {
        Arc::new(TypedBrokerHandler {
            handler: self,
            _types: PhantomData::<fn((A, B, D)) -> O>,
        })
    }
}

impl<A, O, F, Fut, C> BrokerHandler<C> for TypedBrokerHandler<(A,), O, F>
where
    A: FromBrokerMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
    C: Codec,
{
    fn call<'a>(
        &'a self,
        message: RawMessage,
        route: &'a BrokerRoute,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<Option<(String, RawMessage)>, BusError>> {
        Box::pin(async move {
            let output = (self.handler)(A::from_message(&message, codec)?).await;
            encode_handler_reply(&output, message, route, codec)
        })
    }
}

impl<A, B, O, F, Fut, C> BrokerHandler<C> for TypedBrokerHandler<(A, B), O, F>
where
    A: FromBrokerMessage<C> + Send + Sync + 'static,
    B: FromBrokerMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
    C: Codec,
{
    fn call<'a>(
        &'a self,
        message: RawMessage,
        route: &'a BrokerRoute,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<Option<(String, RawMessage)>, BusError>> {
        Box::pin(async move {
            let output = (self.handler)(
                A::from_message(&message, codec)?,
                B::from_message(&message, codec)?,
            )
            .await;
            encode_handler_reply(&output, message, route, codec)
        })
    }
}

impl<A, B, D, O, F, Fut, C> BrokerHandler<C> for TypedBrokerHandler<(A, B, D), O, F>
where
    A: FromBrokerMessage<C> + Send + Sync + 'static,
    B: FromBrokerMessage<C> + Send + Sync + 'static,
    D: FromBrokerMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B, D) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
    C: Codec,
{
    fn call<'a>(
        &'a self,
        message: RawMessage,
        route: &'a BrokerRoute,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<Option<(String, RawMessage)>, BusError>> {
        Box::pin(async move {
            let output = (self.handler)(
                A::from_message(&message, codec)?,
                B::from_message(&message, codec)?,
                D::from_message(&message, codec)?,
            )
            .await;
            encode_handler_reply(&output, message, route, codec)
        })
    }
}

pub trait FromBrokerMessage<C: Codec>: Sized {
    fn from_message(message: &RawMessage, codec: &C) -> Result<Self, BusError>;
}

impl<T, C> FromBrokerMessage<C> for T
where
    T: DeserializeOwned,
    C: Codec,
{
    fn from_message(message: &RawMessage, codec: &C) -> Result<Self, BusError> {
        codec.decode(&message.payload)
    }
}

#[derive(Clone, Debug)]
pub struct BrokerEnvelope(pub Envelope);

impl<C: Codec> FromBrokerMessage<C> for BrokerEnvelope {
    fn from_message(message: &RawMessage, _codec: &C) -> Result<Self, BusError> {
        Ok(Self(message.envelope.clone()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerHeaders(pub std::collections::HashMap<String, String>);

impl<C: Codec> FromBrokerMessage<C> for BrokerHeaders {
    fn from_message(message: &RawMessage, _codec: &C) -> Result<Self, BusError> {
        Ok(Self(message.envelope.headers.clone().unwrap_or_default()))
    }
}

#[derive(Clone, Debug)]
pub struct BrokerHeader<T>(pub T);

pub trait FromBrokerHeader: Sized {
    const NAME: &'static str;

    fn from_header(value: &str) -> Result<Self, BusError>;
}

impl<T, C> FromBrokerMessage<C> for BrokerHeader<T>
where
    T: FromBrokerHeader,
    C: Codec,
{
    fn from_message(message: &RawMessage, _codec: &C) -> Result<Self, BusError> {
        let headers = message
            .envelope
            .headers
            .as_ref()
            .ok_or_else(|| BusError::Internal("message has no headers".into()))?;
        let value = headers
            .get(T::NAME)
            .ok_or_else(|| BusError::Internal(format!("missing broker header '{}'", T::NAME)))?;
        Ok(Self(T::from_header(value)?))
    }
}

#[derive(Clone, Debug)]
pub struct BrokerRawMessage(pub RawMessage);

impl<C: Codec> FromBrokerMessage<C> for BrokerRawMessage {
    fn from_message(message: &RawMessage, _codec: &C) -> Result<Self, BusError> {
        Ok(Self(message.clone()))
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
                let proxy_id = NEXT_PROXY_ID.fetch_add(1, Ordering::Relaxed);
                format!("{prefix}.{:016x}.{proxy_id}", *PROCESS_UNIQUE_ID)
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
        let correlation_id = format!("{reply_subject}.{request_id}");
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
    use crate::message::Envelope;
    use futures::stream;
    use std::collections::HashMap;
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

    impl FromBrokerHeader for CorrelationId {
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
    fn broker_router_builds_broker_source_endpoint() {
        let bus = FakeBus::new();
        let router =
            BrokerRouter::new(bus.clone()).bind("orders.created", "orders.in", handle_test_message);
        let routes: Vec<_> = router.routes().collect();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].pattern, "orders.created");
        assert_eq!(routes[0].group, "orders.in");
        assert_eq!(routes[0].reply_topic_prefix, None);
        assert_eq!(router.bus().group_subscription_count(), 0);
    }

    #[test]
    fn broker_router_accepts_explicit_codec() {
        let bus = FakeBus::new();
        let router = BrokerRouter::with_codec(bus, crate::codec::JsonCodec).bind(
            "orders.created",
            "orders.in",
            handle_test_message,
        );

        let _: &crate::codec::JsonCodec = router.codec();
        assert_eq!(router.route_count(), 1);
    }

    #[test]
    fn broker_router_sets_reply_to_on_last_binding() {
        let bus = FakeBus::new();
        let router = BrokerRouter::new(bus)
            .bind("orders.created", "orders.in", handle_test_message)
            .reply_to("orders.processed");
        let routes: Vec<_> = router.routes().collect();

        assert_eq!(routes[0].reply_to.as_deref(), Some("orders.processed"));
    }

    #[test]
    fn broker_router_sets_reply_topic_prefix_on_last_binding() {
        let bus = FakeBus::new();
        let router = BrokerRouter::new(bus)
            .bind("orders.created", "orders.in", handle_test_message)
            .reply_to("orders.processed")
            .reply_topic_prefix("_qanat.proxy.reply");
        let routes: Vec<_> = router.routes().collect();

        assert_eq!(
            routes[0].reply_topic_prefix.as_deref(),
            Some("_qanat.proxy.reply")
        );
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
        let mut router = BrokerRouter::new(bus).bind(
            "orders.process",
            "orders.rpc",
            move |message: TestMessage| {
                let called = Arc::clone(&called_by_handler);
                async move {
                    called.fetch_add(1, Ordering::Relaxed);
                    TestReply { id: message.id + 1 }
                }
            },
        );
        router.install().await.unwrap();
        let proxy = router
            .proxy(BrokerRoute::new("orders.process", "orders.rpc"))
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
    async fn broker_handler_decodes_message_and_calls_function() {
        let seen = Arc::new(AtomicUsize::new(0));
        let seen_by_handler = Arc::clone(&seen);
        let router = BrokerRouter::new(FakeBus::new()).bind(
            "orders.created",
            "orders.in",
            move |message: TestMessage| {
                let seen = Arc::clone(&seen_by_handler);
                async move {
                    seen.fetch_add(message.id as usize, Ordering::Relaxed);
                }
            },
        );
        let payload = router
            .codec()
            .encode(&serde_json::json!({ "id": 42 }))
            .unwrap();
        let message = raw_message("orders.created", payload, None);

        let binding = &router.bindings[0];
        binding
            .handler
            .call(message, &binding.route, router.codec())
            .await
            .unwrap();

        assert_eq!(seen.load(Ordering::Relaxed), 42);
    }

    #[tokio::test]
    async fn broker_handler_encodes_reply_when_reply_to_is_set() {
        let router = BrokerRouter::new(FakeBus::new())
            .bind(
                "orders.created",
                "orders.in",
                |message: TestMessage| async move { TestReply { id: message.id + 1 } },
            )
            .reply_to("orders.processed");
        let payload = router
            .codec()
            .encode(&serde_json::json!({ "id": 42 }))
            .unwrap();
        let message = raw_message("orders.created", payload, None);
        let binding = &router.bindings[0];

        let (subject, reply) = binding
            .handler
            .call(message, &binding.route, router.codec())
            .await
            .unwrap()
            .unwrap();
        let decoded: TestReply = router.codec().decode(&reply.payload).unwrap();

        assert_eq!(subject, "orders.processed");
        assert_eq!(reply.envelope.subject, "orders.processed");
        assert_eq!(decoded.id, 43);
    }

    #[tokio::test]
    async fn broker_handler_preserves_correlation_id_in_dynamic_reply() {
        let router = BrokerRouter::new(FakeBus::new()).bind(
            "orders.created",
            "orders.in",
            |message: TestMessage| async move { TestReply { id: message.id + 1 } },
        );
        let payload = router.codec().encode(&TestMessage { id: 42 }).unwrap();
        let message = raw_message(
            "orders.created",
            payload,
            Some(HashMap::from([
                (CORRELATION_ID_HEADER.to_string(), "request-42".to_string()),
                (REPLY_TO_HEADER.to_string(), "instance.reply.7".to_string()),
            ])),
        );
        let binding = &router.bindings[0];

        let (subject, reply) = binding
            .handler
            .call(message, &binding.route, router.codec())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(subject, "instance.reply.7");
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
        let (seen, mut seen_rx) = mpsc::channel::<(String, u64)>(1);
        let router = BrokerRouter::new(FakeBus::new()).bind(
            "orders.created",
            "orders.in",
            move |BrokerHeader(correlation): BrokerHeader<CorrelationId>, message: TestMessage| {
                let seen = seen.clone();
                async move {
                    seen.send((correlation.0, message.id)).await.unwrap();
                }
            },
        );
        let payload = router
            .codec()
            .encode(&serde_json::json!({ "id": 42 }))
            .unwrap();
        let message = raw_message(
            "orders.created",
            payload,
            Some(HashMap::from([(
                "correlation_id".to_string(),
                "request-1".to_string(),
            )])),
        );
        let binding = &router.bindings[0];

        binding
            .handler
            .call(message, &binding.route, router.codec())
            .await
            .unwrap();

        assert_eq!(seen_rx.recv().await, Some(("request-1".to_string(), 42)));
    }

    #[tokio::test]
    async fn broker_handler_returns_error_when_required_header_is_missing() {
        let router = BrokerRouter::new(FakeBus::new()).bind(
            "orders.created",
            "orders.in",
            |_correlation: BrokerHeader<CorrelationId>| async {},
        );
        let payload = router
            .codec()
            .encode(&serde_json::json!({ "id": 42 }))
            .unwrap();
        let message = raw_message("orders.created", payload, None);
        let binding = &router.bindings[0];

        let err = binding
            .handler
            .call(message, &binding.route, router.codec())
            .await
            .unwrap_err();

        assert!(err.to_string().contains("message has no headers"));
    }

    #[tokio::test]
    async fn broker_router_installs_routes_into_bus() {
        let bus = FakeBus::new();
        let mut router = BrokerRouter::new(bus.clone())
            .bind("orders.created", "orders.in", handle_test_message)
            .bind("payments.created", "payments.in", handle_test_message);

        router.install().await.unwrap();

        assert_eq!(router.task_count(), 2);
        assert_eq!(bus.group_subscription_count(), 2);
    }
}
