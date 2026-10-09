use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::{BoxFuture, LocalBoxFuture};
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::DeliveryDecision;
use crate::errors::{BackendError, BusError};
pub use serde_value::Value as PayloadValue;
use serde_value::Value;

pub const ROUTE_FAILURE_HEADER: &str = "qanat-route-failure";
pub const HTTP_STATUS_HEADER: &str = "qanat-http-status";

#[derive(Clone, Debug)]
pub struct RouteMessage {
    pub address: String,
    pub timestamp: std::time::Instant,
    pub id: u64,
    pub message_id: String,
    pub headers: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
    pub attempts: u32,
    pub payload: Bytes,
}

impl RouteMessage {
    pub fn new(address: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        let message_id = crate::message::new_message_id();
        Self {
            address: address.into(),
            timestamp: std::time::Instant::now(),
            id: 0,
            message_id,
            headers: HashMap::new(),
            metadata: HashMap::new(),
            attempts: 0,
            payload: payload.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteErrorStage {
    Handler,
    Delivery,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct RouteError {
    pub stage: RouteErrorStage,
    pub code: String,
    pub message: String,
}

impl RouteError {
    pub fn handler(error: impl std::fmt::Display) -> Self {
        Self {
            stage: RouteErrorStage::Handler,
            code: "route.handler".to_string(),
            message: error.to_string(),
        }
    }

    pub fn delivery(error: impl std::fmt::Display) -> Self {
        Self {
            stage: RouteErrorStage::Delivery,
            code: "route.delivery".to_string(),
            message: error.to_string(),
        }
    }
}

impl std::fmt::Display for RouteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for RouteError {}

impl From<BusError> for RouteError {
    fn from(error: BusError) -> Self {
        Self::handler(error)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct FailedRouteMessage {
    pub address: String,
    pub id: u64,
    pub message_id: String,
    pub headers: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
    pub attempts: u32,
    pub payload: Vec<u8>,
    /// A readable view of the original payload when its content type declares
    /// text and the bytes are valid UTF-8. The raw payload remains authoritative.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_text: Option<String>,
}

impl From<RouteMessage> for FailedRouteMessage {
    fn from(message: RouteMessage) -> Self {
        let payload = message.payload.to_vec();
        let payload_text = content_type_is_textual(&message.headers)
            .then(|| String::from_utf8(payload.clone()).ok())
            .flatten();
        Self {
            address: message.address,
            id: message.id,
            message_id: message.message_id,
            headers: message.headers,
            metadata: message.metadata,
            attempts: message.attempts,
            payload,
            payload_text,
        }
    }
}

fn content_type_is_textual(headers: &HashMap<String, String>) -> bool {
    let Some(value) = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value)
    else {
        return false;
    };
    let media_type = value.split(';').next().unwrap_or_default().trim();
    let media_type = media_type.to_ascii_lowercase();
    media_type.starts_with("text/")
        || media_type == "application/json"
        || (media_type.starts_with("application/") && media_type.ends_with("+json"))
        || media_type == "application/xml"
        || (media_type.starts_with("application/") && media_type.ends_with("+xml"))
        || media_type == "application/x-www-form-urlencoded"
        || media_type == "application/javascript"
        || media_type == "application/ecmascript"
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct RouteFailure {
    pub error: RouteError,
    pub original: FailedRouteMessage,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteHeaders(pub HashMap<String, String>);

impl std::ops::Deref for RouteHeaders {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for RouteHeaders {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<HashMap<String, String>> for RouteHeaders {
    fn from(headers: HashMap<String, String>) -> Self {
        Self(headers)
    }
}

impl From<RouteHeaders> for HashMap<String, String> {
    fn from(headers: RouteHeaders) -> Self {
        headers.0
    }
}

#[derive(Clone, Debug)]
pub struct RouteHeader<T>(pub T);

pub trait FromRouteHeader: Sized {
    const NAME: &'static str;

    fn from_header(value: &str) -> Result<Self, BusError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutePayload(pub Bytes);

pub type RouteStream = BoxStream<'static, RouteMessage>;

pub trait RouteSource: Send + Sync + 'static {
    fn decode(&self, _message: &RouteMessage) -> Result<Value, BusError> {
        Err(BusError::Serialization(
            "source does not support typed payload decoding".into(),
        ))
    }

    fn open(&mut self) -> LocalBoxFuture<'_, Result<RouteStream, BusError>>;

    fn settle(
        &self,
        _message: &RouteMessage,
        _decision: DeliveryDecision,
    ) -> BoxFuture<'_, Result<(), BusError>> {
        Box::pin(async { Ok(()) })
    }
}

pub trait RouteTarget: Send + Sync + 'static {
    fn encode(&self, _value: &Value, _message: &mut RouteMessage) -> Result<(), BusError> {
        Err(BusError::Serialization(
            "target does not support typed payload encoding".into(),
        ))
    }

    fn accepts(&self, _message: &RouteMessage) -> bool {
        true
    }

    fn deliver(&self, output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>>;
}

const DEFAULT_PARTITION_CAPACITY: usize = 64;
const DEFAULT_PARTITION_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

type PartitionTask = BoxFuture<'static, ()>;

struct PartitionJob {
    task: PartitionTask,
    completion: tokio::sync::oneshot::Sender<()>,
}

type PartitionSender = tokio::sync::mpsc::Sender<PartitionJob>;
type PartitionSenderReply = tokio::sync::oneshot::Sender<PartitionSender>;

enum PartitionRegistryCommand {
    Acquire {
        key: String,
        reply: PartitionSenderReply,
    },
    BeginRetirement {
        key: String,
        generation: u64,
        reply: tokio::sync::oneshot::Sender<bool>,
    },
    Retired {
        key: String,
        generation: u64,
    },
}

enum PartitionRegistryEntry {
    Active {
        generation: u64,
        sender: PartitionSender,
    },
    Retiring {
        generation: u64,
        waiters: Vec<PartitionSenderReply>,
    },
}

/// A process-local keyed execution domain shared by one or more routes.
///
/// Each key has a bounded Tokio channel and one worker. Jobs for the same key
/// run sequentially, while different keys can run concurrently. Clone and
/// reuse a `Partitioner` to coordinate related routes in the same process.
#[derive(Clone)]
pub struct Partitioner {
    inner: Arc<PartitionerConfig>,
}

struct PartitionerConfig {
    capacity: usize,
    idle_timeout: Duration,
    registry: tokio::sync::OnceCell<tokio::sync::mpsc::UnboundedSender<PartitionRegistryCommand>>,
}

impl Partitioner {
    pub fn new() -> Self {
        Self::with_options(DEFAULT_PARTITION_CAPACITY, DEFAULT_PARTITION_IDLE_TIMEOUT)
    }

    /// Create a partitioner with bounded per-key queues and the default idle
    /// worker timeout.
    ///
    /// # Panics
    ///
    /// Panics when `capacity` is zero.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_options(capacity, DEFAULT_PARTITION_IDLE_TIMEOUT)
    }

    /// Create a partitioner with explicit per-key queue capacity and idle
    /// worker timeout.
    ///
    /// # Panics
    ///
    /// Panics when `capacity` is zero or `idle_timeout` is zero.
    pub fn with_options(capacity: usize, idle_timeout: Duration) -> Self {
        assert!(capacity > 0, "partition queue capacity must be nonzero");
        assert!(
            !idle_timeout.is_zero(),
            "partition idle timeout must be nonzero"
        );
        Self {
            inner: Arc::new(PartitionerConfig {
                capacity,
                idle_timeout,
                registry: tokio::sync::OnceCell::new(),
            }),
        }
    }

    async fn registry(&self) -> &tokio::sync::mpsc::UnboundedSender<PartitionRegistryCommand> {
        self.inner
            .registry
            .get_or_init(|| async {
                let (registry, commands) = tokio::sync::mpsc::unbounded_channel();
                tokio::spawn(partition_registry(
                    commands,
                    registry.downgrade(),
                    self.inner.capacity,
                    self.inner.idle_timeout,
                ));
                registry
            })
            .await
    }

    async fn submit_task(
        &self,
        key: String,
        task: PartitionTask,
    ) -> tokio::sync::oneshot::Receiver<()> {
        let (completion, receiver) = tokio::sync::oneshot::channel();
        let mut job = PartitionJob { task, completion };
        loop {
            let (reply, sender) = tokio::sync::oneshot::channel();
            self.registry()
                .await
                .send(PartitionRegistryCommand::Acquire {
                    key: key.clone(),
                    reply,
                })
                .expect("partition registry stopped while its handle is alive");
            let sender = sender
                .await
                .expect("partition registry dropped an acquisition request");
            match sender.send(job).await {
                Ok(()) => return receiver,
                Err(error) => job = error.0,
            }
        }
    }
}

impl Default for Partitioner {
    fn default() -> Self {
        Self::new()
    }
}

async fn partition_registry(
    mut commands: tokio::sync::mpsc::UnboundedReceiver<PartitionRegistryCommand>,
    registry: tokio::sync::mpsc::WeakUnboundedSender<PartitionRegistryCommand>,
    capacity: usize,
    idle_timeout: Duration,
) {
    let mut entries = HashMap::<String, PartitionRegistryEntry>::new();
    let mut next_generation = 1u64;

    while let Some(command) = commands.recv().await {
        match command {
            PartitionRegistryCommand::Acquire { key, reply } => match entries.get_mut(&key) {
                Some(PartitionRegistryEntry::Active { sender, .. }) => {
                    let _ = reply.send(sender.clone());
                }
                Some(PartitionRegistryEntry::Retiring { waiters, .. }) => waiters.push(reply),
                None => {
                    let generation = next_generation;
                    next_generation = next_generation.wrapping_add(1);
                    let sender = spawn_partition_worker(
                        registry.clone(),
                        key.clone(),
                        generation,
                        capacity,
                        idle_timeout,
                    );
                    let _ = reply.send(sender.clone());
                    entries.insert(key, PartitionRegistryEntry::Active { generation, sender });
                }
            },
            PartitionRegistryCommand::BeginRetirement {
                key,
                generation,
                reply,
            } => {
                let current = entries.remove(&key);
                match current {
                    Some(PartitionRegistryEntry::Active {
                        generation: current_generation,
                        sender,
                    }) if current_generation == generation => {
                        drop(sender);
                        entries.insert(
                            key,
                            PartitionRegistryEntry::Retiring {
                                generation,
                                waiters: Vec::new(),
                            },
                        );
                        let _ = reply.send(true);
                    }
                    Some(entry) => {
                        entries.insert(key, entry);
                        let _ = reply.send(false);
                    }
                    None => {
                        let _ = reply.send(false);
                    }
                }
            }
            PartitionRegistryCommand::Retired { key, generation } => {
                let current = entries.remove(&key);
                match current {
                    Some(PartitionRegistryEntry::Retiring {
                        generation: current_generation,
                        mut waiters,
                    }) if current_generation == generation => {
                        waiters.retain(|waiter| !waiter.is_closed());
                        if waiters.is_empty() {
                            continue;
                        }
                        let next = next_generation;
                        next_generation = next_generation.wrapping_add(1);
                        let sender = spawn_partition_worker(
                            registry.clone(),
                            key.clone(),
                            next,
                            capacity,
                            idle_timeout,
                        );
                        for waiter in waiters {
                            let _ = waiter.send(sender.clone());
                        }
                        entries.insert(
                            key,
                            PartitionRegistryEntry::Active {
                                generation: next,
                                sender,
                            },
                        );
                    }
                    Some(entry) => {
                        entries.insert(key, entry);
                    }
                    None => {}
                }
            }
        }
    }
}

fn spawn_partition_worker(
    registry: tokio::sync::mpsc::WeakUnboundedSender<PartitionRegistryCommand>,
    key: String,
    generation: u64,
    capacity: usize,
    idle_timeout: Duration,
) -> PartitionSender {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(capacity);
    tokio::spawn(async move {
        loop {
            match tokio::time::timeout(idle_timeout, receiver.recv()).await {
                Ok(Some(job)) => {
                    run_partition_job(&key, job).await;
                }
                Ok(None) => break,
                Err(_) => {
                    let Some(registry) = registry.upgrade() else {
                        while let Some(job) = receiver.recv().await {
                            run_partition_job(&key, job).await;
                        }
                        break;
                    };
                    let (reply, retirement) = tokio::sync::oneshot::channel();
                    if registry
                        .send(PartitionRegistryCommand::BeginRetirement {
                            key: key.clone(),
                            generation,
                            reply,
                        })
                        .is_err()
                    {
                        continue;
                    }
                    if retirement.await != Ok(true) {
                        continue;
                    }

                    // The registry has dropped its sender and queues new
                    // acquisitions until this generation drains every sender
                    // that was handed out before retirement began.
                    while let Some(job) = receiver.recv().await {
                        run_partition_job(&key, job).await;
                    }
                    let _ = registry.send(PartitionRegistryCommand::Retired { key, generation });
                    break;
                }
            }
        }
    });
    sender
}

async fn run_partition_job(key: &str, job: PartitionJob) {
    let PartitionJob { task, completion } = job;
    if std::panic::AssertUnwindSafe(task)
        .catch_unwind()
        .await
        .is_err()
    {
        tracing::error!(partition_key = %key, "partition job panicked");
    }
    let _ = completion.send(());
}

trait PartitionResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        message: &'a RouteMessage,
        decoder: &'a dyn RouteSource,
    ) -> BoxFuture<'a, Result<String, RouteError>>;
}

type PartitionResolverTypes<I, K, E, Fut> = fn(I) -> (K, E, Fut);

struct TypedPartitionResolver<I, K, E, F, Fut> {
    resolver: F,
    _types: PhantomData<PartitionResolverTypes<I, K, E, Fut>>,
}

impl<I, K, E, F, Fut> PartitionResolver for TypedPartitionResolver<I, K, E, F, Fut>
where
    I: FromRouteMessage + Send + Sync + 'static,
    K: Into<String> + Send + Sync + 'static,
    E: std::fmt::Display + Send + Sync + 'static,
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<K, E>> + Send + 'static,
{
    fn resolve<'a>(
        &'a self,
        message: &'a RouteMessage,
        decoder: &'a dyn RouteSource,
    ) -> BoxFuture<'a, Result<String, RouteError>> {
        Box::pin(async move {
            let input = I::from_message(message, decoder).map_err(RouteError::handler)?;
            let key = (self.resolver)(input)
                .await
                .map_err(RouteError::handler)?
                .into();
            if key.is_empty() {
                return Err(RouteError::handler("partition key must not be empty"));
            }
            Ok(key)
        })
    }
}

struct PartitionBinding {
    partitioner: Partitioner,
    resolver: Arc<dyn PartitionResolver>,
}

struct RouteBinding {
    source: Option<Box<dyn RouteSource>>,
    handler: Arc<dyn RouteHandler>,
    target: Option<Arc<dyn RouteTarget>>,
    error_target: Option<Arc<dyn RouteTarget>>,
    failure_decision: DeliveryDecision,
    partition: Option<PartitionBinding>,
}

pub struct Router {
    bindings: Vec<RouteBinding>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            bindings: Vec::new(),
            tasks: Vec::new(),
        }
    }

    pub fn bind<H, Args, OutputMode>(self, handler: H) -> Bind<Args>
    where
        H: IntoRouteHandler<Args, OutputMode>,
        Args: Send + Sync + 'static,
    {
        Bind {
            router: self,
            handler: handler.into_handler(),
            error_target: None,
            failure_decision: DeliveryDecision::default(),
            partition: None,
            _args: PhantomData,
        }
    }

    pub fn route_count(&self) -> usize {
        self.bindings.len()
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub async fn install(&mut self) -> Result<(), BusError> {
        for binding in &mut self.bindings {
            let mut source = binding
                .source
                .take()
                .ok_or_else(|| BusError::Internal("route is already installed".into()))?;
            let mut subscription = source.open().await?;
            let source: Arc<dyn RouteSource> = Arc::from(source);
            let handler = Arc::clone(&binding.handler);
            let target = binding.target.clone();
            let error_target = binding.error_target.clone();
            let failure_decision = binding.failure_decision;
            let partition = binding.partition.take();

            self.tasks.push(tokio::spawn(async move {
                let Some(partition) = partition else {
                    while let Some(message) = subscription.next().await {
                        process_route_message(
                            Arc::clone(&source),
                            Arc::clone(&handler),
                            target.clone(),
                            error_target.clone(),
                            failure_decision,
                            message,
                        )
                        .await;
                    }
                    return;
                };

                let mut pending = FuturesUnordered::new();
                let mut source_open = true;
                while source_open || !pending.is_empty() {
                    tokio::select! {
                        message = subscription.next(), if source_open => {
                            let Some(message) = message else {
                                source_open = false;
                                continue;
                            };
                            let key = match partition.resolver.resolve(&message, source.as_ref()).await {
                                Ok(key) => key,
                                Err(error) => {
                                    deliver_failure(
                                        error_target.as_ref(),
                                        message.clone(),
                                        error,
                                        None,
                                    )
                                    .await;
                                    settle(source.as_ref(), &message, failure_decision).await;
                                    continue;
                                }
                            };
                            let job_source = Arc::clone(&source);
                            let job_handler = Arc::clone(&handler);
                            let job_target = target.clone();
                            let job_error_target = error_target.clone();
                            let task = Box::pin(async move {
                                process_route_message(
                                    job_source,
                                    job_handler,
                                    job_target,
                                    job_error_target,
                                    failure_decision,
                                    message,
                                )
                                .await;
                            });
                            pending.push(partition.partitioner.submit_task(key, task).await);
                        }
                        Some(_) = pending.next(), if !pending.is_empty() => {}
                    }
                }
            }));
        }
        Ok(())
    }
}

async fn process_route_message(
    source: Arc<dyn RouteSource>,
    handler: Arc<dyn RouteHandler>,
    target: Option<Arc<dyn RouteTarget>>,
    error_target: Option<Arc<dyn RouteTarget>>,
    failure_decision: DeliveryDecision,
    message: RouteMessage,
) {
    let Some(target) = target.as_ref() else {
        let original = message.clone();
        match handler.consume(message, source.as_ref()).await {
            Ok(()) => settle(source.as_ref(), &original, DeliveryDecision::Ack).await,
            Err(error) => {
                deliver_failure(error_target.as_ref(), original.clone(), error, None).await;
                settle(source.as_ref(), &original, failure_decision).await;
            }
        }
        return;
    };
    if !target.accepts(&message) {
        let error = RouteError::delivery("target rejected the route message");
        deliver_failure(error_target.as_ref(), message.clone(), error, None).await;
        settle(source.as_ref(), &message, failure_decision).await;
        return;
    }
    let original = message.clone();
    match handler
        .call(message, source.as_ref(), target.as_ref())
        .await
    {
        Ok(output) => {
            if let Err(error) = target.deliver(output).await {
                let route_error = RouteError::delivery(&error);
                let response = match error {
                    BusError::Backend(BackendError::Http(response)) => Some(*response),
                    _ => None,
                };
                deliver_failure(
                    error_target.as_ref(),
                    original.clone(),
                    route_error,
                    response,
                )
                .await;
                settle(source.as_ref(), &original, failure_decision).await;
            } else {
                settle(source.as_ref(), &original, DeliveryDecision::Ack).await;
            }
        }
        Err(error) => {
            deliver_failure(error_target.as_ref(), original.clone(), error, None).await;
            settle(source.as_ref(), &original, failure_decision).await;
        }
    }
}

async fn settle(source: &dyn RouteSource, message: &RouteMessage, decision: DeliveryDecision) {
    if let Err(error) = source.settle(message, decision).await {
        tracing::error!(
            error = %error,
            message_id = %message.message_id,
            local_delivery_id = message.id,
            ?decision,
            "source settlement failed"
        );
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Bind<Args> {
    router: Router,
    handler: Arc<dyn RouteHandler>,
    error_target: Option<Arc<dyn RouteTarget>>,
    failure_decision: DeliveryDecision,
    partition: Option<PartitionBinding>,
    _args: PhantomData<fn(Args)>,
}

impl<Args> Bind<Args> {
    pub fn on_failure(mut self, decision: DeliveryDecision) -> Self {
        self.failure_decision = decision;
        self
    }

    pub fn errors_to<T>(mut self, target: T) -> Self
    where
        T: RouteTarget,
    {
        self.error_target = Some(Arc::new(target));
        self
    }

    /// Partition route execution by an asynchronously resolved key.
    ///
    /// This creates a partition domain private to this route. Jobs sharing a
    /// key execute sequentially; different keys may execute concurrently.
    /// Source settlement happens only after the complete partitioned route job
    /// finishes.
    pub fn partition_by<I, K, E, F, Fut>(self, resolver: F) -> Self
    where
        I: FromRouteMessage + Send + Sync + 'static,
        K: Into<String> + Send + Sync + 'static,
        E: std::fmt::Display + Send + Sync + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<K, E>> + Send + 'static,
    {
        self.partition_by_with(Partitioner::new(), resolver)
    }

    /// Partition route execution using a shared process-local domain.
    ///
    /// Clone and pass the same `Partitioner` to related routes when they must
    /// serialize work for the same key.
    pub fn partition_by_with<I, K, E, F, Fut>(
        mut self,
        partitioner: Partitioner,
        resolver: F,
    ) -> Self
    where
        I: FromRouteMessage + Send + Sync + 'static,
        K: Into<String> + Send + Sync + 'static,
        E: std::fmt::Display + Send + Sync + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<K, E>> + Send + 'static,
    {
        self.partition = Some(PartitionBinding {
            partitioner,
            resolver: Arc::new(TypedPartitionResolver::<I, K, E, F, Fut> {
                resolver,
                _types: PhantomData,
            }),
        });
        self
    }

    pub fn from<S>(self, source: S) -> RouteFrom<Args>
    where
        S: RouteSource,
    {
        RouteFrom {
            router: self.router,
            handler: self.handler,
            error_target: self.error_target,
            failure_decision: self.failure_decision,
            partition: self.partition,
            source: Box::new(source),
            _args: PhantomData,
        }
    }
}

pub struct RouteFrom<Args> {
    router: Router,
    handler: Arc<dyn RouteHandler>,
    error_target: Option<Arc<dyn RouteTarget>>,
    failure_decision: DeliveryDecision,
    partition: Option<PartitionBinding>,
    source: Box<dyn RouteSource>,
    _args: PhantomData<fn(Args)>,
}

impl<Args> RouteFrom<Args> {
    /// Register a terminal consumer with no success target.
    ///
    /// Transport-neutral: accepts any RouteSource, including broker, HTTP,
    /// and externally implemented sources. Extraction uses the source decoder.
    ///
    /// The handler is awaited for each message; successful output is discarded
    /// without serialization. Prefer handlers returning Result<(), E>.
    /// Decoding and handler failures are sent to errors_to when configured,
    /// otherwise logged. Successful handling acknowledges sources that support
    /// settlement; failures use the configured `DeliveryDecision` (Reject by
    /// default). This does not spawn a detached task for each message.
    pub fn consume(mut self) -> Router {
        self.router.bindings.push(RouteBinding {
            source: Some(self.source),
            handler: self.handler,
            target: None,
            error_target: self.error_target,
            failure_decision: self.failure_decision,
            partition: self.partition,
        });
        self.router
    }

    pub fn to<T>(mut self, target: T) -> Router
    where
        T: RouteTarget,
    {
        self.router.bindings.push(RouteBinding {
            source: Some(self.source),
            handler: self.handler,
            target: Some(Arc::new(target)),
            error_target: self.error_target,
            failure_decision: self.failure_decision,
            partition: self.partition,
        });
        self.router
    }
}

pub trait RouteHandler: Send + Sync {
    /// Execute without encoding or delivering successful output.
    /// Typed function handlers implement this automatically. Custom handlers
    /// must override it to support terminal routes; existing call-only handlers
    /// retain their previous behavior on routes with targets.
    fn consume<'a>(
        &'a self,
        _message: RouteMessage,
        _decoder: &'a dyn RouteSource,
    ) -> BoxFuture<'a, Result<(), RouteError>> {
        Box::pin(async {
            Err(RouteError::handler(
                "custom handler does not support consume",
            ))
        })
    }

    fn call<'a>(
        &'a self,
        message: RouteMessage,
        decoder: &'a dyn RouteSource,
        encoder: &'a dyn RouteTarget,
    ) -> BoxFuture<'a, Result<RouteMessage, RouteError>>;
}

#[doc(hidden)]
pub struct PreserveRouteHeaders;

#[doc(hidden)]
pub struct ReplaceRouteHeaders;

pub trait IntoRouteHandler<Args, OutputMode = PreserveRouteHeaders>: Send + Sync + 'static {
    fn into_handler(self) -> Arc<dyn RouteHandler>;
}

pub struct TypedRouteHandler<Args, O, F, OutputMode = PreserveRouteHeaders> {
    handler: F,
    _types: PhantomData<fn(Args) -> (O, OutputMode)>,
}

fn encode_handler_output<O: Serialize>(
    output: &O,
    mut message: RouteMessage,
    encoder: &dyn RouteTarget,
) -> Result<RouteMessage, RouteError> {
    message.headers.remove(ROUTE_FAILURE_HEADER);
    message.timestamp = std::time::Instant::now();
    message.attempts = 0;
    let value = serde_value::to_value(output)
        .map_err(|error| RouteError::handler(BusError::Serialization(error.to_string())))?;
    encoder
        .encode(&value, &mut message)
        .map_err(RouteError::delivery)?;
    Ok(message)
}

async fn deliver_failure(
    target: Option<&Arc<dyn RouteTarget>>,
    original: RouteMessage,
    error: RouteError,
    response: Option<crate::http::HttpResponse>,
) {
    let span = tracing::error_span!(
        "route_failure",
        message_id = original.id,
        stage = ?error.stage,
        code = %error.code,
        original_error = %error.message,
    );
    // Keep context across awaits without holding an entered span on the executor.
    use tracing::Instrument;
    async move {
        let Some(target) = target else {
            tracing::error!("route failed with no error target configured");
            return;
        };
        let mut message = original.clone();
        if let Some(response) = response {
            message.payload = response.body;
            message.headers = forwarded_http_error_headers(response.headers);
            if let Some(correlation_id) = original.headers.get(super::CORRELATION_ID_HEADER) {
                message
                    .headers
                    .insert(super::CORRELATION_ID_HEADER.into(), correlation_id.clone());
            }
            // Dynamic broker replies still need the original reply destination.
            if let Some(reply_to) = original.headers.get(super::REPLY_TO_HEADER) {
                message
                    .headers
                    .insert(super::REPLY_TO_HEADER.into(), reply_to.clone());
            }
            message
                .headers
                .insert(HTTP_STATUS_HEADER.into(), response.status.to_string());
        } else {
            let failure = RouteFailure {
                error,
                original: original.into(),
            };
            let value = match serde_value::to_value(&failure) {
                Ok(value) => value,
                Err(error) => {
                    tracing::error!(error = %error, "failed to construct error-target payload");
                    return;
                }
            };
            if let Err(error) = target.encode(&value, &mut message) {
                tracing::error!(error = %error, "failed to encode error-target payload");
                return;
            }
        }
        message.timestamp = std::time::Instant::now();
        message
            .headers
            .insert(ROUTE_FAILURE_HEADER.to_string(), "true".to_string());

        if !target.accepts(&message) {
            tracing::error!("error target rejected failure message");
            return;
        }
        match target.deliver(message).await {
            Ok(()) => tracing::debug!("failure delivered to error target"),
            Err(error) => tracing::error!(error = %error, "error-target delivery failed"),
        }
    }
    .instrument(span)
    .await;
}

fn forwarded_http_error_headers(headers: HashMap<String, String>) -> HashMap<String, String> {
    let connection_headers: Vec<String> = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("connection"))
        .flat_map(|(_, value)| value.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    headers
        .into_iter()
        .filter_map(|(name, value)| {
            let name = name.to_ascii_lowercase();
            let excluded = matches!(
                name.as_str(),
                "connection"
                    | "keep-alive"
                    | "proxy-authenticate"
                    | "proxy-authorization"
                    | "te"
                    | "trailer"
                    | "transfer-encoding"
                    | "upgrade"
                    | "content-length"
                    | "host"
                    | "authorization"
                    | "cookie"
                    | "set-cookie"
            ) || name.starts_with("qanat-")
                || name == super::CORRELATION_ID_HEADER
                || name == super::REPLY_TO_HEADER
                || connection_headers.contains(&name);
            (!excluded).then_some((name, value))
        })
        .collect()
}

macro_rules! impl_route_handler {
    ($($argument:ident),+ $(,)?) => {
        impl<$($argument,)+ O, HandlerError, F, Fut>
            IntoRouteHandler<($($argument,)+), PreserveRouteHeaders> for F
        where
            $($argument: FromRouteMessage + Send + Sync + 'static,)+
            O: Serialize + Send + Sync + 'static,
            HandlerError: std::fmt::Display + Send + Sync + 'static,
            F: Fn($($argument),+) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<O, HandlerError>> + Send + 'static,
        {
            fn into_handler(self) -> Arc<dyn RouteHandler> {
                Arc::new(TypedRouteHandler {
                    handler: self,
                    _types: PhantomData::<
                        fn(($($argument,)+)) -> ((O, HandlerError), PreserveRouteHeaders)
                    >,
                })
            }
        }

        impl<$($argument,)+ O, HandlerError, F, Fut>
            IntoRouteHandler<($($argument,)+), ReplaceRouteHeaders> for F
        where
            $($argument: FromRouteMessage + Send + Sync + 'static,)+
            O: Serialize + Send + Sync + 'static,
            HandlerError: std::fmt::Display + Send + Sync + 'static,
            F: Fn($($argument),+) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<(RouteHeaders, O), HandlerError>> + Send + 'static,
        {
            fn into_handler(self) -> Arc<dyn RouteHandler> {
                Arc::new(TypedRouteHandler {
                    handler: self,
                    _types: PhantomData::<
                        fn(($($argument,)+)) -> ((O, HandlerError), ReplaceRouteHeaders)
                    >,
                })
            }
        }

        impl<$($argument,)+ O, HandlerError, F, Fut> RouteHandler
            for TypedRouteHandler<
                ($($argument,)+),
                (O, HandlerError),
                F,
                PreserveRouteHeaders,
            >
        where
            $($argument: FromRouteMessage + Send + Sync + 'static,)+
            O: Serialize + Send + Sync + 'static,
            HandlerError: std::fmt::Display + Send + Sync + 'static,
            F: Fn($($argument),+) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<O, HandlerError>> + Send + 'static,
        {
            fn consume<'a>(
                &'a self,
                message: RouteMessage,
                decoder: &'a dyn RouteSource,
            ) -> BoxFuture<'a, Result<(), RouteError>> {
                Box::pin(async move {
                    (self.handler)(
                        $($argument::from_message(&message, decoder)?,)+
                    ).await.map_err(RouteError::handler)?;
                    Ok(())
                })
            }

            fn call<'a>(
                &'a self,
                message: RouteMessage,
                decoder: &'a dyn RouteSource,
                encoder: &'a dyn RouteTarget,
            ) -> BoxFuture<'a, Result<RouteMessage, RouteError>> {
                Box::pin(async move {
                    let output = (self.handler)(
                        $($argument::from_message(&message, decoder)?,)+
                    )
                    .await
                    .map_err(RouteError::handler)?;
                    encode_handler_output(&output, message, encoder)
                })
            }
        }

        impl<$($argument,)+ O, HandlerError, F, Fut> RouteHandler
            for TypedRouteHandler<
                ($($argument,)+),
                (O, HandlerError),
                F,
                ReplaceRouteHeaders,
            >
        where
            $($argument: FromRouteMessage + Send + Sync + 'static,)+
            O: Serialize + Send + Sync + 'static,
            HandlerError: std::fmt::Display + Send + Sync + 'static,
            F: Fn($($argument),+) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<(RouteHeaders, O), HandlerError>> + Send + 'static,
        {
            fn consume<'a>(
                &'a self,
                message: RouteMessage,
                decoder: &'a dyn RouteSource,
            ) -> BoxFuture<'a, Result<(), RouteError>> {
                Box::pin(async move {
                    (self.handler)(
                        $($argument::from_message(&message, decoder)?,)+
                    ).await.map_err(RouteError::handler)?;
                    Ok(())
                })
            }

            fn call<'a>(
                &'a self,
                mut message: RouteMessage,
                decoder: &'a dyn RouteSource,
                encoder: &'a dyn RouteTarget,
            ) -> BoxFuture<'a, Result<RouteMessage, RouteError>> {
                Box::pin(async move {
                    let (headers, output) = (self.handler)(
                        $($argument::from_message(&message, decoder)?,)+
                    )
                    .await
                    .map_err(RouteError::handler)?;
                    message.headers = headers.0;
                    encode_handler_output(&output, message, encoder)
                })
            }
        }
    };
}

impl_route_handler!(A);
impl_route_handler!(A, B);
impl_route_handler!(A, B, D);
impl_route_handler!(A, B, D, E);

pub trait FromRouteMessage: Sized {
    fn from_message(message: &RouteMessage, decoder: &dyn RouteSource) -> Result<Self, BusError>;
}

impl<T> FromRouteMessage for T
where
    T: DeserializeOwned,
{
    fn from_message(message: &RouteMessage, decoder: &dyn RouteSource) -> Result<Self, BusError> {
        let value = decoder.decode(message)?;
        T::deserialize(value).map_err(|error| BusError::Serialization(error.to_string()))
    }
}

impl FromRouteMessage for RouteMessage {
    fn from_message(message: &RouteMessage, _decoder: &dyn RouteSource) -> Result<Self, BusError> {
        Ok(message.clone())
    }
}

impl FromRouteMessage for RouteHeaders {
    fn from_message(message: &RouteMessage, _decoder: &dyn RouteSource) -> Result<Self, BusError> {
        Ok(Self(message.headers.clone()))
    }
}

impl<T> FromRouteMessage for RouteHeader<T>
where
    T: FromRouteHeader,
{
    fn from_message(message: &RouteMessage, _decoder: &dyn RouteSource) -> Result<Self, BusError> {
        let value = message
            .headers
            .get(T::NAME)
            .ok_or_else(|| BusError::Internal(format!("missing route header '{}'", T::NAME)))?;
        Ok(Self(T::from_header(value)?))
    }
}

impl FromRouteMessage for RoutePayload {
    fn from_message(message: &RouteMessage, _decoder: &dyn RouteSource) -> Result<Self, BusError> {
        Ok(Self(message.payload.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Codec, JsonCodec};
    use futures::stream;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    #[test]
    fn failed_message_exposes_text_only_for_declared_utf8_text_payloads() {
        type Case<'a> = (&'a str, Option<&'a str>, &'a [u8], Option<&'a str>);
        let cases: [Case<'_>; 5] = [
            (
                "JSON with charset",
                Some("Application/JSON; charset=utf-8"),
                br#"{"ok":true}"#,
                Some(r#"{"ok":true}"#),
            ),
            (
                "generic text",
                Some("text/plain; charset=UTF-8"),
                b"hello",
                Some("hello"),
            ),
            (
                "binary content type",
                Some("application/octet-stream"),
                b"hello",
                None,
            ),
            (
                "invalid UTF-8",
                Some("application/json"),
                &[0xff, 0xfe],
                None,
            ),
            ("missing content type", None, b"hello", None),
        ];

        for (name, content_type, bytes, expected_text) in cases {
            let mut message = RouteMessage::new("test", bytes);
            if let Some(content_type) = content_type {
                message
                    .headers
                    .insert("Content-Type".into(), content_type.into());
            }
            let failed = FailedRouteMessage::from(message);
            assert_eq!(failed.payload, bytes, "{name}: raw bytes changed");
            assert_eq!(failed.payload_text.as_deref(), expected_text, "{name}");
        }
    }

    #[test]
    fn common_structured_text_media_types_are_recognized() {
        for content_type in [
            "application/problem+json; charset=utf-8",
            "application/xml",
            "application/vnd.example+xml",
            "application/x-www-form-urlencoded",
            "application/javascript",
        ] {
            let mut message = RouteMessage::new("test", "text".as_bytes());
            message
                .headers
                .insert("content-type".into(), content_type.into());
            assert_eq!(
                FailedRouteMessage::from(message).payload_text.as_deref(),
                Some("text"),
                "{content_type}"
            );
        }
    }

    #[test]
    fn legacy_route_failure_without_payload_text_deserializes() {
        let legacy = serde_json::json!({
            "error": {
                "stage": "handler",
                "code": "route.handler",
                "message": "failed"
            },
            "original": {
                "address": "orders",
                "id": 7,
                "message_id": "message-7",
                "headers": {},
                "metadata": {},
                "attempts": 2,
                "payload": [123, 125]
            }
        });

        let failure: RouteFailure = serde_json::from_value(legacy).unwrap();
        assert_eq!(failure.original.payload, b"{}".to_vec());
        assert_eq!(failure.original.payload_text, None);
    }

    #[derive(Clone)]
    struct CapturedLogs(std::sync::mpsc::Sender<String>);

    impl tracing::Subscriber for CapturedLogs {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Visitor(String);
            impl tracing::field::Visit for Visitor {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write;
                    write!(&mut self.0, " {}={value:?}", field.name()).unwrap();
                }
            }
            let mut visitor = Visitor(event.metadata().level().to_string());
            event.record(&mut visitor);
            self.0.send(visitor.0).unwrap();
        }
    }

    #[tokio::test]
    async fn unhandled_and_secondary_failures_are_logged_without_recursion() {
        use tracing::instrument::WithSubscriber;
        let (sender, logs) = std::sync::mpsc::channel();
        let captured = CapturedLogs(sender);
        async move {
            // Other parallel tests may have populated tracing's global callsite
            // cache before this task-local subscriber was installed.
            tracing::callsite::rebuild_interest_cache();
            let original = || {
                let mut message = RouteMessage::new("test", b"secret-body".as_slice());
                message
                    .headers
                    .insert("authorization".into(), "secret-header".into());
                message
            };
            deliver_failure(
                None,
                original(),
                RouteError::handler("original failure"),
                None,
            )
            .await;

            let failed: Arc<dyn RouteTarget> = Arc::new(FailingTarget);
            deliver_failure(
                Some(&failed),
                original(),
                RouteError::handler("original failure"),
                None,
            )
            .await;

            let (outputs, mut receiver) = mpsc::channel(1);
            let rejected: Arc<dyn RouteTarget> = Arc::new(RejectingTarget { outputs });
            // A raw HTTP error bypasses encode, reaching the rejection check.
            deliver_failure(
                Some(&rejected),
                original(),
                RouteError::delivery("HTTP failure"),
                Some(crate::http::HttpResponse::new(500)),
            )
            .await;
            assert!(receiver.try_recv().is_err());

            // The default encoder rejects structured failures.
            deliver_failure(
                Some(&rejected),
                original(),
                RouteError::handler("original failure"),
                None,
            )
            .await;
        }
        .with_subscriber(captured)
        .await;

        let entries: Vec<_> = logs.try_iter().collect();
        assert_eq!(entries.len(), 4);
        for (entry, expected) in entries.iter().zip([
            "no error target configured",
            "error-target delivery failed",
            "error target rejected",
            "failed to encode error-target payload",
        ]) {
            assert!(entry.starts_with("ERROR"));
            assert!(entry.contains(expected), "{entry}");
            assert!(!entry.contains("secret-body"));
            assert!(!entry.contains("secret-header"));
        }
    }

    #[tokio::test]
    async fn consume_runs_without_success_target_or_output_serialization() {
        struct Unserializable;
        impl Serialize for Unserializable {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                panic!("terminal output must not be serialized");
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let mut router = Router::new()
            .bind(move |input: Input| {
                observed.fetch_add(input.value as usize, Ordering::SeqCst);
                async { Ok::<_, String>(Unserializable) }
            })
            .from(CustomSource {
                message: RouteMessage::new("input", br#"{"value":7}"#.as_slice()),
            })
            .consume();
        assert_eq!(router.route_count(), 1);
        assert!(router.bindings[0].target.is_none());
        router.install().await.unwrap();
        for task in router.tasks.drain(..) {
            task.await.unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 7);
    }

    #[tokio::test]
    async fn consume_routes_handler_and_decode_errors_with_original_message() {
        for payload in [br#"{"value":7}"#.as_slice(), b"invalid-json".as_slice()] {
            let (outputs, mut receiver) = mpsc::channel(2);
            let mut original = RouteMessage::new("input", payload);
            original
                .headers
                .insert("correlation-id".into(), "request-42".into());
            let mut router = Router::new()
                .bind(|_: Input| async { Err::<(), _>("enforcement failed") })
                .errors_to(CustomTarget { outputs })
                .from(CustomSource {
                    message: original.clone(),
                })
                .consume();
            router.install().await.unwrap();
            for task in router.tasks.drain(..) {
                task.await.unwrap();
            }
            let message = receiver.try_recv().unwrap();
            let failure: RouteFailure = JsonCodec.decode(&message.payload).unwrap();
            assert_eq!(failure.error.stage, RouteErrorStage::Handler);
            assert_eq!(failure.original.payload, original.payload.to_vec());
            assert_eq!(failure.original.headers, original.headers);
            assert_eq!(message.headers[ROUTE_FAILURE_HEADER], "true");
            assert!(receiver.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn consume_supports_unit_and_replaced_headers_without_success_messages() {
        let (outputs, mut receiver) = mpsc::channel(2);
        let mut router = Router::new()
            .bind(|_: Input| async { Ok::<(), String>(()) })
            .errors_to(CustomTarget {
                outputs: outputs.clone(),
            })
            .from(CustomSource {
                message: RouteMessage::new("input", br#"{"value":1}"#.as_slice()),
            })
            .consume()
            .bind(|_: Input| async {
                Ok::<(RouteHeaders, ()), String>((RouteHeaders::default(), ()))
            })
            .errors_to(CustomTarget { outputs })
            .from(CustomSource {
                message: RouteMessage::new("input", br#"{"value":2}"#.as_slice()),
            })
            .consume();
        router.install().await.unwrap();
        for task in router.tasks.drain(..) {
            task.await.unwrap();
        }
        assert!(receiver.try_recv().is_err());
    }

    struct CustomSource {
        message: RouteMessage,
    }

    impl RouteSource for CustomSource {
        fn decode(&self, message: &RouteMessage) -> Result<Value, BusError> {
            JsonCodec.decode(&message.payload)
        }

        fn open(&mut self) -> LocalBoxFuture<'_, Result<RouteStream, BusError>> {
            let message = self.message.clone();
            Box::pin(
                async move { Ok(Box::pin(stream::once(async move { message })) as RouteStream) },
            )
        }
    }

    struct MultiSource {
        messages: Vec<RouteMessage>,
    }

    impl RouteSource for MultiSource {
        fn decode(&self, message: &RouteMessage) -> Result<Value, BusError> {
            JsonCodec.decode(&message.payload)
        }

        fn open(&mut self) -> LocalBoxFuture<'_, Result<RouteStream, BusError>> {
            let messages = self.messages.clone();
            Box::pin(async move { Ok(Box::pin(stream::iter(messages)) as RouteStream) })
        }
    }

    struct SettlingSource {
        message: RouteMessage,
        decisions: mpsc::UnboundedSender<DeliveryDecision>,
    }

    impl RouteSource for SettlingSource {
        fn decode(&self, message: &RouteMessage) -> Result<Value, BusError> {
            JsonCodec.decode(&message.payload)
        }

        fn open(&mut self) -> LocalBoxFuture<'_, Result<RouteStream, BusError>> {
            let message = self.message.clone();
            Box::pin(
                async move { Ok(Box::pin(stream::once(async move { message })) as RouteStream) },
            )
        }

        fn settle(
            &self,
            _message: &RouteMessage,
            decision: DeliveryDecision,
        ) -> BoxFuture<'_, Result<(), BusError>> {
            let decisions = self.decisions.clone();
            Box::pin(async move {
                decisions
                    .send(decision)
                    .map_err(|_| BusError::Internal("settlement receiver closed".into()))
            })
        }
    }

    struct CustomTarget {
        outputs: mpsc::Sender<RouteMessage>,
    }

    impl RouteTarget for CustomTarget {
        fn encode(&self, value: &Value, message: &mut RouteMessage) -> Result<(), BusError> {
            message.payload = JsonCodec.encode(value)?;
            crate::codec::set_content_type(&mut message.headers, "application/json");
            Ok(())
        }

        fn deliver(&self, output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>> {
            Box::pin(async move {
                self.outputs
                    .send(output)
                    .await
                    .map_err(|_| BusError::Internal("custom target closed".into()))
            })
        }
    }

    struct RejectingTarget {
        outputs: mpsc::Sender<RouteMessage>,
    }

    impl RouteTarget for RejectingTarget {
        fn accepts(&self, _message: &RouteMessage) -> bool {
            false
        }

        fn deliver(&self, output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>> {
            Box::pin(async move {
                self.outputs
                    .send(output)
                    .await
                    .map_err(|_| BusError::Internal("rejecting target closed".into()))
            })
        }
    }

    struct FailingTarget;

    impl RouteTarget for FailingTarget {
        fn encode(&self, value: &Value, message: &mut RouteMessage) -> Result<(), BusError> {
            message.payload = JsonCodec.encode(value)?;
            crate::codec::set_content_type(&mut message.headers, "application/json");
            Ok(())
        }

        fn deliver(&self, _output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>> {
            Box::pin(async { Err(BusError::Connection("downstream unavailable".into())) })
        }
    }

    #[derive(Deserialize)]
    struct Input {
        value: u64,
    }

    #[derive(Deserialize, Serialize)]
    struct PartitionInput {
        key: String,
        sequence: u64,
    }

    #[derive(Deserialize, Serialize)]
    struct Output {
        value: u64,
    }

    struct RequestId(String);

    impl FromRouteHeader for RequestId {
        const NAME: &'static str = "x-request-id";

        fn from_header(value: &str) -> Result<Self, BusError> {
            Ok(Self(value.to_string()))
        }
    }

    async fn double(
        RouteHeader(request_id): RouteHeader<RequestId>,
        RoutePayload(raw_payload): RoutePayload,
        input: Input,
    ) -> Result<Output, std::convert::Infallible> {
        assert_eq!(request_id.0, "request-21");
        assert!(!raw_payload.is_empty());
        Ok(Output {
            value: input.value * 2,
        })
    }

    async fn double_with_modified_headers(
        mut headers: RouteHeaders,
        input: Input,
    ) -> Result<(RouteHeaders, Output), std::convert::Infallible> {
        headers.remove("x-remove");
        headers.insert("x-processed-by".into(), "custom-handler".into());
        Ok((
            headers,
            Output {
                value: input.value * 2,
            },
        ))
    }

    async fn reject_input(_input: Input) -> Result<Output, &'static str> {
        Err("input was rejected")
    }

    #[tokio::test]
    async fn successful_route_acknowledges_source_message() {
        let (decisions, mut decision_rx) = mpsc::unbounded_channel();
        let mut router = Router::new()
            .bind(|_: Input| async { Ok::<(), String>(()) })
            .from(SettlingSource {
                message: RouteMessage::new("input", br#"{"value":1}"#.as_slice()),
                decisions,
            })
            .consume();

        router.install().await.unwrap();
        for task in router.tasks.drain(..) {
            task.await.unwrap();
        }

        assert_eq!(decision_rx.recv().await, Some(DeliveryDecision::Ack));
    }

    #[tokio::test]
    async fn failed_route_rejects_by_default() {
        let (decisions, mut decision_rx) = mpsc::unbounded_channel();
        let mut router = Router::new()
            .bind(|_: Input| async { Err::<(), _>("failed") })
            .from(SettlingSource {
                message: RouteMessage::new("input", br#"{"value":1}"#.as_slice()),
                decisions,
            })
            .consume();

        router.install().await.unwrap();
        for task in router.tasks.drain(..) {
            task.await.unwrap();
        }

        assert_eq!(decision_rx.recv().await, Some(DeliveryDecision::Reject));
    }

    #[tokio::test]
    async fn failed_route_can_opt_into_retry() {
        let (decisions, mut decision_rx) = mpsc::unbounded_channel();
        let mut router = Router::new()
            .bind(|_: Input| async { Err::<(), _>("failed") })
            .on_failure(DeliveryDecision::Retry)
            .from(SettlingSource {
                message: RouteMessage::new("input", br#"{"value":1}"#.as_slice()),
                decisions,
            })
            .consume();

        router.install().await.unwrap();
        for task in router.tasks.drain(..) {
            task.await.unwrap();
        }

        assert_eq!(decision_rx.recv().await, Some(DeliveryDecision::Retry));
    }

    #[tokio::test]
    async fn async_partitions_serialize_each_key_and_run_different_keys_concurrently() {
        let codec = JsonCodec;
        let messages = [("switch-a", 1), ("switch-a", 2), ("switch-b", 1)]
            .into_iter()
            .map(|(key, sequence)| {
                RouteMessage::new(
                    "input",
                    codec
                        .encode(&PartitionInput {
                            key: key.into(),
                            sequence,
                        })
                        .unwrap(),
                )
            })
            .collect();
        let active_a = Arc::new(AtomicUsize::new(0));
        let active_b = Arc::new(AtomicUsize::new(0));
        let (observed_order, mut order_rx) = mpsc::unbounded_channel();
        let active = Arc::new(AtomicUsize::new(0));
        let maximum_active = Arc::new(AtomicUsize::new(0));
        let handler_active_a = Arc::clone(&active_a);
        let handler_active_b = Arc::clone(&active_b);
        let handler_active = Arc::clone(&active);
        let handler_maximum = Arc::clone(&maximum_active);

        let mut router = Router::new()
            .bind(move |input: PartitionInput| {
                let active_a = Arc::clone(&handler_active_a);
                let active_b = Arc::clone(&handler_active_b);
                let order = observed_order.clone();
                let active = Arc::clone(&handler_active);
                let maximum = Arc::clone(&handler_maximum);
                async move {
                    let keyed = if input.key == "switch-a" {
                        &active_a
                    } else {
                        &active_b
                    };
                    assert_eq!(keyed.fetch_add(1, Ordering::SeqCst), 0);
                    order.send((input.key, input.sequence)).unwrap();
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    assert_eq!(keyed.fetch_sub(1, Ordering::SeqCst), 1);
                    Ok::<(), std::convert::Infallible>(())
                }
            })
            .partition_by(|input: PartitionInput| async move {
                tokio::task::yield_now().await;
                Ok::<_, std::convert::Infallible>(input.key)
            })
            .from(MultiSource { messages })
            .consume();

        router.install().await.unwrap();
        for task in router.tasks.drain(..) {
            task.await.unwrap();
        }

        let mut order = HashMap::<String, Vec<u64>>::new();
        for _ in 0..3 {
            let (key, sequence) = order_rx.recv().await.unwrap();
            order.entry(key).or_default().push(sequence);
        }
        assert_eq!(order["switch-a"], [1, 2]);
        assert_eq!(order["switch-b"], [1]);
        assert!(maximum_active.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn shared_partitioner_serializes_the_same_key_across_routes() {
        let codec = JsonCodec;
        let message = |sequence| {
            RouteMessage::new(
                "input",
                codec
                    .encode(&PartitionInput {
                        key: "switch-a".into(),
                        sequence,
                    })
                    .unwrap(),
            )
        };
        let partitioner = Partitioner::with_capacity(2);
        let active = Arc::new(AtomicUsize::new(0));
        let maximum_active = Arc::new(AtomicUsize::new(0));

        let first_active = Arc::clone(&active);
        let first_maximum = Arc::clone(&maximum_active);
        let second_active = Arc::clone(&active);
        let second_maximum = Arc::clone(&maximum_active);
        let mut router = Router::new()
            .bind(move |_: PartitionInput| {
                let active = Arc::clone(&first_active);
                let maximum = Arc::clone(&first_maximum);
                async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<(), std::convert::Infallible>(())
                }
            })
            .partition_by_with(partitioner.clone(), |input: PartitionInput| async move {
                Ok::<_, std::convert::Infallible>(input.key)
            })
            .from(CustomSource {
                message: message(1),
            })
            .consume()
            .bind(move |_: PartitionInput| {
                let active = Arc::clone(&second_active);
                let maximum = Arc::clone(&second_maximum);
                async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<(), std::convert::Infallible>(())
                }
            })
            .partition_by_with(partitioner, |input: PartitionInput| async move {
                Ok::<_, std::convert::Infallible>(input.key)
            })
            .from(CustomSource {
                message: message(2),
            })
            .consume();

        router.install().await.unwrap();
        for task in router.tasks.drain(..) {
            task.await.unwrap();
        }

        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(maximum_active.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn full_partition_does_not_block_an_unrelated_key() {
        let partitioner = Partitioner::with_capacity(1);
        let first_started = Arc::new(tokio::sync::Notify::new());
        let release_first = Arc::new(tokio::sync::Notify::new());
        let started = Arc::clone(&first_started);
        let release = Arc::clone(&release_first);
        let first = partitioner
            .submit_task(
                "busy".into(),
                Box::pin(async move {
                    started.notify_one();
                    release.notified().await;
                }),
            )
            .await;
        first_started.notified().await;

        let second = partitioner
            .submit_task("busy".into(), Box::pin(async {}))
            .await;
        let blocked_partitioner = partitioner.clone();
        let blocked = tokio::spawn(async move {
            blocked_partitioner
                .submit_task("busy".into(), Box::pin(async {}))
                .await
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;

        let unrelated = partitioner
            .submit_task("free".into(), Box::pin(async {}))
            .await;
        tokio::time::timeout(Duration::from_secs(1), unrelated)
            .await
            .unwrap()
            .unwrap();

        release_first.notify_one();
        first.await.unwrap();
        second.await.unwrap();
        blocked.await.unwrap();
    }

    #[tokio::test]
    async fn retiring_partition_drains_stale_senders_before_replacement() {
        let idle_timeout = Duration::from_millis(20);
        let partitioner = Partitioner::with_options(2, idle_timeout);
        let (reply, sender) = tokio::sync::oneshot::channel();
        partitioner
            .registry()
            .await
            .send(PartitionRegistryCommand::Acquire {
                key: "shared".into(),
                reply,
            })
            .unwrap();
        let stale_sender = sender.await.unwrap();

        tokio::time::sleep(idle_timeout * 5).await;
        let (order, mut observed) = mpsc::unbounded_channel();
        let next_partitioner = partitioner.clone();
        let next_order = order.clone();
        let next = tokio::spawn(async move {
            next_partitioner
                .submit_task(
                    "shared".into(),
                    Box::pin(async move {
                        next_order.send(2).unwrap();
                    }),
                )
                .await
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;

        assert!(
            stale_sender
                .send(PartitionJob {
                    task: Box::pin(async move {
                        order.send(1).unwrap();
                    }),
                    completion: tokio::sync::oneshot::channel().0,
                })
                .await
                .is_ok()
        );
        drop(stale_sender);

        next.await.unwrap();
        assert_eq!(observed.recv().await, Some(1));
        assert_eq!(observed.recv().await, Some(2));
    }

    #[tokio::test]
    async fn panicking_partition_job_still_completes() {
        let partitioner = Partitioner::new();
        let completion = partitioner
            .submit_task(
                "panic".into(),
                Box::pin(async {
                    panic!("expected test panic");
                }),
            )
            .await;

        tokio::time::timeout(Duration::from_secs(1), completion)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn partition_resolution_failure_uses_error_route_and_failure_settlement() {
        let codec = JsonCodec;
        let message = RouteMessage::new(
            "input",
            codec
                .encode(&PartitionInput {
                    key: "switch-a".into(),
                    sequence: 1,
                })
                .unwrap(),
        );
        let (decisions, mut decision_rx) = mpsc::unbounded_channel();
        let (outputs, mut error_rx) = mpsc::channel(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let mut router = Router::new()
            .bind(move |_: PartitionInput| {
                handler_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok::<(), std::convert::Infallible>(()) }
            })
            .partition_by(|_: PartitionInput| async { Err::<String, _>("partition lookup failed") })
            .errors_to(CustomTarget { outputs })
            .from(SettlingSource { message, decisions })
            .consume();

        router.install().await.unwrap();
        for task in router.tasks.drain(..) {
            task.await.unwrap();
        }

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(decision_rx.recv().await, Some(DeliveryDecision::Reject));
        let failure: RouteFailure = codec
            .decode(&error_rx.recv().await.unwrap().payload)
            .unwrap();
        assert_eq!(failure.error.stage, RouteErrorStage::Handler);
        assert!(failure.error.message.contains("partition lookup failed"));
    }

    #[tokio::test]
    async fn partitioned_source_settlement_waits_for_handler_completion() {
        let codec = JsonCodec;
        let message = RouteMessage::new(
            "input",
            codec
                .encode(&PartitionInput {
                    key: "switch-a".into(),
                    sequence: 1,
                })
                .unwrap(),
        );
        let (decisions, mut decision_rx) = mpsc::unbounded_channel();
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let handler_started = Arc::clone(&started);
        let handler_release = Arc::clone(&release);
        let mut router = Router::new()
            .bind(move |_: PartitionInput| {
                let started = Arc::clone(&handler_started);
                let release = Arc::clone(&handler_release);
                async move {
                    started.notify_one();
                    release.notified().await;
                    Ok::<(), std::convert::Infallible>(())
                }
            })
            .partition_by(|input: PartitionInput| async move {
                Ok::<_, std::convert::Infallible>(input.key)
            })
            .from(SettlingSource { message, decisions })
            .consume();

        router.install().await.unwrap();
        started.notified().await;
        assert!(decision_rx.try_recv().is_err());
        release.notify_one();
        for task in router.tasks.drain(..) {
            task.await.unwrap();
        }
        assert_eq!(decision_rx.recv().await, Some(DeliveryDecision::Ack));
    }

    #[tokio::test]
    async fn user_defined_source_and_target_work_without_transport_types() {
        let codec = JsonCodec;
        let mut message = RouteMessage::new(
            "custom://input",
            codec.encode(&serde_json::json!({ "value": 21 })).unwrap(),
        );
        message
            .headers
            .insert("x-request-id".to_string(), "request-21".to_string());
        let source = CustomSource { message };
        let (outputs, mut output_rx) = mpsc::channel(1);
        let mut router = Router::new()
            .bind(double)
            .from(source)
            .to(CustomTarget { outputs });

        router.install().await.unwrap();

        let output = tokio::time::timeout(std::time::Duration::from_secs(1), output_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let decoded: Output = codec.decode(&output.payload).unwrap();
        assert_eq!(decoded.value, 42);
        assert_eq!(output.address, "custom://input");
        assert_eq!(
            output.headers.get("x-request-id").map(String::as_str),
            Some("request-21")
        );
        assert_eq!(
            output.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
    }

    #[tokio::test]
    async fn handler_can_optionally_replace_modified_headers() {
        let codec = JsonCodec;
        let mut message = RouteMessage::new(
            "custom://input",
            codec.encode(&serde_json::json!({ "value": 21 })).unwrap(),
        );
        message
            .headers
            .insert("x-request-id".to_string(), "request-21".to_string());
        message
            .headers
            .insert("x-remove".to_string(), "private".to_string());
        let (outputs, mut output_rx) = mpsc::channel(1);
        let mut router = Router::new()
            .bind(double_with_modified_headers)
            .from(CustomSource { message })
            .to(CustomTarget { outputs });

        router.install().await.unwrap();

        let output = output_rx.recv().await.unwrap();
        assert_eq!(
            output.headers.get("x-request-id").map(String::as_str),
            Some("request-21")
        );
        assert_eq!(
            output.headers.get("x-processed-by").map(String::as_str),
            Some("custom-handler")
        );
        assert!(!output.headers.contains_key("x-remove"));
        assert_eq!(
            output.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
    }

    #[tokio::test]
    async fn target_can_reject_a_message_before_handler_execution() {
        let codec = JsonCodec;
        let message = RouteMessage::new(
            "custom://input",
            codec.encode(&serde_json::json!({ "value": 21 })).unwrap(),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let (outputs, mut output_rx) = mpsc::channel(1);
        let mut router = Router::new()
            .bind(move |input: Input| {
                let calls = Arc::clone(&handler_calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, std::convert::Infallible>(Output { value: input.value })
                }
            })
            .from(CustomSource { message })
            .to(RejectingTarget { outputs });

        router.install().await.unwrap();

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), output_rx.recv())
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn installing_a_router_twice_returns_an_error() {
        let codec = JsonCodec;
        let message = RouteMessage::new(
            "custom://input",
            codec.encode(&serde_json::json!({ "value": 21 })).unwrap(),
        );
        let (outputs, _output_rx) = mpsc::channel(1);
        let mut router = Router::new()
            .bind(double_with_modified_headers)
            .from(CustomSource { message })
            .to(CustomTarget { outputs });

        router.install().await.unwrap();
        let error = router.install().await.unwrap_err();

        assert!(error.to_string().contains("route is already installed"));
        assert_eq!(router.task_count(), 1);
    }

    #[tokio::test]
    async fn fallible_handler_sends_structured_failure_to_error_target() {
        let codec = JsonCodec;
        let original_payload = codec.encode(&serde_json::json!({ "value": 21 })).unwrap();
        let mut message = RouteMessage::new("custom://input", original_payload.clone());
        message.id = 42;
        message
            .headers
            .insert("x-request-id".into(), "request-42".into());
        let (success_outputs, _success_rx) = mpsc::channel(1);
        let (error_outputs, mut error_rx) = mpsc::channel(1);
        let mut router = Router::new()
            .bind(reject_input)
            .errors_to(CustomTarget {
                outputs: error_outputs,
            })
            .from(CustomSource { message })
            .to(CustomTarget {
                outputs: success_outputs,
            });

        router.install().await.unwrap();

        let routed_error = error_rx.recv().await.unwrap();
        let failure: RouteFailure = codec.decode(&routed_error.payload).unwrap();
        assert_eq!(failure.error.stage, RouteErrorStage::Handler);
        assert_eq!(failure.error.code, "route.handler");
        assert!(failure.error.message.contains("input was rejected"));
        assert_eq!(failure.original.address, "custom://input");
        assert_eq!(failure.original.id, 42);
        assert_eq!(failure.original.payload, original_payload);
        assert_eq!(
            routed_error.headers.get("x-request-id").map(String::as_str),
            Some("request-42")
        );
    }

    #[tokio::test]
    async fn delivery_failure_is_sent_to_error_target() {
        let codec = JsonCodec;
        let message = RouteMessage::new(
            "custom://input",
            codec.encode(&serde_json::json!({ "value": 21 })).unwrap(),
        );
        let (error_outputs, mut error_rx) = mpsc::channel(1);
        let mut router = Router::new()
            .bind(|input: Input| async move {
                Ok::<_, std::convert::Infallible>(Output { value: input.value })
            })
            .errors_to(CustomTarget {
                outputs: error_outputs,
            })
            .from(CustomSource { message })
            .to(FailingTarget);

        router.install().await.unwrap();

        let routed_error = error_rx.recv().await.unwrap();
        let failure: RouteFailure = codec.decode(&routed_error.payload).unwrap();
        assert_eq!(failure.error.stage, RouteErrorStage::Delivery);
        assert_eq!(failure.error.code, "route.delivery");
        assert!(failure.error.message.contains("downstream unavailable"));
    }

    #[tokio::test]
    async fn decoding_failure_is_sent_to_error_target() {
        let message = RouteMessage::new("custom://input", Bytes::from_static(b"not-json"));
        let (success_outputs, _success_rx) = mpsc::channel(1);
        let (error_outputs, mut error_rx) = mpsc::channel(1);
        let mut router = Router::new()
            .bind(|input: Input| async move {
                Ok::<_, std::convert::Infallible>(Output { value: input.value })
            })
            .errors_to(CustomTarget {
                outputs: error_outputs,
            })
            .from(CustomSource { message })
            .to(CustomTarget {
                outputs: success_outputs,
            });

        router.install().await.unwrap();

        let routed_error = error_rx.recv().await.unwrap();
        let failure: RouteFailure = JsonCodec.decode(&routed_error.payload).unwrap();
        assert_eq!(failure.error.stage, RouteErrorStage::Handler);
        assert!(failure.error.message.contains("Serialization error"));
        assert_eq!(failure.original.payload, b"not-json");
    }
}
