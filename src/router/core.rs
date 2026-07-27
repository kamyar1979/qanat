use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::future::{BoxFuture, LocalBoxFuture};
use futures::stream::BoxStream;
use serde::{Serialize, de::DeserializeOwned};

use crate::codec::{Codec, JsonCodec};
use crate::errors::BusError;
#[derive(Clone, Debug)]
pub struct RouteMessage {
    pub address: String,
    pub timestamp: std::time::Instant,
    pub id: u64,
    pub headers: HashMap<String, String>,
    pub attempts: u32,
    pub payload: Bytes,
}

impl RouteMessage {
    pub fn new(address: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            address: address.into(),
            timestamp: std::time::Instant::now(),
            id: 0,
            headers: HashMap::new(),
            attempts: 0,
            payload: payload.into(),
        }
    }
}

pub type RouteStream = BoxStream<'static, RouteMessage>;

pub trait RouteSource: Send + Sync + 'static {
    fn subscribe(&self) -> LocalBoxFuture<'_, Result<RouteStream, BusError>>;
}

pub trait RouteTarget: Send + Sync + 'static {
    fn accepts(&self, _message: &RouteMessage) -> bool {
        true
    }

    fn deliver(&self, output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>>;
}

struct RouteBinding<C: Codec> {
    source: Arc<dyn RouteSource>,
    handler: Arc<dyn RouteHandler<C>>,
    target: Arc<dyn RouteTarget>,
}

pub struct Router<C: Codec = JsonCodec> {
    codec: Arc<C>,
    bindings: Vec<RouteBinding<C>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Router<JsonCodec> {
    pub fn new() -> Self {
        Self::with_codec(JsonCodec)
    }
}

impl<C: Codec> Router<C> {
    pub fn with_codec(codec: C) -> Self {
        Self {
            codec: Arc::new(codec),
            bindings: Vec::new(),
            tasks: Vec::new(),
        }
    }

    pub fn bind<H, Args>(self, handler: H) -> Bind<C, Args>
    where
        H: IntoRouteHandler<C, Args>,
        Args: Send + Sync + 'static,
    {
        Bind {
            router: self,
            handler: handler.into_handler(),
            _args: PhantomData,
        }
    }

    pub fn codec(&self) -> &C {
        self.codec.as_ref()
    }

    pub fn route_count(&self) -> usize {
        self.bindings.len()
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub async fn install(&mut self) -> Result<(), BusError> {
        for binding in &self.bindings {
            let mut subscription = binding.source.subscribe().await?;
            let handler = Arc::clone(&binding.handler);
            let target = Arc::clone(&binding.target);
            let codec = Arc::clone(&self.codec);

            self.tasks.push(tokio::spawn(async move {
                while let Some(message) = subscription.next().await {
                    if !target.accepts(&message) {
                        continue;
                    }
                    if let Ok(output) = handler.call(message, codec.as_ref()).await {
                        let _ = target.deliver(output).await;
                    }
                }
            }));
        }
        Ok(())
    }
}

impl Default for Router<JsonCodec> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Bind<C: Codec, Args> {
    router: Router<C>,
    handler: Arc<dyn RouteHandler<C>>,
    _args: PhantomData<fn(Args)>,
}

impl<C: Codec, Args> Bind<C, Args> {
    pub fn from<S>(self, source: S) -> RouteFrom<C, Args>
    where
        S: RouteSource,
    {
        RouteFrom {
            router: self.router,
            handler: self.handler,
            source: Arc::new(source),
            _args: PhantomData,
        }
    }
}

pub struct RouteFrom<C: Codec, Args> {
    router: Router<C>,
    handler: Arc<dyn RouteHandler<C>>,
    source: Arc<dyn RouteSource>,
    _args: PhantomData<fn(Args)>,
}

impl<C: Codec, Args> RouteFrom<C, Args> {
    pub fn to<T>(mut self, target: T) -> Router<C>
    where
        T: RouteTarget,
    {
        self.router.bindings.push(RouteBinding {
            source: self.source,
            handler: self.handler,
            target: Arc::new(target),
        });
        self.router
    }
}

pub trait RouteHandler<C: Codec>: Send + Sync {
    fn call<'a>(
        &'a self,
        message: RouteMessage,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<RouteMessage, BusError>>;
}

pub trait IntoRouteHandler<C: Codec, Args>: Send + Sync + 'static {
    fn into_handler(self) -> Arc<dyn RouteHandler<C>>;
}

pub struct TypedRouteHandler<Args, O, F> {
    handler: F,
    _types: PhantomData<fn(Args) -> O>,
}

fn encode_handler_output<O: Serialize>(
    output: &O,
    mut message: RouteMessage,
    codec: &impl Codec,
) -> Result<RouteMessage, BusError> {
    message
        .headers
        .entry("content-type".to_string())
        .or_insert_with(|| codec.content_type().to_string());
    message.timestamp = std::time::Instant::now();
    message.attempts = 0;
    message.payload = codec.encode(output)?;
    Ok(message)
}

impl<C, A, O, F, Fut> IntoRouteHandler<C, (A,)> for F
where
    C: Codec,
    A: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    fn into_handler(self) -> Arc<dyn RouteHandler<C>> {
        Arc::new(TypedRouteHandler {
            handler: self,
            _types: PhantomData::<fn((A,)) -> O>,
        })
    }
}

impl<C, A, B, O, F, Fut> IntoRouteHandler<C, (A, B)> for F
where
    C: Codec,
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    fn into_handler(self) -> Arc<dyn RouteHandler<C>> {
        Arc::new(TypedRouteHandler {
            handler: self,
            _types: PhantomData::<fn((A, B)) -> O>,
        })
    }
}

impl<C, A, B, D, O, F, Fut> IntoRouteHandler<C, (A, B, D)> for F
where
    C: Codec,
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    D: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B, D) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    fn into_handler(self) -> Arc<dyn RouteHandler<C>> {
        Arc::new(TypedRouteHandler {
            handler: self,
            _types: PhantomData::<fn((A, B, D)) -> O>,
        })
    }
}

impl<A, O, F, Fut, C> RouteHandler<C> for TypedRouteHandler<(A,), O, F>
where
    A: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
    C: Codec,
{
    fn call<'a>(
        &'a self,
        message: RouteMessage,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<RouteMessage, BusError>> {
        Box::pin(async move {
            let output = (self.handler)(A::from_message(&message, codec)?).await;
            encode_handler_output(&output, message, codec)
        })
    }
}

impl<A, B, O, F, Fut, C> RouteHandler<C> for TypedRouteHandler<(A, B), O, F>
where
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
    C: Codec,
{
    fn call<'a>(
        &'a self,
        message: RouteMessage,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<RouteMessage, BusError>> {
        Box::pin(async move {
            let output = (self.handler)(
                A::from_message(&message, codec)?,
                B::from_message(&message, codec)?,
            )
            .await;
            encode_handler_output(&output, message, codec)
        })
    }
}

impl<A, B, D, O, F, Fut, C> RouteHandler<C> for TypedRouteHandler<(A, B, D), O, F>
where
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    D: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B, D) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
    C: Codec,
{
    fn call<'a>(
        &'a self,
        message: RouteMessage,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<RouteMessage, BusError>> {
        Box::pin(async move {
            let output = (self.handler)(
                A::from_message(&message, codec)?,
                B::from_message(&message, codec)?,
                D::from_message(&message, codec)?,
            )
            .await;
            encode_handler_output(&output, message, codec)
        })
    }
}

pub trait FromRouteMessage<C: Codec>: Sized {
    fn from_message(message: &RouteMessage, codec: &C) -> Result<Self, BusError>;
}

impl<T, C> FromRouteMessage<C> for T
where
    T: DeserializeOwned,
    C: Codec,
{
    fn from_message(message: &RouteMessage, codec: &C) -> Result<Self, BusError> {
        codec.decode(&message.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc;

    struct CustomSource {
        message: RouteMessage,
    }

    impl RouteSource for CustomSource {
        fn subscribe(&self) -> LocalBoxFuture<'_, Result<RouteStream, BusError>> {
            let message = self.message.clone();
            Box::pin(
                async move { Ok(Box::pin(stream::once(async move { message })) as RouteStream) },
            )
        }
    }

    struct CustomTarget {
        outputs: mpsc::Sender<RouteMessage>,
    }

    impl RouteTarget for CustomTarget {
        fn deliver(&self, output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>> {
            Box::pin(async move {
                self.outputs
                    .send(output)
                    .await
                    .map_err(|_| BusError::Internal("custom target closed".into()))
            })
        }
    }

    #[derive(Deserialize)]
    struct Input {
        value: u64,
    }

    #[derive(Deserialize, Serialize)]
    struct Output {
        value: u64,
    }

    async fn double(input: Input) -> Output {
        Output {
            value: input.value * 2,
        }
    }

    #[tokio::test]
    async fn user_defined_source_and_target_work_without_transport_types() {
        let codec = JsonCodec;
        let source = CustomSource {
            message: RouteMessage::new(
                "custom://input",
                codec.encode(&serde_json::json!({ "value": 21 })).unwrap(),
            ),
        };
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
            output.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
    }
}
