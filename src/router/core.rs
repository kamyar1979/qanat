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
    pub metadata: HashMap<String, String>,
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
            metadata: HashMap::new(),
            attempts: 0,
            payload: payload.into(),
        }
    }
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

pub trait RouteSource: Send + 'static {
    fn into_stream(self: Box<Self>) -> LocalBoxFuture<'static, Result<RouteStream, BusError>>;
}

pub trait RouteTarget: Send + Sync + 'static {
    fn accepts(&self, _message: &RouteMessage) -> bool {
        true
    }

    fn deliver(&self, output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>>;
}

struct RouteBinding<C: Codec> {
    source: Option<Box<dyn RouteSource>>,
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

    pub fn bind<H, Args, OutputMode>(self, handler: H) -> Bind<C, Args>
    where
        H: IntoRouteHandler<C, Args, OutputMode>,
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
        for binding in &mut self.bindings {
            let source = binding
                .source
                .take()
                .ok_or_else(|| BusError::Internal("route is already installed".into()))?;
            let mut subscription = source.into_stream().await?;
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
            source: Box::new(source),
            _args: PhantomData,
        }
    }
}

pub struct RouteFrom<C: Codec, Args> {
    router: Router<C>,
    handler: Arc<dyn RouteHandler<C>>,
    source: Box<dyn RouteSource>,
    _args: PhantomData<fn(Args)>,
}

impl<C: Codec, Args> RouteFrom<C, Args> {
    pub fn to<T>(mut self, target: T) -> Router<C>
    where
        T: RouteTarget,
    {
        self.router.bindings.push(RouteBinding {
            source: Some(self.source),
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

#[doc(hidden)]
pub struct PreserveRouteHeaders;

#[doc(hidden)]
pub struct ReplaceRouteHeaders;

pub trait IntoRouteHandler<C: Codec, Args, OutputMode = PreserveRouteHeaders>:
    Send + Sync + 'static
{
    fn into_handler(self) -> Arc<dyn RouteHandler<C>>;
}

pub struct TypedRouteHandler<Args, O, F, OutputMode = PreserveRouteHeaders> {
    handler: F,
    _types: PhantomData<fn(Args) -> (O, OutputMode)>,
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

impl<C, A, O, F, Fut> IntoRouteHandler<C, (A,), PreserveRouteHeaders> for F
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
            _types: PhantomData::<fn((A,)) -> (O, PreserveRouteHeaders)>,
        })
    }
}

impl<C, A, B, O, F, Fut> IntoRouteHandler<C, (A, B), PreserveRouteHeaders> for F
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
            _types: PhantomData::<fn((A, B)) -> (O, PreserveRouteHeaders)>,
        })
    }
}

impl<C, A, B, D, O, F, Fut> IntoRouteHandler<C, (A, B, D), PreserveRouteHeaders> for F
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
            _types: PhantomData::<fn((A, B, D)) -> (O, PreserveRouteHeaders)>,
        })
    }
}

impl<C, A, B, D, E, O, F, Fut> IntoRouteHandler<C, (A, B, D, E), PreserveRouteHeaders> for F
where
    C: Codec,
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    D: FromRouteMessage<C> + Send + Sync + 'static,
    E: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B, D, E) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    fn into_handler(self) -> Arc<dyn RouteHandler<C>> {
        Arc::new(TypedRouteHandler {
            handler: self,
            _types: PhantomData::<fn((A, B, D, E)) -> (O, PreserveRouteHeaders)>,
        })
    }
}

impl<C, A, O, F, Fut> IntoRouteHandler<C, (A,), ReplaceRouteHeaders> for F
where
    C: Codec,
    A: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = (RouteHeaders, O)> + Send + 'static,
{
    fn into_handler(self) -> Arc<dyn RouteHandler<C>> {
        Arc::new(TypedRouteHandler {
            handler: self,
            _types: PhantomData::<fn((A,)) -> (O, ReplaceRouteHeaders)>,
        })
    }
}

impl<C, A, B, O, F, Fut> IntoRouteHandler<C, (A, B), ReplaceRouteHeaders> for F
where
    C: Codec,
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = (RouteHeaders, O)> + Send + 'static,
{
    fn into_handler(self) -> Arc<dyn RouteHandler<C>> {
        Arc::new(TypedRouteHandler {
            handler: self,
            _types: PhantomData::<fn((A, B)) -> (O, ReplaceRouteHeaders)>,
        })
    }
}

impl<C, A, B, D, O, F, Fut> IntoRouteHandler<C, (A, B, D), ReplaceRouteHeaders> for F
where
    C: Codec,
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    D: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B, D) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = (RouteHeaders, O)> + Send + 'static,
{
    fn into_handler(self) -> Arc<dyn RouteHandler<C>> {
        Arc::new(TypedRouteHandler {
            handler: self,
            _types: PhantomData::<fn((A, B, D)) -> (O, ReplaceRouteHeaders)>,
        })
    }
}

impl<C, A, B, D, E, O, F, Fut> IntoRouteHandler<C, (A, B, D, E), ReplaceRouteHeaders> for F
where
    C: Codec,
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    D: FromRouteMessage<C> + Send + Sync + 'static,
    E: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B, D, E) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = (RouteHeaders, O)> + Send + 'static,
{
    fn into_handler(self) -> Arc<dyn RouteHandler<C>> {
        Arc::new(TypedRouteHandler {
            handler: self,
            _types: PhantomData::<fn((A, B, D, E)) -> (O, ReplaceRouteHeaders)>,
        })
    }
}

impl<A, O, F, Fut, C> RouteHandler<C> for TypedRouteHandler<(A,), O, F, PreserveRouteHeaders>
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

impl<A, B, O, F, Fut, C> RouteHandler<C> for TypedRouteHandler<(A, B), O, F, PreserveRouteHeaders>
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

impl<A, B, D, O, F, Fut, C> RouteHandler<C>
    for TypedRouteHandler<(A, B, D), O, F, PreserveRouteHeaders>
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

impl<A, B, D, E, O, F, Fut, C> RouteHandler<C>
    for TypedRouteHandler<(A, B, D, E), O, F, PreserveRouteHeaders>
where
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    D: FromRouteMessage<C> + Send + Sync + 'static,
    E: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B, D, E) -> Fut + Send + Sync + 'static,
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
                E::from_message(&message, codec)?,
            )
            .await;
            encode_handler_output(&output, message, codec)
        })
    }
}

impl<A, O, F, Fut, C> RouteHandler<C> for TypedRouteHandler<(A,), O, F, ReplaceRouteHeaders>
where
    A: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = (RouteHeaders, O)> + Send + 'static,
    C: Codec,
{
    fn call<'a>(
        &'a self,
        mut message: RouteMessage,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<RouteMessage, BusError>> {
        Box::pin(async move {
            let (headers, output) = (self.handler)(A::from_message(&message, codec)?).await;
            message.headers = headers.0;
            encode_handler_output(&output, message, codec)
        })
    }
}

impl<A, B, O, F, Fut, C> RouteHandler<C> for TypedRouteHandler<(A, B), O, F, ReplaceRouteHeaders>
where
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = (RouteHeaders, O)> + Send + 'static,
    C: Codec,
{
    fn call<'a>(
        &'a self,
        mut message: RouteMessage,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<RouteMessage, BusError>> {
        Box::pin(async move {
            let (headers, output) = (self.handler)(
                A::from_message(&message, codec)?,
                B::from_message(&message, codec)?,
            )
            .await;
            message.headers = headers.0;
            encode_handler_output(&output, message, codec)
        })
    }
}

impl<A, B, D, O, F, Fut, C> RouteHandler<C>
    for TypedRouteHandler<(A, B, D), O, F, ReplaceRouteHeaders>
where
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    D: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B, D) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = (RouteHeaders, O)> + Send + 'static,
    C: Codec,
{
    fn call<'a>(
        &'a self,
        mut message: RouteMessage,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<RouteMessage, BusError>> {
        Box::pin(async move {
            let (headers, output) = (self.handler)(
                A::from_message(&message, codec)?,
                B::from_message(&message, codec)?,
                D::from_message(&message, codec)?,
            )
            .await;
            message.headers = headers.0;
            encode_handler_output(&output, message, codec)
        })
    }
}

impl<A, B, D, E, O, F, Fut, C> RouteHandler<C>
    for TypedRouteHandler<(A, B, D, E), O, F, ReplaceRouteHeaders>
where
    A: FromRouteMessage<C> + Send + Sync + 'static,
    B: FromRouteMessage<C> + Send + Sync + 'static,
    D: FromRouteMessage<C> + Send + Sync + 'static,
    E: FromRouteMessage<C> + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(A, B, D, E) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = (RouteHeaders, O)> + Send + 'static,
    C: Codec,
{
    fn call<'a>(
        &'a self,
        mut message: RouteMessage,
        codec: &'a C,
    ) -> BoxFuture<'a, Result<RouteMessage, BusError>> {
        Box::pin(async move {
            let (headers, output) = (self.handler)(
                A::from_message(&message, codec)?,
                B::from_message(&message, codec)?,
                D::from_message(&message, codec)?,
                E::from_message(&message, codec)?,
            )
            .await;
            message.headers = headers.0;
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

impl<C: Codec> FromRouteMessage<C> for RouteMessage {
    fn from_message(message: &RouteMessage, _codec: &C) -> Result<Self, BusError> {
        Ok(message.clone())
    }
}

impl<C: Codec> FromRouteMessage<C> for RouteHeaders {
    fn from_message(message: &RouteMessage, _codec: &C) -> Result<Self, BusError> {
        Ok(Self(message.headers.clone()))
    }
}

impl<T, C> FromRouteMessage<C> for RouteHeader<T>
where
    T: FromRouteHeader,
    C: Codec,
{
    fn from_message(message: &RouteMessage, _codec: &C) -> Result<Self, BusError> {
        let value = message
            .headers
            .get(T::NAME)
            .ok_or_else(|| BusError::Internal(format!("missing route header '{}'", T::NAME)))?;
        Ok(Self(T::from_header(value)?))
    }
}

impl<C: Codec> FromRouteMessage<C> for RoutePayload {
    fn from_message(message: &RouteMessage, _codec: &C) -> Result<Self, BusError> {
        Ok(Self(message.payload.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    struct CustomSource {
        message: RouteMessage,
    }

    impl RouteSource for CustomSource {
        fn into_stream(self: Box<Self>) -> LocalBoxFuture<'static, Result<RouteStream, BusError>> {
            let message = self.message;
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

    #[derive(Deserialize)]
    struct Input {
        value: u64,
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
    ) -> Output {
        assert_eq!(request_id.0, "request-21");
        assert!(!raw_payload.is_empty());
        Output {
            value: input.value * 2,
        }
    }

    async fn double_with_modified_headers(
        mut headers: RouteHeaders,
        input: Input,
    ) -> (RouteHeaders, Output) {
        headers.remove("x-remove");
        headers.insert("x-processed-by".into(), "custom-handler".into());
        (
            headers,
            Output {
                value: input.value * 2,
            },
        )
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
                    Output { value: input.value }
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
}
