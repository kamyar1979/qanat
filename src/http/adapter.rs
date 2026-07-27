#[cfg(feature = "axum")]
mod source_impl {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    use axum::body::to_bytes;
    use axum::extract::{Path, Request};
    use axum::handler::Handler;
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use axum::routing::{MethodRouter, delete, get, patch, post, put};
    use futures::future::LocalBoxFuture;
    use serde::de::DeserializeOwned;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::codec::{Codec, JsonCodec};
    use crate::errors::BusError;
    use crate::http::HttpMethod;
    use crate::router::{FromRouteMessage, RouteMessage, RouteSource, RouteStream};

    pub const DEFAULT_HTTP_SOURCE_CAPACITY: usize = 64;
    pub const DEFAULT_HTTP_BODY_LIMIT: usize = 2 * 1024 * 1024;
    const HTTP_PATH_PARAMETER_PREFIX: &str = "http.path.";

    static NEXT_HTTP_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

    #[derive(Clone, Debug)]
    pub struct HttpPath<T>(pub T);

    #[derive(Clone, Debug)]
    pub struct HttpQuery<T>(pub T);

    impl<T, C> FromRouteMessage<C> for HttpPath<T>
    where
        T: DeserializeOwned,
        C: Codec,
    {
        fn from_message(message: &RouteMessage, _codec: &C) -> Result<Self, BusError> {
            let parameters = message
                .metadata
                .iter()
                .filter_map(|(key, value)| {
                    key.strip_prefix(HTTP_PATH_PARAMETER_PREFIX)
                        .map(|key| (key, value))
                })
                .collect::<HashMap<_, _>>();
            let encoded = serde_urlencoded::to_string(parameters)
                .map_err(|error| BusError::Serialization(error.to_string()))?;
            serde_urlencoded::from_str(&encoded)
                .map(Self)
                .map_err(|error| BusError::Serialization(error.to_string()))
        }
    }

    impl<T, C> FromRouteMessage<C> for HttpQuery<T>
    where
        T: DeserializeOwned,
        C: Codec,
    {
        fn from_message(message: &RouteMessage, _codec: &C) -> Result<Self, BusError> {
            let query = message
                .address
                .split_once('?')
                .map(|(_, query)| query)
                .unwrap_or_default();
            serde_urlencoded::from_str(query)
                .map(Self)
                .map_err(|error| BusError::Serialization(error.to_string()))
        }
    }

    pub struct HttpSource {
        method: HttpMethod,
        path: String,
        sender: mpsc::Sender<RouteMessage>,
        receiver: mpsc::Receiver<RouteMessage>,
        body_limit: usize,
    }

    impl HttpSource {
        pub fn new(method: HttpMethod, path: impl Into<String>) -> Self {
            Self::with_capacity(method, path, DEFAULT_HTTP_SOURCE_CAPACITY)
        }

        pub fn with_capacity(method: HttpMethod, path: impl Into<String>, capacity: usize) -> Self {
            let (sender, receiver) = mpsc::channel(capacity.max(1));
            Self {
                method,
                path: path.into(),
                sender,
                receiver,
                body_limit: DEFAULT_HTTP_BODY_LIMIT,
            }
        }

        pub fn get(path: impl Into<String>) -> Self {
            Self::new(HttpMethod::Get, path)
        }

        pub fn post(path: impl Into<String>) -> Self {
            Self::new(HttpMethod::Post, path)
        }

        pub fn put(path: impl Into<String>) -> Self {
            Self::new(HttpMethod::Put, path)
        }

        pub fn patch(path: impl Into<String>) -> Self {
            Self::new(HttpMethod::Patch, path)
        }

        pub fn delete(path: impl Into<String>) -> Self {
            Self::new(HttpMethod::Delete, path)
        }

        pub fn with_body_limit(mut self, body_limit: usize) -> Self {
            self.body_limit = body_limit;
            self
        }

        pub fn method(&self) -> HttpMethod {
            self.method
        }

        pub fn path(&self) -> &str {
            &self.path
        }

        fn method_router(&self) -> MethodRouter {
            let body_limit = self.body_limit;
            match self.method {
                HttpMethod::Get => {
                    let sender = self.sender.clone();
                    get(move |Path(parameters), request| {
                        enqueue_request(parameters, request, sender, body_limit)
                    })
                }
                HttpMethod::Post => {
                    let sender = self.sender.clone();
                    post(move |Path(parameters), request| {
                        enqueue_request(parameters, request, sender, body_limit)
                    })
                }
                HttpMethod::Put => {
                    let sender = self.sender.clone();
                    put(move |Path(parameters), request| {
                        enqueue_request(parameters, request, sender, body_limit)
                    })
                }
                HttpMethod::Patch => {
                    let sender = self.sender.clone();
                    patch(move |Path(parameters), request| {
                        enqueue_request(parameters, request, sender, body_limit)
                    })
                }
                HttpMethod::Delete => {
                    let sender = self.sender.clone();
                    delete(move |Path(parameters), request| {
                        enqueue_request(parameters, request, sender, body_limit)
                    })
                }
            }
        }
    }

    impl RouteSource for HttpSource {
        fn into_stream(self: Box<Self>) -> LocalBoxFuture<'static, Result<RouteStream, BusError>> {
            Box::pin(async move { Ok(Box::pin(ReceiverStream::new(self.receiver)) as RouteStream) })
        }
    }

    async fn enqueue_request(
        path_parameters: HashMap<String, String>,
        request: Request,
        sender: mpsc::Sender<RouteMessage>,
        body_limit: usize,
    ) -> Response {
        let (parts, body) = request.into_parts();
        let payload = match to_bytes(body, body_limit).await {
            Ok(payload) => payload,
            Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        };
        let headers = parts
            .headers
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
            })
            .collect();
        let metadata = path_parameters
            .into_iter()
            .map(|(key, value)| (format!("{HTTP_PATH_PARAMETER_PREFIX}{key}"), value))
            .collect();
        let message = RouteMessage {
            address: parts.uri.to_string(),
            timestamp: std::time::Instant::now(),
            id: NEXT_HTTP_REQUEST_ID.fetch_add(1, Ordering::Relaxed),
            headers,
            metadata,
            attempts: 0,
            payload,
        };

        match sender.send(message).await {
            Ok(()) => StatusCode::ACCEPTED.into_response(),
            Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        }
    }

    pub struct HttpRouter<C: Codec = JsonCodec> {
        router: axum::Router,
        codec: C,
    }

    impl HttpRouter<JsonCodec> {
        pub fn new() -> Self {
            Self::with_codec(JsonCodec)
        }
    }

    impl<C> HttpRouter<C>
    where
        C: Codec,
    {
        pub fn with_codec(codec: C) -> Self {
            Self {
                router: axum::Router::new(),
                codec,
            }
        }

        pub fn from_router(router: axum::Router, codec: C) -> Self {
            Self { router, codec }
        }

        pub fn route(mut self, path: &str, method_router: MethodRouter) -> Self {
            self.router = self.router.route(path, method_router);
            self
        }

        pub fn source(self, source: &HttpSource) -> Self {
            self.route(source.path(), source.method_router())
        }

        pub fn get<H, T>(self, path: &str, handler: H) -> Self
        where
            H: Handler<T, ()>,
            T: 'static,
        {
            self.route(path, get(handler))
        }

        pub fn post<H, T>(self, path: &str, handler: H) -> Self
        where
            H: Handler<T, ()>,
            T: 'static,
        {
            self.route(path, post(handler))
        }

        pub fn put<H, T>(self, path: &str, handler: H) -> Self
        where
            H: Handler<T, ()>,
            T: 'static,
        {
            self.route(path, put(handler))
        }

        pub fn patch<H, T>(self, path: &str, handler: H) -> Self
        where
            H: Handler<T, ()>,
            T: 'static,
        {
            self.route(path, patch(handler))
        }

        pub fn delete<H, T>(self, path: &str, handler: H) -> Self
        where
            H: Handler<T, ()>,
            T: 'static,
        {
            self.route(path, delete(handler))
        }

        pub fn merge(mut self, router: axum::Router) -> Self {
            self.router = self.router.merge(router);
            self
        }

        pub fn nest(mut self, path: &str, router: axum::Router) -> Self {
            self.router = self.router.nest(path, router);
            self
        }

        pub fn router(&self) -> &axum::Router {
            &self.router
        }

        pub fn codec(&self) -> &C {
            &self.codec
        }

        pub fn into_router(self) -> axum::Router {
            self.router
        }
    }

    impl Default for HttpRouter<JsonCodec> {
        fn default() -> Self {
            Self::new()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use axum::Json;
        use axum::body::{Body, to_bytes};
        use axum::extract::{Path, Query};
        use axum::http::{Request, StatusCode};
        use futures::stream;
        use serde::{Deserialize, Serialize};
        use tokio::sync::mpsc;
        use tower::ServiceExt;

        use crate::bus::Bus;
        use crate::codec::Codec;
        use crate::http::{HttpResponse, HttpTarget};
        use crate::raw_message::RawMessage;
        use crate::router::{BrokerTarget, RouteMessage, RouteTarget, Router};
        use futures::future::BoxFuture;

        #[derive(Deserialize)]
        struct CreateOrder {
            sku: String,
        }

        #[derive(Deserialize)]
        struct OrderQuery {
            include_events: bool,
        }

        #[derive(Deserialize)]
        struct OrderPath {
            id: u64,
        }

        #[derive(Deserialize, Serialize)]
        struct OrderResponse {
            id: u64,
            sku: String,
            include_events: bool,
        }

        async fn create_order(
            Path(id): Path<u64>,
            Query(query): Query<OrderQuery>,
            Json(order): Json<CreateOrder>,
        ) -> (StatusCode, Json<OrderResponse>) {
            (
                StatusCode::CREATED,
                Json(OrderResponse {
                    id,
                    sku: order.sku,
                    include_events: query.include_events,
                }),
            )
        }

        async fn health() -> &'static str {
            "ok"
        }

        struct CaptureTarget {
            outputs: mpsc::Sender<RouteMessage>,
        }

        impl RouteTarget for CaptureTarget {
            fn deliver(&self, output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>> {
                Box::pin(async move {
                    self.outputs
                        .send(output)
                        .await
                        .map_err(|_| BusError::Internal("capture target closed".into()))
                })
            }
        }

        #[derive(Clone)]
        struct RecordingBus {
            outputs: mpsc::Sender<RawMessage>,
        }

        impl Bus for RecordingBus {
            type Message = RawMessage;
            type Subscription = stream::Empty<RawMessage>;

            fn dispatch<'a>(
                &'a self,
                _subject: &'a str,
                message: RawMessage,
            ) -> impl std::future::Future<Output = Result<(), BusError>> + Send + 'a {
                async move {
                    self.outputs
                        .send(message)
                        .await
                        .map_err(|_| BusError::Internal("recording bus closed".into()))
                }
            }

            async fn subscribe(&self, _pattern: &str) -> Result<Self::Subscription, BusError> {
                Ok(stream::empty())
            }

            async fn subscribe_group(
                &self,
                _pattern: &str,
                _group: &str,
            ) -> Result<Self::Subscription, BusError> {
                Ok(stream::empty())
            }
        }

        #[test]
        fn http_router_accepts_axum_handlers_and_extractors() {
            let router = HttpRouter::new()
                .post("/orders/{id}", create_order)
                .get("/health", health)
                .into_router();

            let _: axum::Router = router;
        }

        #[test]
        fn http_router_accepts_explicit_codec() {
            let router = HttpRouter::with_codec(crate::codec::JsonCodec).get("/health", health);

            let _: &crate::codec::JsonCodec = router.codec();
        }

        #[tokio::test]
        async fn http_router_executes_axum_extractors_and_handler() {
            let router = HttpRouter::new()
                .post("/orders/{id}", create_order)
                .into_router();
            let request = Request::builder()
                .method("POST")
                .uri("/orders/17?include_events=true")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"sku":"ABC-123"}"#))
                .unwrap();

            let response = router.oneshot(request).await.unwrap();

            assert_eq!(response.status(), StatusCode::CREATED);
            let body = to_bytes(response.into_body(), 1024).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                body,
                serde_json::json!({
                    "id": 17,
                    "sku": "ABC-123",
                    "include_events": true
                })
            );
        }

        #[tokio::test]
        async fn http_source_feeds_the_neutral_router() {
            let source = HttpSource::post("/orders");
            let http = HttpRouter::new().source(&source).into_router();
            let (outputs, mut output_rx) = mpsc::channel(1);
            let mut routes = Router::new()
                .bind(|order: CreateOrder| async move {
                    OrderResponse {
                        id: 42,
                        sku: order.sku,
                        include_events: false,
                    }
                })
                .from(source)
                .to(CaptureTarget { outputs });
            routes.install().await.unwrap();
            let request = Request::builder()
                .method("POST")
                .uri("/orders")
                .header("content-type", "application/json")
                .header("x-request-id", "request-42")
                .body(Body::from(r#"{"sku":"ABC-123"}"#))
                .unwrap();

            let response = http.oneshot(request).await.unwrap();
            let output = tokio::time::timeout(std::time::Duration::from_secs(1), output_rx.recv())
                .await
                .unwrap()
                .unwrap();
            let decoded: OrderResponse = routes.codec().decode(&output.payload).unwrap();

            assert_eq!(response.status(), StatusCode::ACCEPTED);
            assert_eq!(output.address, "/orders");
            assert_eq!(
                output.headers.get("x-request-id").map(String::as_str),
                Some("request-42")
            );
            assert_eq!(decoded.id, 42);
            assert_eq!(decoded.sku, "ABC-123");
        }

        #[tokio::test]
        async fn http_source_sends_handler_output_to_broker_target() {
            let source = HttpSource::post("/orders/{id}");
            let http = HttpRouter::new().source(&source).into_router();
            let (broker_outputs, mut broker_output_rx) = mpsc::channel(1);
            let bus = RecordingBus {
                outputs: broker_outputs,
            };
            let mut routes = Router::new()
                .bind(
                    |HttpPath(path): HttpPath<OrderPath>,
                     HttpQuery(query): HttpQuery<OrderQuery>,
                     mut headers: crate::router::RouteHeaders,
                     order: CreateOrder| async move {
                        headers.remove("x-internal");
                        headers.insert("x-processed-by".into(), "http-handler".into());
                        (
                            headers,
                            OrderResponse {
                                id: path.id,
                                sku: order.sku,
                                include_events: query.include_events,
                            },
                        )
                    },
                )
                .from(source)
                .to(BrokerTarget::new(bus, "orders.created"));
            routes.install().await.unwrap();
            let request = Request::builder()
                .method("POST")
                .uri("/orders/42?include_events=true")
                .header("content-type", "application/json")
                .header("x-request-id", "request-42")
                .header("x-internal", "secret")
                .body(Body::from(r#"{"sku":"ABC-123"}"#))
                .unwrap();

            let response = http.oneshot(request).await.unwrap();
            let message =
                tokio::time::timeout(std::time::Duration::from_secs(1), broker_output_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            let decoded: OrderResponse = routes.codec().decode(&message.payload).unwrap();
            let headers = message.envelope.headers.unwrap();

            assert_eq!(response.status(), StatusCode::ACCEPTED);
            assert_eq!(message.envelope.subject, "orders.created");
            assert_eq!(decoded.id, 42);
            assert_eq!(decoded.sku, "ABC-123");
            assert!(decoded.include_events);
            assert_eq!(
                headers.get("x-request-id").map(String::as_str),
                Some("request-42")
            );
            assert_eq!(
                headers.get("x-processed-by").map(String::as_str),
                Some("http-handler")
            );
            assert!(!headers.contains_key("x-internal"));
        }

        #[tokio::test]
        async fn http_source_sends_handler_output_to_http_target() {
            let source = HttpSource::post("/orders");
            let http = HttpRouter::new().source(&source).into_router();
            let (http_outputs, mut http_output_rx) = mpsc::channel(1);
            let target = HttpTarget::post("http://downstream.test/events", move |request| {
                let http_outputs = http_outputs.clone();
                async move {
                    http_outputs.send(request).await.unwrap();
                    Ok(HttpResponse::new(202))
                }
            });
            let mut routes = Router::new()
                .bind(
                    |mut headers: crate::router::RouteHeaders, order: CreateOrder| async move {
                        headers.insert("x-processed-by".into(), "http-handler".into());
                        (
                            headers,
                            OrderResponse {
                                id: 42,
                                sku: order.sku,
                                include_events: false,
                            },
                        )
                    },
                )
                .from(source)
                .to(target);
            routes.install().await.unwrap();
            let request = Request::builder()
                .method("POST")
                .uri("/orders")
                .header("content-type", "application/json")
                .header("x-request-id", "request-42")
                .body(Body::from(r#"{"sku":"ABC-123"}"#))
                .unwrap();

            let response = http.oneshot(request).await.unwrap();
            let forwarded =
                tokio::time::timeout(std::time::Duration::from_secs(1), http_output_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            let decoded: OrderResponse = routes.codec().decode(&forwarded.body).unwrap();

            assert_eq!(response.status(), StatusCode::ACCEPTED);
            assert_eq!(forwarded.url, "http://downstream.test/events");
            assert_eq!(decoded.id, 42);
            assert_eq!(decoded.sku, "ABC-123");
            assert_eq!(
                forwarded.headers.get("x-request-id").map(String::as_str),
                Some("request-42")
            );
            assert_eq!(
                forwarded.headers.get("x-processed-by").map(String::as_str),
                Some("http-handler")
            );
        }

        #[tokio::test]
        async fn http_source_rejects_a_body_over_its_limit() {
            let source = HttpSource::post("/orders").with_body_limit(4);
            let http = HttpRouter::new().source(&source).into_router();
            let request = Request::builder()
                .method("POST")
                .uri("/orders")
                .body(Body::from("12345"))
                .unwrap();

            let response = http.oneshot(request).await.unwrap();

            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        }

        #[tokio::test]
        async fn http_source_returns_unavailable_after_its_receiver_is_dropped() {
            let source = HttpSource::post("/orders");
            let http = HttpRouter::new().source(&source).into_router();
            drop(source);
            let request = Request::builder()
                .method("POST")
                .uri("/orders")
                .body(Body::empty())
                .unwrap();

            let response = http.oneshot(request).await.unwrap();

            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
    }
}

#[cfg(feature = "axum")]
pub use source_impl::{
    DEFAULT_HTTP_BODY_LIMIT, DEFAULT_HTTP_SOURCE_CAPACITY, HttpPath, HttpQuery, HttpRouter,
    HttpSource,
};

mod target_impl {
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::future::BoxFuture;

    use crate::errors::{BackendError, BusError};
    use crate::router::{REPLY_TO_HEADER, RouteMessage, RouteTarget};

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum HttpMethod {
        Get,
        Post,
        Put,
        Patch,
        Delete,
    }

    impl HttpMethod {
        pub fn as_str(self) -> &'static str {
            match self {
                Self::Get => "GET",
                Self::Post => "POST",
                Self::Put => "PUT",
                Self::Patch => "PATCH",
                Self::Delete => "DELETE",
            }
        }
    }

    impl std::fmt::Display for HttpMethod {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.as_str())
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct HttpRequest {
        pub method: HttpMethod,
        pub url: String,
        pub headers: HashMap<String, String>,
        pub body: Bytes,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct HttpResponse {
        pub status: u16,
        pub headers: HashMap<String, String>,
        pub body: Bytes,
    }

    impl HttpResponse {
        pub fn new(status: u16) -> Self {
            Self {
                status,
                headers: HashMap::new(),
                body: Bytes::new(),
            }
        }

        pub fn is_success(&self) -> bool {
            (200..300).contains(&self.status)
        }
    }

    pub trait HttpInvoker: Send + Sync + 'static {
        fn invoke<'a>(
            &'a self,
            request: HttpRequest,
        ) -> BoxFuture<'a, Result<HttpResponse, BusError>>;
    }

    impl<F, Fut> HttpInvoker for F
    where
        F: Fn(HttpRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<HttpResponse, BusError>> + Send + 'static,
    {
        fn invoke<'a>(
            &'a self,
            request: HttpRequest,
        ) -> BoxFuture<'a, Result<HttpResponse, BusError>> {
            Box::pin((self)(request))
        }
    }

    #[derive(Clone)]
    pub struct HttpTarget {
        method: HttpMethod,
        url: String,
        invoker: Arc<dyn HttpInvoker>,
    }

    impl HttpTarget {
        pub fn new(method: HttpMethod, url: impl Into<String>, invoker: impl HttpInvoker) -> Self {
            Self {
                method,
                url: url.into(),
                invoker: Arc::new(invoker),
            }
        }

        pub fn get(url: impl Into<String>, invoker: impl HttpInvoker) -> Self {
            Self::new(HttpMethod::Get, url, invoker)
        }

        pub fn post(url: impl Into<String>, invoker: impl HttpInvoker) -> Self {
            Self::new(HttpMethod::Post, url, invoker)
        }

        pub fn put(url: impl Into<String>, invoker: impl HttpInvoker) -> Self {
            Self::new(HttpMethod::Put, url, invoker)
        }

        pub fn patch(url: impl Into<String>, invoker: impl HttpInvoker) -> Self {
            Self::new(HttpMethod::Patch, url, invoker)
        }

        pub fn delete(url: impl Into<String>, invoker: impl HttpInvoker) -> Self {
            Self::new(HttpMethod::Delete, url, invoker)
        }

        pub fn method(&self) -> HttpMethod {
            self.method
        }

        pub fn url(&self) -> &str {
            &self.url
        }

        pub async fn send(
            &self,
            body: Bytes,
            mut headers: HashMap<String, String>,
        ) -> Result<HttpResponse, BusError> {
            headers
                .entry("content-type".to_string())
                .or_insert_with(|| "application/octet-stream".to_string());
            let response = self
                .invoker
                .invoke(HttpRequest {
                    method: self.method,
                    url: self.url.clone(),
                    headers,
                    body,
                })
                .await?;

            if response.is_success() {
                Ok(response)
            } else {
                Err(BusError::Backend(BackendError::Other(format!(
                    "HTTP target '{}' returned status {}",
                    self.url, response.status
                ))))
            }
        }
    }

    impl std::fmt::Debug for HttpTarget {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("HttpTarget")
                .field("method", &self.method)
                .field("url", &self.url)
                .finish_non_exhaustive()
        }
    }

    impl RouteTarget for HttpTarget {
        fn deliver(&self, output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>> {
            Box::pin(async move {
                let mut headers = output.headers;
                headers.remove(REPLY_TO_HEADER);
                self.send(output.payload, headers).await.map(|_| ())
            })
        }
    }

    #[cfg(feature = "http-client")]
    #[derive(Clone, Debug, Default)]
    pub struct ReqwestHttpInvoker {
        client: reqwest::Client,
    }

    #[cfg(feature = "http-client")]
    impl ReqwestHttpInvoker {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn with_client(client: reqwest::Client) -> Self {
            Self { client }
        }
    }

    #[cfg(feature = "http-client")]
    impl HttpInvoker for ReqwestHttpInvoker {
        fn invoke<'a>(
            &'a self,
            request: HttpRequest,
        ) -> BoxFuture<'a, Result<HttpResponse, BusError>> {
            Box::pin(async move {
                let method = match request.method {
                    HttpMethod::Get => reqwest::Method::GET,
                    HttpMethod::Post => reqwest::Method::POST,
                    HttpMethod::Put => reqwest::Method::PUT,
                    HttpMethod::Patch => reqwest::Method::PATCH,
                    HttpMethod::Delete => reqwest::Method::DELETE,
                };
                let mut builder = self.client.request(method, request.url).body(request.body);
                for (name, value) in request.headers {
                    builder = builder.header(name, value);
                }

                let response = builder
                    .send()
                    .await
                    .map_err(|error| BusError::Connection(error.to_string()))?;
                let status = response.status().as_u16();
                let headers = response
                    .headers()
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.to_string(),
                            value.to_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect();
                let body = response
                    .bytes()
                    .await
                    .map_err(|error| BusError::Connection(error.to_string()))?;

                Ok(HttpResponse {
                    status,
                    headers,
                    body,
                })
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn http_target_accepts_an_async_function_invoker() {
            let (requests, mut request_rx) = tokio::sync::mpsc::channel(1);
            let target = HttpTarget::post("/events", move |request| {
                let requests = requests.clone();
                async move {
                    requests.send(request).await.unwrap();
                    Ok(HttpResponse::new(202))
                }
            });

            target
                .send(Bytes::from_static(b"payload"), HashMap::new())
                .await
                .unwrap();

            let request = request_rx.recv().await.unwrap();
            assert_eq!(request.method, HttpMethod::Post);
            assert_eq!(request.url, "/events");
            assert_eq!(request.body, Bytes::from_static(b"payload"));
            assert_eq!(
                request.headers.get("content-type").map(String::as_str),
                Some("application/octet-stream")
            );
        }

        #[tokio::test]
        async fn http_target_rejects_unsuccessful_responses() {
            let target = HttpTarget::post("/events", |_| async { Ok(HttpResponse::new(503)) });

            let error = target.send(Bytes::new(), HashMap::new()).await.unwrap_err();

            assert!(error.to_string().contains("status 503"));
        }

        #[tokio::test]
        async fn route_target_removes_internal_reply_header() {
            let (requests, mut request_rx) = tokio::sync::mpsc::channel(1);
            let target = HttpTarget::post("/events", move |request| {
                let requests = requests.clone();
                async move {
                    requests.send(request).await.unwrap();
                    Ok(HttpResponse::new(202))
                }
            });
            let mut message = RouteMessage::new("/input", Bytes::from_static(b"payload"));
            message
                .headers
                .insert(REPLY_TO_HEADER.to_string(), "private.reply".to_string());
            message
                .headers
                .insert("x-request-id".to_string(), "request-42".to_string());

            target.deliver(message).await.unwrap();

            let request = request_rx.recv().await.unwrap();
            assert!(!request.headers.contains_key(REPLY_TO_HEADER));
            assert_eq!(
                request.headers.get("x-request-id").map(String::as_str),
                Some("request-42")
            );
        }

        #[cfg(all(feature = "axum", feature = "http-client"))]
        #[tokio::test]
        async fn reqwest_invoker_sends_request_to_axum() {
            use axum::body::Body;
            use axum::http::{HeaderMap, StatusCode};
            use axum::routing::post;

            let (captured, mut captured_rx) = tokio::sync::mpsc::channel(1);
            let app = axum::Router::new().route(
                "/events",
                post(move |headers: HeaderMap, body: Body| {
                    let captured = captured.clone();
                    async move {
                        let body = axum::body::to_bytes(body, 1024).await.unwrap();
                        captured.send((headers, body)).await.unwrap();
                        StatusCode::ACCEPTED
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let target = HttpTarget::post(
                format!("http://{address}/events"),
                ReqwestHttpInvoker::new(),
            );

            let response = target
                .send(
                    Bytes::from_static(br#"{"id":42}"#),
                    HashMap::from([
                        ("content-type".to_string(), "application/json".to_string()),
                        ("correlation_id".to_string(), "request-42".to_string()),
                    ]),
                )
                .await
                .unwrap();
            let (headers, body) = captured_rx.recv().await.unwrap();
            server.abort();

            assert_eq!(response.status, 202);
            assert_eq!(body, Bytes::from_static(br#"{"id":42}"#));
            assert_eq!(
                headers
                    .get("correlation_id")
                    .and_then(|value| value.to_str().ok()),
                Some("request-42")
            );
        }
    }
}

#[cfg(feature = "http-client")]
pub use target_impl::ReqwestHttpInvoker;
pub use target_impl::{HttpInvoker, HttpMethod, HttpRequest, HttpResponse, HttpTarget};
