use std::sync::atomic::{AtomicU64, Ordering};

use axum::body::to_bytes;
use axum::extract::Request;
use axum::handler::Handler;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, delete, get, patch, post, put};
use futures::future::LocalBoxFuture;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::codec::{Codec, JsonCodec};
use crate::errors::BusError;
use crate::http::HttpMethod;
use crate::router::{RouteMessage, RouteSource, RouteStream};

pub const DEFAULT_HTTP_SOURCE_CAPACITY: usize = 64;
pub const DEFAULT_HTTP_BODY_LIMIT: usize = 2 * 1024 * 1024;

static NEXT_HTTP_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

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
                get(move |request| enqueue_request(request, sender, body_limit))
            }
            HttpMethod::Post => {
                let sender = self.sender.clone();
                post(move |request| enqueue_request(request, sender, body_limit))
            }
            HttpMethod::Put => {
                let sender = self.sender.clone();
                put(move |request| enqueue_request(request, sender, body_limit))
            }
            HttpMethod::Patch => {
                let sender = self.sender.clone();
                patch(move |request| enqueue_request(request, sender, body_limit))
            }
            HttpMethod::Delete => {
                let sender = self.sender.clone();
                delete(move |request| enqueue_request(request, sender, body_limit))
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
    let message = RouteMessage {
        address: parts.uri.to_string(),
        timestamp: std::time::Instant::now(),
        id: NEXT_HTTP_REQUEST_ID.fetch_add(1, Ordering::Relaxed),
        headers,
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
        let source = HttpSource::post("/orders");
        let http = HttpRouter::new().source(&source).into_router();
        let (broker_outputs, mut broker_output_rx) = mpsc::channel(1);
        let bus = RecordingBus {
            outputs: broker_outputs,
        };
        let mut routes = Router::new()
            .bind(
                |mut headers: crate::router::RouteHeaders, order: CreateOrder| async move {
                    headers.remove("x-internal");
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
            .to(BrokerTarget::new(bus, "orders.created"));
        routes.install().await.unwrap();
        let request = Request::builder()
            .method("POST")
            .uri("/orders")
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
}
