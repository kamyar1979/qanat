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
    fn invoke<'a>(&'a self, request: HttpRequest) -> BoxFuture<'a, Result<HttpResponse, BusError>>;
}

impl<F, Fut> HttpInvoker for F
where
    F: Fn(HttpRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<HttpResponse, BusError>> + Send + 'static,
{
    fn invoke<'a>(&'a self, request: HttpRequest) -> BoxFuture<'a, Result<HttpResponse, BusError>> {
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
    fn invoke<'a>(&'a self, request: HttpRequest) -> BoxFuture<'a, Result<HttpResponse, BusError>> {
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
