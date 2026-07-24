use axum::handler::Handler;
use axum::routing::{MethodRouter, delete, get, patch, post, put};

use crate::codec::{Codec, JsonCodec};

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
    use axum::extract::{Path, Query};
    use axum::http::StatusCode;
    use serde::{Deserialize, Serialize};

    #[derive(Deserialize)]
    struct CreateOrder {
        sku: String,
    }

    #[derive(Deserialize)]
    struct OrderQuery {
        include_events: bool,
    }

    #[derive(Serialize)]
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
}
