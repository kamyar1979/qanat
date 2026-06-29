use std::future::Future;
use std::marker::PhantomData;

use crate::http::{HttpEndpoint, HttpRoute};

pub trait HttpHandler: Send + Sync {
    fn route(&self) -> &HttpRoute;
}

pub struct HttpRouter {
    handlers: Vec<Box<dyn HttpHandler>>,
}

impl HttpRouter {
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
        }
    }

    pub fn get(path: impl Into<String>) -> HttpEndpoint {
        HttpEndpoint::get(path)
    }

    pub fn post(path: impl Into<String>) -> HttpEndpoint {
        HttpEndpoint::post(path)
    }

    pub fn put(path: impl Into<String>) -> HttpEndpoint {
        HttpEndpoint::put(path)
    }

    pub fn patch(path: impl Into<String>) -> HttpEndpoint {
        HttpEndpoint::patch(path)
    }

    pub fn delete(path: impl Into<String>) -> HttpEndpoint {
        HttpEndpoint::delete(path)
    }

    pub fn bind<I, O, F, Fut>(
        mut self,
        route: HttpRoute,
        handler: F,
    ) -> Self
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        self.handlers.push(Box::new(TypedHttpHandler {
            route,
            handler,
            _types: PhantomData,
        }));
        self
    }

    pub fn handlers(&self) -> impl ExactSizeIterator<Item = &dyn HttpHandler> {
        self.handlers.iter().map(Box::as_ref)
    }

    pub fn routes(&self) -> impl ExactSizeIterator<Item = &HttpRoute> {
        self.handlers().map(HttpHandler::route)
    }
}

impl Default for HttpRouter {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TypedHttpHandler<I, O, F> {
    route: HttpRoute,
    handler: F,
    _types: PhantomData<fn(I) -> O>,
}

impl<I, O, F> TypedHttpHandler<I, O, F> {
    pub fn handler(&self) -> &F {
        &self.handler
    }
}

impl<I, O, F, Fut> HttpHandler for TypedHttpHandler<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    fn route(&self) -> &HttpRoute {
        &self.route
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::HttpMethod;

    #[derive(Clone)]
    struct OrderRequest {
        id: u64,
    }

    async fn process_order(req: OrderRequest) -> u64 {
        req.id
    }

    async fn health(_: ()) -> &'static str {
        "ok"
    }

    #[test]
    fn http_router_binds_normal_async_functions() {
        let router = HttpRouter::new()
            .bind(HttpRoute::post("/orders"), process_order)
            .bind(HttpRoute::get("/health"), health);
        let routes: Vec<_> = router.routes().collect();

        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].path, "/orders");
        assert_eq!(routes[1].path, "/health");
    }

    #[test]
    fn http_router_exposes_endpoint_constructors() {
        let endpoint = HttpRouter::post("/orders");

        assert_eq!(endpoint.method, HttpMethod::Post);
        assert_eq!(endpoint.path, "/orders");
    }

    #[test]
    fn http_router_stores_route_and_handler_together() {
        let router = HttpRouter::new().bind(HttpRoute::post("/orders"), process_order);
        let handlers: Vec<_> = router.handlers().collect();

        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].route().path, "/orders");
    }
}
