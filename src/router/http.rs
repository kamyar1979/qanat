use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::LocalBoxFuture;

use crate::errors::BusError;
use crate::http::{HttpEndpoint, HttpRoute};
use crate::router::app::InstallableRouter;

pub trait HttpHandler: Send + Sync {
    fn route(&self) -> &HttpRoute;
}

pub trait HttpRuntime: Send + 'static {
    fn install<'a>(
        &'a mut self,
        handler: Arc<dyn HttpHandler>,
    ) -> LocalBoxFuture<'a, Result<(), BusError>>;
}

#[derive(Clone, Debug, Default)]
pub struct NoopHttpRuntime {
    installed: usize,
}

impl NoopHttpRuntime {
    pub fn installed_count(&self) -> usize {
        self.installed
    }
}

impl HttpRuntime for NoopHttpRuntime {
    fn install<'a>(
        &'a mut self,
        _handler: Arc<dyn HttpHandler>,
    ) -> LocalBoxFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            self.installed += 1;
            Ok(())
        })
    }
}

pub struct HttpRouter<R = NoopHttpRuntime> {
    runtime: R,
    handlers: Vec<Arc<dyn HttpHandler>>,
}

impl HttpRouter<NoopHttpRuntime> {
    pub fn noop() -> Self {
        Self::new(NoopHttpRuntime::default())
    }
}

impl<R> HttpRouter<R> {
    pub fn new(runtime: R) -> Self {
        Self {
            runtime,
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
        self.handlers.push(Arc::new(TypedHttpHandler {
            route,
            handler,
            _types: PhantomData,
        }));
        self
    }

    pub fn handlers(&self) -> impl ExactSizeIterator<Item = &dyn HttpHandler> {
        self.handlers.iter().map(Arc::as_ref)
    }

    pub fn routes(&self) -> impl ExactSizeIterator<Item = &HttpRoute> {
        self.handlers().map(HttpHandler::route)
    }

    pub fn runtime(&self) -> &R {
        &self.runtime
    }
}

impl Default for HttpRouter<NoopHttpRuntime> {
    fn default() -> Self {
        Self::noop()
    }
}

impl<R> InstallableRouter for HttpRouter<R>
where
    R: HttpRuntime,
{
    fn install<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            for handler in &self.handlers {
                self.runtime.install(Arc::clone(handler)).await?;
            }
            Ok(())
        })
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
        let router = HttpRouter::noop()
            .bind(HttpRoute::post("/orders"), process_order)
            .bind(HttpRoute::get("/health"), health);
        let routes: Vec<_> = router.routes().collect();

        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].path, "/orders");
        assert_eq!(routes[1].path, "/health");
    }

    #[test]
    fn http_router_exposes_endpoint_constructors() {
        let endpoint = HttpRouter::<NoopHttpRuntime>::post("/orders");

        assert_eq!(endpoint.method, HttpMethod::Post);
        assert_eq!(endpoint.path, "/orders");
    }

    #[test]
    fn http_router_stores_route_and_handler_together() {
        let router = HttpRouter::noop().bind(HttpRoute::post("/orders"), process_order);
        let handlers: Vec<_> = router.handlers().collect();

        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].route().path, "/orders");
    }

    #[tokio::test]
    async fn http_router_installs_handlers_into_runtime() {
        let mut router = HttpRouter::noop()
            .bind(HttpRoute::post("/orders"), process_order)
            .bind(HttpRoute::get("/health"), health);

        router.install().await.unwrap();

        assert_eq!(router.runtime().installed_count(), 2);
    }
}
