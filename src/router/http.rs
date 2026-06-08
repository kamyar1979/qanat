use std::future::Future;
use std::marker::PhantomData;

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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HttpEndpoint {
    pub method: HttpMethod,
    pub path: String,
}

impl HttpEndpoint {
    pub fn new(method: HttpMethod, path: impl Into<String>) -> Self {
        Self {
            method,
            path: path.into(),
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
}

impl From<(HttpMethod, String)> for HttpEndpoint {
    fn from((method, path): (HttpMethod, String)) -> Self {
        Self::new(method, path)
    }
}

impl From<(HttpMethod, &str)> for HttpEndpoint {
    fn from((method, path): (HttpMethod, &str)) -> Self {
        Self::new(method, path)
    }
}

pub type HttpRoute = HttpEndpoint;

pub struct HttpRouter<R = ()> {
    routes: Vec<HttpRoute>,
    _handlers: R,
}

impl HttpRouter<()> {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            _handlers: (),
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
}

impl Default for HttpRouter<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> HttpRouter<R> {
    pub fn bind<I, O, F, Fut>(
        mut self,
        route: HttpRoute,
        handler: F,
    ) -> HttpRouter<(R, HttpHandler<I, O, F>)>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        self.routes.push(route.clone());
        HttpRouter {
            routes: self.routes,
            _handlers: (
                self._handlers,
                HttpHandler {
                    _route: route,
                    _handler: handler,
                    _types: PhantomData,
                },
            ),
        }
    }

    pub fn routes(&self) -> &[HttpRoute] {
        &self.routes
    }
}

pub struct HttpHandler<I, O, F> {
    _route: HttpRoute,
    _handler: F,
    _types: PhantomData<fn(I) -> O>,
}

#[cfg(test)]
mod tests {
    use super::*;

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

        assert_eq!(router.routes().len(), 2);
        assert_eq!(router.routes()[0].path, "/orders");
        assert_eq!(router.routes()[1].path, "/health");
    }

    #[test]
    fn http_router_exposes_endpoint_constructors() {
        let endpoint = HttpRouter::post("/orders");

        assert_eq!(endpoint.method, HttpMethod::Post);
        assert_eq!(endpoint.path, "/orders");
    }
}
